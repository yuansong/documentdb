#![expect(
    clippy::expect_used,
    reason = "Main binary uses expect for initialization failures that should crash the process"
)]
#![expect(
    clippy::unwrap_used,
    reason = "Main binary uses unwrap for failures that should crash the process"
)]

use std::{sync::Arc, time::Duration};

use documentdb_gateway_core::{
    auth::AuthenticationManager,
    configuration::{
        capacity, DocumentDBSetupConfiguration, DynamicConfiguration, PgConfiguration,
        SetupConfiguration,
    },
    error::Result,
    postgres::{conn_mgmt, create_query_catalog, DocumentDBDataClient},
    run_gateway,
    service::{DefaultRequestRouter, TlsProvider},
    shutdown_controller::SHUTDOWN_CONTROLLER,
    startup::{create_postgres_object, get_service_context},
    time::STARTUP_INSTANT,
};
use pgrx::{
    bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, BgWorkerStartTime, SignalWakeFlags},
    prelude::*,
};
use tokio::time::Instant;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::gucs::{PG_DOCUMENTDB_GATEWAY_DATABASE, PG_DOCUMENTDB_SETUP_CONFIGURATION};

/// Resolves the number of gateway background workers to register.
///
/// Read from the postmaster environment (`DOCUMENTDB_GW_INSTANCES`): `_PG_init`
/// runs while `shared_preload_libraries` is being loaded, before
/// `postgresql.conf` GUC values are applied, so a GUC would not be reliably
/// populated here. Defaults to 1 (single instance, unchanged behavior).
///
/// The value comes from the shared capacity module so that registration and
/// the worker main thread can never disagree about how many instances exist --
/// they both used to parse the environment variable themselves.
fn gateway_instances() -> usize {
    let requested = capacity::requested_gateway_instance_count();
    let instances = capacity::gateway_instance_count();
    if requested > instances {
        eprintln!(
            "GW-HOST: DOCUMENTDB_GW_INSTANCES={requested} exceeds the limit, clamping to {instances}"
        );
    }
    instances
}

/// Install a panic hook that forces a full backtrace to stderr.
///
/// Panics inside the tokio runtime (watchdog threads, connection tasks) are
/// otherwise swallowed by the task machinery: the JoinHandle is dropped or
/// only logged as a one-line warn. Routing every panic through stderr with
/// `Backtrace::force_capture()` makes the postmaster log show exactly where
/// and why the gateway crashed.
fn install_panic_hook() {
    // Only default the env var when the operator hasn't set one: mutating the
    // environment of a running process is process-global (and unsafe under
    // Rust 2024), so never clobber an explicit operator choice.
    if std::env::var_os("RUST_BACKTRACE").is_none() {
        std::env::set_var("RUST_BACKTRACE", "1");
    }
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        eprintln!("GATEWAY PANIC: {info}");
        eprintln!("{:?}", std::backtrace::Backtrace::force_capture());
        default_hook(info);
    }));
}

/// Optional gateway CPU affinity.
///
/// `DOCUMENTDB_GW_CPU_AFFINITY="0-7"` (or a single cpu, e.g. "3") pins the
/// gateway main thread to the given core range. The tokio worker threads are
/// spawned after this call and inherit the creator's mask, so the whole
/// gateway lands on the partitioned cores (cuts cross-core IPIs between the
/// gateway and the backends). Unset / malformed values disable pinning. When
/// multiple gateway instances are registered they all receive the same mask;
/// partition per instance instead if cores must be split.
fn apply_gateway_affinity() {
    let raw = match std::env::var("DOCUMENTDB_GW_CPU_AFFINITY") {
        Ok(v) => v,
        Err(_) => return,
    };
    let raw = raw.trim();
    let parsed: std::result::Result<(usize, usize), _> = match raw.split_once('-') {
        Some((lo, hi)) => lo
            .trim()
            .parse::<usize>()
            .and_then(|lo| hi.trim().parse::<usize>().map(|hi| (lo, hi))),
        None => raw.parse::<usize>().map(|v| (v, v)),
    };
    let (lo, hi) = match parsed {
        Ok(pair) if pair.0 <= pair.1 && pair.1 < 1024 => pair,
        _ => {
            eprintln!("GW-HOST: ignoring malformed DOCUMENTDB_GW_CPU_AFFINITY='{raw}'");
            return;
        }
    };

    // sched_setaffinity via a plain 128-byte cpu_set_t (8 * 1024 bits), no
    // libc CPU_* macros needed.
    extern "C" {
        fn sched_setaffinity(pid: i32, cpusetsize: usize, mask: *const u8) -> i32;
    }
    let mut mask = [0u8; 128];
    for cpu in lo..=hi {
        mask[cpu / 8] |= 1 << (cpu % 8);
    }
    let rc = unsafe { sched_setaffinity(0, mask.len(), mask.as_ptr()) };
    eprintln!(
        "GW-HOST: gateway affinity cpus {lo}-{hi} -> rc={rc} ({})",
        if rc == 0 { "pinned" } else { "failed" }
    );
}

pub fn init() {
    let instances = gateway_instances();
    if instances <= 1 {
        BackgroundWorkerBuilder::new("DocumentDB Gateway Host")
            .set_function("documentdb_gw_worker_main")
            .set_library("pg_documentdb_gw_host")
            .set_restart_time(Some(Duration::from_secs(1)))
            .set_start_time(BgWorkerStartTime::ConsistentState)
            .enable_spi_access()
            .load();
        return;
    }

    // Multi-instance mode: register K workers named "... gw0".."gw{K-1}". Each
    // runs its own tokio runtime and its own PostgreSQL connection pool, and
    // binds the same listening port with SO_REUSEPORT; the Linux kernel
    // load-balances new client connections across the instances by connection
    // 4-tuple hash, so clients only need a single host:port.
    for index in 0..instances {
        BackgroundWorkerBuilder::new(&format!("DocumentDB Gateway Host gw{index}"))
            .set_function("documentdb_gw_worker_main")
            .set_library("pg_documentdb_gw_host")
            .set_restart_time(Some(Duration::from_secs(1)))
            .set_start_time(BgWorkerStartTime::ConsistentState)
            .enable_spi_access()
            .set_argument(Some(pg_sys::Datum::from(index)))
            .load();
    }
}

#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn documentdb_gw_worker_main(arg: pg_sys::Datum) {
    // Direct stderr diagnostics: unlike pgrx log!()/tracing, eprintln! is
    // unfiltered (no log_min_messages, no subscriber init dependency), so
    // the startup path is always visible in the postmaster log.
    eprintln!(
        "GW-HOST: documentdb_gw_worker_main entered (pid={})",
        std::process::id()
    );

    apply_gateway_affinity();

    STARTUP_INSTANT.get_or_init(Instant::now);

    install_panic_hook();
    eprintln!("GW-HOST: panic hook installed");

    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    let instance_index = arg.value();

    let database_name = String::from(
        PG_DOCUMENTDB_GATEWAY_DATABASE
            .get()
            .expect("GUC database not set")
            .to_str()
            .unwrap(),
    );

    let setup_configuration_file = String::from(
        PG_DOCUMENTDB_SETUP_CONFIGURATION
            .get()
            .expect("GUC setup configuration not set")
            .to_str()
            .unwrap(),
    );
    BackgroundWorker::connect_worker_to_spi(Some(database_name.as_str()), None);

    let shutdown_token = SHUTDOWN_CONTROLLER.token();
    let worker_name = BackgroundWorker::get_name();

    // now start the gw on a worker thread.
    //
    // The runtime used to be pinned to a single worker thread, which serialized
    // all socket I/O for every client connection: with 40 clients only one
    // tokio worker accumulated CPU while the PostgreSQL backends sat idle in
    // `ClientRead` waiting to be fed.
    //
    // Sizing it to the whole machine instead oversubscribes it, because this
    // gateway competes for cores with the backends it drives and with the other
    // gateway instances on the same host. `capacity` owns the rule:
    //   * explicit value from the setup configuration file (per instance),
    //   * `DOCUMENTDB_GW_WORKER_THREADS` (per instance),
    //   * derived default: half the cores, split across instances, leaving
    //     room for PostgreSQL's own maintenance/autovacuum/pg_cron workers.
    //
    // The connection pool is sized from the same resolved count, so it must be
    // computed here and from the same inputs (see `PoolManager::data_pool_settings`).
    let instances = gateway_instances();
    let configured_worker_threads =
        DocumentDBSetupConfiguration::new(&std::path::PathBuf::from(
            setup_configuration_file.as_str(),
        ))
        .ok()
        .and_then(|configuration| configuration.async_runtime_worker_threads);
    let worker_threads = capacity::instance_worker_threads(configured_worker_threads);

    let cores = capacity::available_cores();
    let gateway_thread_budget = capacity::default_total_worker_threads(cores);
    eprintln!(
        "GW-HOST: gw{instance_index}/{instances} tokio worker threads: {worker_threads} \
         (cores={cores}, reserved for postgres={}, gateway budget={gateway_thread_budget})",
        capacity::pg_reserved_cores(cores)
    );
    if instances > gateway_thread_budget {
        eprintln!(
            "GW-HOST: {instances} gateway instances exceed the gateway thread budget \
             ({gateway_thread_budget} on {cores} cores); every instance still gets \
             {worker_threads} thread(s), so instances will timeslice. Reduce \
             DOCUMENTDB_GW_INSTANCES or raise the core count."
        );
    }

    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()
        .unwrap();

    tokio_runtime.spawn(async move {
        if let Err(error) =
            run_docdb_gateway(setup_configuration_file.as_str(), instance_index).await
        {
            tracing::error!("Gateway worker failed to start: {error}");
        }
        SHUTDOWN_CONTROLLER.shutdown();
    });

    // wake up every second or if we received a SIGTERM
    while BackgroundWorker::wait_latch(Some(Duration::from_secs(1))) {
        if shutdown_token.is_cancelled() {
            break;
        }
    }

    SHUTDOWN_CONTROLLER.shutdown();
    tokio_runtime.shutdown_timeout(Duration::from_secs(1));
    log!("{} stopped", worker_name);
}

async fn run_docdb_gateway(setup_configuration_file: &str, instance_index: usize) -> Result<()> {
    let cfg_file = std::path::PathBuf::from(setup_configuration_file);

    let shutdown_token = SHUTDOWN_CONTROLLER.token();

    let mut setup_configuration =
        DocumentDBSetupConfiguration::new(&cfg_file).expect("Failed to load configuration.");

    // Multi-instance mode: every instance must set SO_REUSEPORT on its
    // listeners so all of them can bind the same gateway port. The kernel
    // then load-balances client connections across the instances by
    // connection 4-tuple hash.
    //
    // A Unix socket listener cannot be shared this way: each instance would
    // unlink and re-bind the same socket file, silently stealing it from the
    // previously started instances (their accept fds keep pointing at the
    // deleted inode, so clients hashed there lose their connections). Reject
    // the combination loudly instead of misbehaving quietly.
    let instances = gateway_instances();
    if instances > 1 {
        if setup_configuration.unix_socket_path.is_some() {
            eprintln!(
                "GW-HOST: gw{instance_index}/{instances} cannot start: a Unix socket \
                 listener is not compatible with multiple gateway instances sharing one port. \
                 Remove `unix_socket_path` from the setup configuration, or set \
                 DOCUMENTDB_GW_INSTANCES=1."
            );
            return Ok(());
        }
        setup_configuration.gateway_reuse_port = Some(true);
        eprintln!(
            "GW-HOST: gw{instance_index}/{instances}: SO_REUSEPORT listeners enabled"
        );
    }

    // Initialize tracing subscriber to handle all tracing events. `RUST_LOG`
    // selects the verbosity per module (e.g. `RUST_LOG=documentdb_gateway_core=debug`
    // turns on the per-request forwarding-path diagnostics); without it, info
    // and above are kept, which is the quiet default used for benchmarks.
    let rust_log = std::env::var("RUST_LOG").ok();
    let env_filter =
        rust_log.as_deref().map_or_else(|| EnvFilter::new("info"), EnvFilter::new);
    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .init();

    tracing::info!(
        log_filter = rust_log.as_deref().unwrap_or("info (RUST_LOG unset, default)"),
        "Tracing subscriber initialised"
    );

    tracing::info!("Starting server with configuration: {setup_configuration:?}");

    let tls_provider = TlsProvider::new(
        SetupConfiguration::certificate_options(&setup_configuration),
        None,
        None,
    )
    .await
    .expect("Failed to create TLS provider.");

    let connection_pool_manager = match create_postgres_object(
        || async {
            conn_mgmt::create_connection_pool_manager(
                create_query_catalog(),
                Box::new(setup_configuration.clone()),
            )
            .await
        },
        &setup_configuration,
    )
    .await
    {
        Ok(manager) => manager,
        // A cancelled shutdown token means the worker was asked to stop while
        // retrying, not a genuine startup failure; returning early here avoids
        // logging an expected shutdown as a worker failure.
        Err(_) if shutdown_token.is_cancelled() => return Ok(()),
        Err(error) => return Err(error),
    };

    let dynamic_configuration = match create_postgres_object(
        || async {
            PgConfiguration::new(
                &setup_configuration,
                Arc::clone(&connection_pool_manager),
                vec!["documentdb.".to_owned()],
            )
            .await
        },
        &setup_configuration,
    )
    .await
    {
        Ok(configuration) => configuration,
        Err(_) if shutdown_token.is_cancelled() => return Ok(()),
        Err(error) => return Err(error),
    };

    // One structured dump of everything that can move throughput, logged once
    // per instance at startup. The standalone gateway emits the same dump, so
    // a `gw_host` vs standalone performance comparison starts by diffing the
    // two lines (threads, pool sizes, buffers, timeouts) instead of guessing.
    log_performance_parameters(
        &setup_configuration,
        dynamic_configuration.as_ref(),
        instances,
        capacity::instance_worker_threads(setup_configuration.async_runtime_worker_threads),
    );

    let service_context = get_service_context(
        Box::new(setup_configuration),
        dynamic_configuration,
        connection_pool_manager,
        tls_provider,
        AuthenticationManager::new(),
    );

    run_gateway::<DocumentDBDataClient, _>(
        service_context,
        None,
        DefaultRequestRouter {},
        shutdown_token,
    )
    .await
}

/// One-shot, structured dump of every performance-relevant parameter this
/// gateway instance will run with, logged once at info level so the default
/// filter always captures it.
///
/// Every number that decides throughput appears here: runtime threads (and
/// where the value came from), the data-pool capacity derived from cores and
/// `max_connections`, socket and per-connection buffer sizes, and the
/// request/transaction/connect timeouts. When `gw_host` underperforms a
/// standalone gateway deployment, diffing this line between the two shows
/// whether the sizing (threads, pool, buffers) or the timeouts diverged before
/// any per-request debugging is needed.
fn log_performance_parameters(
    setup_configuration: &DocumentDBSetupConfiguration,
    dynamic_configuration: &dyn DynamicConfiguration,
    instances: usize,
    worker_threads: usize,
) {
    let cores = capacity::available_cores();
    let thread_source = if capacity::worker_threads_from_env().is_some() {
        "env (DOCUMENTDB_GW_WORKER_THREADS)"
    } else if setup_configuration.async_runtime_worker_threads.is_some() {
        "setup configuration (AsyncRuntimeWorkerThreads)"
    } else {
        "derived (cores/2 split across instances)"
    };
    let data_pool_override = capacity::data_pool_connections_from_env();
    let data_pool_connections = data_pool_override.unwrap_or_else(|| {
        capacity::per_instance_data_pool_connections(
            dynamic_configuration.max_connections(),
            dynamic_configuration.system_connection_budget(),
            instances,
            worker_threads,
        )
    });

    tracing::info!(
        gateway_instances = instances,
        cores,
        worker_threads,
        worker_thread_source = thread_source,
        pg_reserved_cores = capacity::pg_reserved_cores(cores),
        gateway_thread_budget = capacity::default_total_worker_threads(cores),
        listen_port = setup_configuration.gateway_listen_port(),
        reuse_port = setup_configuration.gateway_reuse_port(),
        v2_runtime = setup_configuration.enable_v2_runtime(),
        enforce_tls = setup_configuration.enforce_tls(),
        stream_read_buffer_bytes = setup_configuration.stream_read_buffer_size(),
        stream_write_buffer_bytes = setup_configuration.stream_write_buffer_size(),
        connection_buffer_bytes = dynamic_configuration.gateway_connection_buffer_size(),
        max_connections = dynamic_configuration.max_connections(),
        system_connection_budget = dynamic_configuration.system_connection_budget(),
        data_pool_connections,
        data_pool_override_set = data_pool_override.is_some(),
        max_request_timeout_sec = dynamic_configuration.max_request_timeout_sec(),
        transaction_timeout_sec = dynamic_configuration.transaction_timeout_sec(),
        // 0 means "not configured" (no client-side connect timeout).
        connect_timeout_ms = dynamic_configuration.connect_timeout().map_or(0, |timeout| {
            u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX)
        }),
        socket_idle_timeout_sec = dynamic_configuration.socket_connection_idle_timeout_sec(),
        pool_prune_interval_sec = dynamic_configuration.gateway_connection_pruning_interval_sec(),
        pool_idle_lifetime_sec = dynamic_configuration.gateway_connection_idle_lifetime_sec(),
        pool_lifetime_sec = dynamic_configuration.gateway_connection_lifetime_sec(),
        cpu_affinity = std::env::var("DOCUMENTDB_GW_CPU_AFFINITY").unwrap_or_default(),
        "gw_host performance parameters (the standalone gateway prints the same dump; \
         diff the two lines when comparing throughput)"
    );
}
