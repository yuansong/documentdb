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
    configuration::{DocumentDBSetupConfiguration, PgConfiguration, SetupConfiguration},
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

/// Upper bound on registered gateway instances. Purely a runaway guard; any
/// real deployment wanting more than 64 gateway workers on one box should
/// rethink its topology first.
const MAX_GATEWAY_INSTANCES: usize = 64;

/// Resolves the number of gateway background workers to register.
///
/// Read from the postmaster environment (`DOCUMENTDB_GW_INSTANCES`): `_PG_init`
/// runs while `shared_preload_libraries` is being loaded, before
/// `postgresql.conf` GUC values are applied, so a GUC would not be reliably
/// populated here. Defaults to 1 (single instance, unchanged behavior).
fn gateway_instances() -> usize {
    let parsed = std::env::var("DOCUMENTDB_GW_INSTANCES")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|instances| *instances > 0)
        .unwrap_or(1);
    if parsed > MAX_GATEWAY_INSTANCES {
        eprintln!(
            "GW-HOST: DOCUMENTDB_GW_INSTANCES={parsed} exceeds the limit, clamping to {MAX_GATEWAY_INSTANCES}"
        );
    }
    parsed.min(MAX_GATEWAY_INSTANCES)
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
    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
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

    // Initialize tracing subscriber to handle all tracing events
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .init();

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
