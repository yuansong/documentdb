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

use crate::gucs::{
    PG_DOCUMENTDB_GATEWAY_DATABASE, PG_DOCUMENTDB_GATEWAY_TRANSPORT,
    PG_DOCUMENTDB_SETUP_CONFIGURATION,
};

pub fn init() {
    BackgroundWorkerBuilder::new("DocumentDB Gateway Host")
        .set_function("documentdb_gw_worker_main")
        .set_library("pg_documentdb_gw_host")
        .set_restart_time(Some(Duration::from_secs(1)))
        .set_start_time(BgWorkerStartTime::ConsistentState)
        .enable_spi_access()
        .load();
}

/// Default number of Tokio worker threads for the embedded gateway.
///
/// The embedded gateway is a PostgreSQL background worker, so every thread it
/// owns competes for the same cores as the PostgreSQL backends that execute its
/// queries. Sizing the pool to the whole machine therefore oversubscribes it:
/// measured on a 16 vCPU box (YCSB workloada, 80 clients, reads only, Unix
/// socket to PostgreSQL) 16 workers reached 97.0k ops/s, 24 reached 97.6k, 6
/// reached 98.0k, 10 reached 99.4k, 4 dropped to 81.5k, and 8 reached 103.8k.
/// Half the cores hits that peak and leaves the rest of the machine to the
/// backends.
fn default_worker_threads() -> usize {
    std::thread::available_parallelism()
        .map_or(1, std::num::NonZero::get)
        .div_ceil(2)
}

/// Install a panic hook that forces a full backtrace to stderr.
///
/// Panics inside the tokio runtime (watchdog threads, connection tasks) are
/// otherwise swallowed by the task machinery: the JoinHandle is dropped or
/// only logged as a one-line warn.  Routing every panic through stderr with
/// `Backtrace::force_capture()` makes postmaster's log show exactly where
/// and why the gateway crashed (crash-localization, 2026-11-22).
fn install_panic_hook() {
    std::env::set_var("RUST_BACKTRACE", "1");
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        eprintln!("GATEWAY PANIC: {info}");
        eprintln!("{:?}", std::backtrace::Backtrace::force_capture());
        default_hook(info);
    }));
}

/// Optional gateway CPU affinity (2026-11-23, P1).
///
/// `DOCUMENTDB_GW_CPU_AFFINITY="0-7"` (or a single cpu, e.g. "3") pins the
/// gateway main thread to the given core range.  The tokio worker threads
/// are spawned after this call and inherit the creator's mask, so the whole
/// gateway lands on the partitioned cores.  Pair with
/// `shm_channel.backend_cpu_start` to partition cores between the gateway
/// and its shm backend workers (cuts cross-core IPIs and ring-cursor
/// cacheline migration).  Unset / malformed values disable pinning.
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

#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn documentdb_gw_worker_main(_arg: pg_sys::Datum) {
    // Direct stderr diagnostics: unlike pgrx log!()/tracing, eprintln! is
    // unfiltered (no log_min_messages, no subscriber init dependency), so
    // the startup path is always visible in the postmaster log.
    eprintln!("GW-HOST: documentdb_gw_worker_main entered (pid={})", std::process::id());

    apply_gateway_affinity();

    STARTUP_INSTANT.get_or_init(Instant::now);

    install_panic_hook();
    eprintln!("GW-HOST: panic hook installed");

    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

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

    // Select the backend transport once at worker start: "shmem" enables
    // the shared-memory ring channel (with automatic network fallback),
    // anything else keeps the original loopback-socket path.
    let transport = PG_DOCUMENTDB_GATEWAY_TRANSPORT
        .get()
        .map(|value| value.to_str().unwrap_or("network").to_string())
        .unwrap_or_else(|| "network".to_string());
    documentdb_gateway_core::postgres::shmem_transport::set_transport_enabled(
        transport == "shmem",
    );
    eprintln!(
        "GW-HOST: transport='{}', transport_enabled={}",
        transport,
        transport == "shmem"
    );
    log!("gateway transport: {transport}");

    let shutdown_token = SHUTDOWN_CONTROLLER.token();
    let worker_name = BackgroundWorker::get_name();

    // now start the gw on a worker thread.
    //
    // The runtime used to be pinned to a single worker thread, which serialized
    // all socket I/O for every client connection: with 40 clients only one
    // tokio worker accumulated CPU while the PostgreSQL backends sat idle in
    // `ClientRead` waiting to be fed. Size the pool from the machine instead
    // (overridable by `DOCUMENTDB_GW_WORKER_THREADS` for experiments).
    //
    // The fallback deliberately uses half the cores rather than all of them:
    // this gateway shares the host with the backends it drives, and the full
    // core count measurably oversubscribes it (see `default_worker_threads`).
    let worker_threads = std::env::var("DOCUMENTDB_GW_WORKER_THREADS")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|threads| *threads > 0)
        .unwrap_or_else(default_worker_threads);

    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()
        .unwrap();

    tokio_runtime.spawn(async move {
        if let Err(error) = run_docdb_gateway(setup_configuration_file.as_str()).await {
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

async fn run_docdb_gateway(setup_configuration_file: &str) -> Result<()> {
    let cfg_file = std::path::PathBuf::from(setup_configuration_file);

    let shutdown_token = SHUTDOWN_CONTROLLER.token();

    let setup_configuration =
        DocumentDBSetupConfiguration::new(&cfg_file).expect("Failed to load configuration.");

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
