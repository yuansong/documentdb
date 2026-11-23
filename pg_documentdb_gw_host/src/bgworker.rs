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

/// Name prefix of the gateway background workers when multiple instances are
/// registered; individual workers are named `{PREFIX}gw{index}`.
const WORKER_NAME_PREFIX: &str = "DocumentDB Gateway Host gw";

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
    std::env::var("DOCUMENTDB_GW_INSTANCES")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|instances| *instances > 0)
        .unwrap_or(1)
        .min(MAX_GATEWAY_INSTANCES)
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
        BackgroundWorkerBuilder::new(&format!("{WORKER_NAME_PREFIX}{index}"))
            .set_function("documentdb_gw_worker_main")
            .set_library("pg_documentdb_gw_host")
            .set_restart_time(Some(Duration::from_secs(1)))
            .set_start_time(BgWorkerStartTime::ConsistentState)
            .enable_spi_access()
            .set_argument(Some(pg_sys::Datum::from(index)))
            .load();
    }
}

/// Fallback number of Tokio worker threads for the embedded gateway.
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

#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn documentdb_gw_worker_main(arg: pg_sys::Datum) {
    STARTUP_INSTANT.get_or_init(Instant::now);

    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    let instance_index = arg.value();
    let instances = gateway_instances();

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
    // Worker thread resolution order (aligned with the standalone
    // `documentdb-gateway` binary, which sizes its runtime from the same
    // setup configuration key):
    //   1. `async_runtime_worker_threads` in the setup configuration file,
    //   2. `DOCUMENTDB_GW_WORKER_THREADS` environment variable,
    //   3. half the machine's cores as fallback (see `default_worker_threads`).
    //
    // The fallback deliberately uses half the cores rather than all of them:
    // this gateway shares the host with the backends it drives, and the full
    // core count measurably oversubscribes it (see `default_worker_threads`).
    let cfg_file = std::path::PathBuf::from(setup_configuration_file.as_str());
    let mut setup_configuration =
        DocumentDBSetupConfiguration::new(&cfg_file).expect("Failed to load configuration.");

    // Multi-instance mode: every instance must set SO_REUSEPORT on its
    // listeners so all of them can bind the same gateway port. The kernel
    // then load-balances client connections across the instances by
    // connection 4-tuple hash.
    if instances > 1 {
        setup_configuration.gateway_reuse_port = Some(true);
        log!(
            "Gateway instance gw{instance_index}/{instances}: SO_REUSEPORT listeners enabled"
        );
    }

    let config_worker_threads = setup_configuration.async_runtime_worker_threads;
    let env_worker_threads = std::env::var("DOCUMENTDB_GW_WORKER_THREADS")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|threads| *threads > 0);

    let (worker_threads, worker_threads_source) = config_worker_threads
        .map(|threads| (threads, "setup configuration"))
        .or_else(|| env_worker_threads.map(|threads| (threads, "DOCUMENTDB_GW_WORKER_THREADS")))
        .unwrap_or_else(|| (default_worker_threads(), "half the cores (default)"));
    log!(
        "Gateway worker threads: {worker_threads} (source: {worker_threads_source}, instance gw{instance_index}/{instances})"
    );

    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()
        .unwrap();

    tokio_runtime.spawn(async move {
        if let Err(error) = run_docdb_gateway(setup_configuration).await {
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

async fn run_docdb_gateway(setup_configuration: DocumentDBSetupConfiguration) -> Result<()> {
    let shutdown_token = SHUTDOWN_CONTROLLER.token();

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
