use pgrx::{ffi::CString, GucContext, GucFlags, GucRegistry, GucSetting};

pub static PG_DOCUMENTDB_GATEWAY_DATABASE: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

pub static PG_DOCUMENTDB_SETUP_CONFIGURATION: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

/// Number of gateway background workers registered by `_PG_init`.
/// `0` (the default) means "not configured": resolution falls back to the
/// `DOCUMENTDB_GW_INSTANCES` environment variable and then to 1.
pub static PG_DOCUMENTDB_GATEWAY_INSTANCES: GucSetting<i32> = GucSetting::<i32>::new(0);

/// Tokio worker threads per gateway instance. `0` means "not configured";
/// resolution then falls back to the `AsyncRuntimeWorkerThreads` setup
/// configuration key, the `DOCUMENTDB_GW_WORKER_THREADS` environment
/// variable, and finally the derived default (cores/2 split across
/// instances).
pub static PG_DOCUMENTDB_ASYNC_RUNTIME_WORKER_THREADS: GucSetting<i32> = GucSetting::<i32>::new(0);

/// Data-pool connection cap per instance and credential. `0` means
/// "not configured"; resolution falls back to the
/// `DOCUMENTDB_GW_DATA_POOL_CONNECTIONS` environment variable and then to
/// the derived capacity rule.
pub static PG_DOCUMENTDB_DATA_POOL_CONNECTIONS: GucSetting<i32> = GucSetting::<i32>::new(0);

/// CPU range (e.g. `0-7` or a single cpu `3`) the gateway is pinned to.
/// `NULL`/empty means "not configured"; the
/// `DOCUMENTDB_GW_CPU_AFFINITY` environment variable is the fallback.
pub static PG_DOCUMENTDB_CPU_AFFINITY: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

/// `RUST_LOG`-style filter selecting the tracing verbosity of the gateway
/// (e.g. `documentdb_gateway_core=debug`). `NULL`/empty means "not
/// configured"; the `RUST_LOG` environment variable is the fallback and
/// plain `info` the default.
pub static PG_DOCUMENTDB_LOG_FILTER: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

/// Directory for auto-generated TLS material. `NULL`/empty means "not
/// configured"; the `DOCUMENTDB_TLS_STATE_DIR` environment variable is the
/// fallback and `/var/lib/documentdb-gateway/tls` (with the usual
/// per-user/tempdir fallback chain) the default.
pub static PG_DOCUMENTDB_TLS_STATE_DIR: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

pub fn init() {
    GucRegistry::define_string_guc(
        c"documentdb_gateway.database",
        c"The database that the pg_documentdb_gateway BGWorker will connect to",
        c"This should be the database that you ran `CREATE EXTENSION pg_documentdb_gateway` in",
        &PG_DOCUMENTDB_GATEWAY_DATABASE,
        GucContext::Postmaster,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_string_guc(
        c"documentdb_gateway.setup_configuration_file",
        c"The setup configuration file for the pg_documentdb_gateway BGWorker",
        c"This should be the path to the setup configuration file",
        &PG_DOCUMENTDB_SETUP_CONFIGURATION,
        GucContext::Postmaster,
        GucFlags::SUPERUSER_ONLY,
    );

    // The remaining GUCs replace the former environment-variable-only
    // channels (`DOCUMENTDB_GW_*`, `RUST_LOG`). They are Postmaster-context:
    // like the env vars they replace, changing them requires a restart of
    // the postmaster. Every value is snapshotted once during background
    // worker initialization and never read per request (GUC FFI is not
    // allowed on the tokio runtime threads).
    GucRegistry::define_int_guc(
        c"documentdb_gateway.gateway_instances",
        c"Number of gateway background worker instances sharing one listening port",
        c"0 (default) falls back to the DOCUMENTDB_GW_INSTANCES environment variable, \
          then to a single instance. Values above 64 are clamped. Requires a restart.",
        &PG_DOCUMENTDB_GATEWAY_INSTANCES,
        0,
        i32::MAX,
        GucContext::Postmaster,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_int_guc(
        c"documentdb_gateway.async_runtime_worker_threads",
        c"Tokio worker threads per gateway instance",
        c"0 (default) falls back to the AsyncRuntimeWorkerThreads setup configuration key, \
          the DOCUMENTDB_GW_WORKER_THREADS environment variable, and finally to the \
          derived default (half the cores, split across instances, leaving room for \
          PostgreSQL's own workers). Requires a restart.",
        &PG_DOCUMENTDB_ASYNC_RUNTIME_WORKER_THREADS,
        0,
        i32::MAX,
        GucContext::Postmaster,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_int_guc(
        c"documentdb_gateway.data_pool_connections",
        c"Data-pool connection cap per gateway instance and credential",
        c"0 (default) falls back to the DOCUMENTDB_GW_DATA_POOL_CONNECTIONS environment \
          variable, then to the derived capacity rule (fair share of max_connections \
          across instances, bounded by what the runtime threads can drive). \
          Requires a restart.",
        &PG_DOCUMENTDB_DATA_POOL_CONNECTIONS,
        0,
        i32::MAX,
        GucContext::Postmaster,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_string_guc(
        c"documentdb_gateway.cpu_affinity",
        c"CPU range the gateway threads are pinned to",
        c"NULL/empty (default) falls back to the DOCUMENTDB_GW_CPU_AFFINITY environment \
          variable. Accepts a single cpu (`3`) or an inclusive range (`0-7`). \
          Unset/malformed values disable pinning. Requires a restart.",
        &PG_DOCUMENTDB_CPU_AFFINITY,
        GucContext::Postmaster,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_string_guc(
        c"documentdb_gateway.log_filter",
        c"Tracing log filter for the gateway (RUST_LOG syntax)",
        c"NULL/empty (default) falls back to the RUST_LOG environment variable, then to \
          `info`. Example: `documentdb_gateway_core=debug` turns on the per-request \
          forwarding-path diagnostics. Read once at worker start; not hot-reloaded.",
        &PG_DOCUMENTDB_LOG_FILTER,
        GucContext::Postmaster,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_string_guc(
        c"documentdb_gateway.tls_state_dir",
        c"Directory where auto-generated TLS material (cert.pem, pkey.pem) is stored",
        c"NULL/empty (default) falls back to the DOCUMENTDB_TLS_STATE_DIR environment \
          variable, then to /var/lib/documentdb-gateway/tls (with per-user/tempdir \
          fallbacks). Requires a restart.",
        &PG_DOCUMENTDB_TLS_STATE_DIR,
        GucContext::Postmaster,
        GucFlags::SUPERUSER_ONLY,
    );
}

/// Positive `int` GUC values act as an override; the `0` default and any
/// negative (rejected by the GUC bounds) mean "not configured".
fn int_guc_override(setting: &GucSetting<i32>) -> Option<usize> {
    usize::try_from(setting.get()).ok().filter(|value| *value > 0)
}

fn string_guc_override(setting: &GucSetting<Option<CString>>) -> Option<String> {
    let raw = setting.get()?;
    let value = raw.to_str().ok()?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

/// `documentdb_gateway.gateway_instances`, if explicitly configured.
#[must_use]
pub fn gateway_instances() -> Option<usize> {
    int_guc_override(&PG_DOCUMENTDB_GATEWAY_INSTANCES)
}

/// `documentdb_gateway.async_runtime_worker_threads`, if explicitly configured.
#[must_use]
pub fn worker_threads() -> Option<usize> {
    int_guc_override(&PG_DOCUMENTDB_ASYNC_RUNTIME_WORKER_THREADS)
}

/// `documentdb_gateway.data_pool_connections`, if explicitly configured.
#[must_use]
pub fn data_pool_connections() -> Option<usize> {
    int_guc_override(&PG_DOCUMENTDB_DATA_POOL_CONNECTIONS)
}

/// `documentdb_gateway.cpu_affinity`, if explicitly configured.
#[must_use]
pub fn cpu_affinity() -> Option<String> {
    string_guc_override(&PG_DOCUMENTDB_CPU_AFFINITY)
}

/// `documentdb_gateway.log_filter`, if explicitly configured.
#[must_use]
pub fn log_filter() -> Option<String> {
    string_guc_override(&PG_DOCUMENTDB_LOG_FILTER)
}

/// `documentdb_gateway.tls_state_dir`, if explicitly configured.
#[must_use]
pub fn tls_state_dir() -> Option<String> {
    string_guc_override(&PG_DOCUMENTDB_TLS_STATE_DIR)
}
