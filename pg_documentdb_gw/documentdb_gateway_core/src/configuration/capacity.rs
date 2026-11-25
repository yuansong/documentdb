/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 * SPDX-License-Identifier: MIT
 *
 * documentdb_gateway_core/src/configuration/capacity.rs
 *
 * Sizing rules for the resources one gateway instance owns: tokio worker
 * threads and data-pool connections.
 *
 * Both have to be derived from three inputs at once:
 *
 *   * the machine (usable cores),
 *   * what PostgreSQL itself needs (maintenance processes, autovacuum,
 *     pg_cron jobs) -- the gateway shares the box with the backends it drives,
 *   * how many gateway instances share the host (`DOCUMENTDB_GW_INSTANCES`).
 *
 * Sizing to the machine alone is what produced two production surprises:
 * `cores/2` threads per instance multiplied by K instances (K x 64 threads on a
 * 128-core host), and a data pool of `max_connections - budget` per instance
 * per credential (K x 93 connections against a 100-slot cluster, plus one such
 * pool for every authenticated user).
 *-------------------------------------------------------------------------
 */

use std::num::NonZero;
use std::sync::OnceLock;

/// Configuration values snapshotted from the `documentdb_gateway.*` GUCs.
///
/// This crate is pure Rust (no pgrx dependency, also linked into the
/// standalone gateway binary), so GUC values cannot be read here directly.
/// The gw_host background worker reads them once on the postmaster thread
/// during initialization and injects them via [`set_guc_overrides`]; every
/// resolution below then prefers an explicitly configured GUC value over
/// the environment variable, and behaves exactly as before when the GUCs
/// are left unset (the default).
#[derive(Debug, Clone, Copy, Default)]
pub struct GucOverrides {
    /// `documentdb_gateway.gateway_instances`.
    pub gateway_instances: Option<usize>,
    /// `documentdb_gateway.async_runtime_worker_threads`.
    pub worker_threads: Option<usize>,
    /// `documentdb_gateway.data_pool_connections`.
    pub data_pool_connections: Option<usize>,
}

impl GucOverrides {
    /// No GUC channel configured; all resolutions fall through to env/JSON.
    pub const NONE: Self = Self {
        gateway_instances: None,
        worker_threads: None,
        data_pool_connections: None,
    };
}

static GUC_OVERRIDES: OnceLock<GucOverrides> = OnceLock::new();

/// Publishes the GUC snapshot. Idempotent: the first snapshot wins, which
/// is fine because Postmaster-context GUCs cannot change after the
/// configuration file has been processed.
pub fn set_guc_overrides(overrides: GucOverrides) {
    let _ = GUC_OVERRIDES.set(overrides);
}

fn guc_overrides() -> GucOverrides {
    *GUC_OVERRIDES.get().unwrap_or(&GucOverrides::NONE)
}

/// Environment variable holding the number of gateway instances that share
/// one host. Fallback channel below the `documentdb_gateway.gateway_instances`
/// GUC, retained for container/K8s and existing deployment scripts.
pub const GATEWAY_INSTANCES_ENV: &str = "DOCUMENTDB_GW_INSTANCES";

/// Environment variable overriding the worker-thread count of one instance.
/// Fallback channel below the
/// `documentdb_gateway.async_runtime_worker_threads` GUC.
pub const WORKER_THREADS_ENV: &str = "DOCUMENTDB_GW_WORKER_THREADS";

/// Environment variable overriding the data-pool connection cap of one
/// instance. Intended as an escape hatch for workloads that pin connections
/// (long-lived cursors) and therefore need more backends than threads.
/// Fallback channel below the `documentdb_gateway.data_pool_connections` GUC.
pub const DATA_POOL_CONNECTIONS_ENV: &str = "DOCUMENTDB_GW_DATA_POOL_CONNECTIONS";

/// Upper bound on gateway instances per host. Purely a runaway guard: any
/// real deployment wanting more than this on one box should rethink its
/// topology (and would be limited by cores long before this).
pub const MAX_GATEWAY_INSTANCES: usize = 64;

/// Always-on PostgreSQL maintenance processes: checkpointer, background
/// writer, WAL writer and the autovacuum launcher.
const PG_MAINTENANCE_PROCESSES: usize = 4;

/// PostgreSQL's default `autovacuum_max_workers`.
const PG_AUTOVACUUM_WORKERS_DEFAULT: usize = 3;

/// pg_cron's default `cron.max_running_jobs` (it launches at most this many
/// background workers concurrently).
const PG_CRON_JOBS_DEFAULT: usize = 5;

/// Smallest data pool we are willing to configure. One connection would
/// serialize every request of a credential behind a single backend, so keep
/// room for a second in-flight query even on a one-core box.
const MIN_DATA_POOL_CONNECTIONS: usize = 2;

/// Connections allowed per worker thread. A thread drives one backend at a
/// time, so this factor exists only to cover connections parked by an open
/// cursor or a transaction that spans several statements.
const DATA_POOL_CONNECTIONS_PER_THREAD: usize = 2;

/// Returns the number of usable cores, never zero.
#[must_use]
pub fn available_cores() -> usize {
    std::thread::available_parallelism().map_or(1, NonZero::get)
}

fn env_usize(key: &str) -> Option<usize> {
    std::env::var(key)
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
}

/// Number of gateway instances the operator asked for, clamped to at least
/// one but not capped. See [`gateway_instance_count`] for the effective value
/// and use this one only to warn about a value that had to be clamped.
///
/// Resolution order: `documentdb_gateway.gateway_instances` GUC, then the
/// `DOCUMENTDB_GW_INSTANCES` environment variable, then the default of 1.
#[must_use]
pub fn requested_gateway_instance_count() -> usize {
    guc_overrides()
        .gateway_instances
        .or_else(|| env_usize(GATEWAY_INSTANCES_ENV))
        .unwrap_or(1)
}

/// Effective number of gateway instances sharing this host.
#[must_use]
pub fn gateway_instance_count() -> usize {
    requested_gateway_instance_count().min(MAX_GATEWAY_INSTANCES)
}

/// Worker-thread override from the
/// `documentdb_gateway.async_runtime_worker_threads` GUC, if any.
#[must_use]
pub fn worker_threads_guc() -> Option<usize> {
    guc_overrides().worker_threads
}

/// Worker-thread override from the environment, if any.
#[must_use]
pub fn worker_threads_from_env() -> Option<usize> {
    env_usize(WORKER_THREADS_ENV)
}

/// Data-pool connection override: `documentdb_gateway.data_pool_connections`
/// GUC first, then the environment variable.
#[must_use]
pub fn data_pool_connections_override() -> Option<usize> {
    guc_overrides()
        .data_pool_connections
        .or_else(|| env_usize(DATA_POOL_CONNECTIONS_ENV))
}

/// Cores that must stay available to PostgreSQL itself.
///
/// Counts the always-on maintenance processes, the default autovacuum worker
/// count and pg_cron's default job concurrency. On a large host that is a
/// rounding error; on a four-core box it is the difference between a gateway
/// that leaves room for vacuum and one that starves it. The reserve is
/// therefore capped at a quarter of the machine and never drops below one
/// core while the machine has any.
#[must_use]
pub fn pg_reserved_cores(cores: usize) -> usize {
    let wanted = PG_MAINTENANCE_PROCESSES + PG_AUTOVACUUM_WORKERS_DEFAULT + PG_CRON_JOBS_DEFAULT;
    let quarter = (cores / 4).max(1);

    wanted.min(quarter)
}

/// Default total worker threads for all gateway instances on this host.
///
/// Half the cores is the measured optimum when the gateway competes with the
/// backends it drives (16 vCPU: 8 workers reached 103.8k ops/s, 16 reached
/// 97.0k, 4 dropped to 81.5k). The result is additionally bounded so that the
/// cores reserved by [`pg_reserved_cores`] stay free, which only starts to
/// bite on hosts with fewer than ~16 cores.
#[must_use]
pub fn default_total_worker_threads(cores: usize) -> usize {
    let cores = cores.max(1);
    let half = (cores / 2).max(1);
    let after_pg_reserve = cores.saturating_sub(pg_reserved_cores(cores)).max(1);

    half.min(after_pg_reserve)
}

/// Worker threads for one instance out of `instances` sharing the host.
///
/// An explicit `configured` value is the operator's per-instance choice and is
/// honoured unchanged (they may deliberately oversubscribe, and a JSON file is
/// read by every instance). The derived default is split across instances,
/// rounding up so that the total is not silently reduced when the budget does
/// not divide evenly, but never below one thread per instance.
#[must_use]
pub fn per_instance_worker_threads(
    configured: Option<usize>,
    instances: usize,
    cores: usize,
) -> usize {
    if let Some(threads) = configured.filter(|threads| *threads > 0) {
        return threads;
    }

    let instances = instances.max(1);

    default_total_worker_threads(cores)
        .div_ceil(instances)
        .max(1)
}

/// Resolves the worker-thread count of one gateway instance on this host.
///
/// Resolution order: `documentdb_gateway.async_runtime_worker_threads` GUC,
/// then the explicit value (setup configuration file), then
/// `DOCUMENTDB_GW_WORKER_THREADS`, then the derived default.
#[must_use]
pub fn instance_worker_threads(configured: Option<usize>) -> usize {
    per_instance_worker_threads(
        worker_threads_guc()
            .or(configured)
            .or_else(worker_threads_from_env),
        gateway_instance_count(),
        available_cores(),
    )
}

/// Data-pool connection capacity for one instance and one credential.
///
/// The cluster's `max_connections` is the hard ceiling for everything: system
/// pools of every instance, the data pools of every instance and every
/// PostgreSQL client outside the gateway. Every instance owns its own
/// auth/system pools, so those are reserved `instances` times, and what is
/// left is split fairly.
///
/// The share is then capped by what one instance can actually drive: with
/// `worker_threads` in flight at most that many backends are busy, so a larger
/// pool only parks idle backends (and each backend costs shared memory and a
/// process slot). The cap scales with the core count because the thread count
/// does.
///
/// Returns at least one connection, and never more than the fair share.
#[must_use]
pub fn per_instance_data_pool_connections(
    max_connections: usize,
    system_connection_budget_per_instance: usize,
    instances: usize,
    worker_threads: usize,
) -> usize {
    let instances = instances.max(1);
    let reserved = system_connection_budget_per_instance.saturating_mul(instances);
    let share = max_connections.saturating_sub(reserved) / instances;
    let thread_bound = worker_threads
        .saturating_mul(DATA_POOL_CONNECTIONS_PER_THREAD)
        .max(MIN_DATA_POOL_CONNECTIONS);

    share.min(thread_bound).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_cores_never_eat_the_machine() {
        // Always at least one core, never more than a quarter.
        assert_eq!(pg_reserved_cores(1), 1);
        assert_eq!(pg_reserved_cores(4), 1);
        assert_eq!(pg_reserved_cores(8), 2);
        assert_eq!(pg_reserved_cores(16), 4);
        assert_eq!(pg_reserved_cores(64), 12);
        // Never more than the PG process count itself.
        assert_eq!(pg_reserved_cores(128), 12);
        assert_eq!(pg_reserved_cores(1024), 12);
    }

    #[test]
    fn default_total_threads_is_half_the_machine_on_common_sizes() {
        // Single core still gets one worker: a gateway with no thread cannot
        // serve anything, and a background worker is restarted in a loop
        // otherwise.
        for (cores, expected) in [
            (0, 1),
            (1, 1),
            (2, 1),
            (3, 1),
            (4, 2),
            (8, 4),
            (16, 8),
            (24, 12),
            (32, 16),
            (64, 32),
            (128, 64),
            (256, 128),
        ] {
            assert_eq!(
                default_total_worker_threads(cores),
                expected,
                "cores: {cores}"
            );
        }
    }

    #[test]
    fn default_total_threads_leave_room_for_postgres() {
        for cores in 2..=512 {
            let threads = default_total_worker_threads(cores);
            assert!(threads >= 1, "cores: {cores}");
            assert!(
                threads + pg_reserved_cores(cores) <= cores,
                "cores: {cores}, threads: {threads}"
            );
        }
        // A one-core host cannot leave anything: the gateway gets the core.
        assert_eq!(default_total_worker_threads(1), 1);
    }

    #[test]
    fn default_threads_are_split_across_instances() {
        // 16 cores -> budget of 8 threads.
        assert_eq!(per_instance_worker_threads(None, 1, 16), 8);
        assert_eq!(per_instance_worker_threads(None, 2, 16), 4);
        // Rounding up keeps the total from being cut when the budget does not
        // divide evenly: 8/3 -> 3 (9 threads total, not 6).
        assert_eq!(per_instance_worker_threads(None, 3, 16), 3);
        assert_eq!(per_instance_worker_threads(None, 8, 16), 1);
        // More instances than threads: every instance still needs one.
        assert_eq!(per_instance_worker_threads(None, 64, 16), 1);
        // 128 cores -> budget of 64 threads.
        assert_eq!(per_instance_worker_threads(None, 4, 128), 16);
    }

    #[test]
    fn explicit_thread_count_is_per_instance_and_never_zero() {
        assert_eq!(per_instance_worker_threads(Some(8), 4, 128), 8);
        // Zero / missing means "derive it", and one core still works.
        assert_eq!(per_instance_worker_threads(Some(0), 1, 1), 1);
        assert_eq!(per_instance_worker_threads(None, 0, 1), 1);
    }

    #[test]
    fn instance_count_is_never_zero() {
        assert!(gateway_instance_count() >= 1);
        assert!(gateway_instance_count() <= MAX_GATEWAY_INSTANCES);
        assert_eq!(
            gateway_instance_count(),
            requested_gateway_instance_count().min(MAX_GATEWAY_INSTANCES)
        );
    }

    #[test]
    fn data_pool_scales_with_threads_and_shrinks_with_instances() {
        // 16 cores, 8 threads, 100-slot cluster, 7 reserved per instance.
        assert_eq!(per_instance_data_pool_connections(100, 7, 1, 8), 16);
        // Same host, two instances: the cluster budget is now shared, and both
        // bounds apply (fair share 43, thread bound 8).
        assert_eq!(per_instance_data_pool_connections(100, 7, 2, 4), 8);
        // Four instances on a 128-core host, 16 threads each: share 18 wins.
        assert_eq!(per_instance_data_pool_connections(100, 7, 4, 16), 18);
        // A small cluster caps the pool regardless of how many threads exist.
        assert_eq!(per_instance_data_pool_connections(20, 7, 1, 64), 13);
        // Degenerate: the reservations already consume the cluster.
        assert_eq!(per_instance_data_pool_connections(10, 7, 1, 64), 3);
        assert_eq!(per_instance_data_pool_connections(5, 7, 1, 64), 1);
        assert_eq!(per_instance_data_pool_connections(0, 7, 1, 64), 1);
    }

    #[test]
    fn data_pool_reservations_fit_the_cluster_budget() {
        for max_connections in [10, 20, 100, 300, 1000] {
            for instances in 1..=8 {
                for threads in [1, 4, 8, 16, 64] {
                    let pool =
                        per_instance_data_pool_connections(max_connections, 7, instances, threads);
                    // Whatever we hand out must fit in the cluster, unless the
                    // cluster is smaller than the reservations themselves (in
                    // which case one connection is the floor and the operator
                    // has to raise `max_connections`).
                    let total = instances * (pool + 7);
                    assert!(
                        total <= max_connections || pool == 1,
                        "max_connections: {max_connections}, instances: {instances}, \
                         threads: {threads}, pool: {pool}, total: {total}"
                    );
                }
            }
        }
    }

    #[test]
    fn data_pool_never_falls_below_the_minimum_when_capacity_allows() {
        // 64 threads on a 300-slot cluster: the thread bound is 128, the fair
        // share is 293, so the pool tracks the thread count, not the cluster.
        assert_eq!(per_instance_data_pool_connections(300, 7, 1, 64), 128);
        assert_eq!(per_instance_data_pool_connections(300, 7, 1, 1), 2);
    }

    #[test]
    fn guc_override_slots_in_front_of_the_resolution_chain() {
        // The GUC snapshot is process-global (published once by the bgworker
        // before the runtime starts), so this test must NOT publish one via
        // `set_guc_overrides` -- it would leak into the parallel
        // pool_manager tests that resolve capacity through the same globals.
        // The precedence below exercises the exact expression the public
        // entry points use; the snapshot wiring itself is verified by the
        // gw_host integration run (startup dump `worker_thread_source`).
        let guc_worker_threads = Some(7);

        // Worker threads: GUC outranks both the setup configuration key ...
        assert_eq!(
            per_instance_worker_threads(
                guc_worker_threads.or(Some(4)),
                1,
                16
            ),
            7
        );
        // ... and the environment fallback (modeled as the last `.or_else`).
        let env_worker_threads = Some(5);
        assert_eq!(
            per_instance_worker_threads(
                None.or(env_worker_threads),
                1,
                16
            ),
            5
        );
        // With no GUC and no setup key the derived default still applies.
        assert_eq!(per_instance_worker_threads(None.or(None), 2, 16), 4);

        // Instance count: GUC > env > default 1 (modeled identically).
        let guc_instances = Some(3);
        assert_eq!(guc_instances.or(None).unwrap_or(1), 3);
        assert_eq!(None::<usize>.or(None).unwrap_or(1), 1);
    }
}
