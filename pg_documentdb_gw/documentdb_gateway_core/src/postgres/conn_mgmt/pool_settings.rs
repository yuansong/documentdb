/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 *
 * documentdb_gateway_core/src/postgres/conn_mgmt/pool_settings.rs
 *
 *-------------------------------------------------------------------------
 */

use tokio::time::Duration;

use crate::configuration::{
    capacity, DynamicConfiguration, MAX_REQUEST_TIMEOUT_DEFAULT_SEC, TRANSACTION_TIMEOUT_DEFAULT_SEC,
};

/// Data connection buffer size (in bytes).
pub const CONN_BUFFER_SIZE: usize = 262_144;
pub const CONN_PRUNE_INTERVAL_SECS: u64 = 10;
pub const CONN_IDLE_LIFETIME_SECS: u64 = 300;
pub const CONN_LIFETIME_SECS: u64 = 3600;

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct PgPoolSettings {
    max_connections: usize,
    system_connection_budget: usize,
    /// Capacity computed for one instance of a multi-instance host. `None`
    /// keeps the plain "cluster limit minus reservation" behaviour, which is
    /// what system pools and the standalone gateway use.
    data_pool_connections: Option<usize>,
    connection_buffer_size: usize,
    connection_pruning_interval: Duration,
    connection_idle_lifetime: Duration,
    connection_lifetime: Duration,
    max_request_timeout: Duration,
    transaction_timeout: Duration,
    connect_timeout: Option<Duration>,
}

impl PgPoolSettings {
    #[must_use]
    pub const fn system_pool_settings(max_connections: usize) -> Self {
        Self::system_pool_settings_with_command_timeout(
            max_connections,
            MAX_REQUEST_TIMEOUT_DEFAULT_SEC,
        )
    }

    #[must_use]
    pub const fn system_pool_settings_with_command_timeout(
        max_connections: usize,
        command_timeout_sec: u64,
    ) -> Self {
        Self {
            max_connections,
            system_connection_budget: 0,
            data_pool_connections: None,
            connection_buffer_size: CONN_BUFFER_SIZE,
            connection_pruning_interval: Duration::from_secs(CONN_PRUNE_INTERVAL_SECS),
            connection_idle_lifetime: Duration::from_secs(CONN_IDLE_LIFETIME_SECS),
            connection_lifetime: Duration::from_secs(CONN_LIFETIME_SECS),
            max_request_timeout: Duration::from_secs(command_timeout_sec),
            transaction_timeout: Duration::from_secs(TRANSACTION_TIMEOUT_DEFAULT_SEC),
            connect_timeout: None,
        }
    }

    pub fn from_configuration(config: &dyn DynamicConfiguration) -> Self {
        let max_connections = config.max_connections();
        let system_connection_budget = config.system_connection_budget();
        let connection_pruning_interval =
            Duration::from_secs(config.gateway_connection_pruning_interval_sec());
        let connection_idle_lifetime =
            Duration::from_secs(config.gateway_connection_idle_lifetime_sec());
        let connection_lifetime = Duration::from_secs(config.gateway_connection_lifetime_sec());
        let connection_buffer_size = config.gateway_connection_buffer_size();
        let max_request_timeout = Duration::from_secs(config.max_request_timeout_sec());
        let transaction_timeout = Duration::from_secs(config.transaction_timeout_sec());
        let connect_timeout = config.connect_timeout();

        Self {
            max_connections,
            system_connection_budget,
            data_pool_connections: None,
            connection_buffer_size,
            connection_pruning_interval,
            connection_idle_lifetime,
            connection_lifetime,
            max_request_timeout,
            transaction_timeout,
            connect_timeout,
        }
    }

    /// Builds data-pool settings for one of `instances` gateway instances that
    /// share this host and its PostgreSQL cluster.
    ///
    /// Two things used to scale wrongly here:
    ///
    /// * every instance sized its data pool to the whole cluster
    ///   (`max_connections - budget`), so K instances could demand K times the
    ///   cluster's capacity, and every authenticated user got another such
    ///   pool;
    /// * the pool ignored the core count, so a 128-core host with 64 runtime
    ///   threads still capped a credential at the same handful of backends as
    ///   a 16-core host.
    ///
    /// The capacity now comes from [`capacity::per_instance_data_pool_connections`]:
    /// the cluster budget is split across instances and then bounded by what
    /// this instance's runtime can drive. Pool sizing deliberately does not
    /// follow `max_connections` alone: the gateway, not PostgreSQL, is the
    /// limit on this path.
    #[must_use]
    pub fn for_gateway_instance(
        config: &dyn DynamicConfiguration,
        instances: usize,
        worker_threads: usize,
    ) -> Self {
        let mut settings = Self::from_configuration(config);
        let data_pool_connections = capacity::data_pool_connections_override().unwrap_or_else(|| {
            capacity::per_instance_data_pool_connections(
                settings.max_connections,
                settings.system_connection_budget,
                instances,
                worker_threads,
            )
        });

        // This runs on every pool allocation (that is, on every authentication),
        // so log only when the computed capacity actually changes. The value is
        // process-wide, and it changes only when `max_connections` or the
        // gateway sizing does.
        static LAST_LOGGED_CAPACITY: std::sync::atomic::AtomicUsize =
            std::sync::atomic::AtomicUsize::new(usize::MAX);
        if LAST_LOGGED_CAPACITY.swap(data_pool_connections, std::sync::atomic::Ordering::Relaxed)
            != data_pool_connections
        {
            tracing::info!(
                max_connections = settings.max_connections,
                system_connection_budget = settings.system_connection_budget,
                instances,
                worker_threads,
                data_pool_connections,
                "Data pool capacity for this gateway instance"
            );
        }

        settings.data_pool_connections = Some(data_pool_connections);

        settings
    }

    /// Returns the data-pool capacity handed to this instance by
    /// [`PgPoolSettings::for_gateway_instance`], if any.
    #[must_use]
    pub const fn data_pool_connections(&self) -> Option<usize> {
        self.data_pool_connections
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) const fn with_connect_timeout(mut self, connect_timeout: Duration) -> Self {
        self.connect_timeout = Some(connect_timeout);
        self
    }

    #[must_use]
    pub const fn connect_timeout(&self) -> Option<Duration> {
        self.connect_timeout
    }

    #[must_use]
    pub const fn adjusted_max_connections(&self) -> usize {
        // A capacity computed for one instance on a multi-instance host wins:
        // it was already bounded by this instance's fair share of the cluster
        // budget, so subtracting the reservation again would double-count it.
        if let Some(data_pool_connections) = self.data_pool_connections {
            return data_pool_connections;
        }

        // Reserve the system budget out of the total, but never let the data
        // pool fall below the budget itself. `saturating_sub` is required
        // because the budget can exceed `max_connections` (e.g. when the
        // backend client-connection limit is configured below the budget); an
        // unchecked `usize` subtraction would wrap to a near-`usize::MAX`
        // value and blow up the downstream pool capacity allocation.
        let real_max_connections = self
            .max_connections
            .saturating_sub(self.system_connection_budget);

        if real_max_connections < self.system_connection_budget {
            self.system_connection_budget
        } else {
            real_max_connections
        }
    }

    #[must_use]
    pub const fn connection_pruning_interval(&self) -> Duration {
        self.connection_pruning_interval
    }

    #[must_use]
    pub const fn connection_idle_lifetime(&self) -> Duration {
        self.connection_idle_lifetime
    }

    #[must_use]
    pub const fn connection_lifetime(&self) -> Duration {
        self.connection_lifetime
    }

    #[must_use]
    pub const fn connection_buffer_size(&self) -> usize {
        self.connection_buffer_size
    }

    /// Returns the dynamically configured maximum request timeout.
    #[must_use]
    pub const fn max_request_timeout(&self) -> Duration {
        self.max_request_timeout
    }

    /// Returns the dynamically configured transaction timeout.
    #[must_use]
    pub const fn transaction_timeout(&self) -> Duration {
        self.transaction_timeout
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(max_connections: usize, system_connection_budget: usize) -> PgPoolSettings {
        PgPoolSettings {
            max_connections,
            system_connection_budget,
            data_pool_connections: None,
            connection_buffer_size: CONN_BUFFER_SIZE,
            connection_pruning_interval: Duration::from_secs(CONN_PRUNE_INTERVAL_SECS),
            connection_idle_lifetime: Duration::from_secs(CONN_IDLE_LIFETIME_SECS),
            connection_lifetime: Duration::from_secs(CONN_LIFETIME_SECS),
            max_request_timeout: Duration::from_secs(MAX_REQUEST_TIMEOUT_DEFAULT_SEC),
            transaction_timeout: Duration::from_secs(TRANSACTION_TIMEOUT_DEFAULT_SEC),
            connect_timeout: None,
        }
    }

    #[test]
    fn adjusted_max_connections_reserves_budget() {
        assert_eq!(settings(25, 10).adjusted_max_connections(), 15);
    }

    #[test]
    fn adjusted_max_connections_floors_to_budget_when_remainder_is_small() {
        // 12 - 10 = 2, which is below the budget, so it floors to the budget.
        assert_eq!(settings(12, 10).adjusted_max_connections(), 10);
    }

    #[test]
    fn adjusted_max_connections_does_not_underflow_when_budget_exceeds_max() {
        // Regression: when the client-connection limit is below the system
        // budget, the unsigned subtraction must not wrap to a near-`usize::MAX`
        // value (which previously caused a capacity-overflow panic when the
        // result was used as a connection-pool capacity).
        assert_eq!(settings(5, 10).adjusted_max_connections(), 10);
        assert_eq!(settings(0, 10).adjusted_max_connections(), 10);
    }

    #[test]
    fn per_instance_capacity_overrides_the_cluster_wide_math() {
        // A sharded capacity is final: the budget was already accounted for
        // when it was computed, so it must not be subtracted a second time.
        let sharded = PgPoolSettings {
            data_pool_connections: Some(4),
            ..settings(100, 7)
        };
        assert_eq!(sharded.adjusted_max_connections(), 4);
        assert_eq!(sharded.data_pool_connections(), Some(4));

        // Unsharded settings keep the historical behaviour.
        assert_eq!(settings(100, 7).adjusted_max_connections(), 93);
        assert_eq!(settings(100, 7).data_pool_connections(), None);
    }
}
