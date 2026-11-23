/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 *
 * SPDX-License-Identifier: MIT
 *
 * documentdb_gateway_core/src/service/listener_config.rs
 *
 * Listener configuration derived once from the setup configuration.
 *
 *-------------------------------------------------------------------------
 */

use crate::configuration::SetupConfiguration;

/// All settings needed to bind the gateway's listeners.
///
/// Derived once from the [`SetupConfiguration`] so that listener creation call
/// sites pass a single value instead of threading individual configuration
/// fields through positional arguments.
#[derive(Debug, Clone)]
pub struct ListenerConfig {
    use_local_host: bool,
    port: u16,
    reuse_port: bool,
    unix_socket_path: Option<String>,
    unix_socket_permissions: u32,
}

impl ListenerConfig {
    /// Builds a minimal localhost configuration for tests.
    #[cfg(test)]
    #[must_use]
    pub const fn for_test(port: u16) -> Self {
        Self {
            use_local_host: true,
            port,
            reuse_port: false,
            unix_socket_path: None,
            unix_socket_permissions: 0o600,
        }
    }

    /// Builds a minimal all-interfaces (non-localhost) configuration for tests.
    #[cfg(test)]
    #[must_use]
    pub const fn for_test_all_interfaces(port: u16) -> Self {
        Self {
            use_local_host: false,
            port,
            reuse_port: false,
            unix_socket_path: None,
            unix_socket_permissions: 0o600,
        }
    }

    /// Whether the gateway should bind to localhost only.
    #[must_use]
    pub const fn use_local_host(&self) -> bool {
        self.use_local_host
    }

    /// The TCP port the gateway listens on.
    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port
    }

    /// Whether the TCP listeners should set SO_REUSEPORT so multiple
    /// processes can share the same listening port (Linux only).
    #[must_use]
    pub const fn reuse_port(&self) -> bool {
        self.reuse_port
    }

    /// The Unix domain socket path, if a Unix socket listener is configured.
    #[must_use]
    pub fn unix_socket_path(&self) -> Option<&str> {
        self.unix_socket_path.as_deref()
    }

    /// The file permissions (octal) to apply to the Unix domain socket file.
    #[must_use]
    pub const fn unix_socket_permissions(&self) -> u32 {
        self.unix_socket_permissions
    }
}

impl From<&dyn SetupConfiguration> for ListenerConfig {
    fn from(cfg: &dyn SetupConfiguration) -> Self {
        Self {
            use_local_host: cfg.use_local_host(),
            port: cfg.gateway_listen_port(),
            reuse_port: cfg.gateway_reuse_port(),
            unix_socket_path: cfg.unix_socket_path().map(ToOwned::to_owned),
            unix_socket_permissions: cfg.unix_socket_file_permissions(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::DocumentDBSetupConfiguration;

    #[test]
    fn from_setup_configuration_maps_all_fields() {
        let setup = DocumentDBSetupConfiguration {
            use_local_host: Some(true),
            gateway_listen_port: Some(12345),
            gateway_reuse_port: Some(true),
            unix_socket_path: Some("/tmp/gw.sock".to_owned()),
            unix_socket_file_permissions: Some("640".to_owned()),
            ..Default::default()
        };

        let cfg = ListenerConfig::from(&setup as &dyn SetupConfiguration);

        assert!(cfg.use_local_host());
        assert_eq!(cfg.port(), 12345);
        assert!(cfg.reuse_port());
        assert_eq!(cfg.unix_socket_path(), Some("/tmp/gw.sock"));
        assert_eq!(cfg.unix_socket_permissions(), 0o640);
    }

    #[test]
    fn from_setup_configuration_uses_defaults_when_unset() {
        let setup = DocumentDBSetupConfiguration::default();

        let cfg = ListenerConfig::from(&setup as &dyn SetupConfiguration);

        // Mirrors the SetupConfiguration trait defaults.
        assert!(!cfg.use_local_host());
        assert_eq!(cfg.port(), 10260);
        assert_eq!(cfg.unix_socket_path(), None);
        assert_eq!(cfg.unix_socket_permissions(), 0o660);
    }
}
