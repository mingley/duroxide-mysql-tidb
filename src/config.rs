//! Connection pool, schema, and runtime tuning configuration for [`crate::MysqlProvider`].

use std::str::FromStr;
use std::time::Duration;

use sqlx::mysql::MySqlConnectOptions;

use crate::error::InitError;

/// Default maximum connections in the provider's `MySqlPool`.
pub const DEFAULT_MAX_CONNECTIONS: u32 = 16;

/// Default candidate window fetched before attempting a compare-and-set claim.
pub const DEFAULT_CANDIDATE_BATCH_SIZE: u32 = 8;

/// Configuration for constructing a [`crate::MysqlProvider`].
///
/// Works identically against MySQL 8.0+ and TiDB endpoints.
#[derive(Debug, Clone)]
pub struct MysqlProviderConfig {
    pub(crate) connect_options: MySqlConnectOptions,
    pub(crate) max_connections: u32,
    pub(crate) min_connections: u32,
    pub(crate) acquire_timeout: Duration,
    pub(crate) idle_timeout: Option<Duration>,
    pub(crate) max_lifetime: Option<Duration>,
    pub(crate) table_prefix: String,
    pub(crate) auto_migrate: bool,
    pub(crate) set_read_committed: bool,
    pub(crate) candidate_batch_size: u32,
}

impl MysqlProviderConfig {
    /// Creates a configuration from a `mysql://` connection URL.
    ///
    /// If `DUROXIDE_MYSQL_POOL_MAX` is set in the environment to a positive integer,
    /// it overrides the default `max_connections` (`16`).
    ///
    /// # Errors
    ///
    /// Returns [`InitError::Database`] if `database_url` cannot be parsed as MySQL connection options.
    pub fn from_url(database_url: &str) -> Result<Self, InitError> {
        let opts = MySqlConnectOptions::from_str(database_url)?;
        Ok(Self::from_connect_options(opts))
    }

    /// Creates a configuration from pre-built [`MySqlConnectOptions`] (useful for custom TLS / mTLS).
    #[must_use]
    pub fn from_connect_options(connect_options: MySqlConnectOptions) -> Self {
        let env_max = std::env::var("DUROXIDE_MYSQL_POOL_MAX")
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .filter(|&n| n > 0);

        Self {
            connect_options,
            max_connections: env_max.unwrap_or(DEFAULT_MAX_CONNECTIONS),
            min_connections: 0,
            acquire_timeout: Duration::from_secs(15),
            idle_timeout: Some(Duration::from_secs(300)),
            max_lifetime: Some(Duration::from_secs(1800)),
            table_prefix: String::new(),
            auto_migrate: true,
            set_read_committed: true,
            candidate_batch_size: DEFAULT_CANDIDATE_BATCH_SIZE,
        }
    }

    /// Sets the maximum number of connections in the pool (must be >= 1).
    #[must_use]
    pub fn max_connections(mut self, max_connections: u32) -> Self {
        self.max_connections = max_connections.max(1);
        self
    }

    /// Sets the minimum number of idle connections maintained by the pool.
    #[must_use]
    pub fn min_connections(mut self, min_connections: u32) -> Self {
        self.min_connections = min_connections;
        self
    }

    /// Sets the pool connection acquisition timeout.
    #[must_use]
    pub fn acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = timeout;
        self
    }

    /// Sets the idle connection timeout (`None` disables idle eviction).
    #[must_use]
    pub fn idle_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Sets the maximum connection lifetime (`None` disables lifetime recycling).
    #[must_use]
    pub fn max_lifetime(mut self, lifetime: Option<Duration>) -> Self {
        self.max_lifetime = lifetime;
        self
    }

    /// Sets an optional table-name prefix (for example `"duroxide_"` or `"app1_"`).
    ///
    /// Useful for multi-tenant isolation within a single MySQL/TiDB logical database.
    /// Only ASCII alphanumeric characters and `_` are allowed, up to 32 characters.
    #[must_use]
    pub fn table_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.table_prefix = prefix.into();
        self
    }

    /// Controls whether `CREATE TABLE IF NOT EXISTS` DDL runs automatically at startup.
    ///
    /// Defaults to `true`. Set to `false` in production environments where schema DDL
    /// is applied out-of-band via an external migration pipeline (see [`crate::schema_ddl`]).
    #[must_use]
    pub fn auto_migrate(mut self, enabled: bool) -> Self {
        self.auto_migrate = enabled;
        self
    }

    /// Controls whether pooled sessions execute `SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED`.
    ///
    /// Defaults to `true`, which reduces InnoDB gap-lock contention on queue tables and matches
    /// TiDB's pessimistic transaction semantics cleanly.
    #[must_use]
    pub fn read_committed_isolation(mut self, enabled: bool) -> Self {
        self.set_read_committed = enabled;
        self
    }

    /// Sets how many candidate queue items are inspected per poll before returning `None`
    /// under contention (defaults to `8`).
    #[must_use]
    pub fn candidate_batch_size(mut self, size: u32) -> Self {
        self.candidate_batch_size = size.clamp(1, 64);
        self
    }

    /// Returns the configured table prefix.
    #[must_use]
    pub fn get_table_prefix(&self) -> &str {
        &self.table_prefix
    }
}
