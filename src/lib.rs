//! MySQL 8.0+ and TiDB storage provider for [Duroxide](https://github.com/microsoft/duroxide).
//!
//! `duroxide-mysql-tidb` implements Duroxide's [`Provider`](duroxide::providers::Provider) and
//! [`ProviderAdmin`](duroxide::providers::ProviderAdmin) traits over `sqlx` (`0.9`) using
//! portable SQL transactions designed to run identically on standalone **MySQL 8.0+** and
//! distributed **TiDB** clusters:
//!
//! - **No stored procedures, triggers, `RETURNING`, or advisory locks** — all lease fencing
//!   and turn commits use explicit ACID transactions in deterministic lock order.
//! - **No `FOR UPDATE SKIP LOCKED` required** — queue claiming uses bounded candidate reads
//!   followed by compare-and-set updates, compatible with strict SQL proxies and validators.
//! - **No `AUTO_INCREMENT`, `FOREIGN KEY`, `CHECK`, or secondary `UNIQUE` constraints** —
//!   uses monotonic 64-bit `BIGINT` sequence IDs, primary-key-only uniqueness, and
//!   `utf8mb4_bin` collation for exact case-sensitive identifier matching.
//! - **External migration friendly** — supports automatic `CREATE TABLE IF NOT EXISTS` at
//!   startup (`auto_migrate(true)`, default) or pre-provisioned schemas (`auto_migrate(false)`)
//!   using [`schema_ddl`] / `sql/0001_init.sql`.
//! - **Optional pure-Rust in-memory provider (`memory` feature)** — enables `MemoryProvider`
//!   for fast, zero-C unit and integration tests without an external database.
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use std::time::Duration;
//! use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationRegistry, OrchestrationStatus};
//! use duroxide::runtime::{self, registry::ActivityRegistry};
//! use duroxide_mysql_tidb::MysqlProvider;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let store = Arc::new(MysqlProvider::new("mysql://root:password@localhost:3306/duroxide").await?);
//!
//! let activities = ActivityRegistry::builder()
//!     .register("Greet", |_ctx: ActivityContext, name: String| async move {
//!         Ok(format!("Hello, {name}!"))
//!     })
//!     .build();
//!
//! let orchestrations = OrchestrationRegistry::builder()
//!     .register("HelloFlow", |ctx: OrchestrationContext, name: String| async move {
//!         ctx.schedule_activity("Greet", name).await
//!     })
//!     .build();
//!
//! let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
//! let client = Client::new(store);
//!
//! client.start_orchestration("greeting-1", "HelloFlow", "TiDB").await?;
//! let status = client.wait_for_orchestration("greeting-1", Duration::from_secs(5)).await?;
//! assert!(matches!(status, OrchestrationStatus::Completed { .. }));
//!
//! rt.shutdown(None).await;
//! # Ok(())
//! # }
//! ```

mod admin;
pub mod config;
pub mod error;
mod id;
#[cfg(feature = "memory")]
pub mod memory;
mod provider;
pub mod schema;

pub use config::{DEFAULT_CANDIDATE_BATCH_SIZE, DEFAULT_MAX_CONNECTIONS, MysqlProviderConfig};
pub use duroxide;
pub use error::{InitError, SqlErrorClass, classify_mysql_code};
#[cfg(feature = "memory")]
pub use memory::MemoryProvider;
pub use provider::{FnPoolResolver, MysqlProvider, PoolResolver};
pub use schema::{schema_ddl, schema_statements, validate_table_prefix};
pub use sqlx::mysql::{MySqlConnectOptions, MySqlPool, MySqlSslMode};
