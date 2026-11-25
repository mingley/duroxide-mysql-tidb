# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.2] - 2026-11-24

### Added
- `PoolResolver` trait, `FnPoolResolver`, `MysqlProvider::from_pool_fn`, `MysqlProvider::from_pool_resolver`, and `MysqlProvider::resolve_pool` so callers backed by external multi-host connection managers (DNS SRV discovery, proxy rotation, or TLS/credential reloads) can resolve a live `MySqlPool` per storage operation without pinning a single startup pool.
- Live pool-rotation failover scenario test (`pool_resolver_fails_over_after_old_pool_closes`) in `tests/runtime_scenarios.rs`.

## [0.1.1] - 2026-11-16

### Added
- Optional `memory` Cargo feature providing `MemoryProvider`, a pure-Rust, zero-C in-memory `Provider` and `ProviderAdmin` implementation passing all 232 Duroxide `provider_validations` tests and runtime scenarios.

### Fixed
- Bounded `MySqlPool::close()` timeout in integration test teardown to prevent hangs when `Runtime::shutdown` aborts in-flight transactions.

## [0.1.0] - 2026-11-12

### Added
- Full `duroxide::providers::Provider` and `ProviderAdmin` implementation for MySQL 8.0+ and TiDB over `sqlx` (`0.9`).
- Strict-DDL canonical schema (`sql/0001_init.sql` and `schema_ddl`) with `utf8mb4_bin` collation, monotonic 64-bit `BIGINT` sequence IDs, and no stored procedures, `SKIP LOCKED`, `AUTO_INCREMENT`, `FOREIGN KEY`s, `CHECK` constraints, or secondary `UNIQUE` indexes.
- Multi-tenant `table_prefix` isolation, configurable connection pooling, custom `MySqlConnectOptions` TLS/mTLS support, and optional `auto_migrate(false)` mode for external DDL pipelines.
- Conformance test harness covering all 232 official Duroxide provider validation tests plus runtime scenario tests on MySQL 8.0 and TiDB 8.5.
