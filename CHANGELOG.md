# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-11-12

### Added
- Full `duroxide::providers::Provider` and `ProviderAdmin` implementation for MySQL 8.0+ and TiDB over `sqlx` (`0.9`).
- Strict-DDL canonical schema (`sql/0001_init.sql` and `schema_ddl`) with `utf8mb4_bin` collation, monotonic 64-bit `BIGINT` sequence IDs, and no stored procedures, `SKIP LOCKED`, `AUTO_INCREMENT`, `FOREIGN KEY`s, `CHECK` constraints, or secondary `UNIQUE` indexes.
- Multi-tenant `table_prefix` isolation, configurable connection pooling, custom `MySqlConnectOptions` TLS/mTLS support, and optional `auto_migrate(false)` mode for external DDL pipelines.
- Conformance test harness covering all 232 official Duroxide provider validation tests plus runtime scenario tests on MySQL 8.0 and TiDB 8.5.
