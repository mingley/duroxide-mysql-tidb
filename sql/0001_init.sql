-- Canonical DDL for duroxide-mysql-tidb (MySQL 8.0+ and TiDB compatible).
--
-- Replace `__PREFIX__` with an empty string (default) or a validated `[A-Za-z0-9_]+`
-- table prefix (for example `duroxide_`).
--
-- Schema design guarantees for broad MySQL and distributed-SQL (TiDB) compatibility:
--   * No stored procedures, triggers, views, or advisory locks
--   * No AUTO_INCREMENT, CHECK constraints, generated columns, or FOREIGN KEYs
--   * No secondary UNIQUE indexes (uniqueness is enforced solely via PRIMARY KEYs)
--   * Explicit BIGINT integer columns and binary UTF-8 collation (utf8mb4_bin)
--     for exact, case-sensitive identifier matching across MySQL and TiDB
--   * All composite primary keys and indexes stay under InnoDB's 3072-byte limit

CREATE TABLE IF NOT EXISTS __PREFIX__instances (
    instance_id VARCHAR(255) NOT NULL,
    orchestration_name VARCHAR(512) NOT NULL,
    orchestration_version VARCHAR(255) NULL,
    current_execution_id BIGINT NOT NULL DEFAULT 1,
    custom_status LONGTEXT NULL,
    custom_status_version BIGINT NOT NULL DEFAULT 0,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    seq_id BIGINT NOT NULL,
    parent_instance_id VARCHAR(255) NULL,
    PRIMARY KEY (instance_id),
    INDEX idx_instances_parent (parent_instance_id),
    INDEX idx_instances_created (created_at, seq_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS __PREFIX__executions (
    instance_id VARCHAR(255) NOT NULL,
    execution_id BIGINT NOT NULL,
    status VARCHAR(64) NOT NULL DEFAULT 'Running',
    output LONGTEXT NULL,
    duroxide_version_major BIGINT NULL,
    duroxide_version_minor BIGINT NULL,
    duroxide_version_patch BIGINT NULL,
    started_at BIGINT NOT NULL,
    completed_at BIGINT NULL,
    PRIMARY KEY (instance_id, execution_id),
    INDEX idx_executions_status_completed (status, completed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS __PREFIX__history (
    instance_id VARCHAR(255) NOT NULL,
    execution_id BIGINT NOT NULL,
    event_id BIGINT NOT NULL,
    event_type VARCHAR(128) NOT NULL,
    event_data LONGTEXT NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (instance_id, execution_id, event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS __PREFIX__orchestrator_queue (
    id BIGINT NOT NULL,
    instance_id VARCHAR(255) NOT NULL,
    work_item LONGTEXT NOT NULL,
    visible_at BIGINT NOT NULL,
    lock_token VARCHAR(128) NULL,
    locked_until BIGINT NULL,
    attempt_count BIGINT NOT NULL DEFAULT 0,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (id),
    INDEX idx_orch_visible (visible_at, id),
    INDEX idx_orch_instance_visible (instance_id, visible_at, id),
    INDEX idx_orch_lock (lock_token)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS __PREFIX__worker_queue (
    id BIGINT NOT NULL,
    work_item LONGTEXT NOT NULL,
    visible_at BIGINT NOT NULL,
    lock_token VARCHAR(128) NULL,
    locked_until BIGINT NULL,
    attempt_count BIGINT NOT NULL DEFAULT 0,
    created_at BIGINT NOT NULL,
    instance_id VARCHAR(255) NULL,
    execution_id BIGINT NULL,
    activity_id BIGINT NULL,
    session_id VARCHAR(255) NULL,
    tag VARCHAR(255) NULL,
    PRIMARY KEY (id),
    INDEX idx_worker_visible (visible_at, id),
    INDEX idx_worker_lock (lock_token),
    INDEX idx_worker_identity (instance_id, execution_id, activity_id),
    INDEX idx_worker_session (session_id, visible_at, id),
    INDEX idx_worker_tag (tag, visible_at, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS __PREFIX__instance_locks (
    instance_id VARCHAR(255) NOT NULL,
    lock_token VARCHAR(128) NOT NULL,
    locked_until BIGINT NOT NULL,
    locked_at BIGINT NOT NULL,
    PRIMARY KEY (instance_id),
    INDEX idx_instance_locks_token (lock_token)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS __PREFIX__sessions (
    session_id VARCHAR(255) NOT NULL,
    worker_id VARCHAR(255) NOT NULL,
    locked_until BIGINT NOT NULL,
    last_activity_at BIGINT NOT NULL,
    PRIMARY KEY (session_id),
    INDEX idx_sessions_worker_lock (worker_id, locked_until)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS __PREFIX__kv_store (
    instance_id VARCHAR(255) NOT NULL,
    kv_key VARCHAR(511) NOT NULL,
    value LONGTEXT NOT NULL,
    last_updated_at_ms BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (instance_id, kv_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS __PREFIX__kv_delta (
    instance_id VARCHAR(255) NOT NULL,
    kv_key VARCHAR(511) NOT NULL,
    value LONGTEXT NULL,
    last_updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (instance_id, kv_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
