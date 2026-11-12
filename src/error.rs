//! Error types and MySQL / TiDB SQLSTATE + vendor error code classification.

use duroxide::providers::ProviderError;
use thiserror::Error;

/// Errors returned while configuring, connecting, or initializing [`crate::MysqlProvider`].
#[derive(Debug, Error)]
pub enum InitError {
    /// Invalid configuration parameter (for example, an unsafe table prefix).
    #[error("invalid provider configuration: {0}")]
    InvalidConfig(String),

    /// Underlying `sqlx` connection, pool, or DDL error.
    #[error("database initialization failed: {0}")]
    Database(#[from] sqlx::Error),
}

/// Classification of a MySQL or TiDB error into Duroxide's retryable vs. permanent model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlErrorClass {
    /// Transient infrastructure/concurrency error; safe for the runtime to retry.
    Retryable,
    /// Permanent constraint, schema, or protocol error; retrying will not succeed.
    Permanent,
}

/// Classifies a MySQL/TiDB SQLSTATE and optional vendor error number.
///
/// Handles standard MySQL 8.0+ InnoDB error codes as well as TiDB-specific
/// distributed transaction and schema-lease error codes.
pub fn classify_mysql_code(
    sqlstate: Option<&str>,
    vendor_code: Option<u16>,
    message: &str,
) -> SqlErrorClass {
    if let Some(code) = vendor_code {
        match code {
            // MySQL / TiDB transient concurrency & lock errors:
            // 1205: ER_LOCK_WAIT_TIMEOUT
            // 1213: ER_LOCK_DEADLOCK
            // 1040: ER_CON_COUNT_ERROR (too many connections)
            // 1158..=1161: network packet read/write errors
            // 2002..=2006, 2013: connection lost / server gone away
            // 3101: group replication / storage transient rollback
            1040 | 1158..=1161 | 1205 | 1213 | 2002..=2006 | 2013 | 3101 => {
                return SqlErrorClass::Retryable;
            }
            // TiDB-specific transient codes:
            // 8002..=8005, 8022, 8027, 8028: TiDB txn commit / schema changed (info schema lease)
            // 9001..=9007: TiKV region unavailable, PD timeout, LockNotFound, WriteConflict (9007)
            8002..=8005 | 8022 | 8027 | 8028 | 9001..=9007 => {
                return SqlErrorClass::Retryable;
            }
            // TiDB 1105 (ER_UNKNOWN_ERROR) is used for transient TiKV/region backoff errors
            1105 if is_transient_tidb_message(message) => {
                return SqlErrorClass::Retryable;
            }
            // Permanent constraint / syntax / schema errors:
            // 1062: ER_DUP_ENTRY (duplicate primary/unique key)
            // 1048: ER_BAD_NULL_ERROR
            // 1054: ER_BAD_FIELD_ERROR
            // 1064: ER_PARSE_ERROR
            // 1146: ER_NO_SUCH_TABLE
            // 1406: ER_DATA_TOO_LONG
            1048 | 1054 | 1062 | 1064 | 1146 | 1406 => {
                return SqlErrorClass::Permanent;
            }
            _ => {}
        }
    }

    if let Some(state) = sqlstate {
        match state {
            // 40001: Serialization failure / deadlock / write conflict
            // 40P01: Deadlock detected
            // HY000 with timeout/lock indicators handled below
            // 08xxx: Connection exception class
            "40001" | "40P01" => return SqlErrorClass::Retryable,
            s if s.starts_with("08") => return SqlErrorClass::Retryable,
            // 23000 / 23505: Integrity constraint violation (duplicate event/instance)
            // 22xxx: Data exception
            // 42xxx: Syntax error or access rule violation
            // 28xxx: Invalid authorization specification
            s if s.starts_with("23")
                || s.starts_with("22")
                || s.starts_with("42")
                || s.starts_with("28") =>
            {
                return SqlErrorClass::Permanent;
            }
            _ => {}
        }
    }

    if is_transient_tidb_message(message) {
        return SqlErrorClass::Retryable;
    }

    SqlErrorClass::Permanent
}

fn is_transient_tidb_message(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("write conflict")
        || lower.contains("try again later")
        || lower.contains("region is unavailable")
        || lower.contains("information schema is changed")
        || lower.contains("deadlock")
        || lower.contains("lock wait timeout")
        || lower.contains("server has gone away")
        || lower.contains("connection reset")
}

/// Returns `true` if `err` represents a MySQL/TiDB duplicate-key violation (`1062` / `23000`).
pub(crate) fn is_duplicate_key_error(err: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(db_err) = err {
        if db_err.is_unique_violation() {
            return true;
        }
        if let Some(code) = db_err.code()
            && code == "23000"
        {
            return true;
        }
        if parse_mysql_vendor_code(db_err.as_ref()) == Some(1062) {
            return true;
        }
    }
    false
}

/// Returns `true` if `err` is a transient lock/conflict error suitable for fast local retry.
pub(crate) fn is_transient_conflict(err: &sqlx::Error) -> bool {
    matches!(sqlx_error_class(err), SqlErrorClass::Retryable)
}

pub(crate) fn sqlx_error_class(err: &sqlx::Error) -> SqlErrorClass {
    match err {
        sqlx::Error::Io(_)
        | sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed => SqlErrorClass::Retryable,
        sqlx::Error::Tls(_)
        | sqlx::Error::Configuration(_)
        | sqlx::Error::ColumnNotFound(_)
        | sqlx::Error::ColumnDecode { .. }
        | sqlx::Error::Decode(_)
        | sqlx::Error::TypeNotFound { .. }
        | sqlx::Error::RowNotFound => SqlErrorClass::Permanent,
        sqlx::Error::Database(db_err) => {
            let state = db_err.code();
            let vendor = parse_mysql_vendor_code(db_err.as_ref());
            classify_mysql_code(state.as_deref(), vendor, db_err.message())
        }
        other => {
            let msg = other.to_string();
            classify_mysql_code(None, None, &msg)
        }
    }
}

fn parse_mysql_vendor_code(db_err: &dyn sqlx::error::DatabaseError) -> Option<u16> {
    // sqlx MySqlDatabaseError formats Display as `<number> (<sqlstate>): <message>`.
    let rendered = db_err.to_string();
    let prefix = rendered.split_whitespace().next()?;
    prefix.parse::<u16>().ok()
}

/// Converts a `sqlx::Error` into Duroxide's [`ProviderError`] with accurate `retryable` metadata.
pub(crate) fn sqlx_to_provider_error(operation: &str, err: sqlx::Error) -> ProviderError {
    let class = sqlx_error_class(&err);
    let msg = err.to_string();
    match class {
        SqlErrorClass::Retryable => ProviderError::retryable(operation, msg),
        SqlErrorClass::Permanent => ProviderError::permanent(operation, msg),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_mysql_and_tidb_codes() {
        assert_eq!(
            classify_mysql_code(Some("40001"), Some(1213), "Deadlock found"),
            SqlErrorClass::Retryable
        );
        assert_eq!(
            classify_mysql_code(Some("HY000"), Some(1205), "Lock wait timeout exceeded"),
            SqlErrorClass::Retryable
        );
        assert_eq!(
            classify_mysql_code(Some("HY000"), Some(9007), "Write conflict, txnStartTS=123"),
            SqlErrorClass::Retryable
        );
        assert_eq!(
            classify_mysql_code(Some("HY000"), Some(8028), "Information schema is changed"),
            SqlErrorClass::Retryable
        );
        assert_eq!(
            classify_mysql_code(
                Some("HY000"),
                Some(1105),
                "region is unavailable, try again later"
            ),
            SqlErrorClass::Retryable
        );
        assert_eq!(
            classify_mysql_code(
                Some("23000"),
                Some(1062),
                "Duplicate entry 'x' for key 'PRIMARY'"
            ),
            SqlErrorClass::Permanent
        );
        assert_eq!(
            classify_mysql_code(Some("28000"), Some(1045), "Access denied for user"),
            SqlErrorClass::Permanent
        );
        assert_eq!(
            classify_mysql_code(Some("42S02"), Some(1146), "Table doesn't exist"),
            SqlErrorClass::Permanent
        );
    }
}
