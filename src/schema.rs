//! Table naming, prefix validation, and canonical DDL rendering/migration helpers.

use sqlx::MySqlPool;

use crate::error::InitError;

const INIT_DDL_TEMPLATE: &str = include_str!("../sql/0001_init.sql");
const MAX_TABLE_PREFIX_LEN: usize = 32;

/// Validates that `prefix` is safe to interpolate into SQL identifiers.
///
/// Permits empty string `""` or up to 32 ASCII alphanumeric / underscore characters,
/// starting with an ASCII letter or underscore when non-empty.
pub fn validate_table_prefix(prefix: &str) -> Result<(), InitError> {
    if prefix.is_empty() {
        return Ok(());
    }
    if prefix.len() > MAX_TABLE_PREFIX_LEN {
        return Err(InitError::InvalidConfig(format!(
            "table_prefix length {} exceeds maximum of {MAX_TABLE_PREFIX_LEN} characters",
            prefix.len()
        )));
    }
    let mut chars = prefix.chars();
    if let Some(first) = chars.next()
        && !(first.is_ascii_alphabetic() || first == '_')
    {
        return Err(InitError::InvalidConfig(
            "table_prefix must start with an ASCII letter or underscore".to_string(),
        ));
    }
    if !prefix
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(InitError::InvalidConfig(
            "table_prefix may contain only ASCII letters, digits, and '_'".to_string(),
        ));
    }
    Ok(())
}

/// Renders the canonical MySQL 8.0+ / TiDB DDL script for the given `table_prefix`.
///
/// Useful when `auto_migrate(false)` is configured and schema DDL is managed by an
/// external migration tool.
///
/// # Errors
///
/// Returns [`InitError::InvalidConfig`] if `table_prefix` contains invalid characters.
pub fn schema_ddl(table_prefix: &str) -> Result<String, InitError> {
    validate_table_prefix(table_prefix)?;
    Ok(INIT_DDL_TEMPLATE.replace("__PREFIX__", table_prefix))
}

/// Splits the rendered canonical DDL into individual executable `CREATE TABLE` statements.
///
/// # Errors
///
/// Returns [`InitError::InvalidConfig`] if `table_prefix` contains invalid characters.
pub fn schema_statements(table_prefix: &str) -> Result<Vec<String>, InitError> {
    let rendered = schema_ddl(table_prefix)?;
    let stmts = rendered
        .split(';')
        .map(|chunk| {
            chunk
                .lines()
                .filter(|line| !line.trim_start().starts_with("--"))
                .collect::<Vec<_>>()
                .join("\n")
                .trim()
                .to_string()
        })
        .filter(|stmt| !stmt.is_empty())
        .collect();
    Ok(stmts)
}

/// Pre-resolved SQL table identifiers for a [`crate::MysqlProvider`] instance.
#[derive(Debug, Clone)]
pub(crate) struct SchemaNames {
    pub(crate) instances: String,
    pub(crate) executions: String,
    pub(crate) history: String,
    pub(crate) orchestrator_queue: String,
    pub(crate) worker_queue: String,
    pub(crate) instance_locks: String,
    pub(crate) sessions: String,
    pub(crate) kv_store: String,
    pub(crate) kv_delta: String,
}

impl SchemaNames {
    pub(crate) fn new(prefix: &str) -> Result<Self, InitError> {
        validate_table_prefix(prefix)?;
        Ok(Self {
            instances: format!("`{prefix}instances`"),
            executions: format!("`{prefix}executions`"),
            history: format!("`{prefix}history`"),
            orchestrator_queue: format!("`{prefix}orchestrator_queue`"),
            worker_queue: format!("`{prefix}worker_queue`"),
            instance_locks: format!("`{prefix}instance_locks`"),
            sessions: format!("`{prefix}sessions`"),
            kv_store: format!("`{prefix}kv_store`"),
            kv_delta: format!("`{prefix}kv_delta`"),
        })
    }
}

/// Builds a `sqlx` query from SQL constructed exclusively from validated [`SchemaNames`]
/// identifiers and positional `?` placeholders (never untrusted user input).
pub(crate) fn sql_query(
    sql: String,
) -> sqlx::query::Query<'static, sqlx::MySql, sqlx::mysql::MySqlArguments> {
    sqlx::query(sqlx::AssertSqlSafe(sql))
}

/// Executes all `CREATE TABLE IF NOT EXISTS` statements sequentially against `pool`.
pub(crate) async fn run_migrations(pool: &MySqlPool, prefix: &str) -> Result<(), InitError> {
    let statements = schema_statements(prefix)?;
    for stmt in statements {
        sql_query(stmt).execute(pool).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_table_prefixes() {
        assert!(validate_table_prefix("").is_ok());
        assert!(validate_table_prefix("duroxide_").is_ok());
        assert!(validate_table_prefix("_t1_").is_ok());
        assert!(validate_table_prefix("9bad").is_err());
        assert!(validate_table_prefix("bad-prefix").is_err());
        assert!(validate_table_prefix("bad;drop").is_err());
        assert!(validate_table_prefix(&"a".repeat(33)).is_err());
    }

    #[test]
    fn renders_nine_ddl_statements_without_prohibited_constructs() {
        let stmts = schema_statements("dx_").unwrap_or_default();
        assert_eq!(stmts.len(), 9);
        for stmt in &stmts {
            let upper = stmt.to_ascii_uppercase();
            assert!(
                !upper.contains("AUTO_INCREMENT"),
                "found AUTO_INCREMENT in {stmt}"
            );
            assert!(
                !upper.contains("FOREIGN KEY"),
                "found FOREIGN KEY in {stmt}"
            );
            assert!(!upper.contains("REFERENCES"), "found REFERENCES in {stmt}");
            assert!(!upper.contains("CHECK"), "found CHECK in {stmt}");
            assert!(
                !upper.contains("UNIQUE"),
                "found secondary UNIQUE in {stmt}"
            );
            assert!(stmt.contains("dx_"));
        }
    }
}
