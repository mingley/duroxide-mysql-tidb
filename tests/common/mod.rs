#![allow(clippy::unwrap_used, clippy::expect_used, dead_code)]

use std::str::FromStr;
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::Duration;

use duroxide::provider_validations::ProviderFactory;
use duroxide::providers::Provider;
use duroxide_mysql_tidb::{MySqlConnectOptions, MySqlPool, MysqlProvider};
use sqlx::mysql::MySqlPoolOptions;
use tokio::sync::Mutex;

pub const TEST_LOCK_TIMEOUT: Duration = Duration::from_millis(2_500);

static REUSABLE_PREFIXES: OnceLock<StdMutex<Vec<String>>> = OnceLock::new();

pub fn test_database_url() -> Option<String> {
    match std::env::var("DUROXIDE_MYSQL_TEST_URL") {
        Ok(val) if !val.trim().is_empty() => Some(val),
        _ => {
            if std::env::var("DUROXIDE_MYSQL_REQUIRE_DB")
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false)
            {
                panic!(
                    "DUROXIDE_MYSQL_REQUIRE_DB=1 is set, but DUROXIDE_MYSQL_TEST_URL is missing"
                );
            }
            None
        }
    }
}

pub async fn new_test_pool(url: &str) -> MySqlPool {
    let opts = MySqlConnectOptions::from_str(url).expect("valid DUROXIDE_MYSQL_TEST_URL");
    MySqlPoolOptions::new()
        .max_connections(12)
        .acquire_timeout(Duration::from_secs(30))
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                sqlx::Executor::execute(
                    conn,
                    "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
                )
                .await?;
                Ok(())
            })
        })
        .connect_with(opts)
        .await
        .expect("failed to connect test MySqlPool")
}

async fn clear_prefix_tables(pool: &MySqlPool, prefix: &str) {
    for suffix in [
        "instance_locks",
        "orchestrator_queue",
        "worker_queue",
        "sessions",
        "kv_delta",
        "kv_store",
        "history",
        "executions",
        "instances",
    ] {
        let sql = format!("DELETE FROM `{prefix}{suffix}`");
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .execute(pool)
            .await
            .expect("failed to clear test table");
    }
}

async fn checkout_clean_prefix(pool: &MySqlPool) -> (String, Arc<MysqlProvider>) {
    let free_list = REUSABLE_PREFIXES.get_or_init(|| StdMutex::new(Vec::new()));
    let reused = {
        let mut guard = free_list.lock().expect("prefix pool mutex poisoned");
        guard.pop()
    };

    if let Some(prefix) = reused {
        clear_prefix_tables(pool, &prefix).await;
        let provider = MysqlProvider::from_pool(pool.clone(), &prefix, false)
            .await
            .expect("failed to wrap reused prefix");
        (prefix, Arc::new(provider))
    } else {
        let rand_id = uuid::Uuid::new_v4().simple().to_string();
        let prefix = format!("t_{}_", &rand_id[..12]);
        let provider = MysqlProvider::from_pool(pool.clone(), &prefix, true)
            .await
            .expect("failed to migrate new test prefix");
        (prefix, Arc::new(provider))
    }
}

fn release_prefixes(prefixes: Vec<String>) {
    if prefixes.is_empty() {
        return;
    }
    let free_list = REUSABLE_PREFIXES.get_or_init(|| StdMutex::new(Vec::new()));
    let mut guard = free_list.lock().expect("prefix pool mutex poisoned");
    guard.extend(prefixes);
}

/// Test factory where each `create_provider()` call within a test gets its own isolated
/// table set on a runtime-local `MySqlPool`, and `finish()` closes the pool and returns
/// table prefixes to the reusable pool.
pub struct IsolatedMysqlTestFactory {
    pool: MySqlPool,
    leased_prefixes: Mutex<Vec<String>>,
}

impl IsolatedMysqlTestFactory {
    pub async fn new_or_skip() -> Option<Self> {
        let url = test_database_url()?;
        let pool = new_test_pool(&url).await;
        Some(Self {
            pool,
            leased_prefixes: Mutex::new(Vec::new()),
        })
    }

    pub async fn finish(self) {
        let prefixes = self.leased_prefixes.into_inner();
        if tokio::time::timeout(Duration::from_secs(5), self.pool.close())
            .await
            .is_ok()
        {
            release_prefixes(prefixes);
        }
    }
}

#[async_trait::async_trait]
impl ProviderFactory for IsolatedMysqlTestFactory {
    async fn create_provider(&self) -> Arc<dyn Provider> {
        let (prefix, provider) = checkout_clean_prefix(&self.pool).await;
        self.leased_prefixes.lock().await.push(prefix);
        provider
    }

    fn lock_timeout(&self) -> Duration {
        TEST_LOCK_TIMEOUT
    }

    fn short_poll_threshold(&self) -> Duration {
        Duration::from_millis(500)
    }
}

/// Test factory backed by a single shared provider prefix so corruption/inspection
/// hooks mutate the exact tables used by `create_provider()`.
pub struct SharedMysqlTestFactory {
    pool: MySqlPool,
    prefix: String,
    provider: Arc<MysqlProvider>,
}

impl SharedMysqlTestFactory {
    pub async fn new_or_skip() -> Option<Self> {
        let url = test_database_url()?;
        let pool = new_test_pool(&url).await;
        let (prefix, provider) = checkout_clean_prefix(&pool).await;
        Some(Self {
            pool,
            prefix,
            provider,
        })
    }

    pub async fn finish(self) {
        let prefix = self.prefix;
        if tokio::time::timeout(Duration::from_secs(5), self.pool.close())
            .await
            .is_ok()
        {
            release_prefixes(vec![prefix]);
        }
    }
}

#[async_trait::async_trait]
impl ProviderFactory for SharedMysqlTestFactory {
    async fn create_provider(&self) -> Arc<dyn Provider> {
        self.provider.clone()
    }

    fn lock_timeout(&self) -> Duration {
        TEST_LOCK_TIMEOUT
    }

    fn short_poll_threshold(&self) -> Duration {
        Duration::from_millis(500)
    }

    async fn corrupt_instance_history(&self, instance: &str) {
        let sql = format!(
            "UPDATE `{}history` SET event_data = 'NOT_VALID_JSON{{{{{{' WHERE instance_id = ?",
            self.prefix
        );
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(instance)
            .execute(&self.pool)
            .await
            .expect("failed to corrupt history");
    }

    async fn get_max_attempt_count(&self, instance: &str) -> u32 {
        let sql = format!(
            "SELECT COALESCE(MAX(attempt_count), 0) FROM `{}orchestrator_queue` WHERE instance_id = ?",
            self.prefix
        );
        let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(instance)
            .fetch_one(&self.pool)
            .await
            .expect("failed to query max attempt_count");
        count.max(0) as u32
    }
}
