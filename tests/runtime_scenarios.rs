#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{IsolatedMysqlTestFactory, new_test_pool, test_database_url};
use duroxide::provider_validations::ProviderFactory;
use duroxide::providers::{ExecutionMetadata, Provider, WorkItem};
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self, RuntimeOptions};
use duroxide::{
    ActivityContext, Client, Either2, OrchestrationContext, OrchestrationRegistry,
    OrchestrationStatus,
};
use duroxide_mysql_tidb::{MysqlProvider, MysqlProviderConfig, schema_statements};

#[tokio::test]
async fn end_to_end_fanout_timers_events_and_kv() {
    let Some(factory) = IsolatedMysqlTestFactory::new_or_skip().await else {
        return;
    };
    let store = factory.create_provider().await;

    let activities = ActivityRegistry::builder()
        .register("Upper", |_ctx: ActivityContext, input: String| async move {
            Ok(input.to_uppercase())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "Pipeline",
            |ctx: OrchestrationContext, input: String| async move {
                ctx.set_kv_value("phase", "fanout");

                let f1 = ctx.schedule_activity("Upper", format!("{input}-a"));
                let f2 = ctx.schedule_activity("Upper", format!("{input}-b"));
                let parts: Vec<String> = ctx
                    .join(vec![f1, f2])
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()?;

                ctx.set_kv_value("phase", "awaiting-signal");
                ctx.set_custom_status("awaiting-approval");
                let timer = ctx.schedule_timer(Duration::from_secs(30));
                let gate = ctx.schedule_wait("Approve");
                let gate_val = match ctx.select2(timer, gate).await {
                    Either2::First(()) => return Err("timed out".to_string()),
                    Either2::Second(val) => val,
                };

                ctx.set_kv_value("phase", "done");
                Ok(format!("{}:{}:{gate_val}", parts[0], parts[1]))
            },
        )
        .build();

    let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
    let client = Client::new(store);

    client
        .start_orchestration("e2e-1", "Pipeline", "item")
        .await
        .unwrap();

    // Wait until custom_status reflects that fanout ran, then send external approval signal.
    let _ = client
        .wait_for_status_change(
            "e2e-1",
            0,
            Duration::from_millis(25),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    client
        .raise_event("e2e-1", "Approve", "approved")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("e2e-1", Duration::from_secs(10))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed {
            output,
            custom_status,
            ..
        } => {
            assert_eq!(output, "ITEM-A:ITEM-B:approved");
            assert_eq!(custom_status.as_deref(), Some("awaiting-approval"));
        }
        other => panic!("unexpected orchestration status: {other:?}"),
    }

    assert_eq!(
        client
            .get_kv_value("e2e-1", "phase")
            .await
            .unwrap()
            .as_deref(),
        Some("done")
    );

    rt.shutdown(None).await;
    factory.finish().await;
}

#[tokio::test(flavor = "current_thread")]
async fn works_on_single_threaded_current_thread_runtime() {
    let Some(url) = test_database_url() else {
        return;
    };
    let config = MysqlProviderConfig::from_url(&url)
        .unwrap()
        .max_connections(4)
        .table_prefix("st1_");
    let provider = Arc::new(MysqlProvider::new_with_config(config).await.unwrap());

    // Ensure clean state if re-run locally.
    let pool = new_test_pool(&url).await;
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
        let sql = format!("DELETE FROM `st1_{suffix}`");
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .execute(&pool)
            .await
            .unwrap();
    }

    let activities = ActivityRegistry::builder()
        .register("AddOne", |_ctx: ActivityContext, val: String| async move {
            let n: i64 = val.parse().unwrap_or(0);
            Ok((n + 1).to_string())
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "Step",
            |ctx: OrchestrationContext, val: String| async move {
                ctx.schedule_activity("AddOne", val).await
            },
        )
        .build();

    let opts = RuntimeOptions {
        orchestration_concurrency: 1,
        worker_concurrency: 1,
        ..Default::default()
    };
    let rt =
        runtime::Runtime::start_with_options(provider.clone(), activities, orchestrations, opts)
            .await;
    let client = Client::new(provider);

    client
        .start_orchestration("st-inst-1", "Step", "41")
        .await
        .unwrap();

    match client
        .wait_for_orchestration("st-inst-1", Duration::from_secs(10))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "42"),
        other => panic!("unexpected status: {other:?}"),
    }

    rt.shutdown(None).await;
}

#[tokio::test]
async fn external_ddl_mode_with_auto_migrate_disabled() {
    let Some(url) = test_database_url() else {
        return;
    };
    let pool = new_test_pool(&url).await;
    let prefix = "extddl_";

    for stmt in schema_statements(prefix).unwrap() {
        sqlx::query(sqlx::AssertSqlSafe(stmt.as_str()))
            .execute(&pool)
            .await
            .unwrap();
    }

    let cfg = MysqlProviderConfig::from_url(&url)
        .unwrap()
        .table_prefix(prefix)
        .auto_migrate(false);
    let provider = MysqlProvider::new_with_config(cfg).await.unwrap();

    provider
        .enqueue_for_orchestrator(
            WorkItem::StartOrchestration {
                instance: "ext-1".to_string(),
                orchestration: "NoOp".to_string(),
                input: "{}".to_string(),
                version: Some("1.0.0".to_string()),
                parent_instance: None,
                parent_id: None,
                parent_execution_id: None,
                execution_id: 1,
            },
            None,
        )
        .await
        .unwrap();

    let (item, token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(10), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item.instance, "ext-1");
    provider
        .ack_orchestration_item(
            &token,
            1,
            vec![],
            vec![],
            vec![],
            ExecutionMetadata::default(),
            vec![],
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn lock_renewal_fetch_ack_abandon_contention_no_deadlocks() {
    let Some(factory) = IsolatedMysqlTestFactory::new_or_skip().await else {
        return;
    };
    let provider = factory.create_provider().await;

    for idx in 0..8 {
        provider
            .enqueue_for_orchestrator(
                WorkItem::StartOrchestration {
                    instance: format!("contend-{idx}"),
                    orchestration: "Contend".to_string(),
                    input: "{}".to_string(),
                    version: Some("1.0.0".to_string()),
                    parent_instance: None,
                    parent_id: None,
                    parent_execution_id: None,
                    execution_id: 1,
                },
                None,
            )
            .await
            .unwrap();
    }

    let mut handles = Vec::new();
    for worker in 0..8usize {
        let p = provider.clone();
        handles.push(tokio::spawn(async move {
            for round in 0..12usize {
                if let Some((_item, token, _attempt)) = p
                    .fetch_orchestration_item(Duration::from_secs(5), Duration::ZERO, None)
                    .await
                    .unwrap()
                {
                    let p_renew = p.clone();
                    let t_renew = token.clone();
                    let renew_task = tokio::spawn(async move {
                        let _ = p_renew
                            .renew_orchestration_item_lock(&t_renew, Duration::from_secs(5))
                            .await;
                    });

                    if (worker + round).is_multiple_of(2) {
                        let _ = p.abandon_orchestration_item(&token, None, true).await;
                    } else {
                        let _ = p
                            .ack_orchestration_item(
                                &token,
                                1,
                                vec![],
                                vec![],
                                vec![],
                                ExecutionMetadata::default(),
                                vec![],
                            )
                            .await;
                    }
                    let _ = renew_task.await;
                }
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    factory.finish().await;
}

#[tokio::test]
async fn pool_resolver_fails_over_after_old_pool_closes() {
    let Some(url) = test_database_url() else {
        return;
    };

    let pool_a = new_test_pool(&url).await;
    let pool_b = new_test_pool(&url).await;
    let rand_id = uuid::Uuid::new_v4().simple().to_string();
    let prefix = format!("pr_{}_", &rand_id[..12]);

    let active_pool = Arc::new(std::sync::Mutex::new(pool_a.clone()));
    let active_for_resolver = Arc::clone(&active_pool);
    let store: Arc<dyn Provider> = Arc::new(
        MysqlProvider::from_pool_fn(
            move || -> Result<duroxide_mysql_tidb::MySqlPool, &'static str> {
                Ok(active_for_resolver.lock().unwrap().clone())
            },
            &prefix,
            true,
        )
        .await
        .unwrap(),
    );

    let activities = ActivityRegistry::builder()
        .register("Echo", |_ctx: ActivityContext, input: String| async move {
            Ok(format!("echo:{input}"))
        })
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register(
            "EchoFlow",
            |ctx: OrchestrationContext, input: String| async move {
                ctx.schedule_activity("Echo", input).await
            },
        )
        .build();

    let rt = runtime::Runtime::start_with_options(
        store.clone(),
        activities,
        orchestrations,
        RuntimeOptions {
            dispatcher_min_poll_interval: Duration::from_millis(10),
            ..RuntimeOptions::default()
        },
    )
    .await;
    let client = Client::new(store);

    client
        .start_orchestration("failover-1", "EchoFlow", "before")
        .await
        .unwrap();
    match client
        .wait_for_orchestration("failover-1", Duration::from_secs(10))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "echo:before"),
        other => panic!("unexpected status before failover: {other:?}"),
    }

    // Rotate to pool_b and close pool_a to simulate an SRV target leaving rotation.
    *active_pool.lock().unwrap() = pool_b.clone();
    pool_a.close().await;

    client
        .start_orchestration("failover-2", "EchoFlow", "after")
        .await
        .unwrap();
    match client
        .wait_for_orchestration("failover-2", Duration::from_secs(10))
        .await
        .unwrap()
    {
        OrchestrationStatus::Completed { output, .. } => assert_eq!(output, "echo:after"),
        other => panic!("unexpected status after failover: {other:?}"),
    }

    rt.shutdown(None).await;

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
        let sql = format!("DROP TABLE IF EXISTS `{prefix}{suffix}`");
        let _ = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .execute(&pool_b)
            .await;
    }
    pool_b.close().await;
}
