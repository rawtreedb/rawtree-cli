use super::LiveCli;
use serde_json::{json, Value};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn wait_for(
    description: &str,
    mut read: impl FnMut() -> Value,
    ready: impl Fn(&Value) -> bool,
) -> Value {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let value = read();
        if ready(&value) {
            return value;
        }
        assert!(
            Instant::now() < deadline,
            "Timed out waiting for {description}: {value}"
        );
        std::thread::sleep(Duration::from_millis(250));
    }
}

fn with_workflow(sql: &str, test: impl FnOnce(&LiveCli, &str, &str)) {
    let cli = LiveCli::from_environment();
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let database = format!("cli_workflow_{suffix}");
    cli.json(&["database", "create", &database]);
    let mut workflow_id = None;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let created = cli.json(&[
            "workflow",
            "create",
            "--name",
            &database,
            "--database",
            &database,
            "--sql",
            sql,
        ]);
        let id = created["id"].as_str().expect("workflow ID").to_string();
        workflow_id = Some(id.clone());
        assert_eq!(created["interval_seconds"], Value::Null);
        test(&cli, &database, &id);
    }));
    if let Some(id) = workflow_id {
        assert_eq!(cli.json(&["workflow", "delete", &id])["deleted"], true);
    }
    assert_eq!(
        cli.json(&["database", "delete", &database])["deleted"],
        true
    );
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

fn wait_for_run(cli: &LiveCli, id: &str, run_id: &str, status: &str) -> Value {
    wait_for(
        status,
        || cli.json(&["workflow", "runs", id]),
        |page| {
            page["runs"]
                .as_array()
                .unwrap()
                .iter()
                .any(|run| run["id"] == run_id && run["status"] == status)
        },
    )
}

#[test]
#[ignore = "requires a bootstrapped Platform Docker Compose stack"]
fn workflow_manual_runs_deliver_rows_and_expose_logs_and_metrics() {
    with_workflow("SELECT 42 AS value", |cli, database, id| {
        cli.json(&["table", "create", "results", "--database", database]);
        let sink = json!({"type": "table", "settings": {"database": database, "table": "results"}})
            .to_string();
        let definition = cli.json(&["workflow", "update", id, "--disable", "--sink", &sink]);
        assert_eq!(definition["sinks"].as_array().unwrap().len(), 1);
        let run = cli.json(&["workflow", "run", id, "--idempotency-key", "cli-e2e-run"]);
        assert_eq!(run["workflow_id"], id);
        assert_eq!(run["source"], "manual");
        assert_eq!(
            cli.json(&["workflow", "run", id, "--idempotency-key", "cli-e2e-run"]),
            run
        );
        let run_id = run["id"].as_str().unwrap();
        wait_for_run(cli, id, run_id, "completed");
        assert_eq!(
            cli.json(&["workflow", "run", id, "--idempotency-key", "cli-e2e-run"]),
            run
        );
        wait_for(
            "sink delivery",
            || {
                cli.json(&[
                    "query",
                    "--database",
                    database,
                    "SELECT 1 AS delivered FROM results",
                ])
            },
            |result| result["data"] == json!([{"delivered": 1}]),
        );
        let rows = cli.json(&["query", "--database", database, "SELECT value FROM results"]);
        assert_eq!(rows["data"], json!([{"value": 42}]));
        let logs = wait_for(
            "workflow logs",
            || cli.json(&["workflow", "logs", id, "--since", "1h"]),
            |result| {
                result["logs"].as_array().unwrap().iter().any(|log| {
                    log["event_id"] == run_id
                        && log["event_type"] == "workflow.evaluated"
                        && log["outcome"] == "success"
                })
            },
        );
        assert!(logs["from"].as_i64().unwrap() < logs["to"].as_i64().unwrap());
        wait_for(
            "workflow metrics",
            || cli.json(&["workflow", "metrics", id, "--since", "1h"]),
            |result| {
                result["totals"]["executions"] == 1
                    && result["totals"]["matches"] == 1
                    && result["totals"]["sink_events"] == 1
            },
        );
        assert_eq!(cli.json(&["workflow", "get", id]), definition);
        let cleared = cli.json(&["workflow", "update", id, "--clear-sinks"]);
        assert_eq!(cleared["sinks"], json!([]));
    });
}

#[test]
#[ignore = "requires a bootstrapped Platform Docker Compose stack"]
fn workflow_scheduled_runs_can_be_paused_and_switched_to_manual() {
    with_workflow("SELECT 1", |cli, _, id| {
        let scheduled = cli.json(&["workflow", "update", id, "--interval-seconds", "1"]);
        assert_eq!(scheduled["interval_seconds"], 1);
        assert_eq!(scheduled["enabled"], true);
        wait_for(
            "scheduled execution",
            || cli.json(&["workflow", "runs", id]),
            |page| {
                page["runs"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|run| run["source"] == "scheduled" && run["status"] == "completed")
            },
        );
        let paused = cli.json(&["workflow", "update", id, "--disable"]);
        assert_eq!(paused["enabled"], false);
        assert_eq!(paused["interval_seconds"], 1);
        assert_eq!(paused["next_run_at"], Value::Null);
        let manual = cli.json(&["workflow", "update", id, "--interval-seconds", "null"]);
        assert_eq!(manual["interval_seconds"], Value::Null);
        assert_eq!(manual["next_run_at"], Value::Null);
        let run = cli.json(&["workflow", "run", id]);
        assert_eq!(run["source"], "manual");
        wait_for_run(cli, id, run["id"].as_str().unwrap(), "completed");
        let resumed = cli.json(&[
            "workflow",
            "update",
            id,
            "--interval-seconds",
            "3600",
            "--enable",
        ]);
        assert_eq!(resumed["enabled"], true);
        assert_eq!(resumed["interval_seconds"], 3600);
        assert!(resumed["next_run_at"].is_string());
    });
}

#[test]
#[ignore = "requires a bootstrapped Platform Docker Compose stack"]
fn workflow_cancel_stops_a_run_and_preserves_the_definition() {
    with_workflow("SELECT sleepEachRow(0.1) AS value FROM numbers(1000) SETTINGS max_execution_time = 0, max_block_size = 1, max_threads = 1", |cli, _, id| {
        let definition = cli.json(&["workflow", "get", id]);
        let run = cli.json(&["workflow", "run", id]);
        let run_id = run["id"].as_str().unwrap();
        wait_for_run(cli, id, run_id, "running");
        assert_eq!(cli.json(&["workflow", "cancel", id, run_id]), json!({"workflow_id": id, "run_id": run_id, "cancel_requested": true}));
        wait_for_run(cli, id, run_id, "canceled");
        assert_eq!(cli.json(&["workflow", "cancel", id, run_id])["cancel_requested"], true);
        assert_eq!(cli.json(&["workflow", "get", id]), definition);
        cli.json(&["workflow", "update", id, "--sql", "SELECT 1"]);
        let next = cli.json(&["workflow", "run", id]);
        wait_for_run(cli, id, next["id"].as_str().unwrap(), "completed");
    });
}

#[test]
#[ignore = "requires a bootstrapped Platform Docker Compose stack"]
fn workflow_query_and_schedule_round_trip_through_real_platform() {
    let cli = LiveCli::from_environment();
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos();
    let name = format!("cli-workflow-{suffix}");
    let created = cli.json(&[
        "workflow",
        "create",
        "--name",
        &name,
        "--database",
        "default",
        "--sql",
        "SELECT 1",
        "--disabled",
    ]);
    let id = created["id"].as_str().expect("workflow ID");
    let result = std::panic::catch_unwind(|| {
        assert_eq!(
            created["query"],
            json!({"database": "default", "sql": "SELECT 1"})
        );
        assert_eq!(created["interval_seconds"], Value::Null);
        assert_eq!(created["next_run_at"], Value::Null);
        assert_eq!(created["enabled"], false);
        let listed = cli.json(&["workflow", "list"]);
        assert!(listed["workflows"]
            .as_array()
            .unwrap()
            .iter()
            .any(|workflow| workflow == &created));
        assert_eq!(cli.json(&["workflow", "get", id]), created);

        let updated = cli.json(&["workflow", "update", id, "--sql", "SELECT 2"]);
        assert_eq!(
            updated["query"],
            json!({"database": "default", "sql": "SELECT 2"})
        );
        assert_eq!(updated["interval_seconds"], Value::Null);
        let updated = cli.json(&["workflow", "update", id, "--database", "default"]);
        assert_eq!(
            updated["query"],
            json!({"database": "default", "sql": "SELECT 2"})
        );

        let scheduled = cli.json(&["workflow", "update", id, "--interval-seconds", "300"]);
        assert_eq!(scheduled["interval_seconds"], 300);
        assert_eq!(scheduled["enabled"], false);
        let renamed = cli.json(&[
            "workflow",
            "update",
            id,
            "--name",
            &format!("{name}-renamed"),
        ]);
        assert_eq!(renamed["interval_seconds"], 300);
        assert_eq!(renamed["query"], updated["query"]);
        let manual = cli.json(&["workflow", "update", id, "--interval-seconds", "null"]);
        assert_eq!(manual["interval_seconds"], Value::Null);
        assert_eq!(manual["next_run_at"], Value::Null);
        assert_eq!(manual["query"], updated["query"]);
        assert_eq!(cli.json(&["workflow", "get", id]), manual);
    });
    let deleted = cli.json(&["workflow", "delete", id]);
    assert_eq!(deleted["deleted"], true);
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}
