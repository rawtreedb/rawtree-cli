use std::env;
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};

struct LiveCli {
    home: tempfile::TempDir,
    url: String,
    token: String,
    organization: String,
    cluster: String,
}

impl LiveCli {
    fn from_environment() -> Self {
        Self {
            home: tempfile::tempdir().expect("temporary home"),
            url: env::var("RAWTREE_LIVE_API_URL").expect("RAWTREE_LIVE_API_URL is required"),
            token: env::var("RAWTREE_LIVE_SESSION_TOKEN")
                .expect("RAWTREE_LIVE_SESSION_TOKEN is required"),
            organization: env::var("RAWTREE_LIVE_ORGANIZATION")
                .expect("RAWTREE_LIVE_ORGANIZATION is required"),
            cluster: env::var("RAWTREE_LIVE_CLUSTER").expect("RAWTREE_LIVE_CLUSTER is required"),
        }
    }

    fn execute(&self, args: &[&str], token: Option<&str>) -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_rtree"));
        for (name, _) in env::vars().filter(|(name, _)| name.starts_with("RAWTREE_")) {
            command.env_remove(name);
        }
        command
            .env("HOME", self.home.path())
            .env("RAWTREE_API_URL", &self.url)
            .env("RAWTREE_ORG", &self.organization)
            .env("RAWTREE_CLUSTER", &self.cluster)
            .arg("--json")
            .args(args);
        if let Some(token) = token {
            command.env("RAWTREE_API_KEY", token);
        }
        command.output().expect("run rtree")
    }

    fn json(&self, args: &[&str]) -> Value {
        self.json_with_token(args, Some(&self.token))
    }

    fn json_with_token(&self, args: &[&str], token: Option<&str>) -> Value {
        let output = self.execute(args, token);
        assert!(
            output.status.success(),
            "rtree failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout).expect("rtree JSON output")
    }
}

#[test]
#[ignore = "requires a bootstrapped Platform Docker Compose stack"]
fn cli_data_and_api_key_flow_through_real_platform() {
    let cli = LiveCli::from_environment();
    assert_eq!(cli.json(&["ping"])["status"], "ok");

    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos();
    let database = format!("cli_live_{suffix}");
    let table = "events";

    let created = cli.json(&["database", "create", &database]);
    assert_eq!(created["database"]["name"], database);

    let databases = cli.json(&["database", "list"]);
    assert!(databases["databases"]
        .as_array()
        .expect("databases array")
        .iter()
        .any(|entry| entry["name"] == database));

    let inserted = cli.json(&[
        "insert",
        "--database",
        &database,
        "--table",
        table,
        "--data",
        r#"{"event_id":"cli-live-1","value":42}"#,
    ]);
    assert_eq!(inserted["inserted"], 1);

    let tables = cli.json(&["table", "list", "--database", &database]);
    assert!(tables["tables"]
        .as_array()
        .expect("tables array")
        .iter()
        .any(|entry| entry["name"] == table));

    let queried = cli.json(&[
        "query",
        "--database",
        &database,
        "SELECT event_id, value FROM events WHERE event_id = 'cli-live-1'",
    ]);
    assert_eq!(
        queried["data"],
        json!([{"event_id": "cli-live-1", "value": 42}])
    );

    let key = cli.json(&[
        "key",
        "create",
        "--database",
        &database,
        "--name",
        "cli-live",
        "--permission",
        "read_only",
    ]);
    let key_token = key["token"].as_str().expect("new API key token");
    assert!(key_token.starts_with("rt_"));
    let key_id = key["id"].as_str().expect("new API key ID");

    let login = cli.json(&[
        "--api-key",
        key_token,
        "--org",
        &cli.organization,
        "--cluster",
        &cli.cluster,
        "login",
        "--database",
        &database,
    ]);
    assert_eq!(login["success"], true);
    assert_eq!(login["database"], database);
    // With no token in the environment, the CLI must use the key saved by login.
    let key_query = cli.json_with_token(
        &[
            "query",
            "--database",
            &database,
            "SELECT event_id, value FROM events WHERE event_id = 'cli-live-1'",
        ],
        None,
    );
    assert_eq!(key_query["data"], queried["data"]);
    assert_eq!(cli.json(&["key", "delete", key_id])["deleted"], true);

    let deleted = cli.json(&["database", "delete", &database]);
    assert_eq!(deleted, json!({"deleted": true, "name": database}));
}
