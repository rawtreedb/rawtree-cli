use std::path::Path;
use std::process::{Command, Output};

use serde_json::{json, Value};

fn run(home: &Path, args: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rtree"));
    for (name, _) in std::env::vars().filter(|(name, _)| name.starts_with("RAWTREE_")) {
        command.env_remove(name);
    }
    command
        .env("HOME", home)
        // Local logout must succeed even when the API cannot be reached.
        .args(["--api-url", "http://127.0.0.1:1"])
        .args(args)
        .output()
        .expect("run rtree")
}

fn saved_config(home: &Path) -> Value {
    let bytes = std::fs::read(home.join(".config/rtree/config.json")).expect("read saved config");
    serde_json::from_slice(&bytes).expect("parse saved config")
}

fn assert_config_cleared(home: &Path) {
    assert_eq!(
        saved_config(home),
        json!({
            "token": null,
            "email": null,
            "url": null,
            "database": null,
            "default_organization": null,
            "cluster": null
        })
    );
}

#[test]
fn logout_clears_saved_credentials_and_defaults_in_json_and_human_modes() {
    for args in [&["--json", "logout"][..], &["logout"][..]] {
        let home = tempfile::tempdir().expect("temporary home");
        let config_path = home.path().join(".config/rtree/config.json");
        std::fs::create_dir_all(config_path.parent().expect("config parent"))
            .expect("create config directory");
        std::fs::write(
            &config_path,
            json!({
                "token": "rt_logout_fixture",
                "email": "logout@example.test",
                "url": "http://127.0.0.1:1",
                "database": "analytics",
                "default_organization": "team",
                "cluster": "production"
            })
            .to_string(),
        )
        .expect("seed config");

        let output = run(home.path(), args);
        assert!(output.status.success());
        assert!(output.stderr.is_empty());
        if args.contains(&"--json") {
            let result: Value = serde_json::from_slice(&output.stdout).expect("logout JSON");
            assert_eq!(result, json!({"status": "logged_out"}));
        } else {
            assert_eq!(
                String::from_utf8(output.stdout).expect("logout text"),
                "Logged out. Local config reset to defaults.\n"
            );
        }
        assert_config_cleared(home.path());

        let status = run(home.path(), &["--json", "status"]);
        assert!(status.status.success());
        let status: Value = serde_json::from_slice(&status.stdout).expect("status JSON");
        assert_eq!(status["authenticated"], false);
        for field in ["user", "database", "organization", "cluster"] {
            assert_eq!(status[field], Value::Null, "status retained {field}");
        }
    }
}

#[test]
fn logout_succeeds_without_config_and_is_repeatable() {
    let home = tempfile::tempdir().expect("temporary home");
    assert!(!home.path().join(".config/rtree/config.json").exists());

    for _ in 0..2 {
        let output = run(home.path(), &["--json", "logout"]);
        assert!(output.status.success());
        assert!(output.stderr.is_empty());
        let result: Value = serde_json::from_slice(&output.stdout).expect("logout JSON");
        assert_eq!(result, json!({"status": "logged_out"}));
        assert_config_cleared(home.path());
    }
}

#[test]
fn logout_reports_config_write_failure_without_claiming_success() {
    let home = tempfile::tempdir().expect("temporary home");
    std::fs::create_dir(home.path().join(".config")).expect("create config parent");
    // A file where the config directory belongs blocks writes on every OS,
    // including when the tests run as root.
    let blocker = home.path().join(".config/rtree");
    std::fs::write(&blocker, "keep this file").expect("block config directory");

    let output = run(home.path(), &["--json", "logout"]);
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let error: Value = serde_json::from_slice(&output.stderr).expect("error JSON");
    assert!(error["error"]["message"]
        .as_str()
        .expect("error message")
        .contains("failed to create config directory"));
    assert_eq!(std::fs::read_to_string(blocker).unwrap(), "keep this file");
}

#[test]
fn logout_reports_invalid_config_without_overwriting_it() {
    let home = tempfile::tempdir().expect("temporary home");
    let config_path = home.path().join(".config/rtree/config.json");
    std::fs::create_dir_all(config_path.parent().expect("config parent"))
        .expect("create config directory");
    std::fs::write(&config_path, "{invalid JSON").expect("seed invalid config");

    let output = run(home.path(), &["--json", "logout"]);
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let error: Value = serde_json::from_slice(&output.stderr).expect("error JSON");
    assert!(error["error"]["message"]
        .as_str()
        .expect("error message")
        .contains("invalid config JSON"));
    assert_eq!(
        std::fs::read_to_string(config_path).unwrap(),
        "{invalid JSON"
    );
}
