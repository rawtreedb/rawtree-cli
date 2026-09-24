use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::process::{Command, Output};
use std::time::{Duration, Instant};

fn login(responses: &[(&str, &str, Value)], selectors: &[&str]) -> (Output, Value, Value) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let responses = responses
        .iter()
        .map(|(path, status, body)| (path.to_string(), status.to_string(), body.to_string()))
        .collect::<Vec<_>>();
    let server = std::thread::spawn(move || {
        for (path, status, body) in responses {
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut socket = loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(Instant::now() < deadline, "missing request to {path}");
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("accept failed: {error}"),
                }
            };
            socket
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            let mut reader = BufReader::new(socket.try_clone().unwrap());
            let mut request = String::new();
            reader.read_line(&mut request).unwrap();
            assert_eq!(request.trim(), format!("GET {path} HTTP/1.1"));
            loop {
                let mut line = String::new();
                assert!(reader.read_line(&mut line).unwrap() > 0);
                if line == "\r\n" {
                    break;
                }
            }
            write!(socket, "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
        }
    });

    let home = tempfile::tempdir().unwrap();
    let config_path = home.path().join(".config/rtree/config.json");
    std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
    let original = json!({
        "token": "old-test-credential", "email": "old@example.test",
        "default_organization": "old-org", "cluster": "old-cluster", "database": "old-db"
    });
    std::fs::write(&config_path, original.to_string()).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_rtree"));
    for (name, _) in std::env::vars().filter(|(name, _)| name.starts_with("RAWTREE_")) {
        command.env_remove(name);
    }
    let output = command
        .env("HOME", home.path())
        .args([
            "--api-url",
            &url,
            "--api-key",
            "rt_login_fixture",
            "--json",
            "login",
        ])
        .args(selectors)
        .output()
        .unwrap();
    server.join().unwrap();
    let saved = serde_json::from_slice(&std::fs::read(config_path).unwrap()).unwrap();
    (output, saved, original)
}

#[test]
fn api_key_login_accepts_current_responses_and_replaces_stale_defaults() {
    for fallback in [false, true] {
        for explicit in [false, true] {
            let query = if explicit {
                "?database=analytics&organization=team%20alpha&cluster=prod%2Feu"
            } else {
                ""
            };
            let keys = format!("/v1/keys{query}");
            let tables = format!("/v1/tables{query}");
            let responses = if fallback {
                vec![
                    (
                        keys.as_str(),
                        "403 Forbidden",
                        json!({"message": "Admin required"}),
                    ),
                    (tables.as_str(), "200 OK", json!({"tables": []})),
                ]
            } else {
                vec![(keys.as_str(), "200 OK", json!({"keys": []}))]
            };
            let selectors = if explicit {
                vec![
                    "--org",
                    "team alpha",
                    "--cluster",
                    "prod/eu",
                    "--database",
                    "analytics",
                ]
            } else {
                vec![]
            };
            let (output, saved, _) = login(&responses, &selectors);
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let result: Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(result["success"], true);
            assert_eq!(saved["token"], "rt_login_fixture");
            assert_eq!(saved["email"], Value::Null);
            for (field, config_field, selected) in [
                ("organization", "default_organization", "team alpha"),
                ("cluster", "cluster", "prod/eu"),
                ("database", "database", "analytics"),
            ] {
                let expected = if explicit {
                    json!(selected)
                } else {
                    Value::Null
                };
                assert_eq!(result[field], expected);
                assert_eq!(saved[config_field], expected);
            }
        }
    }
}

#[test]
fn api_key_login_failures_preserve_saved_credentials() {
    for (status, body) in [
        ("401 Unauthorized", json!({"message": "Invalid API key"})),
        (
            "403 Forbidden",
            json!({"message": "Key is not scoped to the requested cluster"}),
        ),
        ("200 OK", json!({})),
    ] {
        let (output, saved, original) = login(
            &[
                ("/v1/keys?cluster=other", status, body.clone()),
                ("/v1/tables?cluster=other", status, body),
            ],
            &["--cluster", "other"],
        );
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("validation_failed"));
        assert_eq!(saved, original);
    }
}

#[test]
fn api_key_login_preserves_legacy_response_metadata() {
    let (output, saved, _) = login(
        &[(
            "/v1/keys",
            "200 OK",
            json!({
                "keys": [], "organization": {"name": "team"}, "database": {"name": "analytics"}
            }),
        )],
        &[],
    );
    assert!(output.status.success());
    assert_eq!(saved["default_organization"], "team");
    assert_eq!(saved["database"], "analytics");
}
