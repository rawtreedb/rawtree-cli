use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn request(listener: &TcpListener) -> (TcpStream, String, String) {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut stream = loop {
        match listener.accept() {
            Ok((stream, _)) => break stream,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < deadline, "expected CLI request");
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("{error}"),
        }
    };
    stream.set_nonblocking(false).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut reader = BufReader::new(&mut stream);
    let mut first = String::new();
    reader.read_line(&mut first).unwrap();
    let mut headers = String::new();
    let mut length = 0;
    loop {
        let mut line = String::new();
        assert!(reader.read_line(&mut line).unwrap() > 0);
        if line == "\r\n" {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                length = value.trim().parse().unwrap();
            }
        }
        headers.push_str(&line);
    }
    reader.read_exact(&mut vec![0; length]).unwrap();
    (stream, first, headers)
}

fn reply(mut stream: TcpStream, value: Value) {
    let body = value.to_string();
    write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
}

fn command(home: &std::path::Path, url: &str, args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rtree"));
    for (name, _) in std::env::vars().filter(|(name, _)| name.starts_with("RAWTREE_")) {
        command.env_remove(name);
    }
    command
        .env("HOME", home)
        .args(["--api-url", url, "--json"])
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

#[test]
fn concurrent_processes_rotate_once_and_streaming_senders_refresh_later_batches() {
    for streaming in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let home = tempfile::tempdir().unwrap();
        let path = home.path().join(".config/rtree/config.json");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let original = json!({"token":"old-access", "url":url, "oauth":{
            "api_url":url, "resource":url, "client_id":"installation", "refresh_token":"old-refresh",
            "token_endpoint":format!("{url}/oauth/token"), "revocation_endpoint":format!("{url}/oauth/revoke"),
            "expires_at":if streaming {i64::MAX} else {0}
        }});
        std::fs::write(&path, original.to_string()).unwrap();
        let saved_path = path.clone();
        let server = std::thread::spawn(move || {
            if streaming {
                // The main client starts with a valid token. Expire it while it
                // discovers defaults, before any sender thread sends a batch.
                let (socket, first, headers) = request(&listener);
                assert!(first.starts_with("GET /v1/organizations "));
                assert!(headers.contains("Bearer old-access"));
                let mut cfg: Value =
                    serde_json::from_slice(&std::fs::read(&saved_path).unwrap()).unwrap();
                cfg["oauth"]["expires_at"] = json!(0);
                std::fs::write(&saved_path, cfg.to_string()).unwrap();
                reply(socket, json!({"organizations":[]}));
            }
            let (socket, first, _) = request(&listener);
            assert!(first.starts_with("POST /oauth/token "));
            // Give other CLI processes/senders time to contend for the lock.
            std::thread::sleep(Duration::from_millis(100));
            reply(
                socket,
                json!({"access_token":"new-access", "refresh_token":"new-refresh", "token_type":"Bearer", "expires_in":900}),
            );
            let count = if streaming { 2 } else { 4 };
            for _ in 0..count {
                let (socket, first, headers) = request(&listener);
                assert!(headers.contains("Bearer new-access"));
                if streaming {
                    assert!(first.starts_with("POST /v1/tables/events?database=analytics "));
                    reply(socket, json!({"inserted":5000}));
                } else {
                    assert!(
                        first.starts_with("GET /v1/organizations "),
                        "refresh was replayed"
                    );
                    reply(socket, json!({"organizations":[]}));
                }
            }
        });
        let mut children = Vec::new();
        if streaming {
            let data = home.path().join("events.jsonl");
            std::fs::write(&data, "{\"value\":1}\n".repeat(10000)).unwrap();
            children.push(
                command(
                    home.path(),
                    &url,
                    &[
                        "insert",
                        "--database",
                        "analytics",
                        "--table",
                        "events",
                        "--file",
                        data.to_str().unwrap(),
                    ],
                )
                .spawn()
                .unwrap(),
            );
        } else {
            for _ in 0..4 {
                children.push(
                    command(home.path(), &url, &["organization", "list"])
                        .spawn()
                        .unwrap(),
                );
            }
        }
        for child in children {
            let output = child.wait_with_output().unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        server.join().unwrap();
        let saved: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(saved["oauth"]["refresh_token"], "new-refresh");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }
}

#[test]
fn key_precedence_and_device_override_apply_to_real_requests() {
    for (args, environment, expected, device) in [
        (
            vec!["organization", "list"],
            Some("rt_env"),
            Some("rt_env"),
            false,
        ),
        (
            vec!["organization", "list", "--api-key", "rt_cli"],
            Some("rt_env"),
            Some("rt_cli"),
            false,
        ),
        (vec!["organization", "list"], None, Some("rt_saved"), false),
        (vec!["login"], Some("rt_env"), Some("rt_env"), false),
        (
            vec!["login", "--api-key", "rt_cli"],
            Some("rt_env"),
            Some("rt_cli"),
            false,
        ),
        (vec!["login", "--device-auth"], Some("rt_env"), None, true),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let home = tempfile::tempdir().unwrap();
        let path = home.path().join(".config/rtree/config.json");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, json!({"token":"rt_saved"}).to_string()).unwrap();
        let login = args[0] == "login";
        let server = std::thread::spawn(move || {
            let (socket, first, headers) = request(&listener);
            match expected {
                Some(token) => assert!(headers.contains(&format!("Bearer {token}"))),
                None => assert!(!headers.to_ascii_lowercase().contains("authorization:")),
            }
            if device {
                assert!(first.starts_with("POST /v1/auth/cli/device/start "));
                reply(socket, json!({})); // Stop at malformed response; no browser or poll.
            } else if login {
                assert!(first.starts_with("GET /v1/keys "));
                reply(socket, json!({"keys":[]}));
            } else {
                reply(socket, json!({"organizations":[]}));
            }
        });
        let mut cli = command(home.path(), &url, &args);
        if let Some(value) = environment {
            cli.env("RAWTREE_API_KEY", value);
        }
        let output = cli.output().unwrap();
        assert_eq!(output.status.success(), !device);
        server.join().unwrap();
    }
}

#[test]
fn running_command_refuses_a_changed_or_removed_oauth_identity() {
    for removed in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let home = tempfile::tempdir().unwrap();
        let path = home.path().join(".config/rtree/config.json");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let original = json!({"token":"old-access", "url":url, "oauth":{
            "api_url":url, "resource":url, "client_id":"installation", "refresh_token":"old-refresh",
            "token_endpoint":format!("{url}/oauth/token"), "revocation_endpoint":format!("{url}/oauth/revoke"),
            "expires_at":i64::MAX
        }});
        std::fs::write(&path, original.to_string()).unwrap();
        let server = std::thread::spawn(move || {
            let (socket, first, _) = request(&listener);
            assert!(first.starts_with("GET /v1/organizations "));
            let mut cfg = original;
            if removed {
                cfg["oauth"] = Value::Null;
            } else {
                cfg["oauth"]["client_id"] = json!("replacement");
            }
            std::fs::write(&path, cfg.to_string()).unwrap();
            reply(socket, json!({"organizations":[]}));
        });
        let output = command(
            home.path(),
            &url,
            &["query", "--database", "analytics", "SELECT 1"],
        )
        .output()
        .unwrap();
        server.join().unwrap();
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("login changed"));
    }
}

#[test]
fn deleting_an_organization_does_not_overwrite_concurrently_selected_defaults() {
    for changed in [false, true] {
        for next in [None, Some("next")] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let home = tempfile::tempdir().unwrap();
            let path = home.path().join(".config/rtree/config.json");
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(
                &path,
                json!({"token":"rt_saved", "default_organization":"old"}).to_string(),
            )
            .unwrap();
            let server_path = path.clone();
            let server = std::thread::spawn(move || {
                let (socket, first, _) = request(&listener);
                assert!(first.starts_with("DELETE /v1/organizations/old "));
                reply(socket, json!({"deleted":true}));
                let (socket, first, _) = request(&listener);
                assert!(first.starts_with("GET /v1/organizations "));
                if changed {
                    std::fs::write(
                        server_path,
                        json!({"token":"rt_saved", "default_organization":"concurrent"})
                            .to_string(),
                    )
                    .unwrap();
                }
                let organizations: Vec<_> = next
                    .into_iter()
                    .map(|name| json!({"name":name,"role":"owner"}))
                    .collect();
                reply(socket, json!({"organizations":organizations}));
            });
            let output = command(home.path(), &url, &["organization", "delete", "old"])
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            server.join().unwrap();
            let saved: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
            assert_eq!(
                saved["default_organization"],
                json!(if changed { Some("concurrent") } else { next })
            );
        }
    }
}
