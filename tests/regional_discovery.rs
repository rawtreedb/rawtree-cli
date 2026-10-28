use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Command, Output};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone, Copy)]
enum Failure {
    Http(u16),
    Disconnect,
    Stall,
    SlowBody,
}

fn reply(socket: &mut TcpStream, status: u16, payload: Value) {
    let body = payload.to_string();
    write!(socket, "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
}

// Exercise the actual CLI, using routing headers to distinguish deployment catalogs.
fn run(args: &[&str], failure: Failure, all_fail: bool) -> (Output, Vec<String>, Duration) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let recorded = requests.clone();
    let (stop, stopped) = std::sync::mpsc::channel();
    let server = thread::spawn(move || {
        let mut workers = Vec::new();
        while stopped.try_recv().is_err() {
            let (mut socket, _) = match listener.accept() {
                Ok(connection) => connection,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(2));
                    continue;
                }
                Err(error) => panic!("accept: {error}"),
            };
            let requests = recorded.clone();
            workers.push(thread::spawn(move || {
                socket.set_nonblocking(false).unwrap();
                socket.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
                let mut reader = BufReader::new(socket.try_clone().unwrap());
                let mut first = String::new();
                reader.read_line(&mut first).unwrap();
                let mut headers = HashMap::new();
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" { break; }
                    let (name, value) = line.split_once(':').unwrap();
                    headers.insert(name.to_ascii_lowercase(), value.trim().to_owned());
                }
                assert_eq!(headers["authorization"], "Bearer session.fixture");
                requests.lock().unwrap().push(first.clone());
                if first.starts_with("GET /v1/organizations ") {
                    reply(&mut socket, 200, json!({"organizations":[{"organization_id":"org-id","name":"acme"}]}));
                } else if first.starts_with("GET /v1/organizations/org-id/regions ") {
                    reply(&mut socket, 200, json!({"regions":[
                        {"id":"healthy","name":"Virginia","cloud_region":"us-east-1","deployment_id":"11111111","enabled":true},
                        {"id":"failed","name":"Oregon","cloud_region":"us-west-2","deployment_id":"22222222","enabled":true},
                        {"id":"duplicate","name":"Oregon duplicate","cloud_region":"us-west-2","deployment_id":"22222222","enabled":true},
                        {"id":"failed2","name":"Ireland","cloud_region":"eu-west-1","deployment_id":"33333333","enabled":true}
                    ]}));
                } else if first.starts_with("GET /v1/clusters?") {
                    if !all_fail && headers["x-rawtree-deployment-id"] == "11111111" {
                        reply(&mut socket, 200, json!({"clusters":[{
                            "id":"healthy-id", "name":"production", "region_id":"healthy", "created_at":"2026-10-28T00:00:00Z", "shared":false,
                            "status":{"phase":"running", "ready":true}, "idle_timeout_minutes":30, "can_pause":true, "can_resume":false
                        }]}));
                    } else {
                        match failure {
                            Failure::Http(status) => reply(&mut socket, status, json!({"message":"Regional failure"})),
                            Failure::Disconnect => (),
                            Failure::Stall => thread::sleep(Duration::from_secs(6)),
                            Failure::SlowBody => {
                                write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 200\r\n\r\n{{").unwrap();
                                socket.flush().unwrap();
                                thread::sleep(Duration::from_secs(6));
                            }
                        }
                    }
                } else {
                    assert!(first.starts_with("GET /v1/tables?"), "{first}");
                    assert_eq!(headers["x-rawtree-deployment-id"], "11111111");
                    assert_eq!(headers["x-rawtree-region"], "us-east-1");
                    reply(&mut socket, 200, json!({"tables":[{"name":"events","total_rows":12,"total_bytes":34}]}));
                }
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
    });
    let started = Instant::now();
    let output = Command::new(env!("CARGO_BIN_EXE_rtree"))
        .env_remove("RAWTREE_API_KEY")
        .env_remove("RAWTREE_API_URL")
        .env("RAWTREE_DATABASE", "default")
        .env("RAWTREE_CLUSTER", "production")
        .args([
            "--api-url",
            &url,
            "--api-key",
            "session.fixture",
            "--org",
            "acme",
        ])
        .args(args)
        .output()
        .unwrap();
    let elapsed = started.elapsed();
    stop.send(()).unwrap();
    server.join().unwrap();
    let requests = requests.lock().unwrap().clone();
    (output, requests, elapsed)
}

#[test]
fn table_list_survives_http_and_connection_failures_in_other_regions() {
    for failure in [Failure::Http(503), Failure::Disconnect] {
        let (output, requests, _) = run(&["--json", "table", "list"], failure, false);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&output.stdout).unwrap()["tables"][0]["name"],
            "events"
        );
        assert_eq!(
            requests
                .iter()
                .filter(|path| path.starts_with("GET /v1/clusters?"))
                .count(),
            3
        );
        assert_eq!(requests.len(), 6);
    }
}

#[test]
fn stalled_regions_and_bodies_share_one_five_second_discovery_window() {
    for failure in [Failure::Stall, Failure::SlowBody] {
        let (output, requests, elapsed) = run(&["--json", "table", "list"], failure, false);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(elapsed >= Duration::from_secs(4), "{elapsed:?}");
        assert!(
            elapsed < Duration::from_secs(7),
            "regional waits must overlap: {elapsed:?}"
        );
        assert_eq!(requests.len(), 6);
    }
}

#[test]
fn cluster_list_exposes_partial_catalog_in_json_and_warns_in_text() {
    for json_mode in [true, false] {
        let args = if json_mode {
            vec!["--json", "cluster", "list"]
        } else {
            vec!["cluster", "list"]
        };
        let (output, _, _) = run(&args, Failure::Http(503), false);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        if json_mode {
            let result: Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(result["clusters"].as_array().unwrap().len(), 1);
            assert_eq!(result["unavailable_regions"].as_array().unwrap().len(), 3);
            assert!(output.stderr.is_empty());
        } else {
            assert!(String::from_utf8_lossy(&output.stderr)
                .contains("Oregon, Oregon duplicate, Ireland"));
        }
    }
}

#[test]
fn authorization_and_total_outages_fail_without_executing_table_request() {
    for (status, all_fail) in [(401, false), (403, false), (503, true)] {
        let (output, requests, _) = run(
            &["--json", "table", "list"],
            Failure::Http(status),
            all_fail,
        );
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        let error: Value = serde_json::from_slice(&output.stderr).unwrap();
        assert!(error["error"]["message"]
            .as_str()
            .unwrap()
            .contains("Regional failure"));
        assert!(!requests
            .iter()
            .any(|path| path.starts_with("GET /v1/tables?")));
    }
}

#[test]
fn missing_cluster_in_partial_catalog_never_falls_back_to_gateway() {
    let (output, requests, _) = run(
        &["--json", "--cluster", "missing", "table", "list"],
        Failure::Http(503),
        false,
    );
    assert!(!output.status.success());
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert!(error["error"]["message"]
        .as_str()
        .unwrap()
        .contains("Some regions are unavailable"));
    assert!(!requests
        .iter()
        .any(|path| path.starts_with("GET /v1/tables?")));
}

#[test]
fn management_commands_report_outages_for_missing_targets_in_partial_catalogs() {
    for target in ["missing-cluster", "44444444-4444-4444-8444-444444444444"] {
        for command in ["status", "update", "stop", "resume", "delete"] {
            let mut args = vec!["--json", "cluster", command, target];
            if command == "update" {
                args.extend(["--idle-timeout-minutes", "60"]);
            }
            let (output, requests, _) = run(&args, Failure::Http(503), false);
            assert!(!output.status.success(), "{command} {target}");
            assert!(output.stdout.is_empty());
            let error: Value = serde_json::from_slice(&output.stderr).unwrap();
            assert!(
                error["error"]["message"]
                    .as_str()
                    .unwrap()
                    .contains("Some regions are unavailable"),
                "{command} {target}: {error}"
            );
            assert_ne!(error["error"]["code"], "cluster_not_found");
            assert_eq!(requests.len(), 5, "only discovery should run: {requests:?}");
            assert!(requests.iter().all(|request| request.starts_with("GET ")));
        }
    }
}
