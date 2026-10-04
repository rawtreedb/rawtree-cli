use serde_json::Value;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

pub fn run_cli(
    responses: &[(String, &str, Value)],
    args: &[&str],
    original: &Value,
) -> (Output, Value, Vec<Value>) {
    run_cli_with_progress(responses, args, original, None)
}

pub fn run_cli_with_progress(
    responses: &[(String, &str, Value)],
    args: &[&str],
    original: &Value,
    on_progress: Option<fn(&str)>,
) -> (Output, Value, Vec<Value>) {
    let (mut outputs, mut configs, bodies) = if on_progress.is_none() {
        run_cli_sequence(responses, &[args], original)
    } else {
        run_sequence_with_progress(responses, &[args], original, on_progress)
    };
    (outputs.remove(0), configs.remove(0), bodies)
}

pub fn run_cli_sequence(
    responses: &[(String, &str, Value)],
    commands: &[&[&str]],
    original: &Value,
) -> (Vec<Output>, Vec<Value>, Vec<Value>) {
    run_sequence_with_progress(responses, commands, original, None)
}

fn run_sequence_with_progress(
    responses: &[(String, &str, Value)],
    commands: &[&[&str]],
    original: &Value,
    on_progress: Option<fn(&str)>,
) -> (Vec<Output>, Vec<Value>, Vec<Value>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let responses = responses
        .iter()
        .map(|(path, status, body)| (path.to_string(), status.to_string(), body.to_string()))
        .collect::<Vec<_>>();
    let (progress_tx, progress_rx) = std::sync::mpsc::channel();
    let check_auth = commands.len() > 1;
    let mut expected_token = original["token"].as_str().map(str::to_string);
    let server = std::thread::spawn(move || {
        let mut bodies = Vec::new();
        for (index, (path, status, body)) in responses.into_iter().enumerate() {
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
            socket.set_nonblocking(false).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            let mut reader = BufReader::new(socket.try_clone().unwrap());
            let mut request = String::new();
            reader.read_line(&mut request).unwrap();
            assert_eq!(request.trim(), format!("{path} HTTP/1.1"));
            let mut content_length = 0;
            let mut authorization = None;
            loop {
                let mut line = String::new();
                assert!(reader.read_line(&mut line).unwrap() > 0);
                if let Some((name, value)) = line.split_once(':') {
                    if name.eq_ignore_ascii_case("authorization") {
                        authorization = Some(value.trim().to_string());
                    }
                    if name.eq_ignore_ascii_case("content-length") {
                        content_length = value.trim().parse::<usize>().unwrap();
                    }
                }
                if line == "\r\n" {
                    break;
                }
            }
            if check_auth && !path.contains("/v1/auth/") {
                assert_eq!(
                    authorization,
                    expected_token
                        .as_ref()
                        .map(|token| format!("Bearer {token}"))
                );
            }
            if path == "POST /v1/auth/cli/device/token" && status == "200 OK" {
                let response: Value = serde_json::from_str(&body).unwrap();
                expected_token = response["token"].as_str().map(str::to_string);
            }
            let mut body_bytes = vec![0; content_length];
            reader.read_exact(&mut body_bytes).unwrap();
            bodies.push(if body_bytes.is_empty() {
                Value::Null
            } else {
                serde_json::from_slice(&body_bytes).unwrap()
            });
            if index == 1 && on_progress.is_some() {
                // Withhold the second response until the CLI has reported progress.
                progress_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            }
            write!(socket, "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
        }
        bodies
    });

    let home = tempfile::tempdir().unwrap();
    let config_path = home.path().join(".config/rtree/config.json");
    std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
    std::fs::write(&config_path, original.to_string()).unwrap();
    let mut outputs = Vec::new();
    let mut configs = Vec::new();
    for (index, args) in commands.iter().enumerate() {
        let mut command = Command::new(env!("CARGO_BIN_EXE_rtree"));
        for (name, _) in std::env::vars().filter(|(name, _)| name.starts_with("RAWTREE_")) {
            command.env_remove(name);
        }
        let mut child = command
            .env("HOME", home.path())
            .args(["--api-url", &url])
            .args(*args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let progress = on_progress.filter(|_| index == 0).map(|observe| {
            let stderr = child.stderr.take().unwrap();
            let progress_tx = progress_tx.clone();
            std::thread::spawn(move || {
                let mut reader = BufReader::new(stderr);
                let mut line = String::new();
                assert!(reader.read_line(&mut line).unwrap() > 0);
                observe(&line);
                progress_tx.send(()).unwrap();
                let mut stderr = line.into_bytes();
                reader.read_to_end(&mut stderr).unwrap();
                stderr
            })
        });
        let mut output = child.wait_with_output().unwrap();
        if let Some(progress) = progress {
            output.stderr = progress.join().unwrap();
        }
        outputs.push(output);
        configs.push(serde_json::from_slice(&std::fs::read(&config_path).unwrap()).unwrap());
    }
    let bodies = server.join().unwrap();
    (outputs, configs, bodies)
}
