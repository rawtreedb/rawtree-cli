use serde_json::Value;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::process::{Command, Output};
use std::time::{Duration, Instant};

pub fn run_cli(
    responses: &[(String, &str, Value)],
    args: &[&str],
    original: &Value,
) -> (Output, Value, Vec<Value>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let responses = responses
        .iter()
        .map(|(path, status, body)| (path.to_string(), status.to_string(), body.to_string()))
        .collect::<Vec<_>>();
    let server = std::thread::spawn(move || {
        let mut bodies = Vec::new();
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
            assert_eq!(request.trim(), format!("{path} HTTP/1.1"));
            let mut content_length = 0;
            loop {
                let mut line = String::new();
                assert!(reader.read_line(&mut line).unwrap() > 0);
                if let Some((name, value)) = line.split_once(':') {
                    if name.eq_ignore_ascii_case("content-length") {
                        content_length = value.trim().parse::<usize>().unwrap();
                    }
                }
                if line == "\r\n" {
                    break;
                }
            }
            let mut body_bytes = vec![0; content_length];
            reader.read_exact(&mut body_bytes).unwrap();
            bodies.push(if body_bytes.is_empty() {
                Value::Null
            } else {
                serde_json::from_slice(&body_bytes).unwrap()
            });
            write!(socket, "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
        }
        bodies
    });

    let home = tempfile::tempdir().unwrap();
    let config_path = home.path().join(".config/rtree/config.json");
    std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
    std::fs::write(&config_path, original.to_string()).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_rtree"));
    for (name, _) in std::env::vars().filter(|(name, _)| name.starts_with("RAWTREE_")) {
        command.env_remove(name);
    }
    let output = command
        .env("HOME", home.path())
        .args(["--api-url", &url])
        .args(args)
        .output()
        .unwrap();
    let bodies = server.join().unwrap();
    let saved = serde_json::from_slice(&std::fs::read(config_path).unwrap()).unwrap();
    (output, saved, bodies)
}
