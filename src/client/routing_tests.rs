use super::ApiClient;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::thread;
use std::time::{Duration, Instant};

const ORG: &str = "00000000-0000-4000-8000-000000000001";
struct Request {
    path: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

fn server(responses: Vec<(&'static str, Value)>) -> (String, thread::JoinHandle<Vec<Request>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let handle = thread::spawn(move || {
        let mut requests = Vec::new();
        for (status, payload) in responses {
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut socket = loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(Instant::now() < deadline, "missing HTTP request");
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("accept: {error}"),
                }
            };
            socket.set_nonblocking(false).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut reader = BufReader::new(socket.try_clone().unwrap());
            let mut first = String::new();
            reader.read_line(&mut first).unwrap();
            let mut headers = HashMap::new();
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" {
                    break;
                }
                let (key, value) = line.split_once(':').unwrap();
                headers.insert(key.to_ascii_lowercase(), value.trim().to_owned());
            }
            let length = headers
                .get("content-length")
                .map(|value| value.parse().unwrap())
                .unwrap_or(0);
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            let response = payload.to_string();
            write!(socket, "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).unwrap();
            requests.push(Request {
                path: first.trim().to_owned(),
                headers,
                body,
            });
        }
        requests
    });
    (base, handle)
}

fn metadata(pair: Value) -> Vec<(&'static str, Value)> {
    vec![
        (
            "200 OK",
            json!({"organizations": [{"organization_id": ORG, "name": "acme"}]}),
        ),
        (
            "200 OK",
            json!({"clusters": [{"id": "cluster-id", "name": "production", "deployment_id": pair["deployment_id"], "cloud_region": pair["cloud_region"]}]}),
        ),
    ]
}

#[test]
fn session_requests_resolve_scope_and_reuse_metadata_for_queries_and_gzip() {
    let mut responses = metadata(json!({"deployment_id": "12abcdef", "cloud_region": "eu-west-1"}));
    responses.extend([
        ("200 OK", json!({"ok":true})),
        ("200 OK", json!({"ok":true})),
    ]);
    let (base, server) = server(responses);
    let client = ApiClient::new(base, Some("session.fixture".to_string()));
    let path = "/v1/query?organization=acme&cluster=production";
    let _: Value = client.post(path, &json!({"query": "SELECT 1"})).unwrap();
    let _: Value = client
        .post_compressed(
            "/v1/tables/events?organization=acme&cluster=production",
            "{\"value\":1}",
        )
        .unwrap();
    let requests = server.join().unwrap();
    assert_eq!(requests[0].path, "GET /v1/organizations HTTP/1.1");
    assert_eq!(
        requests[1].path,
        format!("GET /v1/organizations/{ORG}/clusters HTTP/1.1")
    );
    assert!(!requests[0].headers.contains_key("x-rawtree-region"));
    for request in &requests[2..] {
        assert_eq!(request.headers["x-rawtree-deployment-id"], "12abcdef");
        assert_eq!(request.headers["x-rawtree-region"], "eu-west-1");
        assert_eq!(request.headers["authorization"], "Bearer session.fixture");
        assert_eq!(request.headers["x-rawtree-client"], "cli");
    }
    assert_eq!(requests[3].headers["content-encoding"], "gzip");
    let mut decoded = String::new();
    flate2::read::GzDecoder::new(requests[3].body.as_slice())
        .read_to_string(&mut decoded)
        .unwrap();
    assert_eq!(decoded, "{\"value\":1}");
}

#[test]
fn absent_partial_and_invalid_metadata_keep_default_routing() {
    for pair in [
        json!({}),
        json!({"deployment_id": "12abcdef"}),
        json!({"deployment_id": "12abcdef99", "cloud_region": "eu-west-1"}),
        json!({"deployment_id": "12abcdef", "cloud_region": "bad.invalid"}),
    ] {
        let mut responses = metadata(pair);
        responses.push(("200 OK", json!({})));
        let (base, server) = server(responses);
        let client = ApiClient::new(base, Some("oauth.fixture".to_string()));
        let _: Value = client
            .get("/v1/databases?organization=acme&cluster=production")
            .unwrap();
        let requests = server.join().unwrap();
        assert!(!requests[2].headers.contains_key("x-rawtree-region"));
        assert!(!requests[2].headers.contains_key("x-rawtree-deployment-id"));
    }
}

#[test]
fn api_keys_skip_discovery_and_routing_headers() {
    let (base, server) = server(vec![("200 OK", json!({}))]);
    let client = ApiClient::new(base, Some("rt_fixture".to_string()));
    client
        .post_raw(
            "/v1/query?organization=acme&cluster=production",
            &json!({"query":"SELECT 1"}),
        )
        .unwrap();
    let requests = server.join().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(!requests[0].headers.contains_key("x-rawtree-region"));
    assert_eq!(requests[0].headers["authorization"], "Bearer rt_fixture");
}

#[test]
fn cluster_id_actions_use_organization_id_and_serving_headers() {
    let mut responses = metadata(json!({"deployment_id": "12abcdef", "cloud_region": "us-east-1"}));
    responses.remove(0); // Organization ID is already known: no name resolution.
    responses.push(("200 OK", json!({})));
    let (base, server) = server(responses);
    let client = ApiClient::new(base, Some("session.fixture".to_string()));
    let collection = client.clusters_path(Some(ORG)).unwrap();
    let _: Value = client
        .post_empty(&format!("{collection}/cluster-id/stop"))
        .unwrap();
    let requests = server.join().unwrap();
    assert_eq!(
        requests[1].path,
        format!("POST {collection}/cluster-id/stop HTTP/1.1")
    );
    assert_eq!(requests[1].headers["x-rawtree-deployment-id"], "12abcdef");
}

#[test]
fn discovery_failure_does_not_execute_the_operation() {
    let mut responses = metadata(json!({}));
    responses[1] = (
        "503 Service Unavailable",
        json!({"message":"discovery unavailable"}),
    );
    let (base, server) = server(responses);
    let client = ApiClient::new(base, Some("session.fixture".to_string()));
    assert!(client
        .get::<Value>("/v1/databases?organization=acme&cluster=production")
        .is_err());
    assert_eq!(server.join().unwrap().len(), 2);
}

#[test]
fn cluster_creation_invalidates_previously_discovered_empty_collection() {
    let responses = vec![
        ("200 OK", json!({"clusters": []})),
        ("200 OK", json!({"id": "new-id"})),
        (
            "200 OK",
            json!({"clusters": [{"id":"new-id", "name":"new", "deployment_id":"12abcdef", "cloud_region":"eu-west-1"}]}),
        ),
        ("200 OK", json!({"data":[]})),
    ];
    let (base, server) = server(responses);
    let client = ApiClient::new(base, Some("session.fixture".to_string()));
    let collection = client.clusters_path(Some(ORG)).unwrap();
    let _: Value = client.get(&collection).unwrap();
    let _: Value = client.post(&collection, &json!({"name":"new"})).unwrap();
    let _: Value = client
        .get(&format!(
            "/v1/databases?organization_id={ORG}&cluster_id=new-id"
        ))
        .unwrap();
    let requests = server.join().unwrap();
    assert_eq!(requests[2].path, format!("GET {collection} HTTP/1.1"));
    assert_eq!(requests[3].headers["x-rawtree-region"], "eu-west-1");
}

#[test]
fn explicit_timeout_includes_routing_discovery() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut reader = BufReader::new(socket);
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert_eq!(line.trim(), "GET /v1/organizations HTTP/1.1");
        thread::sleep(Duration::from_millis(400));
    });
    let client = ApiClient::new(base, Some("session.fixture".to_string()));
    let started = Instant::now();
    let result = client.post_with_timeout::<Value>(
        "/v1/query?organization=acme&cluster=production",
        &json!({"query":"SELECT 1"}),
        Duration::from_millis(50),
    );
    assert!(result.is_err());
    assert!(started.elapsed() < Duration::from_millis(300));
    server.join().unwrap();
}

#[test]
fn explicit_id_and_name_selectors_cannot_collide() {
    let (base, server) = server(vec![
        (
            "200 OK",
            json!({"clusters":[
                {"id":"other-id", "name":"target-id", "deployment_id":"abcdef12", "cloud_region":"ap-south-1"},
                {"id":"target-id", "name":"production", "deployment_id":"12abcdef", "cloud_region":"eu-west-1"}
            ]}),
        ),
        ("200 OK", json!({})),
        ("200 OK", json!({})),
        ("200 OK", json!({})),
    ]);
    let client = ApiClient::new(base, Some("session.fixture".to_string()));
    let _: Value = client
        .get(&format!("/v1/organizations/{ORG}/clusters/target-id"))
        .unwrap();
    let _: Value = client
        .get(&format!(
            "/v1/metrics?organization_id={ORG}&cluster_id=target-id"
        ))
        .unwrap();
    let _: Value = client
        .get(&format!(
            "/v1/databases?organization_id={ORG}&cluster=target-id"
        ))
        .unwrap();
    let requests = server.join().unwrap();
    assert_eq!(requests[1].headers["x-rawtree-region"], "eu-west-1");
    assert_eq!(requests[2].headers["x-rawtree-region"], "eu-west-1");
    assert_eq!(requests[3].headers["x-rawtree-region"], "ap-south-1");
}
