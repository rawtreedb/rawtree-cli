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
        let mut responses: Vec<_> = responses.into_iter().enumerate().collect();
        let mut regional_targets = Vec::new();
        while !responses.is_empty() {
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
            let index = if first.starts_with("GET /v1/clusters?") && !regional_targets.is_empty() {
                let pair = headers
                    .get("x-rawtree-deployment-id")
                    .zip(headers.get("x-rawtree-region"))
                    .map(|(id, region)| (id.clone(), region.clone()));
                regional_targets
                    .iter()
                    .position(|target| *target == pair)
                    .expect("unexpected regional request")
            } else {
                0
            };
            if first.starts_with("GET /v1/clusters?") && !regional_targets.is_empty() {
                regional_targets.remove(index);
            }
            let (order, (status, payload)) = responses.remove(index);
            if let Some(regions) = payload["regions"].as_array() {
                regional_targets.clear();
                for region in regions.iter().filter(|region| region["enabled"] == true) {
                    let pair = region["deployment_id"]
                        .as_str()
                        .zip(region["cloud_region"].as_str())
                        .filter(|(id, region)| super::routing::valid_routing(id, region))
                        .map(|(id, region)| (id.to_owned(), region.to_owned()));
                    if !regional_targets.contains(&pair) {
                        regional_targets.push(pair);
                    }
                }
            }
            let response = payload.to_string();
            write!(socket, "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).unwrap();
            requests.push((
                order,
                Request {
                    path: first.trim().to_owned(),
                    headers,
                    body,
                },
            ));
        }
        // Return requests in fixture order so assertions do not depend on scheduling.
        requests.sort_by_key(|(order, _)| *order);
        requests.into_iter().map(|(_, request)| request).collect()
    });
    (base, handle)
}

fn discovery_catalog() -> Vec<(&'static str, Value)> {
    vec![
        (
            "200 OK",
            json!({"organizations":[{"organization_id":ORG,"name":"acme"}]}),
        ),
        (
            "200 OK",
            json!({"regions":[{"id":"legacy", "name":"Virginia", "cloud_region":"us-east-1", "deployment_id":null,"enabled":true,"organization_id":null}]}),
        ),
    ]
}

fn metadata(pair: Value) -> Vec<(&'static str, Value)> {
    let mut responses = discovery_catalog();
    responses.push((
        "200 OK",
        json!({"clusters": [{"id": "cluster-id", "name": "production", "deployment_id": pair["deployment_id"], "cloud_region": pair["cloud_region"]}]}),
    ));
    responses
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
    assert_eq!(
        requests[2].path,
        "GET /v1/clusters?organization=acme HTTP/1.1"
    );
    assert!(!requests[2].headers.contains_key("x-rawtree-region"));
    for request in &requests[3..] {
        assert_eq!(request.headers["x-rawtree-deployment-id"], "12abcdef");
        assert_eq!(request.headers["x-rawtree-region"], "eu-west-1");
        assert_eq!(request.headers["authorization"], "Bearer session.fixture");
        assert_eq!(request.headers["x-rawtree-client"], "cli");
    }
    assert_eq!(requests[4].headers["content-encoding"], "gzip");
    let mut decoded = String::new();
    flate2::read::GzDecoder::new(requests[4].body.as_slice())
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
        assert!(!requests[3].headers.contains_key("x-rawtree-region"));
        assert!(!requests[3].headers.contains_key("x-rawtree-deployment-id"));
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
fn cluster_id_actions_preserve_organization_query_and_serving_headers() {
    let mut responses = metadata(json!({"deployment_id": "12abcdef", "cloud_region": "us-east-1"}));
    responses.push(("200 OK", json!({})));
    let (base, server) = server(responses);
    let client = ApiClient::new(base, Some("session.fixture".to_string()));
    let _: Value = client
        .post_empty("/v1/clusters/cluster-id/stop?organization=acme")
        .unwrap();
    let requests = server.join().unwrap();
    assert_eq!(
        requests[3].path,
        "POST /v1/clusters/cluster-id/stop?organization=acme HTTP/1.1"
    );
    assert_eq!(requests[3].headers["x-rawtree-deployment-id"], "12abcdef");
}

#[test]
fn discovery_failure_does_not_execute_the_operation() {
    let mut responses = metadata(json!({}));
    responses.truncate(1);
    responses[0] = (
        "503 Service Unavailable",
        json!({"message":"discovery unavailable"}),
    );
    let (base, server) = server(responses);
    let client = ApiClient::new(base, Some("session.fixture".to_string()));
    assert!(client
        .get::<Value>("/v1/databases?organization=acme&cluster=production")
        .is_err());
    assert_eq!(server.join().unwrap().len(), 1);
}

#[test]
fn cluster_creation_invalidates_previously_discovered_empty_collection() {
    let mut responses = discovery_catalog();
    responses.extend([
        ("200 OK", json!({"clusters": []})),
        ("200 OK", json!({"id": "new-id"})),
        (
            "200 OK",
            json!({"clusters": [{"id":"new-id", "name":"new", "deployment_id":"12abcdef", "cloud_region":"eu-west-1"}]}),
        ),
        ("200 OK", json!({"data":[]})),
    ]);
    responses.splice(4..4, discovery_catalog());
    let (base, server) = server(responses);
    let client = ApiClient::new(base, Some("session.fixture".to_string()));
    let collection = "/v1/clusters?organization=acme";
    let _: Value = client.get(&collection).unwrap();
    let _: Value = client.post(&collection, &json!({"name":"new"})).unwrap();
    let _: Value = client
        .get("/v1/databases?organization=acme&cluster_id=new-id")
        .unwrap();
    let requests = server.join().unwrap();
    assert_eq!(requests[6].path, format!("GET {collection} HTTP/1.1"));
    assert_eq!(requests[7].headers["x-rawtree-region"], "eu-west-1");
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
    let mut responses = discovery_catalog();
    responses.extend([
        (
            "200 OK",
            json!({"clusters":[
                {"id":"other-id", "name":"target-id", "deployment_id":"abcdef12", "cloud_region":"ap-south-1"},
                {"id":"target-id", "name":"production", "deployment_id":"12abcdef", "cloud_region":"eu-west-1"}
            ]}),
        ),
        ("200 OK", json!({})),
        (
            "200 OK",
            json!({"organizations": [{"organization_id": ORG, "name": "acme"}]}),
        ),
        ("200 OK", json!({})),
        ("200 OK", json!({})),
    ]);
    let (base, server) = server(responses);
    let client = ApiClient::new(base, Some("session.fixture".to_string()));
    let _: Value = client
        .get("/v1/clusters/target-id?organization=acme")
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
    assert_eq!(requests[3].headers["x-rawtree-region"], "eu-west-1");
    assert_eq!(requests[5].headers["x-rawtree-region"], "eu-west-1");
    assert_eq!(requests[6].headers["x-rawtree-region"], "ap-south-1");
}

fn region_catalog(overrides: Value) -> Vec<(&'static str, Value)> {
    let mut region = json!({"id":"region-id", "name":"Europe", "cloud_region":"eu-central-1", "organization_id":null, "deployment_id":"abcd1234", "enabled":true});
    region
        .as_object_mut()
        .unwrap()
        .extend(overrides.as_object().unwrap().clone());
    vec![
        (
            "200 OK",
            json!({"organizations":[{"organization_id":ORG,"name":"acme"}]}),
        ),
        ("200 OK", json!({"regions":[region]})),
    ]
}

#[test]
fn creation_and_sizes_use_selected_region_without_mutating_default_client() {
    let mut responses = region_catalog(json!({}));
    responses.extend([
        ("200 OK", json!({"sizes":[]})),
        ("200 OK", json!({"id":"new"})),
        ("200 OK", json!({"clusters":[]})),
    ]);
    let (base, server) = server(responses);
    let client = ApiClient::new(base, Some("session.fixture".into()));
    let regional = client.for_region(Some("acme"), Some("region-id")).unwrap();
    let _: Value = regional.get("/v1/clusters/sizes").unwrap();
    let _: Value = regional
        .post(
            "/v1/clusters?organization=acme",
            &json!({"region_id":"region-id"}),
        )
        .unwrap();
    let _: Value = client.get("/v1/clusters/sizes").unwrap();
    let requests = server.join().unwrap();
    assert_eq!(
        requests[1].path,
        format!("GET /v1/organizations/{ORG}/regions HTTP/1.1")
    );
    for request in &requests[2..4] {
        assert_eq!(request.headers["x-rawtree-deployment-id"], "abcd1234");
        assert_eq!(request.headers["x-rawtree-region"], "eu-central-1");
        assert_eq!(request.headers["authorization"], "Bearer session.fixture");
    }
    for index in [0, 1, 4] {
        assert!(!requests[index].headers.contains_key("x-rawtree-region"));
    }
}

#[test]
fn explicit_unusable_regions_never_reach_creation() {
    for overrides in [
        json!({"enabled":false}),
        json!({"deployment_id":null}),
        json!({"deployment_id":"bad"}),
        json!({"cloud_region":"bad"}),
        json!({"id":"other"}),
    ] {
        let (base, server) = server(region_catalog(overrides));
        let client = ApiClient::new(base, Some("session.fixture".into()));
        assert!(client.for_region(Some("acme"), Some("region-id")).is_err());
        assert_eq!(server.join().unwrap().len(), 2);
    }
}

#[test]
fn legacy_region_and_omitted_selection_preserve_default_routing() {
    let id = "0c0f0a56-7f03-4d39-8e5d-000000000001";
    let mut responses =
        region_catalog(json!({"id":id,"deployment_id":null,"cloud_region":"us-east-1"}));
    responses.extend([("200 OK", json!({})), ("200 OK", json!({}))]);
    let (base, server) = server(responses);
    let client = ApiClient::new(base, Some("session.fixture".into()));
    for selected in [Some(id), None] {
        let regional = client.for_region(Some("acme"), selected).unwrap();
        let _: Value = regional.get("/v1/clusters/sizes").unwrap();
    }
    for request in server.join().unwrap() {
        assert!(!request.headers.contains_key("x-rawtree-region"));
    }
}

#[test]
fn api_keys_cannot_override_their_embedded_deployment() {
    let client = ApiClient::new("http://127.0.0.1:1".into(), Some("rt_fixture".into()));
    assert!(client
        .for_region(Some("acme"), Some("region-id"))
        .err()
        .unwrap()
        .to_string()
        .contains("user credential"));
}

fn multi_region_catalog(deployment: &str) -> Vec<(&'static str, Value)> {
    vec![
        (
            "200 OK",
            json!({"organizations":[{"organization_id":ORG,"name":"acme"}]}),
        ),
        (
            "200 OK",
            json!({"regions":[
                {"id":"legacy","name":"Virginia","cloud_region":"us-east-1","deployment_id":null,"organization_id":null,"enabled":true},
                {"id":"eu","name":"Ireland","cloud_region":"eu-west-1","deployment_id":deployment,"organization_id":null,"enabled":true},
                {"id":"duplicate","name":"Same deployment","cloud_region":"eu-west-1","deployment_id":deployment,"organization_id":null,"enabled":true},
                {"id":"pending","name":"Pending","cloud_region":"bad.region","deployment_id":"bad","organization_id":null,"enabled":true},
                {"id":"disabled","name":"Disabled","cloud_region":"ap-south-1","deployment_id":"abcd1234","organization_id":null,"enabled":false}
            ]}),
        ),
        (
            "200 OK",
            json!({"organization":{"name":"acme"},"clusters":[{"id":"us","name":"america","region_id":"legacy"}]}),
        ),
        (
            "200 OK",
            json!({"organization":{"name":"acme"},"clusters":[
                {"id":"eu","name":"europe","region_id":"eu","deployment_id":"wrong","cloud_region":"us-east-1","s3_storage":{"data":{"bucket":"data"}}},
                {"id":"eu","name":"europe","region_id":"eu","s3_storage":{"data":{"bucket":"data"}}}
            ]}),
        ),
    ]
}

#[test]
fn discovery_merges_regions_and_uses_authoritative_metadata_for_queries() {
    for deployment in ["1234abcd", "0123456789abcdef0123456789abcdef"] {
        let mut responses = multi_region_catalog(deployment);
        responses.push(("200 OK", json!({"data":[]})));
        let (base, server) = server(responses);
        let client = ApiClient::new(base, Some("session.fixture".into()));
        let result: Value = client.get("/v1/clusters?organization=acme").unwrap();
        assert_eq!(result["clusters"].as_array().unwrap().len(), 2);
        assert_eq!(result["organization"]["name"], "acme");
        assert_eq!(result["clusters"][1]["deployment_id"], deployment);
        assert_eq!(result["clusters"][1]["cloud_region"], "eu-west-1");
        assert_eq!(
            result["clusters"][1]["s3_storage"]["data"]["bucket"],
            "data"
        );
        let _: Value = client
            .get("/v1/databases?organization=acme&cluster=europe")
            .unwrap();
        let requests = server.join().unwrap();
        assert_eq!(requests.len(), 5);
        assert!(!requests[2].headers.contains_key("x-rawtree-region"));
        for index in [3, 4] {
            assert_eq!(
                requests[index].headers["x-rawtree-deployment-id"],
                deployment
            );
            assert_eq!(requests[index].headers["x-rawtree-region"], "eu-west-1");
            assert_eq!(
                requests[index].headers["authorization"],
                "Bearer session.fixture"
            );
        }
    }
}

#[test]
fn discovery_runs_before_a_direct_cluster_request_and_retries_after_regional_failure() {
    let mut responses = multi_region_catalog("1234abcd");
    responses[3] = (
        "503 Service Unavailable",
        json!({"message":"Europe unavailable"}),
    );
    responses.push(("200 OK", json!({"tables":[]})));
    responses.extend(multi_region_catalog("1234abcd"));
    responses.push(("200 OK", json!({"data":[]})));
    let (base, server) = server(responses);
    let client = ApiClient::new(base, Some("session.fixture".into()));
    let partial: Value = client.get("/v1/clusters?organization=acme").unwrap();
    assert_eq!(partial["clusters"].as_array().unwrap().len(), 1);
    assert_eq!(partial["unavailable_regions"].as_array().unwrap().len(), 2);
    let _: Value = client
        .get("/v1/tables?organization=acme&cluster=america")
        .unwrap();
    assert!(client
        .get::<Value>("/v1/databases?organization=acme&cluster=europe")
        .unwrap_err()
        .to_string()
        .contains("Some regions are unavailable"));
    let _: Value = client
        .get("/v1/databases?organization=acme&cluster=europe")
        .unwrap();
    let requests = server.join().unwrap();
    assert_eq!(requests.len(), 10);
    assert_eq!(requests[9].headers["x-rawtree-region"], "eu-west-1");
}

#[test]
fn discovery_rejects_unknown_region_references() {
    let mut responses = multi_region_catalog("1234abcd");
    responses[3].1["clusters"][0]["region_id"] = json!("missing");
    let (base, server) = server(responses);
    let client = ApiClient::new(base, Some("session.fixture".into()));
    assert!(client
        .get::<Value>("/v1/databases?organization=acme&cluster=europe")
        .unwrap_err()
        .to_string()
        .contains("Region 'missing'"));
    assert_eq!(server.join().unwrap().len(), 4);
}

#[test]
fn public_origins_use_deployment_hosts_and_keep_paths_queries_and_headers() {
    for deployment in ["1234abcd", "0123456789abcdef0123456789abcdef"] {
        for env in ["com", "dev"] {
            let client = ApiClient::new(
                format!("https://api.rawtree.{env}"),
                Some("session.fixture".into()),
            );
            client.remember_routing_metadata("/v1/clusters?organization=acme", &json!({"clusters":[{
                "id":"eu", "name":"europe", "deployment_id":deployment,"cloud_region":"eu-west-1"
            }]}));
            let request = client
                .routed_request(
                    reqwest::Method::GET,
                    "/v1/databases?organization=acme&cluster=europe",
                )
                .unwrap()
                .build()
                .unwrap();
            assert_eq!(request.url().as_str(), format!("https://api.{deployment}.eu-west-1.aws.rawtree.{env}/v1/databases?organization=acme&cluster=europe"));
            assert_eq!(request.headers()["x-rawtree-deployment-id"], deployment);
        }
    }
    for url in [
        "http://localhost:8080/v1/query",
        "https://custom.example.com/v1/query",
        "https://api.rawtree.com:8443/v1/query",
    ] {
        assert_eq!(
            super::routing::deployment_url(url, Some(&("1234abcd".into(), "eu-west-1".into()))),
            url
        );
    }
}

#[test]
fn owning_catalog_wins_over_duplicates_in_either_order() {
    for source in ["gateway", "same-region-deployment"] {
        for region_id in [Value::Null, json!("eu")] {
            for owner_first in [true, false] {
                let mut responses = multi_region_catalog("1234abcd");
                if source == "same-region-deployment" {
                    responses[1].1["regions"][3]["enabled"] = json!(false);
                    responses[1].1["regions"][0] = json!({
                        "id":"other", "name":"Other Ireland", "cloud_region":"eu-west-1",
                        "deployment_id":"abcd1234", "organization_id":ORG, "enabled":true
                    });
                }
                responses[2].1["clusters"] = json!([{
                    "id":"eu", "name":"europe", "region_id":region_id,
                    "deployment_id":"stale", "cloud_region":"us-east-1", "status":"stale"
                }]);
                for cluster in responses[3].1["clusters"].as_array_mut().unwrap() {
                    cluster["status"] = json!("running");
                }
                if owner_first {
                    responses[1].1["regions"].as_array_mut().unwrap().swap(0, 1);
                    responses.swap(2, 3);
                }
                responses.push(("200 OK", json!({})));
                let (base, server) = server(responses);
                let client = ApiClient::new(base, Some("session.fixture".into()));
                let catalog: Value = client.get("/v1/clusters?organization=acme").unwrap();
                assert_eq!(catalog["clusters"].as_array().unwrap().len(), 1);
                assert_eq!(
                    catalog["clusters"][0]["s3_storage"],
                    json!({"data":{"bucket":"data"}})
                );
                assert_eq!(catalog["clusters"][0]["status"], "running");
                let _: Value = client
                    .get("/v1/databases?organization=acme&cluster=europe")
                    .unwrap();
                let requests = server.join().unwrap();
                let headers = &requests.last().unwrap().headers;
                assert_eq!(headers["x-rawtree-region"], "eu-west-1");
                assert_eq!(headers["x-rawtree-deployment-id"], "1234abcd");
            }
        }
    }
}

#[test]
fn api_keys_preserve_the_public_gateway_url() {
    let client = ApiClient::new("https://api.rawtree.com".into(), Some("rt_fixture".into()));
    let request = client
        .routed_request(
            reqwest::Method::GET,
            "/v1/databases?organization=acme&cluster=europe",
        )
        .unwrap()
        .build()
        .unwrap();
    assert_eq!(request.url().host_str(), Some("api.rawtree.com"));
    assert!(!request.headers().contains_key("x-rawtree-region"));
}

#[test]
fn empty_partial_catalog_is_marked_and_explicit_refresh_replaces_it() {
    let mut responses = multi_region_catalog("1234abcd");
    responses[2].1["clusters"] = json!([]);
    responses[3] = (
        "503 Service Unavailable",
        json!({"message":"Europe unavailable"}),
    );
    responses.extend(multi_region_catalog("1234abcd"));
    responses.push(("200 OK", json!({"tables":[]})));
    let (base, server) = server(responses);
    let client = ApiClient::new(base, Some("session.fixture".into()));
    let partial: Value = client.get("/v1/clusters?organization=acme").unwrap();
    assert_eq!(partial["clusters"], json!([]));
    assert_eq!(partial["unavailable_regions"].as_array().unwrap().len(), 2);
    let refreshed: Value = client.get("/v1/clusters?organization=acme").unwrap();
    assert!(refreshed.get("unavailable_regions").is_none());
    let _: Value = client
        .get("/v1/tables?organization=acme&cluster=europe")
        .unwrap();
    let requests = server.join().unwrap();
    assert_eq!(requests.len(), 9);
    assert_eq!(requests[8].headers["x-rawtree-deployment-id"], "1234abcd");
}

#[test]
fn all_organization_discovery_is_concurrent_and_retains_healthy_catalogs() {
    for reverse in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = thread::spawn(move || {
            let read = || {
                let deadline = Instant::now() + Duration::from_secs(2);
                let socket = loop {
                    match listener.accept() {
                        Ok((socket, _)) => break socket,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            assert!(
                                Instant::now() < deadline,
                                "catalog requests must run concurrently across organizations"
                            );
                            thread::sleep(Duration::from_millis(2));
                        }
                        Err(error) => panic!("accept: {error}"),
                    }
                };
                socket.set_nonblocking(false).unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut reader = BufReader::new(socket.try_clone().unwrap());
                let mut first = String::new();
                reader.read_line(&mut first).unwrap();
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" {
                        break;
                    }
                }
                (socket, first)
            };
            let reply = |socket: &mut std::net::TcpStream, status: u16, body: Value| {
                let body = body.to_string();
                write!(socket, "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
            };
            let (mut socket, first) = read();
            assert!(first.starts_with("GET /v1/organizations "));
            let mut organizations = vec![
                json!({"organization_id":"acme-id","name":"acme"}),
                json!({"organization_id":"other-id","name":"other"}),
            ];
            if reverse {
                organizations.reverse();
            }
            reply(&mut socket, 200, json!({"organizations":organizations}));
            for _ in 0..2 {
                let (mut socket, first) = read();
                assert!(first.contains("/regions "));
                reply(
                    &mut socket,
                    200,
                    json!({"regions":[{"id":"legacy", "name":"Virginia", "cloud_region":"us-east-1", "deployment_id":null, "enabled":true}]}),
                );
            }
            // Accept both catalog requests before replying to either. A serial
            // implementation cannot satisfy this barrier, even with a short timeout.
            let catalogs = [read(), read()];
            for (mut socket, first) in catalogs {
                if first.contains("organization=acme ") {
                    reply(
                        &mut socket,
                        200,
                        json!({"clusters":[{"id":"healthy", "name":"production", "region_id":"legacy"}]}),
                    );
                } else {
                    assert!(first.contains("organization=other "));
                    reply(
                        &mut socket,
                        503,
                        json!({"message":"Other organization unavailable"}),
                    );
                }
            }
        });
        let client = ApiClient::new(base, Some("session.fixture".into()));
        let catalog: Value = client.get("/v1/clusters").unwrap();
        assert_eq!(catalog["clusters"].as_array().unwrap().len(), 1);
        assert_eq!(catalog["clusters"][0]["id"], "healthy");
        assert_eq!(
            catalog["unavailable_regions"],
            json!([{"id":"legacy", "name":"Virginia"}])
        );
        server.join().unwrap();
    }
}
