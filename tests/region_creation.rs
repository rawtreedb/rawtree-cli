mod common;
use serde_json::{json, Value};

fn discovery() -> Vec<(String, &'static str, Value)> {
    vec![
        (
            "GET /v1/organizations".into(),
            "200 OK",
            json!({"organizations":[{"organization_id":"org-id","name":"acme"}]}),
        ),
        (
            "GET /v1/regions".into(),
            "200 OK",
            json!({"regions":[{"id":"region-id","name":"Europe","cloud_region":"eu-central-1","deployment_id":"abcd1234","organization_id":null,"enabled":true}]}),
        ),
        (
            "GET /v1/organizations/org-id/regions".into(),
            "200 OK",
            json!({"regions":[{"id":"byoc-id","name":"Private Europe","cloud_region":"eu-central-1","deployment_id":"feed1234","organization_id":"org-id","enabled":true}]}),
        ),
    ]
}

#[test]
fn create_flag_routes_size_discovery_and_preserves_region_in_body() {
    let mut responses = discovery();
    responses.extend([
        ("GET /v1/clusters/sizes".into(), "200 OK", json!({"sizes":[{"cpu_cores":2,"memory_gib":8}]})),
        ("POST /v1/clusters?organization=acme".into(), "200 OK", json!({"id":"new","name":"production","created_at":"2026-10-18T00:00:00Z","status":{"phase":"provisioning","ready":false,"message":null},"resources":null,"can_pause":false,"can_resume":false,"idle_timeout_minutes":30})),
    ]);
    let (output, _, bodies) = common::run_cli(
        &responses,
        &[
            "--org",
            "acme",
            "--json",
            "cluster",
            "create",
            "--region-id",
            "region-id",
            "--name",
            "production",
            "--replicas",
            "2",
            "--min-size",
            "2:8",
        ],
        &json!({"token":"session.fixture"}),
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(bodies[4]["region_id"], "region-id");
    assert_eq!(bodies[4]["size"], json!({"cpu_cores":2,"memory_gib":8}));
    assert!(bodies[4].get("deployment_id").is_none());
}

#[test]
fn region_discovery_and_region_size_commands_are_available() {
    for action in ["regions", "sizes"] {
        let mut responses = discovery();
        let mut args = vec!["--org", "acme", "--json", "cluster", action];
        if action == "sizes" {
            args.extend(["--region-id", "region-id"]);
            responses.push((
                "GET /v1/clusters/sizes".into(),
                "200 OK",
                json!({"sizes":[]}),
            ));
        }
        let (output, _, _) =
            common::run_cli(&responses, &args, &json!({"token":"session.fixture"}));
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let body: Value = serde_json::from_slice(&output.stdout).unwrap();
        if action == "regions" {
            let ids: Vec<_> = body["regions"]
                .as_array()
                .unwrap()
                .iter()
                .map(|row| row["id"].as_str().unwrap())
                .collect();
            assert_eq!(ids, ["region-id", "byoc-id"]);
        } else {
            assert_eq!(body["sizes"], json!([]));
        }
    }
}

#[test]
fn catalog_failure_does_not_return_a_partial_list() {
    for index in [1, 2] {
        let mut responses = discovery();
        responses.truncate(index + 1);
        responses[index].1 = "503 Service Unavailable";
        responses[index].2 = json!({"message":"Catalog unavailable"});
        let (output, _, _) = common::run_cli(
            &responses,
            &["--org", "acme", "--json", "cluster", "regions"],
            &json!({"token":"session.fixture"}),
        );
        assert!(!output.status.success());
        assert!(!String::from_utf8_lossy(&output.stdout).contains("region-id"));
        assert!(String::from_utf8_lossy(&output.stderr).contains("Catalog unavailable"));
    }
}
