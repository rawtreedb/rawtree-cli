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
            "GET /v1/organizations/org-id/regions".into(),
            "200 OK",
            json!({"regions":[{"id":"region-id","name":"Europe","cloud_region":"eu-central-1","deployment_id":"abcd1234","organization_id":null,"enabled":true}]}),
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
    assert_eq!(bodies[3]["region_id"], "region-id");
    assert_eq!(bodies[3]["size"], json!({"cpu_cores":2,"memory_gib":8}));
    assert!(bodies[3].get("deployment_id").is_none());
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
        assert!(body
            .get(if action == "regions" {
                "regions"
            } else {
                "sizes"
            })
            .is_some());
    }
}
