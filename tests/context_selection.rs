mod common;

use serde_json::{json, Value};

fn configured() -> Value {
    json!({"token":"session-fixture", "default_organization":"team", "cluster":"production", "database":"analytics"})
}

fn organizations(names: &[&str]) -> (String, &'static str, Value) {
    (
        "GET /v1/organizations".into(),
        "200 OK",
        json!({"organizations": names.iter().map(|name| json!({"name":name, "role":"owner"})).collect::<Vec<_>>()}),
    )
}

#[test]
fn changing_defaults_clears_children_but_reselecting_a_parent_preserves_them() {
    let commands: Vec<&[&str]> = vec![
        &["organization", "use", "team", "--json"],
        &["cluster", "use", "production", "--json"],
        &["cluster", "use", "staging", "--json"],
        &["database", "use", "billing", "--json"],
        &["organization", "use", "other", "--json"],
    ];
    let (outputs, configs, _) = common::run_cli_sequence(&[], &commands, &configured());
    assert!(outputs.iter().all(|output| output.status.success()));
    assert_eq!(configs[0]["database"], "analytics");
    assert_eq!(configs[1]["database"], "analytics");
    assert!(configs[2]["database"].is_null());
    assert_eq!(configs[3]["database"], "billing");
    assert_eq!(configs[4]["default_organization"], "other");
    assert!(configs[4]["cluster"].is_null());
    assert!(configs[4]["database"].is_null());
    assert_eq!(configs[4]["token"], "session-fixture");
}

#[test]
fn use_rejects_conflicting_transient_parent_overrides() {
    for args in [
        vec!["--org", "other", "cluster", "use", "staging", "--json"],
        vec!["--org", "other", "database", "use", "billing", "--json"],
        vec![
            "--cluster",
            "staging",
            "database",
            "use",
            "billing",
            "--json",
        ],
    ] {
        let original = configured();
        let (output, saved, _) = common::run_cli(&[], &args, &original);
        assert_eq!(output.status.code(), Some(2));
        let result: Value = serde_json::from_slice(&output.stderr).unwrap();
        assert_eq!(result["error"]["code"], "context_conflict");
        assert_eq!(saved, original);
    }
}

#[test]
fn ambiguous_or_empty_organizations_return_choices_without_a_data_request() {
    for names in [vec![], vec!["team", "other"]] {
        let original = json!({"token":"session-fixture"});
        let (output, saved, _) = common::run_cli(
            &[organizations(&names)],
            &["database", "list", "--json"],
            &original,
        );
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        let error: Value = serde_json::from_slice(&output.stderr).unwrap();
        assert_eq!(error["needs"], "org");
        assert_eq!(error["orgs"], json!(names));
        assert_eq!(error["error"]["code"], "selection_required");
        assert_eq!(saved, original);
    }
}

#[test]
fn ambiguous_or_empty_clusters_return_choices_without_a_data_request() {
    for names in [vec![], vec!["production", "staging"]] {
        let original = json!({"token":"session-fixture", "default_organization":"team"});
        let responses = [(
            "GET /v1/clusters?organization=team".into(),
            "200 OK",
            json!({"clusters":names.iter().map(|name|json!({"name":name})).collect::<Vec<_>>() }),
        )];
        let (output, saved, _) =
            common::run_cli(&responses, &["database", "list", "--json"], &original);
        assert_eq!(output.status.code(), Some(2));
        let error: Value = serde_json::from_slice(&output.stderr).unwrap();
        assert_eq!(error["needs"], "cluster");
        assert_eq!(error["clusters"], json!(names));
        assert_eq!(saved, original);
    }
}

#[test]
fn single_resources_apply_to_one_command_without_saving_defaults() {
    let original = json!({"token":"session-fixture"});
    let responses = [
        organizations(&["team"]),
        (
            "GET /v1/clusters?organization=team".into(),
            "200 OK",
            json!({"clusters":[{"name":"production"}]}),
        ),
        (
            "GET /v1/databases?organization=team&cluster=production".into(),
            "200 OK",
            json!({"databases":[]}),
        ),
    ];
    let (output, saved, _) =
        common::run_cli(&responses, &["database", "list", "--json"], &original);
    assert!(output.status.success());
    assert_eq!(saved, original);
}

#[test]
fn discovery_errors_keep_their_error_code() {
    for (status, expected) in [("401 Unauthorized", 1), ("503 Service Unavailable", 3)] {
        let responses = [(
            "GET /v1/organizations".into(),
            status,
            json!({"message":"Request failed"}),
        )];
        let (output, _, _) = common::run_cli(
            &responses,
            &["database", "list", "--json"],
            &json!({"token":"session-fixture"}),
        );
        assert_eq!(output.status.code(), Some(expected));
        assert!(output.stdout.is_empty());
        let error: Value = serde_json::from_slice(&output.stderr).unwrap();
        assert!(error.get("needs").is_none());
    }
}

#[test]
fn parent_flags_do_not_reuse_saved_children_or_change_config() {
    let original = configured();
    let responses = [
        (
            "GET /v1/clusters?organization=other".into(),
            "200 OK",
            json!({"clusters":[{"name":"staging"}]}),
        ),
        (
            "GET /v1/databases?organization=other&cluster=staging".into(),
            "200 OK",
            json!({"databases":[]}),
        ),
    ];
    let (output, saved, _) = common::run_cli(
        &responses,
        &["--org", "other", "database", "list", "--json"],
        &original,
    );
    assert!(output.status.success());
    assert_eq!(saved, original);
    let (output, saved, _) = common::run_cli(
        &[],
        &[
            "--org",
            "other",
            "--cluster",
            "staging",
            "table",
            "list",
            "--json",
        ],
        &original,
    );
    assert_eq!(output.status.code(), Some(2));
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["needs"], "database");
    assert_eq!(saved, original);
    let (output, _, _) = common::run_cli(
        &[],
        &["--cluster", "staging", "table", "list", "--json"],
        &original,
    );
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["needs"], "database");
}

#[test]
fn api_key_commands_keep_server_scope_without_resource_discovery() {
    let original = json!({"token":"rt_key_fixture"});
    let (output, saved, _) = common::run_cli(
        &[(
            "GET /v1/databases".into(),
            "200 OK",
            json!({"databases":[]}),
        )],
        &["database", "list", "--json"],
        &original,
    );
    assert!(output.status.success());
    assert_eq!(saved, original);
    let (output, _, _) = common::run_cli(
        &[(
            "GET /v1/tables?database=analytics".into(),
            "200 OK",
            json!({"tables":[]}),
        )],
        &["table", "list", "--database", "analytics", "--json"],
        &original,
    );
    assert!(output.status.success());
}

#[test]
fn deleting_a_selected_organization_clears_all_defaults_without_choosing_another() {
    let original = configured();
    let responses = [(
        "DELETE /v1/organizations/team".into(),
        "200 OK",
        json!({"deleted":true}),
    )];
    let (output, saved, _) = common::run_cli(
        &responses,
        &["organization", "delete", "team", "--json"],
        &original,
    );
    assert!(output.status.success());
    assert!(saved["default_organization"].is_null());
    assert!(saved["cluster"].is_null());
    assert!(saved["database"].is_null());
    assert_eq!(saved["token"], "session-fixture");
}
