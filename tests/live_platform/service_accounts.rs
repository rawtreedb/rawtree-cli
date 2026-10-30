use super::LiveCli;

#[test]
#[ignore = "requires a bootstrapped Platform Docker Compose stack"]
fn service_account_creation_and_listing_round_trip() {
    let cli = LiveCli::from_environment();
    let name = format!("cli-test-{}", uuid::Uuid::new_v4());
    let account = cli.json(&[
        "service-account",
        "create",
        &name,
        "--description",
        "CLI integration test",
        "--role",
        "admin",
    ]);
    let id = account["id"].as_str().expect("service account ID");
    let organization_id = account["organization_id"]
        .as_str()
        .expect("organization ID");
    let listed = cli.json(&["service-account", "list"]);

    // Remove the test identity through the session-only API; deletion is not a CLI command.
    reqwest::blocking::Client::new()
        .delete(format!(
            "{}/v1/organizations/{organization_id}/service-accounts/{id}",
            cli.url
        ))
        .bearer_auth(&cli.token)
        .send()
        .expect("delete test service account")
        .error_for_status()
        .expect("test service account deleted");

    assert_eq!(account["name"], name);
    assert_eq!(account["description"], "CLI integration test");
    assert_eq!(account["role"], "admin");
    assert!(listed["service_accounts"]
        .as_array()
        .expect("service accounts array")
        .contains(&account));
}
