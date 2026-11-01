use super::LiveCli;
use serde_json::Value;

#[test]
#[ignore = "requires a bootstrapped Platform Docker Compose stack"]
fn federation_issuer_and_rule_management_round_trip() {
    let cli = LiveCli::from_environment();
    let name = format!("cli-{}", uuid::Uuid::new_v4());
    let issuer = cli.json(&[
        "federation-issuer",
        "create",
        "--name",
        &name,
        "--issuer-url",
        "https://issuer.example.invalid",
        "--description",
        "CLI live test",
    ]);
    let issuer_id = issuer["id"].as_str().unwrap();
    assert_eq!(cli.json(&["federation-issuer", "get", issuer_id]), issuer);
    assert!(cli
        .json(&["federation-issuer", "list"])
        .as_array()
        .unwrap()
        .contains(&issuer));
    let user: Value = reqwest::blocking::Client::new()
        .get(format!("{}/v1/auth/me", cli.url))
        .bearer_auth(&cli.token)
        .send()
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .unwrap();
    let user_id = user["user_id"].as_str().unwrap();
    let account = cli.json(&["service-account", "create", &name]);
    for (kind, principal_id) in [
        ("user", user_id),
        ("service_account", account["id"].as_str().unwrap()),
    ] {
        let fields = [
            "--name",
            &name,
            "--issuer-id",
            issuer_id,
            "--allowed-audiences",
            "rawtree",
            "--claim-conditions",
            r#"{"sub":"repo:acme/app:ref:refs/heads/main"}"#,
            "--principal-type",
            kind,
            "--principal-id",
            principal_id,
            "--permission",
            "read_only",
            "--database",
            "default",
            "--enabled",
            "false",
        ];
        let mut args = vec!["federation-rule", "create"];
        args.extend(fields);
        let rule = cli.json(&args);
        let rule_id = rule["id"].as_str().unwrap();
        assert_eq!(rule["principal_type"], kind);
        assert_eq!(rule["enabled"], false);
        assert_eq!(cli.json(&["federation-rule", "get", rule_id]), rule);
        assert!(cli
            .json(&["federation-rule", "list"])
            .as_array()
            .unwrap()
            .contains(&rule));
        let mut args = vec!["federation-rule", "update", rule_id];
        args.extend(fields);
        args.extend(["--description", "updated"]);
        assert_eq!(cli.json(&args)["description"], "updated");
        assert!(!cli
            .execute(
                &["federation-issuer", "delete", issuer_id],
                Some(&cli.token)
            )
            .status
            .success());
        assert_eq!(
            cli.json(&["federation-rule", "delete", rule_id]),
            Value::Null
        );
        assert!(!cli
            .execute(&["federation-rule", "get", rule_id], Some(&cli.token))
            .status
            .success());
    }
    let updated = cli.json(&[
        "federation-issuer",
        "update",
        issuer_id,
        "--name",
        &name,
        "--issuer-url",
        "https://issuer.example.invalid",
        "--jwks-source",
        "explicit_url",
        "--jwks-url",
        "https://keys.example.invalid/jwks",
    ]);
    assert_eq!(updated["jwks_source"], "explicit_url");
    assert!(updated["description"].is_null());
    assert_eq!(
        cli.json(&["federation-issuer", "delete", issuer_id]),
        Value::Null
    );
    reqwest::blocking::Client::new()
        .delete(format!(
            "{}/v1/organizations/{}/service-accounts/{}",
            cli.url,
            account["organization_id"].as_str().unwrap(),
            account["id"].as_str().unwrap()
        ))
        .bearer_auth(&cli.token)
        .send()
        .unwrap()
        .error_for_status()
        .unwrap();
}
