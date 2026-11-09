mod common;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use reqwest::Url;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

type Response = (String, &'static str, Value);

fn access_token() -> String {
    format!(
        "header.{}.signature",
        URL_SAFE_NO_PAD.encode(r#"{"email":"user@example.test"}"#)
    )
}

fn login_responses() -> Vec<Response> {
    vec![
        (
            "GET /.well-known/oauth-authorization-server".into(),
            "200 OK",
            json!({
                "issuer": "__API_URL__", "authorization_endpoint": "__API_URL__/oauth/authorize",
                "registration_endpoint": "__API_URL__/oauth/register", "token_endpoint": "__API_URL__/oauth/token",
                "revocation_endpoint": "__API_URL__/oauth/revoke"
            }),
        ),
        (
            "POST /oauth/register".into(),
            "201 Created",
            json!({"client_id":"cli-installation"}),
        ),
        (
            "POST /oauth/token".into(),
            "200 OK",
            json!({"access_token":access_token(),"refresh_token":"refresh-secret","expires_in":900,"token_type":"Bearer"}),
        ),
        (
            "GET /v1/organizations".into(),
            "200 OK",
            json!({"organizations":[{"name":"team","organization_id":"team-id","role":"owner"}]}),
        ),
        (
            "GET /v1/organizations".into(),
            "200 OK",
            json!({"organizations":[{"name":"team","organization_id":"team-id","role":"owner"}]}),
        ),
        (
            "GET /v1/regions?organization_id=team-id".into(),
            "200 OK",
            json!({"regions":[{"id":"legacy","name":"Virginia","cloud_region":"us-east-1","deployment_id":null,"enabled":true}]}),
        ),
        (
            "GET /v1/clusters?organization=team".into(),
            "200 OK",
            json!({"clusters":[{"id":"production","name":"production"}]}),
        ),
        (
            "GET /v1/databases?organization=team&cluster=production".into(),
            "200 OK",
            json!({"databases":[{"name":"analytics"}]}),
        ),
    ]
}

fn callback(line: &str) {
    let event: Value = serde_json::from_str(line).unwrap();
    assert_eq!(event["event"], "oauth_authorization_required");
    let auth = Url::parse(event["authorization_url"].as_str().unwrap()).unwrap();
    let params: std::collections::HashMap<_, _> = auth.query_pairs().into_owned().collect();
    assert_eq!(params["response_type"], "code");
    assert_eq!(params["client_id"], "cli-installation");
    assert_eq!(params["code_challenge_method"], "S256");
    assert_eq!(params["scope"], "full_access");
    assert_eq!(params["resource"], auth.origin().ascii_serialization());
    assert_eq!(params["state"].len(), 43);
    assert_eq!(params["code_challenge"].len(), 43);
    let mut callback = Url::parse(&params["redirect_uri"]).unwrap();
    assert_eq!(callback.host_str(), Some("127.0.0.1"));
    let http = reqwest::blocking::Client::new();
    // Unsolicited and forged callbacks must not consume the listener.
    let favicon = callback.join("/favicon.ico").unwrap();
    assert_eq!(http.get(favicon).send().unwrap().status(), 400);
    callback
        .query_pairs_mut()
        .extend_pairs([("code", "injected"), ("state", "wrong")]);
    assert_eq!(http.get(callback.clone()).send().unwrap().status(), 400);
    callback.set_query(None);
    callback.query_pairs_mut().extend_pairs([
        ("code", "authorization-secret"),
        ("state", &params["state"]),
    ]);
    let response = http.get(callback).send().unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(
        response.headers()["content-type"],
        "text/plain; charset=utf-8"
    );
    assert_eq!(
        response.headers()["content-security-policy"],
        "default-src 'none'"
    );
    assert_eq!(response.headers()["connection"], "close");
    assert!(
        response.headers()["content-length"]
            .to_str()
            .unwrap()
            .parse::<usize>()
            .unwrap()
            > 0
    );
    assert!(!response.text().unwrap().contains("authorization-secret"));
}

fn run_login(
    responses: &[Response],
    args: &[&str],
    original: &Value,
) -> (std::process::Output, Value, Vec<Value>) {
    common::run_cli_with_progress(
        responses,
        args,
        original,
        Some(common::ProgressExpectation {
            before_response: "POST /oauth/token",
            observe: callback,
        }),
    )
}

#[test]
fn default_login_uses_oauth_pkce_and_persists_refresh_credentials() {
    let (output, saved, bodies) = run_login(
        &login_responses(),
        &["login", "--no-browser"],
        &json!({"token":"previous-credential","url":"https://old.example"}),
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["method"], "oauth");
    assert_eq!(result["email"], "user@example.test");
    assert_eq!(saved["token"], access_token());
    assert_eq!(saved["oauth"]["refresh_token"], "refresh-secret");
    assert_eq!(saved["oauth"]["client_id"], "cli-installation");
    assert_eq!(saved["url"], saved["oauth"]["api_url"]);
    assert_eq!(saved["database"], "analytics");
    assert_eq!(
        bodies[1],
        json!({
            "client_name":"RawTree CLI", "application_type":"native",
            "redirect_uris":[bodies[2]["redirect_uri"]], "grant_types":["authorization_code","refresh_token"],
            "response_types":["code"], "token_endpoint_auth_method":"none", "scope":"full_access"
        })
    );
    assert_eq!(bodies[1]["application_type"], "native");
    assert_eq!(bodies[1]["token_endpoint_auth_method"], "none");
    assert_eq!(bodies[2]["grant_type"], "authorization_code");
    assert_eq!(bodies[2]["code"], "authorization-secret");
    assert_eq!(bodies[2]["client_id"], "cli-installation");
    assert_eq!(bodies[2]["resource"], saved["oauth"]["resource"]);
    assert_eq!(bodies[2]["redirect_uri"], bodies[1]["redirect_uris"][0]);
    let event: Value = serde_json::from_str(
        String::from_utf8_lossy(&output.stderr)
            .lines()
            .next()
            .unwrap(),
    )
    .unwrap();
    let url = Url::parse(event["authorization_url"].as_str().unwrap()).unwrap();
    let challenge = url
        .query_pairs()
        .find(|(key, _)| key == "code_challenge")
        .unwrap()
        .1
        .into_owned();
    assert_eq!(
        challenge,
        URL_SAFE_NO_PAD.encode(Sha256::digest(
            bodies[2]["code_verifier"].as_str().unwrap().as_bytes()
        ))
    );
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    for secret in [
        access_token(),
        "refresh-secret".into(),
        "authorization-secret".into(),
        bodies[2]["code_verifier"].as_str().unwrap().into(),
    ] {
        assert!(!text.contains(&secret));
    }
}

fn saved_oauth(expires_at: i64) -> Value {
    json!({"token":"old-access", "url":"__API_URL__", "oauth":{
        "api_url":"__API_URL__", "client_id":"cli-installation", "resource":"__API_URL__",
        "token_endpoint":"__API_URL__/oauth/token", "revocation_endpoint":"__API_URL__/oauth/revoke",
        "refresh_token":"old-refresh", "expires_at":expires_at
    }})
}

#[test]
fn expired_access_refreshes_and_persists_rotation_before_the_command() {
    let responses = [
        (
            "POST /oauth/token".into(),
            "200 OK",
            json!({"access_token":"new-access", "refresh_token":"new-refresh", "expires_in":900, "token_type":"Bearer"}),
        ),
        (
            "GET /v1/organizations".into(),
            "200 OK",
            json!({"organizations":[]}),
        ),
    ];
    let (output, saved, bodies) = common::run_cli(
        &responses,
        &["organization", "list", "--json"],
        &saved_oauth(0),
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(bodies[0]["grant_type"], "refresh_token");
    assert_eq!(bodies[0]["refresh_token"], "old-refresh");
    assert_eq!(bodies[0]["client_id"], "cli-installation");
    assert_eq!(bodies[0]["resource"], saved["oauth"]["resource"]);
    assert_eq!(saved["token"], "new-access");
    assert_eq!(saved["oauth"]["refresh_token"], "new-refresh");
}

#[test]
fn valid_access_and_explicit_api_key_skip_refresh() {
    for args in [
        vec!["organization", "list", "--json"],
        vec!["organization", "list", "--json", "--api-key", "rt_explicit"],
    ] {
        let expiry = if args.len() == 3 { i64::MAX } else { 0 };
        let responses = [(
            "GET /v1/organizations".into(),
            "200 OK",
            json!({"organizations":[]}),
        )];
        let (output, saved, _) = common::run_cli(&responses, &args, &saved_oauth(expiry));
        assert!(output.status.success());
        assert_eq!(saved["oauth"]["refresh_token"], "old-refresh");
    }
}

#[test]
fn refresh_errors_do_not_call_api_or_expose_response_secrets() {
    for status in ["400 Bad Request", "503 Service Unavailable"] {
        let responses = [(
            "POST /oauth/token".into(),
            status,
            json!({"error":"invalid_grant", "error_description":"old-refresh"}),
        )];
        let (output, saved, _) = common::run_cli(
            &responses,
            &["organization", "list", "--json"],
            &saved_oauth(0),
        );
        assert!(!output.status.success());
        assert_eq!(saved["token"], "old-access");
        assert_eq!(saved["oauth"]["refresh_token"], "old-refresh");
        assert!(!String::from_utf8_lossy(&output.stderr).contains("old-refresh"));
    }
}

#[test]
fn logout_revokes_without_refresh_and_retains_credentials_on_failure() {
    for status in ["200 OK", "503 Service Unavailable"] {
        let responses = [("POST /oauth/revoke".into(), status, json!({}))];
        let (output, saved, bodies) =
            common::run_cli(&responses, &["logout", "--json"], &saved_oauth(0));
        assert_eq!(bodies[0]["token"], "old-refresh");
        assert_eq!(bodies[0]["token_type_hint"], "refresh_token");
        assert_eq!(bodies[0]["client_id"], "cli-installation");
        if status == "200 OK" {
            assert!(output.status.success());
            assert_eq!(saved["token"], Value::Null);
            assert_eq!(saved["oauth"], Value::Null);
        } else {
            assert!(!output.status.success());
            assert_eq!(saved["token"], "old-access");
        }
    }
}

#[test]
fn api_key_login_clears_old_oauth_state() {
    let responses = [("GET /v1/keys".into(), "200 OK", json!({"keys":[]}))];
    let (output, saved, _) = common::run_cli(
        &responses,
        &["login", "--api-key", "rt_explicit"],
        &saved_oauth(0),
    );
    assert!(output.status.success());
    assert_eq!(saved["token"], "rt_explicit");
    assert_eq!(saved["oauth"], Value::Null);
}

#[test]
fn incompatible_device_and_api_key_flags_fail_without_network() {
    let (output, _, _) = common::run_cli(
        &[],
        &["login", "--device-auth", "--api-key", "rt_key"],
        &json!({}),
    );
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("cannot be used"));
}

#[test]
fn regional_api_can_discover_a_canonical_authorization_server() {
    let mut responses = login_responses();
    responses[0].2 = serde_json::from_str(
        &responses[0]
            .2
            .to_string()
            .replace("__API_URL__", "__ISSUER_URL__"),
    )
    .unwrap();
    let (output, saved, _) = run_login(&responses, &["login", "--no-browser"], &json!({}));
    assert!(output.status.success());
    assert_ne!(saved["oauth"]["resource"], saved["oauth"]["api_url"]);
    for action in ["refresh", "revoke"] {
        let mut cfg = saved_oauth(0);
        for field in ["resource", "token_endpoint", "revocation_endpoint"] {
            cfg["oauth"][field] = json!(cfg["oauth"][field]
                .as_str()
                .unwrap()
                .replace("__API_URL__", "__ISSUER_URL__"));
        }
        let (responses, args) = if action == "refresh" {
            (
                vec![
                    (
                        "POST /oauth/token".into(),
                        "200 OK",
                        json!({"access_token":"new-access", "refresh_token":"new-refresh", "expires_in":900, "token_type":"Bearer"}),
                    ),
                    (
                        "GET /v1/organizations".into(),
                        "200 OK",
                        json!({"organizations":[]}),
                    ),
                ],
                vec!["organization", "list", "--json"],
            )
        } else {
            (
                vec![("POST /oauth/revoke".into(), "200 OK", json!({}))],
                vec!["logout", "--json"],
            )
        };
        let (output, _, _) = common::run_cli(&responses, &args, &cfg);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn local_commands_do_not_refresh_expired_oauth() {
    for args in [
        vec!["status"],
        vec!["database", "use", "analytics"],
        vec!["cluster", "use", "production"],
        vec!["organization", "use", "team"],
    ] {
        let (output, saved, _) = common::run_cli(&[], &args, &saved_oauth(0));
        assert!(output.status.success());
        assert_eq!(saved["oauth"]["refresh_token"], "old-refresh");
    }
}

#[test]
fn oauth_refuses_to_send_saved_credentials_to_another_api() {
    let mut original = saved_oauth(i64::MAX);
    original["oauth"]["api_url"] = json!("https://different.example");
    let (output, _, _) = common::run_cli(&[], &["organization", "list", "--json"], &original);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("different API URL"));
}

#[test]
fn callback_timeout_preserves_existing_credentials() {
    let responses = login_responses();
    let original = json!({"token":"previous-credential"});
    let (output, saved, _) = common::run_cli(
        &responses[..2],
        &["login", "--no-browser", "--timeout-seconds", "1"],
        &original,
    );
    assert!(!output.status.success());
    assert_eq!(saved, original);
    assert!(String::from_utf8_lossy(&output.stderr).contains("timed out"));
}

#[test]
fn token_exchange_failure_and_incomplete_selection_preserve_config() {
    for incomplete_selection in [false, true] {
        let mut responses = login_responses();
        if incomplete_selection {
            responses.truncate(4);
            responses[3].2 = json!({"organizations":[]});
        } else {
            responses.truncate(3);
            responses[2].1 = "400 Bad Request";
            responses[2].2 = json!({"error":"invalid_grant"});
        }
        let original = json!({"token":"previous-credential"});
        let (output, saved, _) = run_login(&responses, &["login", "--no-browser"], &original);
        assert!(!output.status.success());
        assert_eq!(saved, original);
        if incomplete_selection {
            assert_eq!(output.status.code(), Some(2));
        }
    }
}

#[test]
fn malformed_successful_tokens_never_expose_response_values() {
    for field in ["access_token", "refresh_token", "token_type", "expires_in"] {
        let mut body = json!({"access_token":"new-access", "refresh_token":"new-refresh", "expires_in":900, "token_type":"Bearer"});
        body[field] = if field == "expires_in" {
            json!("private-token-marker")
        } else {
            json!({"private-token-marker":true})
        };
        let responses = [("POST /oauth/token".into(), "200 OK", body)];
        let (output, _, _) = common::run_cli(
            &responses,
            &["organization", "list", "--json"],
            &saved_oauth(0),
        );
        assert!(!output.status.success());
        assert!(
            !String::from_utf8_lossy(&output.stderr).contains("private-token-marker"),
            "token response data leaked"
        );
    }
}

#[test]
fn token_responses_require_each_field_and_accept_case_insensitive_bearer() {
    for exchange in [false, true] {
        for (field, value) in [
            ("token_type", json!("Basic")),
            ("access_token", json!("")),
            ("refresh_token", json!("")),
            ("expires_in", json!(0)),
        ] {
            let mut responses = login_responses();
            responses.truncate(3);
            responses[2].2[field] = value;
            let (output, saved, _) = if exchange {
                run_login(
                    &responses,
                    &["login", "--no-browser"],
                    &json!({"token":"old-access"}),
                )
            } else {
                common::run_cli(
                    &responses[2..],
                    &["organization", "list", "--json"],
                    &saved_oauth(0),
                )
            };
            assert!(!output.status.success(), "invalid {field} accepted");
            assert_eq!(saved["token"], "old-access");
        }
    }
    let mut responses = login_responses();
    responses[2].2["token_type"] = json!("bEaReR");
    let (output, _, _) = run_login(&responses, &["login", "--no-browser"], &json!({}));
    assert!(output.status.success());
}

#[test]
fn empty_registration_client_id_is_rejected_before_authorization() {
    let mut responses = login_responses();
    responses.truncate(2);
    responses[1].2["client_id"] = json!("");
    let (output, saved, _) = common::run_cli(
        &responses,
        &["login", "--no-browser"],
        &json!({"token":"old"}),
    );
    assert!(!output.status.success());
    assert_eq!(saved["token"], "old");
    assert!(!String::from_utf8_lossy(&output.stderr).contains("oauth_authorization_required"));
}

#[test]
fn every_discovered_endpoint_is_bound_to_the_issuer() {
    for field in [
        "authorization_endpoint",
        "registration_endpoint",
        "token_endpoint",
        "revocation_endpoint",
    ] {
        let mut responses = login_responses();
        responses.truncate(1);
        responses[0].2[field] = json!("https://different.example/oauth");
        let (output, _, _) = common::run_cli(&responses, &["login"], &json!({}));
        assert!(!output.status.success(), "{field}");
        assert!(String::from_utf8_lossy(&output.stderr).contains("origin"));
    }
}

#[test]
fn discovery_registration_and_exchange_do_not_follow_redirects_or_echo_errors() {
    for stage in 0..3 {
        for status in ["302 Found", "500 Internal Server Error"] {
            let mut responses = login_responses();
            responses.truncate(stage + 1);
            responses[stage].1 = status;
            responses[stage].2 = json!({"secret":"private-marker"});
            let (output, _, _) = if stage == 2 {
                run_login(&responses, &["login"], &json!({}))
            } else {
                common::run_cli(&responses, &["login"], &json!({}))
            };
            assert!(!output.status.success());
            assert!(!String::from_utf8_lossy(&output.stderr).contains("private-marker"));
        }
    }
}

#[test]
fn oauth_timeout_zero_and_large_values_are_capped_at_ten_minutes() {
    for (value, expected) in [("0", 600), ("1", 1), ("600", 600), ("900", 600)] {
        let (output, _, _) = run_login(
            &login_responses(),
            &["login", "--timeout-seconds", value],
            &json!({}),
        );
        assert!(output.status.success());
        let event: Value = serde_json::from_str(
            String::from_utf8_lossy(&output.stderr)
                .lines()
                .next()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(event["expires_in"], expected);
    }
}

#[test]
fn deleting_other_or_absent_organizations_preserves_defaults() {
    for (deleted, selected) in [(false, "old"), (false, "other"), (true, "other")] {
        let responses = [(
            "DELETE /v1/organizations/old".into(),
            "200 OK",
            json!({"deleted":deleted}),
        )];
        let (output, saved, _) = common::run_cli(
            &responses,
            &["organization", "delete", "old", "--json"],
            &json!({"token":"rt_saved","default_organization":selected}),
        );
        assert!(output.status.success());
        assert_eq!(saved["default_organization"], selected);
    }
}

#[test]
fn saved_oauth_accepts_a_trailing_slash_and_refreshes_within_the_safety_window() {
    for remaining in [30, 60, 120] {
        let mut responses = vec![];
        if remaining <= 60 {
            responses.push(("POST /oauth/token".into(), "200 OK", json!({"access_token":"new-access","refresh_token":"new-refresh","token_type":"Bearer","expires_in":900})));
        }
        responses.push((
            "GET /v1/organizations".into(),
            "200 OK",
            json!({"organizations":[]}),
        ));
        let (output, saved, _) = common::run_cli(
            &responses,
            &[
                "--api-url",
                "__API_URL__/",
                "organization",
                "list",
                "--json",
            ],
            &saved_oauth(chrono::Utc::now().timestamp() + remaining),
        );
        assert!(output.status.success());
        assert_eq!(
            saved["token"],
            if remaining <= 60 {
                "new-access"
            } else {
                "old-access"
            }
        );
    }
}
