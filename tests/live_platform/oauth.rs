use super::*;

#[test]
#[ignore = "requires a bootstrapped Platform Docker Compose stack"]
fn oauth_login_refresh_and_logout_against_real_platform() {
    let cli = LiveCli::from_environment();
    let mut child = cli
        .command(
            &[
                "login",
                "--no-browser",
                "--org",
                &cli.organization,
                "--cluster",
                &cli.cluster,
                "--database",
                "default",
                "--timeout-seconds",
                "60",
            ],
            None,
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut line = String::new();
    BufReader::new(child.stderr.take().unwrap())
        .read_line(&mut line)
        .unwrap();
    let event: Value = serde_json::from_str(&line).expect("OAuth event");
    assert_eq!(event["event"], "oauth_authorization_required");
    let http = reqwest::blocking::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let authorization = http
        .get(event["authorization_url"].as_str().unwrap())
        .send()
        .unwrap();
    assert_eq!(authorization.status(), 302);
    let consent =
        reqwest::Url::parse(authorization.headers()["location"].to_str().unwrap()).unwrap();
    let request_id = consent.path_segments().unwrap().next_back().unwrap();
    let decision: Value = http
        .post(format!(
            "{}/v1/oauth/authorization-requests/{request_id}/decision",
            cli.url
        ))
        .bearer_auth(&cli.token)
        .json(&json!({"decision":"approve"}))
        .send()
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .unwrap();
    assert!(http
        .get(decision["redirect_to"].as_str().unwrap())
        .send()
        .unwrap()
        .status()
        .is_success());
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "OAuth login failed");
    let login: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(login["method"], "oauth");
    let path = cli.home.path().join(".config/rtree/config.json");
    let mut saved: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let old_refresh = saved["oauth"]["refresh_token"].clone();
    // Force the refresh path without waiting fifteen minutes.
    saved["oauth"]["expires_at"] = json!(0);
    std::fs::write(&path, saved.to_string()).unwrap();
    assert!(cli.json_with_token(&["database", "list"], None)["databases"].is_array());
    let refreshed: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert!(
        refreshed["oauth"]["refresh_token"] != old_refresh,
        "refresh token did not rotate"
    );
    let access = refreshed["token"].as_str().unwrap();
    assert_eq!(
        cli.json_with_token(&["logout"], None)["status"],
        "logged_out"
    );
    assert_eq!(
        http.get(format!("{}/v1/organizations", cli.url))
            .bearer_auth(access)
            .send()
            .unwrap()
            .status(),
        401
    );
    // Revoking CLI OAuth must leave the independent first-party seed session active.
    assert!(cli.json(&["database", "list"])["databases"].is_array());
}
