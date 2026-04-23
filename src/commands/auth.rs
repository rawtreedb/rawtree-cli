use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use reqwest::blocking::Client as HttpClient;
use serde::Deserialize;
use serde_json::json;

use crate::client::ApiClient;
use crate::config;
use crate::org;
use crate::output;

#[derive(Deserialize)]
struct AuthResponse {
    token: String,
    email: String,
}

#[derive(Deserialize)]
struct CliDeviceStartResponse {
    device_code: String,
    user_code: String,
    verification_uri: String,
    verification_uri_complete: String,
    expires_in: u64,
    interval: u64,
}

#[derive(Deserialize)]
struct CliDeviceTokenResponse {
    token: String,
    user_id: String,
    email: String,
}

#[derive(Deserialize)]
struct ApiErrorResponse {
    error: String,
    message: String,
    hint: Option<String>,
}

enum CliDeviceTokenPoll {
    Pending,
    Approved(CliDeviceTokenResponse),
}

fn apply_auth_config(
    cfg: &mut config::Config,
    resp: &AuthResponse,
    default_organization: Option<String>,
) {
    cfg.token = Some(resp.token.clone());
    cfg.email = Some(resp.email.clone());
    cfg.default_organization = default_organization;
}

fn resolve_default_organization(base_url: &str, token: &str) -> Option<String> {
    let authed_client = ApiClient::new(base_url.to_string(), Some(token.to_string()));
    org::first_organization_name(&authed_client)
}

fn update_and_save_config(client: &ApiClient, resp: &AuthResponse) -> Result<()> {
    let mut cfg = config::load()?;
    let default_organization = resolve_default_organization(&client.base_url, &resp.token);
    apply_auth_config(&mut cfg, resp, default_organization);
    config::save(&cfg)?;
    Ok(())
}

fn clear_auth_config(cfg: &mut config::Config) {
    cfg.token = None;
    cfg.email = None;
    cfg.default_organization = None;
}

pub fn register(client: &ApiClient, email: &str, password: &str, json_mode: bool) -> Result<()> {
    let resp: AuthResponse = client.post(
        "/v1/auth/register",
        &json!({"email": email, "password": password}),
    )?;

    update_and_save_config(client, &resp)?;

    output::print_result(
        &json!({"email": resp.email, "status": "registered"}),
        json_mode,
        |_| println!("Registered and logged in as {}.", resp.email),
    );
    Ok(())
}

pub fn login(client: &ApiClient, email: &str, password: &str, json_mode: bool) -> Result<()> {
    let resp: AuthResponse = client.post(
        "/v1/auth/login",
        &json!({"email": email, "password": password}),
    )?;

    update_and_save_config(client, &resp)?;

    output::print_result(
        &json!({"email": resp.email, "status": "logged_in"}),
        json_mode,
        |_| println!("Logged in as {}.", resp.email),
    );
    Ok(())
}

fn effective_timeout_seconds(requested_timeout_seconds: u64, expires_in: u64) -> u64 {
    if requested_timeout_seconds == 0 {
        return expires_in;
    }
    requested_timeout_seconds.min(expires_in)
}

fn format_api_error(status: u16, body: &str) -> anyhow::Error {
    if let Ok(parsed) = serde_json::from_str::<ApiErrorResponse>(body) {
        if let Some(hint) = parsed.hint.as_deref() {
            if !hint.is_empty() {
                return anyhow::anyhow!(
                    "Server error ({}): {}\nHint: {}",
                    status,
                    parsed.message,
                    hint
                );
            }
        }
        return anyhow::anyhow!("Server error ({}): {}", status, parsed.message);
    }
    anyhow::anyhow!("Server error ({}): {}", status, body)
}

fn poll_cli_device_token(base_url: &str, device_code: &str) -> Result<CliDeviceTokenPoll> {
    let url = format!("{}{}", base_url, "/v1/auth/cli/device/token");
    let response = HttpClient::new()
        .post(&url)
        .json(&json!({"device_code": device_code}))
        .send()
        .context("failed to connect to server")?;

    let status = response.status();
    let status_code = status.as_u16();
    let body = response.text().context("failed to read response body")?;

    if status.is_success() {
        let parsed = serde_json::from_str::<CliDeviceTokenResponse>(&body)
            .context("failed to parse server response")?;
        return Ok(CliDeviceTokenPoll::Approved(parsed));
    }

    if status_code == 428 {
        return Ok(CliDeviceTokenPoll::Pending);
    }

    if let Ok(parsed) = serde_json::from_str::<ApiErrorResponse>(&body) {
        if parsed.error == "authorization_pending" {
            return Ok(CliDeviceTokenPoll::Pending);
        }
    }

    Err(format_api_error(status_code, &body))
}

pub fn login_with_browser(
    client: &ApiClient,
    no_browser: bool,
    timeout_seconds: u64,
    json_mode: bool,
) -> Result<()> {
    let start: CliDeviceStartResponse = client.post("/v1/auth/cli/device/start", &json!({}))?;
    let total_timeout_seconds = effective_timeout_seconds(timeout_seconds, start.expires_in);
    let poll_interval_seconds = start.interval.max(1);

    if !json_mode {
        println!("CLI login code: {}", start.user_code);
        if no_browser {
            println!(
                "Open this URL to continue login: {}",
                start.verification_uri_complete
            );
        } else if let Err(error) = webbrowser::open(&start.verification_uri_complete) {
            eprintln!("Warning: failed to open browser automatically ({}).", error);
            println!(
                "Open this URL to continue login: {}",
                start.verification_uri_complete
            );
        } else {
            println!("Opened browser for login: {}", start.verification_uri);
            println!(
                "If it did not open correctly, visit: {}",
                start.verification_uri_complete
            );
        }
        println!("Waiting for approval...");
    }

    let deadline = Instant::now() + Duration::from_secs(total_timeout_seconds);
    loop {
        match poll_cli_device_token(&client.base_url, &start.device_code)? {
            CliDeviceTokenPoll::Approved(resp) => {
                let CliDeviceTokenResponse {
                    token,
                    user_id: _user_id,
                    email,
                } = resp;
                let auth = AuthResponse { token, email };
                update_and_save_config(client, &auth)?;
                output::print_result(
                    &json!({"email": auth.email, "status": "logged_in", "method": "browser"}),
                    json_mode,
                    |_| println!("Logged in as {}.", auth.email),
                );
                return Ok(());
            }
            CliDeviceTokenPoll::Pending => {
                if Instant::now() >= deadline {
                    anyhow::bail!(
                        "Browser login timed out after {} seconds. Run `rtree login` to try again.",
                        total_timeout_seconds
                    );
                }
                thread::sleep(Duration::from_secs(poll_interval_seconds));
            }
        }
    }
}

pub fn logout(json_mode: bool) -> Result<()> {
    let mut cfg = config::load()?;
    clear_auth_config(&mut cfg);
    config::save(&cfg)?;

    output::print_result(&json!({"status": "logged_out"}), json_mode, |_| {
        println!("Logged out. Credentials removed from local config.");
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{apply_auth_config, clear_auth_config, effective_timeout_seconds, AuthResponse};
    use crate::config::Config;

    fn sample_auth_response() -> AuthResponse {
        AuthResponse {
            token: "jwt".to_string(),
            email: "user@example.com".to_string(),
        }
    }

    #[test]
    fn apply_auth_config_sets_default_organization() {
        let mut cfg = Config::default();
        let resp = sample_auth_response();
        apply_auth_config(
            &mut cfg,
            &resp,
            Some("team_alpha".to_string()),
        );

        assert_eq!(cfg.token.as_deref(), Some("jwt"));
        assert_eq!(cfg.email.as_deref(), Some("user@example.com"));
        assert_eq!(cfg.default_organization.as_deref(), Some("team_alpha"));
    }

    #[test]
    fn apply_auth_config_clears_default_organization_when_missing() {
        let mut cfg = Config {
            default_organization: Some("old_team".to_string()),
            ..Config::default()
        };
        let resp = sample_auth_response();
        apply_auth_config(&mut cfg, &resp, None);

        assert_eq!(cfg.default_organization, None);
    }

    #[test]
    fn clear_auth_config_removes_local_credentials() {
        let mut cfg = Config {
            token: Some("rw_temp".to_string()),
            email: Some("user@example.com".to_string()),
            default_organization: Some("team_alpha".to_string()),
            ..Config::default()
        };

        clear_auth_config(&mut cfg);

        assert_eq!(cfg.token, None);
        assert_eq!(cfg.email, None);
        assert_eq!(cfg.default_organization, None);
    }

    #[test]
    fn timeout_uses_smaller_of_requested_and_expiry() {
        assert_eq!(effective_timeout_seconds(300, 600), 300);
        assert_eq!(effective_timeout_seconds(900, 600), 600);
    }

    #[test]
    fn timeout_uses_expiry_when_requested_is_zero() {
        assert_eq!(effective_timeout_seconds(0, 600), 600);
    }
}
