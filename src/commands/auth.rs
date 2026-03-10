use anyhow::Result;
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

fn apply_auth_config(
    cfg: &mut config::Config,
    base_url: &str,
    resp: &AuthResponse,
    default_organization: Option<String>,
) {
    cfg.token = Some(resp.token.clone());
    cfg.email = Some(resp.email.clone());
    cfg.default_organization = default_organization;
    if cfg.url.is_none() && base_url != "https://app.rawtree.dev" {
        cfg.url = Some(base_url.to_string());
    }
}

fn resolve_default_organization(base_url: &str, token: &str) -> Option<String> {
    let authed_client = ApiClient::new(base_url.to_string(), Some(token.to_string()));
    org::first_organization_name(&authed_client)
}

fn update_and_save_config(client: &ApiClient, resp: &AuthResponse) -> Result<()> {
    let mut cfg = config::load()?;
    let default_organization = resolve_default_organization(&client.base_url, &resp.token);
    apply_auth_config(&mut cfg, &client.base_url, resp, default_organization);
    config::save(&cfg)?;
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::{apply_auth_config, AuthResponse};
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
            "https://api.rawtree.dev",
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
        apply_auth_config(&mut cfg, "https://api.rawtree.dev", &resp, None);

        assert_eq!(cfg.default_organization, None);
    }
}
