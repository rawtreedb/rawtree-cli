use anyhow::Result;
use serde::Deserialize;
use serde_json::json;

use crate::client::ApiClient;
use crate::config;
use crate::output;

#[derive(Deserialize)]
struct AuthResponse {
    token: String,
    email: String,
}

fn update_and_save_config(client: &ApiClient, resp: &AuthResponse) -> Result<()> {
    let mut cfg = config::load()?;
    cfg.token = Some(resp.token.clone());
    cfg.email = Some(resp.email.clone());
    if cfg.url.is_none() && client.base_url != "https://app.rawtree.dev" {
        cfg.url = Some(client.base_url.clone());
    }
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
