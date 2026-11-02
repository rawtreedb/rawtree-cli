use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::Value;

use super::open;
use super::table_output::new_cli_table;
use crate::client::ApiClient;
use crate::output;

#[derive(Deserialize)]
struct IntegrationsResponse {
    integrations: Vec<Integration>,
    #[serde(default)]
    slack_available: bool,
}

#[derive(Deserialize)]
struct Integration {
    id: String,
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    settings: Value,
    created_at: String,
}

fn integrations_path(organization: &str) -> String {
    format!(
        "/v1/organizations/{}/integrations",
        urlencoding::encode(organization)
    )
}

pub(crate) fn integrations_url(base_url: &str, organization: &str) -> String {
    format!(
        "{}/{}/~/settings#integrations",
        base_url.trim_end_matches('/'),
        urlencoding::encode(organization)
    )
}

fn print_response<T: serde::de::DeserializeOwned>(
    value: Value,
    json_mode: bool,
    human: impl FnOnce(T),
) -> Result<()> {
    if json_mode {
        output::print_result(&value, true, |_| {});
    } else {
        human(serde_json::from_value(value).context("failed to parse server response")?);
    }
    Ok(())
}

fn describe_settings(integration: &Integration) -> String {
    let settings = &integration.settings;
    match integration.kind.as_str() {
        "slack" => format!(
            "{} ({})",
            settings["team_name"].as_str().unwrap_or("?"),
            settings["team_id"].as_str().unwrap_or("?")
        ),
        _ => settings.to_string(),
    }
}

pub fn list(client: &ApiClient, organization: &str, json_mode: bool) -> Result<()> {
    let value: Value = client.get(&integrations_path(organization))?;
    print_response(value, json_mode, |resp: IntegrationsResponse| {
        if resp.integrations.is_empty() {
            if resp.slack_available {
                println!("No integrations. Connect Slack with `rtree integration add slack`.");
            } else {
                println!("No integrations.");
            }
            return;
        }
        let mut table = new_cli_table();
        table.set_header(vec!["id", "type", "workspace", "created"]);
        for integration in &resp.integrations {
            table.add_row(vec![
                integration.id.clone(),
                integration.kind.clone(),
                describe_settings(integration),
                integration.created_at.clone(),
            ]);
        }
        println!("{table}");
    })
}

pub fn add_slack(ui_base_url: &str, organization: &str, json_mode: bool) -> Result<()> {
    open::open_url(&integrations_url(ui_base_url, organization), json_mode)?;
    if !json_mode {
        println!("Connect Slack there, then find its ID with `rtree integration list`.");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_encoded_settings_url() {
        assert_eq!(
            integrations_url("https://rawtree.com/", "team alpha"),
            "https://rawtree.com/team%20alpha/~/settings#integrations"
        );
    }
}
