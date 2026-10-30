use anyhow::{Context, Result};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{client::ApiClient, output};

#[derive(Clone, Copy, Debug, Deserialize, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum ServiceAccountRole {
    Member,
    Admin,
}

impl std::fmt::Display for ServiceAccountRole {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Member => "member",
            Self::Admin => "admin",
        })
    }
}

#[derive(Serialize)]
pub struct CreateServiceAccount {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub role: ServiceAccountRole,
}

#[derive(Deserialize, Serialize)]
struct ServiceAccount {
    id: String,
    organization_id: String,
    name: String,
    description: Option<String>,
    role: ServiceAccountRole,
}

#[derive(Deserialize)]
struct Organization {
    organization_id: String,
    name: String,
}

#[derive(Deserialize)]
struct OrganizationsResponse {
    organizations: Vec<Organization>,
}

pub fn parse_name(name: &str) -> Result<String, String> {
    if name.len() > 128
        || !name.split('-').all(|part| {
            !part.is_empty()
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        })
    {
        return Err("Use 1–128 lowercase letters or digits, separated by single hyphens.".into());
    }
    Ok(name.to_string())
}

pub fn parse_description(description: &str) -> Result<String, String> {
    if description.len() > 2000 || description.contains('\0') {
        return Err("Use at most 2000 UTF-8 bytes without null characters.".into());
    }
    Ok(description.to_string())
}

fn collection_path(client: &ApiClient, organization: Option<&str>) -> Result<String> {
    let response: OrganizationsResponse = client.get("/v1/organizations")?;
    let selected = match organization {
        Some(name) => response
            .organizations
            .into_iter()
            .find(|item| item.name == name)
            .with_context(|| {
                format!("Organization '{name}' was not found. Run `rtree organization list`.")
            })?,
        None => response.organizations.into_iter().next().context(
            "No organization available. Run `rtree organization list` or select one with --org.",
        )?,
    };
    Ok(format!(
        "/v1/organizations/{}/service-accounts",
        urlencoding::encode(&selected.organization_id)
    ))
}

pub fn list(client: &ApiClient, organization: Option<&str>, json_mode: bool) -> Result<()> {
    let accounts: Vec<ServiceAccount> = client.get(&collection_path(client, organization)?)?;
    output::print_result(&json!({ "service_accounts": accounts }), json_mode, |_| {
        if accounts.is_empty() {
            println!("No service accounts. Create one with `rtree service-account create <name>`.");
        } else {
            for account in &accounts {
                println!(
                    "{:<36} {:<20} {:<8} {}",
                    account.id,
                    account.name,
                    account.role,
                    account.description.as_deref().unwrap_or_default()
                );
            }
        }
    });
    Ok(())
}

pub fn create(
    client: &ApiClient,
    organization: Option<&str>,
    request: CreateServiceAccount,
    json_mode: bool,
) -> Result<()> {
    let account: ServiceAccount = client.post(
        &collection_path(client, organization)?,
        &serde_json::to_value(request)?,
    )?;
    output::print_result(&account, json_mode, |account| {
        println!(
            "Service account '{}' created (id={}, role={}).",
            account.name, account.id, account.role
        );
    });
    Ok(())
}
