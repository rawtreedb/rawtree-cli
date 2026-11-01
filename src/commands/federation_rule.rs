use std::collections::BTreeMap;

use anyhow::{Context, Result};
use clap::{Args, Subcommand, ValueEnum};
use serde::Serialize;
use serde_json::Value;

use super::service_account::{parse_description, parse_name};
use crate::{client::ApiClient, org, output};

#[derive(Clone, Debug, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
#[value(rename_all = "snake_case")]
pub enum PrincipalType {
    User,
    ServiceAccount,
}

#[derive(Clone, Debug, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
#[value(rename_all = "snake_case")]
pub enum Permission {
    Admin,
    ReadOnly,
    ReadWrite,
}

#[derive(Args, Serialize)]
pub struct Configuration {
    #[arg(long, value_parser = parse_name)]
    name: String,
    #[arg(long, value_parser = parse_description)]
    description: Option<String>,
    #[arg(long)]
    issuer_id: String,
    /// Repeat this flag for each allowed audience
    #[arg(long, required = true)]
    allowed_audiences: Vec<String>,
    /// JSON object of exact string claim/value matches, e.g. '{"sub":"repo:acme/app:ref:refs/heads/main"}'
    #[arg(long, value_parser = parse_conditions)]
    claim_conditions: BTreeMap<String, String>,
    #[arg(long, value_enum)]
    principal_type: PrincipalType,
    #[arg(long)]
    principal_id: String,
    #[arg(
        long,
        value_enum,
        required_unless_present = "database_roles",
        conflicts_with = "database_roles"
    )]
    #[serde(skip_serializing_if = "Option::is_none")]
    permission: Option<Permission>,
    /// Repeat this flag for each database role; cannot be combined with permission
    #[arg(
        long,
        required_unless_present = "permission",
        conflicts_with = "permission"
    )]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    database_roles: Vec<String>,
    /// Default database for the workload, not the CLI's active database
    #[arg(long)]
    database: String,
    /// Whether the rule permits token exchange: true or false
    #[arg(long, action = clap::ArgAction::Set)]
    enabled: bool,
}

fn parse_conditions(value: &str) -> Result<BTreeMap<String, String>, String> {
    let conditions: BTreeMap<String, String> = serde_json::from_str(value)
        .map_err(|_| "Expected a JSON object with string claim names and string values")?;
    if conditions.is_empty()
        || conditions.len() > 32
        || conditions.iter().any(|(key, value)| {
            key.is_empty()
                || value.is_empty()
                || matches!(key.as_str(), "iss" | "aud" | "exp" | "nbf" | "iat" | "jti")
        })
    {
        return Err("Provide 1–32 nonempty claim conditions; iss/aud and time/token-ID claims are not conditions".into());
    }
    Ok(conditions)
}

#[derive(Subcommand)]
pub enum Command {
    List,
    Get {
        id: String,
    },
    Create(Configuration),
    /// Replace the complete configuration (PUT); omitted optional fields are cleared
    Update {
        id: String,
        #[command(flatten)]
        configuration: Configuration,
    },
    Delete {
        id: String,
    },
}

pub fn run(
    client: &ApiClient,
    organization: Option<&str>,
    cluster: Option<&str>,
    action: Command,
    json: bool,
) -> Result<()> {
    let organization = organization
        .context("Select an organization with --org, RAWTREE_ORG or `rtree organization use`")?;
    let cluster = cluster
        .context("Select a cluster with --cluster, RAWTREE_CLUSTER or `rtree cluster use`")?;
    let path = |id: Option<&str>| {
        let base = match id {
            Some(id) => format!("/v1/federation-rules/{}", urlencoding::encode(id)),
            None => "/v1/federation-rules".into(),
        };
        org::scoped_path(&base, Some(organization), Some(cluster))
    };
    let result: Value = match action {
        Command::List => client.get(&path(None))?,
        Command::Get { id } => client.get(&path(Some(&id)))?,
        Command::Create(configuration) => {
            client.post(&path(None), &serde_json::to_value(configuration)?)?
        }
        Command::Update { id, configuration } => {
            client.put(&path(Some(&id)), &serde_json::to_value(configuration)?)?
        }
        Command::Delete { id } => client.delete(&path(Some(&id)))?,
    };
    output::print_result(&result, json, |value| println!("{value:#}"));
    Ok(())
}
