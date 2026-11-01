use anyhow::Result;
use clap::{Args, Subcommand, ValueEnum};
use serde::Serialize;
use serde_json::Value;

use super::service_account::{parse_description, parse_name};
use crate::{client::ApiClient, org, output};

#[derive(Clone, Debug, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
#[value(rename_all = "snake_case")]
pub enum JwksSource {
    Discovery,
    ExplicitUrl,
}

#[derive(Args, Serialize)]
pub struct Configuration {
    #[arg(long, value_parser = parse_name)]
    name: String,
    #[arg(long, value_parser = parse_description)]
    description: Option<String>,
    /// Exact HTTPS issuer URL; preserved without normalization
    #[arg(long)]
    issuer_url: String,
    #[arg(long, value_enum, default_value = "discovery")]
    jwks_source: JwksSource,
    /// Required with explicit_url; forbidden with discovery
    #[arg(long)]
    jwks_url: Option<String>,
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
    /// Delete an issuer; rules referencing it must be removed first
    Delete {
        id: String,
    },
}

impl Configuration {
    fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            matches!(
                (&self.jwks_source, &self.jwks_url),
                (JwksSource::Discovery, None) | (JwksSource::ExplicitUrl, Some(_))
            ),
            "Provide --jwks-url only with --jwks-source explicit_url"
        );
        Ok(())
    }
}

pub fn run(
    client: &ApiClient,
    organization: Option<&str>,
    action: Command,
    json: bool,
) -> Result<()> {
    // Validate mutually dependent inputs before issuing any requests.
    match &action {
        Command::Create(config)
        | Command::Update {
            configuration: config,
            ..
        } => {
            config.validate()?;
        }
        _ => {}
    }
    let collection = format!(
        "{}/federation-issuers",
        org::organization_path(client, organization)?
    );
    let item = |id: &str| format!("{collection}/{}", urlencoding::encode(id));
    let result: Value = match action {
        Command::List => client.get(&collection)?,
        Command::Get { id } => client.get(&item(&id))?,
        Command::Create(config) => client.post(&collection, &serde_json::to_value(config)?)?,
        Command::Update { id, configuration } => {
            client.put(&item(&id), &serde_json::to_value(configuration)?)?
        }
        Command::Delete { id } => client.delete(&item(&id))?,
    };
    output::print_result(&result, json, |value| println!("{value:#}"));
    Ok(())
}
