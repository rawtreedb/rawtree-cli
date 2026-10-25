use std::io::{self, Write};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use super::{read_selection_input, AuthSelection};
use crate::{client::ApiClient, org};

#[derive(Debug, Serialize)]
#[serde(tag = "needs", rename_all = "lowercase")]
pub(super) enum Choices {
    Org {
        orgs: Vec<String>,
    },
    Cluster {
        organization: String,
        clusters: Vec<String>,
    },
    Database {
        organization: String,
        cluster: Option<String>,
        databases: Vec<String>,
    },
}

impl Choices {
    fn label(&self) -> &'static str {
        match self {
            Self::Org { .. } => "organization",
            Self::Cluster { .. } => "cluster",
            Self::Database { .. } => "database",
        }
    }

    fn names(&self) -> &[String] {
        match self {
            Self::Org { orgs } => orgs,
            Self::Cluster { clusters, .. } => clusters,
            Self::Database { databases, .. } => databases,
        }
    }

    fn not_found(&self, name: &str) -> anyhow::Error {
        match self {
            Self::Org { .. } => {
                anyhow::anyhow!("Organization '{}' not found for current user.", name)
            }
            Self::Cluster { organization, .. } => anyhow::anyhow!(
                "Cluster '{}' not found in organization '{}'.",
                name,
                organization
            ),
            Self::Database { organization, .. } => anyhow::anyhow!(
                "Database '{}' not found in organization '{}'.",
                name,
                organization
            ),
        }
    }
}

#[derive(Debug)]
pub(super) enum SelectionError {
    Required(Choices),
    Failed(anyhow::Error),
}

impl From<anyhow::Error> for SelectionError {
    fn from(error: anyhow::Error) -> Self {
        Self::Failed(error)
    }
}

// The sole browser-selection policy: explicit name, sole option, then either
// machine-readable choices or an interactive prompt. Empty interactive lists
// deliberately leave the default unset, preserving the existing login behavior.
fn select_or_prompt(
    choices: Choices,
    requested: Option<&str>,
    json_mode: bool,
) -> Result<Option<String>, SelectionError> {
    if let Some(name) = requested {
        return choices
            .names()
            .iter()
            .find(|candidate| candidate.as_str() == name)
            .cloned()
            .map(Some)
            .ok_or_else(|| choices.not_found(name).into());
    }
    if let [name] = choices.names() {
        return Ok(Some(name.clone()));
    }
    if json_mode {
        return Err(SelectionError::Required(choices));
    }
    if choices.names().is_empty() {
        return Ok(None);
    }
    prompt_for_selection(choices.label(), choices.names()).map_err(Into::into)
}

#[derive(Deserialize)]
struct DatabaseItem {
    name: String,
}

#[derive(Deserialize)]
struct ListDatabasesResponse {
    databases: Vec<DatabaseItem>,
}

#[derive(Deserialize)]
struct ClusterSelectionItem {
    name: String,
}

#[derive(Deserialize)]
struct ListClustersResponse {
    clusters: Vec<ClusterSelectionItem>,
}

fn prompt_for_selection(label: &str, names: &[String]) -> Result<Option<String>> {
    println!("Select {}:", label);
    for (index, name) in names.iter().enumerate() {
        println!("  {}. {}", index + 1, name);
    }

    loop {
        print!("{}: ", label);
        io::stdout().flush()?;

        let input = read_selection_input(label)?;
        if input.is_empty() {
            eprintln!("Enter a {} name or number.", label);
            continue;
        }

        if let Some(index) = parse_selection_number(&input, names.len()) {
            if let Some(name) = names.get(index) {
                return Ok(Some(name.clone()));
            }
        }

        if let Some(name) = names.iter().find(|name| name.as_str() == input.as_str()) {
            return Ok(Some(name.clone()));
        }

        eprintln!("{} '{}' was not found in the list.", label, input);
    }
}

fn parse_selection_number(input: &str, item_count: usize) -> Option<usize> {
    let selected = input.parse::<usize>().ok()?;
    if selected == 0 || selected > item_count {
        return None;
    }
    Some(selected - 1)
}

fn list_databases_for_organization(
    client: &ApiClient,
    organization_name: &str,
    cluster: Option<&str>,
) -> Result<Vec<String>> {
    let path = org::databases_collection_path(Some(organization_name), cluster);
    let resp: ListDatabasesResponse = client.get(&path)?;
    Ok(resp.databases.into_iter().map(|item| item.name).collect())
}

fn list_clusters_for_organization(
    client: &ApiClient,
    organization_name: &str,
) -> Result<Vec<String>> {
    let path = org::scoped_path("/v1/clusters", Some(organization_name), None);
    let resp: ListClustersResponse = client.get(&path)?;
    Ok(resp.clusters.into_iter().map(|item| item.name).collect())
}

pub(super) fn resolve_browser_auth_selection(
    base_url: &str,
    token: &str,
    cli_org: Option<&str>,
    cli_cluster: Option<&str>,
    cli_database: Option<&str>,
    json_mode: bool,
) -> Result<AuthSelection, SelectionError> {
    let client = ApiClient::new(base_url.to_string(), Some(token.to_string()));
    let organizations = match org::list_organizations(&client) {
        Ok(items) => items,
        Err(err)
            if json_mode
                || cli_org.is_some()
                || cli_cluster.is_some()
                || cli_database.is_some() =>
        {
            return Err(err
                .context("failed to list organizations for auth-time selection")
                .into());
        }
        Err(_) => return Ok(AuthSelection::default()),
    };
    let organization = select_or_prompt(
        Choices::Org {
            orgs: organizations.into_iter().map(|org| org.name).collect(),
        },
        cli_org,
        json_mode,
    )?;
    let Some(organization) = organization else {
        if let Some(name) = cli_cluster {
            return Err(anyhow::anyhow!(
                "Cannot select cluster '{}' because no organization is available.",
                name
            )
            .into());
        }
        if let Some(name) = cli_database {
            return Err(anyhow::anyhow!(
                "Cannot select database '{}' because no organization is available.",
                name
            )
            .into());
        }
        return Ok(AuthSelection::default());
    };
    let clusters = list_clusters_for_organization(&client, &organization).with_context(|| {
        format!(
            "failed to list clusters for organization '{}'",
            organization
        )
    })?;
    let cluster = select_or_prompt(
        Choices::Cluster {
            organization: organization.clone(),
            clusters,
        },
        cli_cluster,
        json_mode,
    )?;
    let databases = list_databases_for_organization(&client, &organization, cluster.as_deref())
        .with_context(|| {
            format!(
                "failed to list databases for organization '{}'",
                organization
            )
        });
    let database = match databases {
        Ok(databases) => select_or_prompt(
            Choices::Database {
                organization: organization.clone(),
                cluster: cluster.clone(),
                databases,
            },
            cli_database,
            json_mode,
        )?,
        Err(err) if json_mode || cli_database.is_some() => return Err(err.into()),
        Err(_) => None,
    };
    Ok(AuthSelection {
        organization: Some(organization),
        cluster,
        database,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn selection_number_is_one_based() {
        assert_eq!(parse_selection_number("1", 2), Some(0));
        assert_eq!(parse_selection_number("2", 2), Some(1));
        assert_eq!(parse_selection_number("0", 2), None);
        assert_eq!(parse_selection_number("3", 2), None);
        assert_eq!(parse_selection_number("analytics", 2), None);
    }

    #[test]
    fn browser_selection_preserves_empty_and_single_choice_behavior() {
        for json_mode in [false, true] {
            let one = Choices::Org {
                orgs: vec!["team".into()],
            };
            assert_eq!(
                select_or_prompt(one, None, json_mode).unwrap().as_deref(),
                Some("team")
            );
            let empty = Choices::Org { orgs: vec![] };
            match select_or_prompt(empty, None, json_mode) {
                Err(SelectionError::Required(Choices::Org { orgs })) if json_mode => {
                    assert!(orgs.is_empty())
                }
                Ok(None) if !json_mode => {}
                result => panic!("unexpected selection: {result:?}"),
            }
        }
    }

    #[test]
    fn browser_selection_validates_explicit_names_before_auto_selection() {
        for json_mode in [false, true] {
            for names in [
                vec![],
                vec!["team".into()],
                vec!["team".into(), "other".into()],
            ] {
                let choices = Choices::Org { orgs: names };
                let error = select_or_prompt(choices, Some("missing"), json_mode).unwrap_err();
                assert!(matches!(error, SelectionError::Failed(_)));
            }
            let choices = Choices::Org {
                orgs: vec!["team".into(), "other".into()],
            };
            assert_eq!(
                select_or_prompt(choices, Some("other"), json_mode)
                    .unwrap()
                    .as_deref(),
                Some("other")
            );
        }
    }

    #[test]
    fn browser_selection_returns_context_with_choices() {
        let choices = Choices::Database {
            organization: "team".into(),
            cluster: Some("production".into()),
            databases: vec!["analytics".into(), "billing".into()],
        };
        let Err(SelectionError::Required(choices)) = select_or_prompt(choices, None, true) else {
            panic!("expected database choices");
        };
        assert_eq!(
            serde_json::to_value(choices).unwrap(),
            serde_json::json!({
                "needs": "database", "organization": "team", "cluster": "production",
                "databases": ["analytics", "billing"],
            })
        );
    }
}
