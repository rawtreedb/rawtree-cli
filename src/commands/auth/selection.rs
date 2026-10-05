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

fn organization_by_name<'a>(
    organizations: &'a [org::OrganizationItem],
    name: &str,
) -> Option<&'a org::OrganizationItem> {
    organizations.iter().find(|item| item.name == name)
}

fn select_organization(
    organizations: &[org::OrganizationItem],
    cli_org: Option<&str>,
    env_org: Option<&str>,
    cfg_org: Option<&str>,
) -> Result<Option<org::OrganizationItem>> {
    if let Some(name) = cli_org {
        return organization_by_name(organizations, name)
            .cloned()
            .map(Some)
            .ok_or_else(|| anyhow::anyhow!("Organization '{}' not found for current user.", name));
    }

    if let Some(name) = env_org {
        if let Some(found) = organization_by_name(organizations, name) {
            return Ok(Some(found.clone()));
        }
    }

    if let Some(name) = cfg_org {
        if let Some(found) = organization_by_name(organizations, name) {
            return Ok(Some(found.clone()));
        }
    }

    Ok(organizations.first().cloned())
}

fn select_database(
    database_names: &[String],
    selected_org: &str,
    cli_database: Option<&str>,
) -> Result<Option<String>> {
    if let Some(name) = cli_database {
        return database_names
            .iter()
            .find(|database| database.as_str() == name)
            .cloned()
            .map(Some)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Database '{}' not found in organization '{}'.",
                    name,
                    selected_org
                )
            });
    }

    Ok(database_names.first().cloned())
}

fn select_cluster(
    cluster_names: &[String],
    selected_org: &str,
    requested_cluster: Option<&str>,
) -> Result<Option<String>> {
    if let Some(name) = requested_cluster {
        return cluster_names
            .iter()
            .find(|cluster| cluster.as_str() == name)
            .cloned()
            .map(Some)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Cluster '{}' not found in organization '{}'.",
                    name,
                    selected_org
                )
            });
    }

    Ok(cluster_names.first().cloned())
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

fn resolve_selected_database(
    database_names_result: Result<Vec<String>>,
    selected_org: &str,
    cli_database: Option<&str>,
) -> Result<Option<String>> {
    match database_names_result {
        Ok(database_names) => select_database(&database_names, selected_org, cli_database),
        Err(err) if cli_database.is_some() => Err(err),
        Err(_err) => Ok(None),
    }
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

pub(super) fn resolve_auth_selection(
    base_url: &str,
    token: &str,
    cli_org: Option<&str>,
    cli_cluster: Option<&str>,
    cli_database: Option<&str>,
    env_org: Option<&str>,
    cfg_org: Option<&str>,
) -> Result<AuthSelection> {
    let authed_client = ApiClient::new(base_url.to_string(), Some(token.to_string()));
    let organizations = match org::list_organizations(&authed_client) {
        Ok(items) => items,
        Err(err) if cli_org.is_some() || cli_cluster.is_some() || cli_database.is_some() => {
            return Err(err.context("failed to list organizations for auth-time selection"));
        }
        Err(_err) => return Ok(AuthSelection::default()),
    };

    let selected_org = select_organization(&organizations, cli_org, env_org, cfg_org)?;
    let selected_org = match selected_org {
        Some(item) => item,
        None => {
            if let Some(cluster_name) = cli_cluster {
                anyhow::bail!(
                    "Cannot select cluster '{}' because no organization is available.",
                    cluster_name
                );
            }
            if let Some(database_name) = cli_database {
                anyhow::bail!(
                    "Cannot select database '{}' because no organization is available.",
                    database_name
                );
            }
            return Ok(AuthSelection::default());
        }
    };

    let selected_cluster = select_cluster(
        &list_clusters_for_organization(&authed_client, &selected_org.name).with_context(|| {
            format!(
                "failed to list clusters for organization '{}'",
                selected_org.name
            )
        })?,
        &selected_org.name,
        cli_cluster,
    )?;

    let selected_database = resolve_selected_database(
        list_databases_for_organization(
            &authed_client,
            &selected_org.name,
            selected_cluster.as_deref(),
        )
        .with_context(|| {
            format!(
                "failed to list databases for organization '{}'",
                selected_org.name
            )
        }),
        &selected_org.name,
        cli_database,
    )?;

    Ok(AuthSelection {
        organization: Some(selected_org.name),
        cluster: selected_cluster,
        database: selected_database,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::org::OrganizationItem;

    fn sample_org(name: &str) -> OrganizationItem {
        OrganizationItem {
            name: name.to_string(),
            role: "owner".to_string(),
        }
    }

    #[test]
    fn select_organization_uses_cli_when_present() {
        let organizations = vec![sample_org("team_alpha"), sample_org("team_beta")];
        let selected = select_organization(
            &organizations,
            Some("team_beta"),
            Some("team_alpha"),
            Some("team_alpha"),
        )
        .expect("selection should succeed")
        .expect("organization should be selected");

        assert_eq!(selected.name, "team_beta");
    }

    #[test]
    fn select_organization_errors_for_unknown_cli_org() {
        let organizations = vec![sample_org("team_alpha")];
        let result = select_organization(&organizations, Some("missing"), None, None);
        assert!(result.is_err(), "unknown CLI org should fail");
    }

    #[test]
    fn select_organization_uses_env_then_cfg_then_first() {
        let organizations = vec![sample_org("team_alpha"), sample_org("team_beta")];

        let env_selected = select_organization(&organizations, None, Some("team_beta"), None)
            .expect("env selection should succeed")
            .expect("organization should exist");
        assert_eq!(env_selected.name, "team_beta");

        let cfg_selected =
            select_organization(&organizations, None, Some("missing"), Some("team_beta"))
                .expect("cfg selection should succeed")
                .expect("organization should exist");
        assert_eq!(cfg_selected.name, "team_beta");

        let first_selected =
            select_organization(&organizations, None, Some("missing"), Some("also_missing"))
                .expect("fallback selection should succeed")
                .expect("organization should exist");
        assert_eq!(first_selected.name, "team_alpha");
    }

    #[test]
    fn select_database_prefers_cli_and_fails_when_unknown() {
        let databases = vec!["analytics".to_string(), "billing".to_string()];

        let selected = select_database(&databases, "team_alpha", Some("billing"))
            .expect("selection should succeed")
            .expect("database should exist");
        assert_eq!(selected, "billing");

        let err = select_database(&databases, "team_alpha", Some("missing"));
        assert!(err.is_err(), "unknown CLI database should fail");
    }

    #[test]
    fn select_cluster_prefers_requested_name_and_fails_when_unknown() {
        let clusters = vec!["production".to_string(), "staging".to_string()];

        let selected = select_cluster(&clusters, "team_alpha", Some("staging"))
            .expect("selection should succeed")
            .expect("cluster should exist");
        assert_eq!(selected, "staging");

        let err = select_cluster(&clusters, "team_alpha", Some("missing"));
        assert!(err.is_err(), "unknown cluster should fail");
    }

    #[test]
    fn select_database_defaults_to_first_when_cli_missing() {
        let databases = vec!["analytics".to_string(), "billing".to_string()];
        let selected = select_database(&databases, "team_alpha", None)
            .expect("selection should succeed")
            .expect("first database should be selected");
        assert_eq!(selected, "analytics");
    }

    #[test]
    fn selection_number_is_one_based() {
        assert_eq!(parse_selection_number("1", 2), Some(0));
        assert_eq!(parse_selection_number("2", 2), Some(1));
        assert_eq!(parse_selection_number("0", 2), None);
        assert_eq!(parse_selection_number("3", 2), None);
        assert_eq!(parse_selection_number("analytics", 2), None);
    }

    #[test]
    fn resolve_selected_database_tolerates_fetch_errors_when_cli_database_missing() {
        let result = resolve_selected_database(
            Err(anyhow::anyhow!("failed to list databases")),
            "team_alpha",
            None,
        )
        .expect("implicit selection should not fail");
        assert_eq!(result, None);
    }

    #[test]
    fn resolve_selected_database_fails_on_fetch_errors_when_cli_database_provided() {
        let result = resolve_selected_database(
            Err(anyhow::anyhow!("failed to list databases")),
            "team_alpha",
            Some("analytics"),
        );
        assert!(result.is_err(), "explicit database should remain strict");
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
