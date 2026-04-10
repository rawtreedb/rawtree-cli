use anyhow::Result;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::client::ApiClient;
use crate::config;
use crate::org;
use crate::output;

#[derive(Deserialize)]
struct ProjectItem {
    #[serde(alias = "project_name")]
    name: String,
    #[serde(default, alias = "organization_id")]
    organization_name: Option<String>,
    created_at: String,
}

#[derive(Deserialize)]
struct ListProjectsResponse {
    projects: Vec<ProjectItem>,
}

#[derive(Deserialize)]
struct CreateProjectResponse {
    #[serde(alias = "project")]
    name: String,
    api_key: String,
    organization_name: Option<String>,
    #[serde(default)]
    temporary: bool,
    #[serde(default)]
    claim_token: Option<String>,
}

pub struct CreatedProjectInfo {
    pub project: String,
    pub api_key: String,
    pub claim_token: Option<String>,
}

fn build_claim_dashboard_url(claim_token: &str) -> String {
    let ui_base_url = crate::commands::open::resolve_ui_base_url();
    format!(
        "{}/claim/{}/dashboard",
        ui_base_url.trim_end_matches('/'),
        urlencoding::encode(claim_token)
    )
}

fn claim_url_from_response(resp: &CreateProjectResponse) -> Option<String> {
    resp.claim_token.as_deref().map(build_claim_dashboard_url)
}

fn is_temporary_project(resp: &CreateProjectResponse) -> bool {
    resp.temporary || resp.claim_token.is_some()
}

fn apply_project_create_config(cfg: &mut config::Config, resp: &CreateProjectResponse) {
    let is_temporary = is_temporary_project(resp);
    cfg.default_project = Some(resp.name.clone());
    cfg.last_claim_token = resp.claim_token.clone();

    if is_temporary {
        cfg.default_organization = resp.organization_name.clone();
    } else if let Some(ref organization_name) = resp.organization_name {
        cfg.default_organization = Some(organization_name.clone());
    }

    if is_temporary {
        cfg.token = Some(resp.api_key.clone());
        cfg.email = None;
    }
}

fn create_project_response(
    client: &ApiClient,
    name: Option<&str>,
    organization: Option<&str>,
) -> Result<CreateProjectResponse> {
    let path = projects_collection_path(client, organization)?;

    let mut payload = serde_json::Map::new();
    if let Some(project_name) = name {
        payload.insert(
            "project".to_string(),
            Value::String(project_name.to_string()),
        );
    }

    client.post(&path, &Value::Object(payload))
}

fn create_and_persist(
    client: &ApiClient,
    name: Option<&str>,
    organization: Option<&str>,
) -> Result<CreateProjectResponse> {
    let resp = create_project_response(client, name, organization)?;
    let mut cfg = config::load()?;
    apply_project_create_config(&mut cfg, &resp);
    config::save(&cfg)?;
    Ok(resp)
}

fn projects_collection_path(client: &ApiClient, organization: Option<&str>) -> Result<String> {
    match organization {
        Some(org_name) => {
            let org_id = org::resolve_organization_id(client, org_name)?;
            Ok(format!(
                "/v1/projects?organization_id={}",
                urlencoding::encode(&org_id)
            ))
        }
        None => Ok("/v1/projects".to_string()),
    }
}

pub fn list(client: &ApiClient, organization: Option<&str>, json_mode: bool) -> Result<()> {
    let path = projects_collection_path(client, organization)?;
    let resp: ListProjectsResponse = client.get(&path)?;
    output::print_result(
        &json!({
            "projects": resp.projects.iter().map(|p| json!({
                "name": p.name,
                "organization_name": p.organization_name,
                "created_at": p.created_at,
            })).collect::<Vec<_>>()
        }),
        json_mode,
        |_| {
            if resp.projects.is_empty() {
                println!("No projects yet. Create one with `rtree project create <name>`.");
            } else {
                for p in &resp.projects {
                    let organization_name = p.organization_name.as_deref().unwrap_or("unknown");
                    println!(
                        "{:<20} org={} created={}",
                        p.name, organization_name, p.created_at
                    );
                }
            }
        },
    );
    Ok(())
}

pub fn create(
    client: &ApiClient,
    name: &str,
    organization: Option<&str>,
    json_mode: bool,
) -> Result<()> {
    let resp = create_and_persist(client, Some(name), organization)?;
    let claim_url = claim_url_from_response(&resp);
    let claim_token = resp.claim_token.clone();

    output::print_result(
        &json!({
            "name": resp.name,
            "organization_name": resp.organization_name,
            "claim_token": claim_token,
            "claim_url": claim_url,
        }),
        json_mode,
        |_| {
            let organization_name = resp.organization_name.as_deref().unwrap_or("unknown");
            println!(
                "Project '{}' created in organization '{}'.",
                resp.name, organization_name
            );
            if let Some(ref claim_url) = claim_url {
                println!("Use '{}' to claim your project.", claim_url);
            }
        },
    );
    Ok(())
}

pub fn create_for_insert(client: &ApiClient, name: Option<&str>) -> Result<CreatedProjectInfo> {
    let resp = create_and_persist(client, name, None)?;
    Ok(CreatedProjectInfo {
        project: resp.name,
        api_key: resp.api_key,
        claim_token: resp.claim_token,
    })
}

pub fn use_project(name: &str, json_mode: bool) -> Result<()> {
    let mut cfg = config::load()?;
    cfg.default_project = Some(name.to_string());
    config::save(&cfg)?;

    output::print_result(&json!({"default_project": name}), json_mode, |_| {
        println!("Default project set to '{}'.", name)
    });
    Ok(())
}

#[derive(Deserialize)]
struct RenameProjectResponse {
    #[serde(alias = "project")]
    name: String,
}

#[derive(Deserialize)]
struct DeleteProjectResponse {
    deleted: bool,
}

pub fn rename(
    client: &ApiClient,
    old: &str,
    new_name: &str,
    organization: Option<&str>,
    json_mode: bool,
) -> Result<()> {
    let path = match organization {
        Some(org_name) => format!("/v1/{org_name}/{old}"),
        None => format!("/v1/projects/{old}"),
    };
    let resp: RenameProjectResponse = client.patch(&path, &json!({"project": new_name}))?;
    output::print_result(
        &json!({"old_name": old, "name": resp.name}),
        json_mode,
        |_| println!("Project '{}' renamed to '{}'.", old, resp.name),
    );
    Ok(())
}

pub fn delete(
    client: &ApiClient,
    name: &str,
    organization: Option<&str>,
    json_mode: bool,
) -> Result<()> {
    let path = match organization {
        Some(org_name) => format!("/v1/{org_name}/{name}"),
        None => format!("/v1/projects/{name}"),
    };
    let resp: DeleteProjectResponse = client.delete(&path)?;
    output::print_result(
        &json!({"deleted": resp.deleted, "project": name}),
        json_mode,
        |_| {
            if resp.deleted {
                println!("Project '{}' deleted.", name);
            }
        },
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{apply_project_create_config, CreateProjectResponse};
    use crate::config::Config;

    #[test]
    fn apply_project_create_config_overwrites_auth_for_temporary_projects() {
        let mut cfg = Config {
            token: Some("jwt.token.value".to_string()),
            email: Some("user@example.com".to_string()),
            default_organization: Some("team_alpha".to_string()),
            ..Config::default()
        };
        let resp = CreateProjectResponse {
            name: "tmp_project".to_string(),
            api_key: "rw_temporary".to_string(),
            organization_name: Some("temp_org".to_string()),
            temporary: true,
            claim_token: Some("abc".to_string()),
        };

        apply_project_create_config(&mut cfg, &resp);

        assert_eq!(cfg.token.as_deref(), Some("rw_temporary"));
        assert_eq!(cfg.email, None);
        assert_eq!(cfg.default_organization.as_deref(), Some("temp_org"));
        assert_eq!(cfg.default_project.as_deref(), Some("tmp_project"));
        assert_eq!(cfg.last_claim_token.as_deref(), Some("abc"));
    }

    #[test]
    fn apply_project_create_config_preserves_jwt_for_standard_projects() {
        let mut cfg = Config {
            token: Some("jwt.token.value".to_string()),
            email: Some("user@example.com".to_string()),
            default_organization: Some("team_alpha".to_string()),
            ..Config::default()
        };
        let resp = CreateProjectResponse {
            name: "analytics".to_string(),
            api_key: "rw_regular".to_string(),
            organization_name: None,
            temporary: false,
            claim_token: None,
        };

        apply_project_create_config(&mut cfg, &resp);

        assert_eq!(cfg.token.as_deref(), Some("jwt.token.value"));
        assert_eq!(cfg.email.as_deref(), Some("user@example.com"));
        assert_eq!(cfg.default_organization.as_deref(), Some("team_alpha"));
        assert_eq!(cfg.default_project.as_deref(), Some("analytics"));
        assert_eq!(cfg.last_claim_token, None);
    }

    #[test]
    fn apply_project_create_config_clears_default_org_for_temporary_when_missing_org_name() {
        let mut cfg = Config {
            default_organization: Some("team_alpha".to_string()),
            ..Config::default()
        };
        let resp = CreateProjectResponse {
            name: "tmp_project".to_string(),
            api_key: "rw_temp".to_string(),
            organization_name: None,
            temporary: true,
            claim_token: None,
        };

        apply_project_create_config(&mut cfg, &resp);

        assert_eq!(cfg.default_organization, None);
    }

    #[test]
    fn apply_project_create_config_updates_default_org_when_standard_response_has_org_name() {
        let mut cfg = Config {
            default_organization: Some("old_team".to_string()),
            ..Config::default()
        };
        let resp = CreateProjectResponse {
            name: "analytics".to_string(),
            api_key: "rw_regular".to_string(),
            organization_name: Some("new_team".to_string()),
            temporary: false,
            claim_token: None,
        };

        apply_project_create_config(&mut cfg, &resp);

        assert_eq!(cfg.default_organization.as_deref(), Some("new_team"));
    }

    #[test]
    fn apply_project_create_config_infers_temporary_from_claim_token() {
        let mut cfg = Config {
            token: Some("jwt.token.value".to_string()),
            email: Some("user@example.com".to_string()),
            default_organization: Some("team_alpha".to_string()),
            ..Config::default()
        };
        let resp = CreateProjectResponse {
            name: "tmp_project".to_string(),
            api_key: "rw_temporary".to_string(),
            organization_name: None,
            temporary: false,
            claim_token: Some("abc".to_string()),
        };

        apply_project_create_config(&mut cfg, &resp);

        assert_eq!(cfg.token.as_deref(), Some("rw_temporary"));
        assert_eq!(cfg.email, None);
        assert_eq!(cfg.default_organization, None);
        assert_eq!(cfg.default_project.as_deref(), Some("tmp_project"));
        assert_eq!(cfg.last_claim_token.as_deref(), Some("abc"));
    }
}
