use anyhow::Result;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::client::ApiClient;
use crate::config;
use crate::org;
use crate::output;

#[derive(Deserialize)]
struct ProjectItem {
    name: String,
    #[serde(default)]
    organization: Option<OrganizationRef>,
    created_at: String,
}

#[derive(Deserialize)]
struct ListProjectsResponse {
    #[serde(default)]
    organization: Option<OrganizationRef>,
    projects: Vec<ProjectItem>,
}

#[derive(Clone, Deserialize)]
struct OrganizationRef {
    name: String,
}

#[derive(Deserialize)]
struct CreateProjectResponse {
    name: String,
    organization: OrganizationRef,
    token: String,
    #[serde(default)]
    claim_token: Option<String>,
}

pub struct CreatedProjectInfo {
    pub project: String,
    pub api_key: String,
    pub claim_token: Option<String>,
}

fn claim_url_from_response(resp: &CreateProjectResponse) -> Option<String> {
    let ui_base_url = crate::commands::open::resolve_ui_base_url();
    resp.claim_token.as_deref().map(|claim_token| {
        crate::commands::open::build_claim_dashboard_url(&ui_base_url, claim_token)
    })
}

fn apply_project_create_config(cfg: &mut config::Config, resp: &CreateProjectResponse) {
    cfg.default_project = Some(resp.name.clone());
    cfg.default_organization = Some(resp.organization.name.clone());
    cfg.last_claim_token = resp.claim_token.clone();

    if resp.claim_token.is_some() {
        cfg.token = Some(resp.token.clone());
        cfg.email = None;
    }
}

fn create_project_response(
    client: &ApiClient,
    name: Option<&str>,
    organization: Option<&str>,
) -> Result<CreateProjectResponse> {
    let path = org::projects_collection_path(organization);

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

pub fn list(client: &ApiClient, organization: Option<&str>, json_mode: bool) -> Result<()> {
    let path = org::projects_collection_path(organization);
    let resp: ListProjectsResponse = client.get(&path)?;
    output::print_result(
        &json!({
            "projects": resp.projects.iter().map(|p| json!({
                "name": p.name,
                "organization": p
                    .organization
                    .as_ref()
                    .or(resp.organization.as_ref())
                    .map(|org| json!({"name": org.name})),
                "created_at": p.created_at,
            })).collect::<Vec<_>>()
        }),
        json_mode,
        |_| {
            if resp.projects.is_empty() {
                println!("No projects yet. Create one with `rtree project create <name>`.");
            } else {
                for p in &resp.projects {
                    let organization = p
                        .organization
                        .as_ref()
                        .or(resp.organization.as_ref())
                        .map(|org| org.name.as_str())
                        .unwrap_or("unknown");
                    println!(
                        "{:<20} org={} created={}",
                        p.name, organization, p.created_at
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
            "organization": {"name": resp.organization.name},
            "claim_token": claim_token,
            "claim_url": claim_url,
        }),
        json_mode,
        |_| {
            println!(
                "Project '{}' created in organization '{}'.",
                resp.name, resp.organization.name
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
        api_key: resp.token,
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
    project: ProjectEntity,
    organization: OrganizationRef,
}

#[derive(Deserialize)]
struct ProjectEntity {
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
        &json!({
            "old_name": old,
            "name": resp.project.name,
            "organization": {"name": resp.organization.name},
        }),
        json_mode,
        |_| println!("Project '{}' renamed to '{}'.", old, resp.project.name),
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
        &json!({"deleted": resp.deleted, "name": name}),
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
    use super::{
        apply_project_create_config, CreateProjectResponse, OrganizationRef, ProjectItem,
    };
    use crate::config::Config;
    use serde_json::json;

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
            organization: OrganizationRef {
                name: "new_team".to_string(),
            },
            token: "rw_regular".to_string(),
            claim_token: None,
        };

        apply_project_create_config(&mut cfg, &resp);

        assert_eq!(cfg.token.as_deref(), Some("jwt.token.value"));
        assert_eq!(cfg.email.as_deref(), Some("user@example.com"));
        assert_eq!(cfg.default_organization.as_deref(), Some("new_team"));
        assert_eq!(cfg.default_project.as_deref(), Some("analytics"));
        assert_eq!(cfg.last_claim_token, None);
    }

    #[test]
    fn apply_project_create_config_uses_project_token_for_claimable_projects() {
        let mut cfg = Config {
            token: Some("jwt.token.value".to_string()),
            email: Some("user@example.com".to_string()),
            default_organization: Some("team_alpha".to_string()),
            ..Config::default()
        };
        let resp = CreateProjectResponse {
            name: "tmp_project".to_string(),
            organization: OrganizationRef {
                name: "temp_org".to_string(),
            },
            token: "rw_temporary".to_string(),
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
    fn project_item_deserializes_nested_organization_field() {
        let item: ProjectItem = serde_json::from_value(json!({
            "name": "analytics",
            "organization": {"name": "team_alpha"},
            "created_at": "2026-04-10T00:00:00Z"
        }))
        .expect("project item should deserialize");

        assert_eq!(item.name, "analytics");
        assert_eq!(item.organization.expect("organization").name, "team_alpha");
    }
}
