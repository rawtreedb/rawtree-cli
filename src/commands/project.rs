use anyhow::Result;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::client::ApiClient;
use crate::config;
use crate::org;
use crate::output;

#[derive(Deserialize)]
struct ProjectItem {
    project_name: String,
    organization_id: String,
    created_at: String,
}

#[derive(Deserialize)]
struct ListProjectsResponse {
    projects: Vec<ProjectItem>,
}

#[derive(Deserialize)]
struct CreateProjectResponse {
    project: String,
    api_key: String,
    organization_name: Option<String>,
    #[serde(default)]
    temporary: bool,
    claim_url: Option<String>,
    claim_token: Option<String>,
    expires_in_seconds: Option<u64>,
}

pub struct CreatedProjectInfo {
    pub project: String,
    pub api_key: String,
    pub claim_url: Option<String>,
}

fn token_looks_like_jwt(token: &str) -> bool {
    let mut parts = token.split('.');
    parts.next().is_some()
        && parts.next().is_some()
        && parts.next().is_some()
        && parts.next().is_none()
}

fn jwt_token_for_project_create(token: Option<&str>) -> Option<String> {
    token
        .filter(|candidate| token_looks_like_jwt(candidate))
        .map(ToString::to_string)
}

fn apply_project_create_config(cfg: &mut config::Config, resp: &CreateProjectResponse) {
    cfg.default_project = Some(resp.project.clone());
    cfg.last_claim_url = resp.claim_url.clone();

    if resp.temporary {
        cfg.default_organization = resp.organization_name.clone();
    } else if let Some(ref organization_name) = resp.organization_name {
        cfg.default_organization = Some(organization_name.clone());
    }

    if resp.temporary {
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
    let create_client = ApiClient::new(
        client.base_url.clone(),
        jwt_token_for_project_create(client.token.as_deref()),
    );

    let mut payload = serde_json::Map::new();
    if let Some(project_name) = name {
        payload.insert(
            "project".to_string(),
            Value::String(project_name.to_string()),
        );
    }

    create_client.post(&path, &Value::Object(payload))
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
                "project_name": p.project_name,
                "organization_id": p.organization_id,
                "created_at": p.created_at,
            })).collect::<Vec<_>>()
        }),
        json_mode,
        |_| {
            if resp.projects.is_empty() {
                println!("No projects yet. Create one with `rtree project create <name>`.");
            } else {
                for p in &resp.projects {
                    println!(
                        "{:<20} org={} created={}",
                        p.project_name, p.organization_id, p.created_at
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

    output::print_result(
        &json!({
            "project": resp.project,
            "api_key": resp.api_key,
            "organization_name": resp.organization_name,
            "temporary": resp.temporary,
            "claim_url": resp.claim_url,
            "claim_token": resp.claim_token,
            "expires_in_seconds": resp.expires_in_seconds,
        }),
        json_mode,
        |_| {
            println!("Project '{}' created.", resp.project);
            println!("  api_key: {}", resp.api_key);
            if let Some(ref organization_name) = resp.organization_name {
                println!("  organization_name: {}", organization_name);
            }
            println!("  temporary: {}", resp.temporary);
            if let Some(ref claim_url) = resp.claim_url {
                println!("  claim_url: {}", claim_url);
            }
            if let Some(ref claim_token) = resp.claim_token {
                println!("  claim_token: {}", claim_token);
            }
            if let Some(expires_in_seconds) = resp.expires_in_seconds {
                println!("  expires_in_seconds: {}", expires_in_seconds);
            }
        },
    );
    Ok(())
}

pub fn create_for_insert(client: &ApiClient, name: Option<&str>) -> Result<CreatedProjectInfo> {
    let resp = create_and_persist(client, name, None)?;
    Ok(CreatedProjectInfo {
        project: resp.project,
        api_key: resp.api_key,
        claim_url: resp.claim_url,
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
    project: String,
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
        &json!({"old_name": old, "project": resp.project}),
        json_mode,
        |_| println!("Project '{}' renamed to '{}'.", old, resp.project),
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
    use super::{
        apply_project_create_config, jwt_token_for_project_create, token_looks_like_jwt,
        CreateProjectResponse,
    };
    use crate::config::Config;

    #[test]
    fn token_looks_like_jwt_detects_three_part_tokens() {
        assert!(token_looks_like_jwt("a.b.c"));
        assert!(!token_looks_like_jwt("rw_example"));
        assert!(!token_looks_like_jwt("a.b"));
    }

    #[test]
    fn jwt_token_for_project_create_drops_api_key_tokens() {
        let token = jwt_token_for_project_create(Some("rw_abc123"));
        assert_eq!(token, None);
    }

    #[test]
    fn apply_project_create_config_overwrites_auth_for_temporary_projects() {
        let mut cfg = Config {
            token: Some("jwt.token.value".to_string()),
            email: Some("user@example.com".to_string()),
            default_organization: Some("team_alpha".to_string()),
            ..Config::default()
        };
        let resp = CreateProjectResponse {
            project: "tmp_project".to_string(),
            api_key: "rw_temporary".to_string(),
            organization_name: Some("temp_org".to_string()),
            temporary: true,
            claim_url: Some("https://app.rawtree.dev/claim/project?token=abc".to_string()),
            claim_token: Some("abc".to_string()),
            expires_in_seconds: Some(86400),
        };

        apply_project_create_config(&mut cfg, &resp);

        assert_eq!(cfg.token.as_deref(), Some("rw_temporary"));
        assert_eq!(cfg.email, None);
        assert_eq!(cfg.default_organization.as_deref(), Some("temp_org"));
        assert_eq!(cfg.default_project.as_deref(), Some("tmp_project"));
        assert_eq!(
            cfg.last_claim_url.as_deref(),
            Some("https://app.rawtree.dev/claim/project?token=abc")
        );
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
            project: "analytics".to_string(),
            api_key: "rw_regular".to_string(),
            organization_name: None,
            temporary: false,
            claim_url: None,
            claim_token: None,
            expires_in_seconds: None,
        };

        apply_project_create_config(&mut cfg, &resp);

        assert_eq!(cfg.token.as_deref(), Some("jwt.token.value"));
        assert_eq!(cfg.email.as_deref(), Some("user@example.com"));
        assert_eq!(cfg.default_organization.as_deref(), Some("team_alpha"));
        assert_eq!(cfg.default_project.as_deref(), Some("analytics"));
        assert_eq!(cfg.last_claim_url, None);
    }

    #[test]
    fn apply_project_create_config_clears_default_org_for_temporary_when_missing_org_name() {
        let mut cfg = Config {
            default_organization: Some("team_alpha".to_string()),
            ..Config::default()
        };
        let resp = CreateProjectResponse {
            project: "tmp_project".to_string(),
            api_key: "rw_temp".to_string(),
            organization_name: None,
            temporary: true,
            claim_url: None,
            claim_token: None,
            expires_in_seconds: Some(86400),
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
            project: "analytics".to_string(),
            api_key: "rw_regular".to_string(),
            organization_name: Some("new_team".to_string()),
            temporary: false,
            claim_url: None,
            claim_token: None,
            expires_in_seconds: None,
        };

        apply_project_create_config(&mut cfg, &resp);

        assert_eq!(cfg.default_organization.as_deref(), Some("new_team"));
    }
}
