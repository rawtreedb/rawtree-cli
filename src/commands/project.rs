use anyhow::Result;
use serde::Deserialize;
use serde_json::json;

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
    let path = projects_collection_path(client, organization)?;
    let resp: CreateProjectResponse = client.post(&path, &json!({"project": name}))?;
    output::print_result(
        &json!({"project": resp.project, "api_key": resp.api_key}),
        json_mode,
        |_| {
            println!("Project '{}' created.", resp.project);
            println!("  api_key: {}", resp.api_key);
        },
    );
    Ok(())
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
