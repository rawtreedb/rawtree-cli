use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::client::ApiClient;
use crate::config;
use crate::output;
use crate::s3_storage::{format_storage, S3StorageArgs, S3StorageMetadata};

#[derive(Deserialize, Serialize)]
struct DatabaseRef {
    name: String,
}

#[derive(Deserialize, Serialize)]
struct DatabaseItem {
    name: String,
    s3_storage: Option<S3StorageMetadata>,
}

#[derive(Deserialize, Serialize)]
struct ListDatabasesResponse {
    databases: Vec<DatabaseItem>,
}

#[derive(Deserialize, Serialize)]
struct CreateDatabaseResponse {
    database: DatabaseRef,
}

fn apply_database_create_config(
    cfg: &mut config::Config,
    resp: &CreateDatabaseResponse,
    organization: Option<&str>,
    cluster: Option<&str>,
    ids: (Option<String>, Option<String>),
) {
    cfg.default_database = Some(resp.database.name.clone());
    cfg.set_default_organization(organization.map(str::to_string), ids.0);
    if let Some(cluster) = cluster {
        cfg.set_default_cluster(Some(cluster.to_string()), ids.1);
    }
}

pub fn list(client: &ApiClient, json_mode: bool) -> Result<()> {
    let resp: ListDatabasesResponse = client.get("/v1/databases")?;
    output::print_result(&resp, json_mode, |resp| {
        if resp.databases.is_empty() {
            println!("No databases yet. Create one with `rtree database create <name>`.");
        } else {
            for database in &resp.databases {
                println!(
                    "{:<20} storage={}",
                    database.name,
                    format_storage(database.s3_storage.as_ref(), "cluster default")
                );
            }
        }
    });
    Ok(())
}

pub fn create(
    client: &ApiClient,
    name: &str,
    organization: Option<&str>,
    cluster: Option<&str>,
    s3_storage: S3StorageArgs,
    json_mode: bool,
) -> Result<()> {
    let s3_storage = s3_storage.to_json()?;
    let storage_configured = s3_storage.is_some();
    let mut body = json!({ "name": name });
    if let Some(storage) = s3_storage {
        body["s3_storage"] = storage;
    }
    let resp: CreateDatabaseResponse = client.post("/v1/databases", &body)?;
    let _config_lock = config::lock()?;
    let mut cfg = config::load()?;
    // Best effort: without IDs the names keep resolving on each request.
    let ids = client.resolve_scope_ids().unwrap_or_default();
    apply_database_create_config(&mut cfg, &resp, organization, cluster, ids);
    config::save(&cfg)?;
    output::print_result(&resp, json_mode, |resp| {
        println!("Database '{}' created.", resp.database.name);
        if storage_configured {
            println!("Using customer-owned S3 storage.");
        }
    });
    Ok(())
}

pub fn use_database(name: &str, json_mode: bool) -> Result<()> {
    let _config_lock = config::lock()?;
    let mut cfg = config::load()?;
    cfg.default_database = Some(name.to_string());
    config::save(&cfg)?;

    output::print_result(&json!({"default_database": name}), json_mode, |_| {
        println!("Default database set to '{}'.", name)
    });
    Ok(())
}

#[derive(Deserialize)]
struct DeleteDatabaseResponse {
    deleted: bool,
}

pub fn delete(client: &ApiClient, name: &str, json_mode: bool) -> Result<()> {
    let path = format!("/v1/databases/{}", urlencoding::encode(name));
    let resp: DeleteDatabaseResponse = client.delete(&path)?;
    output::print_result(
        &json!({"deleted": resp.deleted, "name": name}),
        json_mode,
        |_| {
            if resp.deleted {
                println!("Database '{}' deleted.", name);
            }
        },
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{apply_database_create_config, CreateDatabaseResponse, DatabaseRef};
    use crate::config::Config;

    #[test]
    fn apply_database_create_config_preserves_jwt_for_standard_databases() {
        let mut cfg = Config {
            token: Some("jwt.token.value".to_string()),
            email: Some("user@example.com".to_string()),
            default_organization: Some("team_alpha".to_string()),
            ..Config::default()
        };
        let resp = CreateDatabaseResponse {
            database: DatabaseRef {
                name: "analytics".to_string(),
            },
        };

        apply_database_create_config(
            &mut cfg,
            &resp,
            Some("new_team"),
            Some("production"),
            (Some("org-id".into()), Some("cluster-id".into())),
        );

        assert_eq!(cfg.token.as_deref(), Some("jwt.token.value"));
        assert_eq!(cfg.email.as_deref(), Some("user@example.com"));
        assert_eq!(cfg.default_organization.as_deref(), Some("new_team"));
        assert_eq!(cfg.default_cluster.as_deref(), Some("production"));
        assert_eq!(cfg.default_organization_id.as_deref(), Some("org-id"));
        assert_eq!(cfg.default_cluster_id.as_deref(), Some("cluster-id"));
        assert_eq!(cfg.default_database.as_deref(), Some("analytics"));
    }
}
