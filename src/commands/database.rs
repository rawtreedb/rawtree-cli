use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::client::ApiClient;
use crate::config;
use crate::org;
use crate::output;

#[derive(Deserialize, Serialize)]
struct DatabaseRef {
    name: String,
}

#[derive(Deserialize, Serialize)]
struct DatabaseItem {
    name: String,
    s3_storage: Option<serde_json::Value>,
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
) {
    cfg.default_database = Some(resp.database.name.clone());
    cfg.default_organization = organization.map(str::to_string);
    if let Some(cluster) = cluster {
        cfg.default_cluster = Some(cluster.to_string());
    }
}

fn database_create_collection_path(organization: Option<&str>, cluster: Option<&str>) -> String {
    org::databases_collection_path(organization, cluster)
}

fn create_database_response(
    client: &ApiClient,
    name: &str,
    organization: Option<&str>,
    cluster: Option<&str>,
) -> Result<CreateDatabaseResponse> {
    let path = database_create_collection_path(organization, cluster);
    client.post(&path, &json!({ "name": name }))
}

fn create_and_persist(
    client: &ApiClient,
    name: &str,
    organization: Option<&str>,
    cluster: Option<&str>,
) -> Result<CreateDatabaseResponse> {
    let resp = create_database_response(client, name, organization, cluster)?;
    let mut cfg = config::load()?;
    apply_database_create_config(&mut cfg, &resp, organization, cluster);
    config::save(&cfg)?;
    Ok(resp)
}

pub fn list(
    client: &ApiClient,
    organization: Option<&str>,
    cluster: Option<&str>,
    json_mode: bool,
) -> Result<()> {
    let path = org::databases_collection_path(organization, cluster);
    let resp: ListDatabasesResponse = client.get(&path)?;
    output::print_result(&resp, json_mode, |resp| {
        if resp.databases.is_empty() {
            println!("No databases yet. Create one with `rtree database create <name>`.");
        } else {
            for database in &resp.databases {
                println!("{}", database.name);
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
    json_mode: bool,
) -> Result<()> {
    let resp = create_and_persist(client, name, organization, cluster)?;
    output::print_result(&resp, json_mode, |resp| {
        println!("Database '{}' created.", resp.database.name);
    });
    Ok(())
}

pub fn use_database(name: &str, json_mode: bool) -> Result<()> {
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

pub fn delete(
    client: &ApiClient,
    name: &str,
    organization: Option<&str>,
    cluster: Option<&str>,
    json_mode: bool,
) -> Result<()> {
    let path = org::scoped_path(
        &format!("/v1/databases/{}", urlencoding::encode(name)),
        organization,
        cluster,
    );
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
    use super::{
        apply_database_create_config, database_create_collection_path, CreateDatabaseResponse,
        DatabaseRef,
    };
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

        apply_database_create_config(&mut cfg, &resp, Some("new_team"), Some("production"));

        assert_eq!(cfg.token.as_deref(), Some("jwt.token.value"));
        assert_eq!(cfg.email.as_deref(), Some("user@example.com"));
        assert_eq!(cfg.default_organization.as_deref(), Some("new_team"));
        assert_eq!(cfg.default_cluster.as_deref(), Some("production"));
        assert_eq!(cfg.default_database.as_deref(), Some("analytics"));
    }

    #[test]
    fn database_create_collection_path_uses_databases_endpoint() {
        assert_eq!(database_create_collection_path(None, None), "/v1/databases");
        assert_eq!(
            database_create_collection_path(Some("team alpha"), Some("prod/eu")),
            "/v1/databases?organization=team%20alpha&cluster=prod%2Feu"
        );
    }
}
