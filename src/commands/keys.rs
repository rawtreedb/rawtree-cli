use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::client::ApiClient;
use crate::org;
use crate::output;

#[derive(Deserialize, Serialize)]
struct ApiKeyItem {
    id: String,
    token: String,
    name: String,
    permission: String,
    database: ApiKeyDatabaseRef,
    created_at: String,
}

#[derive(Deserialize, Serialize)]
struct ListApiKeysResponse {
    keys: Vec<ApiKeyItem>,
}

#[derive(Deserialize, Serialize)]
struct CreateApiKeyResponse {
    id: String,
    token: String,
    name: String,
    database: ApiKeyDatabaseRef,
    permission: String,
}

#[derive(Deserialize, Serialize)]
struct ApiKeyDatabaseRef {
    name: String,
}

#[derive(Deserialize, Serialize)]
struct DeleteApiKeyResponse {
    deleted: bool,
}

pub fn list(
    client: &ApiClient,
    organization: Option<&str>,
    cluster: Option<&str>,
    json_mode: bool,
) -> Result<()> {
    let path = org::scoped_path("/v1/keys", organization, cluster);
    let resp: ListApiKeysResponse = client.get(&path)?;
    output::print_result(&resp, json_mode, |_| {
        if resp.keys.is_empty() {
            println!("No API keys.");
        } else {
            for k in &resp.keys {
                println!(
                    "{:<38} {:<12} {:<14} {}  database={}  created={}",
                    k.id, k.name, k.permission, k.token, k.database.name, k.created_at
                );
            }
        }
    });
    Ok(())
}

pub fn create(
    client: &ApiClient,
    database: Option<&str>,
    organization: Option<&str>,
    cluster: Option<&str>,
    name: &str,
    permission: &str,
    json_mode: bool,
) -> Result<()> {
    let body = json!({ "name": name, "permission": permission });
    let path = match database {
        Some(database) => org::database_scoped_path(database, "/keys", organization, cluster),
        None => org::scoped_path("/v1/keys", organization, cluster),
    };
    let resp: CreateApiKeyResponse = client.post(&path, &body)?;
    output::print_result(&resp, json_mode, |_| {
        println!("API key created:");
        println!("  id:         {}", resp.id);
        println!("  token:      {}", resp.token);
        println!("  name:       {}", resp.name);
        println!("  permission: {}", resp.permission);
    });
    Ok(())
}

pub fn delete(
    client: &ApiClient,
    organization: Option<&str>,
    cluster: Option<&str>,
    id_or_token: &str,
    json_mode: bool,
) -> Result<()> {
    let encoded_key = urlencoding::encode(id_or_token);
    let path = org::scoped_path(&format!("/v1/keys/{encoded_key}"), organization, cluster);
    let resp: DeleteApiKeyResponse = client.delete(&path)?;
    output::print_result(
        &json!({"deleted": resp.deleted, "id_or_token": id_or_token}),
        json_mode,
        |_| {
            if resp.deleted {
                println!("API key '{}' deleted.", id_or_token);
            }
        },
    );
    Ok(())
}
