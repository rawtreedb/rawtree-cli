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
    #[serde(flatten)]
    access: ApiKeyAccess,
    expires_at: Option<String>,
    database: ApiKeyDatabaseRef,
    created_at: String,
}

#[derive(Deserialize, Serialize)]
#[serde(untagged)]
enum ApiKeyAccess {
    Permission { permission: String },
    DatabaseRoles { database_roles: Vec<String> },
}

impl std::fmt::Display for ApiKeyAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Permission { permission } => f.pad(permission),
            Self::DatabaseRoles { database_roles } => f.pad(&database_roles.join(", ")),
        }
    }
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
    expires_at: Option<String>,
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
                    "{:<38} {:<12} {:<14} {}  database={}  created={}  expires={}",
                    k.id,
                    k.name,
                    k.access,
                    k.token,
                    k.database.name,
                    k.created_at,
                    k.expires_at.as_deref().unwrap_or("never")
                );
            }
        }
    });
    Ok(())
}

#[derive(Serialize)]
pub struct CreateApiKeyRequest<'a> {
    pub name: &'a str,
    pub permission: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<&'a str>,
}

pub fn create(
    client: &ApiClient,
    database: Option<&str>,
    organization: Option<&str>,
    cluster: Option<&str>,
    request: CreateApiKeyRequest<'_>,
    json_mode: bool,
) -> Result<()> {
    let path = match database {
        Some(database) => org::database_scoped_path(database, "/keys", organization, cluster),
        None => org::scoped_path("/v1/keys", organization, cluster),
    };
    let resp: CreateApiKeyResponse = client.post(&path, &serde_json::to_value(request)?)?;
    output::print_result(&resp, json_mode, |_| {
        println!("API key created:");
        println!("  id:         {}", resp.id);
        println!("  token:      {}", resp.token);
        println!("  name:       {}", resp.name);
        println!("  permission: {}", resp.permission);
        println!(
            "  expires:    {}",
            resp.expires_at.as_deref().unwrap_or("never")
        );
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
