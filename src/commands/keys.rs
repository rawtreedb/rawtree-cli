use anyhow::Result;
use serde::Deserialize;
use serde_json::json;

use crate::client::ApiClient;
use crate::output;

#[derive(Deserialize)]
struct ApiKeyItem {
    key_id: String,
    api_key: String,
    label: String,
    permission: String,
    created_at: String,
}

#[derive(Deserialize)]
struct ListApiKeysResponse {
    keys: Vec<ApiKeyItem>,
}

#[derive(Deserialize)]
struct CreateApiKeyResponse {
    key_id: String,
    api_key: String,
    label: String,
    permission: String,
}

#[derive(Deserialize)]
struct DeleteApiKeyResponse {
    deleted: bool,
}

pub fn list(client: &ApiClient, project: &str, json_mode: bool) -> Result<()> {
    let resp: ListApiKeysResponse = client.get(&format!("/v1/{}/keys", project))?;
    output::print_result(
        &json!({"keys": resp.keys.iter().map(|k| json!({
            "key_id": k.key_id,
            "api_key": k.api_key,
            "label": k.label,
            "permission": k.permission,
            "created_at": k.created_at,
        })).collect::<Vec<_>>()}),
        json_mode,
        |_| {
            if resp.keys.is_empty() {
                println!("No API keys.");
            } else {
                for k in &resp.keys {
                    println!(
                        "{:<38} {:<12} {:<14} {}  created={}",
                        k.key_id, k.label, k.permission, k.api_key, k.created_at
                    );
                }
            }
        },
    );
    Ok(())
}

pub fn create(
    client: &ApiClient,
    project: &str,
    label: &str,
    permission: &str,
    json_mode: bool,
) -> Result<()> {
    let body = json!({ "label": label, "permission": permission });
    let resp: CreateApiKeyResponse = client.post(&format!("/v1/{}/keys", project), &body)?;
    output::print_result(
        &json!({
            "key_id": resp.key_id,
            "api_key": resp.api_key,
            "label": resp.label,
            "permission": resp.permission,
        }),
        json_mode,
        |_| {
            println!("API key created:");
            println!("  key_id:     {}", resp.key_id);
            println!("  api_key:    {}", resp.api_key);
            println!("  label:      {}", resp.label);
            println!("  permission: {}", resp.permission);
        },
    );
    Ok(())
}

pub fn delete(client: &ApiClient, project: &str, key_id: &str, json_mode: bool) -> Result<()> {
    let resp: DeleteApiKeyResponse =
        client.delete(&format!("/v1/{}/keys/{}", project, key_id))?;
    output::print_result(
        &json!({"deleted": resp.deleted, "key_id": key_id}),
        json_mode,
        |_| {
            if resp.deleted {
                println!("API key '{}' deleted.", key_id);
            }
        },
    );
    Ok(())
}
