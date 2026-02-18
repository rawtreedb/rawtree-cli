use anyhow::Result;
use serde_json::json;

use crate::client::ApiClient;

pub fn sample(
    client: &ApiClient,
    project: &str,
    table: &str,
    limit: u64,
    format: Option<&str>,
) -> Result<()> {
    let sql = format!("SELECT * FROM {} LIMIT {}", table, limit);
    let mut body = json!({ "sql": sql });
    if let Some(fmt) = format {
        body["format"] = json!(fmt);
    }

    let raw = client.post_raw(&format!("/v1/{}/query", project), &body)?;

    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&raw) {
        println!("{}", serde_json::to_string_pretty(&json)?);
    } else {
        print!("{}", raw);
    }

    Ok(())
}
