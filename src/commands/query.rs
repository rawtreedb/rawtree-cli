use anyhow::Result;
use serde_json::json;

use crate::client::ApiClient;

pub fn query(
    client: &ApiClient,
    project: &str,
    sql: &str,
    format: Option<&str>,
    limit: Option<u64>,
) -> Result<()> {
    let sql = match limit {
        Some(n) => format!("{} LIMIT {}", sql.trim().trim_end_matches(';'), n),
        None => sql.to_string(),
    };

    let mut body = json!({ "sql": sql });
    if let Some(fmt) = format {
        body["format"] = json!(fmt);
    }

    let raw = client.post_raw(&format!("/v1/{}/query", project), &body)?;

    // Pretty-print if JSON, otherwise print as-is (CSV)
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&raw) {
        println!("{}", serde_json::to_string_pretty(&json)?);
    } else {
        print!("{}", raw);
    }

    Ok(())
}
