use anyhow::Result;
use serde_json::json;

use crate::client::ApiClient;
use crate::org;

pub fn sample(
    client: &ApiClient,
    project: &str,
    organization: Option<&str>,
    table: &str,
    limit: u64,
    format: Option<&str>,
) -> Result<()> {
    let sql = format!("SELECT * FROM {} LIMIT {}", table, limit);
    let mut body = json!({ "sql": sql });
    if let Some(fmt) = format {
        body["format"] = json!(fmt);
    }

    let path = org::project_scoped_path(project, "/query", organization);
    let raw = client.post_raw(&path, &body)?;

    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&raw) {
        println!("{}", serde_json::to_string_pretty(&json)?);
    } else {
        print!("{}", raw);
    }

    Ok(())
}
