use std::fs;

use anyhow::{Context, Result};
use serde_json::json;

use crate::client::ApiClient;

pub fn export(
    client: &ApiClient,
    project: &str,
    sql: &str,
    output_path: &str,
    format: Option<&str>,
) -> Result<()> {
    let fmt = format.unwrap_or_else(|| {
        if output_path.ends_with(".csv") {
            "csv"
        } else {
            "json"
        }
    });

    let body = json!({ "sql": sql, "format": fmt });
    let raw = client.post_raw(&format!("/v1/{}/query", project), &body)?;

    fs::write(output_path, &raw)
        .with_context(|| format!("failed to write to '{}'", output_path))?;

    eprintln!("Exported to {} ({} bytes)", output_path, raw.len());
    Ok(())
}
