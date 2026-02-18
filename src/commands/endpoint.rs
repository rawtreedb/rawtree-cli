use anyhow::Result;
use serde::Deserialize;
use serde_json::json;

use crate::client::ApiClient;
use crate::output;

#[derive(Deserialize)]
struct QueryParameter {
    name: String,
    #[serde(rename = "type")]
    param_type: String,
}

#[derive(Deserialize)]
struct EndpointRecord {
    name: String,
    sql: String,
    description: String,
    #[serde(default)]
    format: Option<String>,
    #[serde(default)]
    parameters: Vec<QueryParameter>,
}

#[derive(Deserialize)]
struct EndpointListResponse {
    endpoints: Vec<EndpointRecord>,
}

fn endpoint_to_json(ep: &EndpointRecord) -> serde_json::Value {
    json!({
        "name": ep.name,
        "sql": ep.sql,
        "description": ep.description,
        "format": ep.format,
        "parameters": ep.parameters.iter().map(|p| json!({
            "name": p.name,
            "type": p.param_type,
        })).collect::<Vec<_>>(),
    })
}

fn fetch_endpoints(client: &ApiClient, project: &str) -> Result<EndpointListResponse> {
    client.get(&format!("/v1/{}/endpoints", project))
}

pub fn list(client: &ApiClient, project: &str, json_mode: bool) -> Result<()> {
    let resp: EndpointListResponse = fetch_endpoints(client, project)?;
    output::print_result(
        &json!({"endpoints": resp.endpoints.iter().map(endpoint_to_json).collect::<Vec<_>>()}),
        json_mode,
        |_| {
            if resp.endpoints.is_empty() {
                println!("No endpoints yet. Create one with `rtree endpoint create`.");
            } else {
                for ep in &resp.endpoints {
                    if ep.description.is_empty() {
                        println!("{:<20} sql={}", ep.name, ep.sql);
                    } else {
                        println!("{:<20} sql={}  ({})", ep.name, ep.sql, ep.description);
                    }
                }
            }
        },
    );
    Ok(())
}

pub fn create(
    client: &ApiClient,
    project: &str,
    name: &str,
    sql: &str,
    description: Option<&str>,
    json_mode: bool,
) -> Result<()> {
    let body = json!({
        "name": name,
        "sql": sql,
        "description": description.unwrap_or(""),
    });
    let resp: EndpointRecord = client.post(&format!("/v1/{}/endpoints", project), &body)?;
    output::print_result(
        &endpoint_to_json(&resp),
        json_mode,
        |_| println!("Endpoint '{}' created.", resp.name),
    );
    Ok(())
}

pub fn get(client: &ApiClient, project: &str, name: &str, json_mode: bool) -> Result<()> {
    let resp: EndpointListResponse = fetch_endpoints(client, project)?;
    let ep = resp
        .endpoints
        .iter()
        .find(|ep| ep.name == name)
        .ok_or_else(|| anyhow::anyhow!("Endpoint '{}' not found.", name))?;

    output::print_result(&endpoint_to_json(ep), json_mode, |_| {
        println!("Name:        {}", ep.name);
        println!("SQL:         {}", ep.sql);
        if !ep.description.is_empty() {
            println!("Description: {}", ep.description);
        }
        if let Some(ref fmt) = ep.format {
            println!("Format:      {}", fmt);
        }
        if !ep.parameters.is_empty() {
            println!("Parameters:");
            for p in &ep.parameters {
                println!("  {:<20} {}", p.name, p.param_type);
            }
        }
    });
    Ok(())
}

pub fn delete(client: &ApiClient, project: &str, name: &str, json_mode: bool) -> Result<()> {
    let resp: serde_json::Value =
        client.delete(&format!("/v1/{}/endpoints/{}", project, name))?;
    let deleted = resp.get("deleted").and_then(|v| v.as_bool()).unwrap_or(false);
    output::print_result(
        &json!({"deleted": deleted, "name": name}),
        json_mode,
        |_| {
            if deleted {
                println!("Endpoint '{}' deleted.", name);
            }
        },
    );
    Ok(())
}

pub fn exec(client: &ApiClient, project: &str, name: &str, params: &[String]) -> Result<()> {
    let mut url = format!("/v1/{}/endpoints/{}", project, name);
    if !params.is_empty() {
        let pairs: Vec<(&str, &str)> = params
            .iter()
            .map(|p| {
                let (k, v) = p.split_once('=').unwrap_or((p, ""));
                (k, v)
            })
            .collect();
        let qs: Vec<String> = pairs
            .iter()
            .map(|(k, v)| {
                format!(
                    "{}={}",
                    urlencoding::encode(k),
                    urlencoding::encode(v)
                )
            })
            .collect();
        url = format!("{}?{}", url, qs.join("&"));
    }
    let raw = match client.get_raw(&url) {
        Ok(raw) => raw,
        Err(e) => {
            let msg = format!("{:#}", e);
            if msg.contains("Missing required parameter(s)") {
                // Extract parameter names from "Missing required parameter(s): foo, bar"
                let params_hint = msg
                    .split("Missing required parameter(s): ")
                    .nth(1)
                    .and_then(|s| s.split('\n').next())
                    .unwrap_or("key=value");
                let param_flags: String = params_hint
                    .split(", ")
                    .map(|p| format!("--param {}=<value>", p.trim()))
                    .collect::<Vec<_>>()
                    .join(" ");
                anyhow::bail!(
                    "Endpoint '{}' requires parameters: {}\n\
                     Usage: rtree endpoint exec --project {} {} {}",
                    name, params_hint, project, name, param_flags
                );
            }
            return Err(e);
        }
    };
    // Pretty-print if valid JSON, otherwise print as-is
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&raw) {
        println!("{}", serde_json::to_string_pretty(&json)?);
    } else {
        println!("{}", raw);
    }
    Ok(())
}
