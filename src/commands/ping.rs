use anyhow::Result;
use serde::Deserialize;
use serde_json::json;

use crate::client::ApiClient;
use crate::output;

#[derive(Deserialize)]
struct HealthResponse {
    status: String,
}

pub fn ping(client: &ApiClient, json_mode: bool) -> Result<()> {
    let resp: HealthResponse = client.get("/health")?;
    output::print_result(
        &json!({"status": resp.status, "url": client.base_url}),
        json_mode,
        |_| println!("OK ({})", client.base_url),
    );
    Ok(())
}
