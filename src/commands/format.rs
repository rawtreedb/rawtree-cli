use std::io::Write;
use std::time::Duration;

use anyhow::Result;
use clap::Args;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{client::ApiClient, org};

#[derive(Args)]
pub struct FormatArgs {
    /// Optional database override for authentication scope
    #[arg(long)]
    pub database: Option<String>,
    /// SQL to format. Use "-" or omit SQL to read from piped stdin.
    #[arg(value_name = "SQL", conflicts_with = "sql")]
    pub sql_positional: Option<String>,
    /// SQL to format. Use "-" to read from stdin.
    #[arg(long, short = 'q', visible_alias = "query")]
    pub sql: Option<String>,
}

#[derive(Deserialize, Serialize)]
struct QueryFormatResponse {
    formatted: String,
}

pub fn format(
    client: &ApiClient,
    database: Option<&str>,
    organization: Option<&str>,
    cluster: Option<&str>,
    sql: &str,
    json_mode: bool,
) -> Result<()> {
    let path = match database {
        Some(database) => {
            org::database_scoped_path(database, "/query-format", organization, cluster)
        }
        None => org::scoped_path("/v1/query-format", organization, cluster),
    };
    // Allow upload/download time in addition to the server's 30-second formatter timeout.
    let response: QueryFormatResponse =
        client.post_with_timeout(&path, &json!({ "sql": sql }), Duration::from_secs(120))?;
    let mut stdout = std::io::stdout().lock();
    if json_mode {
        serde_json::to_writer_pretty(&mut stdout, &response)?;
        writeln!(stdout)?;
    } else {
        stdout.write_all(response.formatted.as_bytes())?;
    }
    Ok(())
}
