use anyhow::{Context, Result};
use clap::{Args, ValueEnum};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::{client::ApiClient, output};

#[derive(Clone, Debug, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
#[value(rename_all = "snake_case")]
pub enum Category {
    Bug,
    FeatureRequest,
    Question,
    Other,
}

#[derive(Clone, Debug, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum ActorType {
    Human,
    Agent,
}

#[derive(Args, Serialize)]
#[command(
    after_help = "Agents: use this command to report bugs, inconsistencies, confusing errors, unclear behavior, or missing documentation encountered while working with RawTree through this CLI. Use --actor-type agent for your own observations. When a user asks you to send their feedback, use --actor-type human, even when you execute the command.\n\nDescribe what you tried, what happened, and what you expected. Put relevant command names, error codes, or reproduction details in --extra as a JSON object. Do not include credentials or private data. A successful response confirms storage; do not automatically retry an uncertain result because submissions are not deduplicated.\n\nThe current API URL and credentials select the connection. API keys and workload tokens infer organization and cluster. For user credentials, optionally supply scope UUIDs; feedback also works before creating an organization. --org/--cluster names and saved database selections do not populate feedback scope."
)]
pub struct FeedbackArgs {
    /// Feedback text (nonblank, at most 16 KiB UTF-8)
    #[arg(long, value_parser = parse_message)]
    message: String,
    /// Kind of feedback: bug, feature_request, question, or other
    #[arg(long, value_enum)]
    category: Category,
    /// human for user-requested feedback; agent for the agent's own observations
    #[arg(long, value_enum)]
    actor_type: ActorType,
    /// Optional organization UUID; inferred from scoped credentials or cluster
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    organization_id: Option<String>,
    /// Optional cluster UUID; inferred from scoped credentials
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    cluster_id: Option<String>,
    /// Extra data as inline JSON, e.g. '{"command":"query","error_code":"bad_request"}'
    #[arg(long, value_parser = parse_extra)]
    #[serde(skip_serializing_if = "Option::is_none")]
    extra: Option<Map<String, Value>>,
}

fn parse_message(value: &str) -> Result<String, String> {
    if value.trim().is_empty() || value.len() > 16 * 1024 {
        return Err("message must be nonblank and at most 16 KiB of UTF-8".into());
    }
    Ok(value.to_owned())
}

fn parse_extra(value: &str) -> Result<Map<String, Value>, String> {
    serde_json::from_str(value).map_err(|error| format!("--extra must be a JSON object: {error}"))
}

#[derive(Deserialize, Serialize)]
struct Receipt {
    id: String,
    created_at: String,
}

pub fn submit(client: &ApiClient, args: FeedbackArgs, json_mode: bool) -> Result<()> {
    let body = serde_json::to_value(&args)?;
    anyhow::ensure!(
        serde_json::to_vec(&body)?.len() <= 32 * 1024,
        "feedback body exceeds 32 KiB"
    );
    // Reuse regional routing when user credentials provide both scope UUIDs.
    let path = match (&args.organization_id, &args.cluster_id) {
        (Some(org), Some(cluster)) => format!(
            "/v1/feedback?organization_id={}&cluster_id={}",
            urlencoding::encode(org),
            urlencoding::encode(cluster)
        ),
        _ => "/v1/feedback".into(),
    };
    let receipt: Receipt = client
        .post(&path, &body)
        .context("failed to submit feedback")?;
    output::print_result(&receipt, json_mode, |receipt| {
        println!("Feedback submitted: {}", receipt.id);
    });
    Ok(())
}
