use std::fs;
use std::io::{BufRead, BufReader, IsTerminal};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use indicatif::{ProgressBar, ProgressStyle};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::client::ApiClient;
use crate::org;
use crate::output;

const BATCH_SIZE: usize = 5000;
const READ_BUF_SIZE: usize = 1024 * 1024;

fn num_senders() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

#[derive(Deserialize)]
struct InsertResponse {
    inserted: usize,
    query_id: Option<String>,
    status: Option<String>,
}

#[derive(Deserialize)]
struct ProcessStatusResponse {
    query_id: String,
    status: String,
    total_rows: Option<usize>,
    error: Option<String>,
}

fn is_jsonl(path: &str) -> bool {
    Path::new(path)
        .extension()
        .map(|ext| ext.eq_ignore_ascii_case("jsonl"))
        .unwrap_or(false)
}

/// Build the JSON request body into a reusable buffer by string concatenation.
fn build_body_into(body: &mut String, lines: &[String]) {
    body.clear();
    body.push('[');
    for (i, line) in lines.iter().enumerate() {
        if i > 0 {
            body.push(',');
        }
        body.push_str(line);
    }
    body.push(']');
}

fn print_inserted(count: usize, json_mode: bool) {
    output::print_result(&json!({"inserted": count}), json_mode, |_| {
        println!("Inserted {} row(s).", count)
    });
}

fn print_process_not_running(query_id: &str, json_mode: bool) {
    output::print_result(
        &json!({"query_id": query_id, "status": "not_running"}),
        json_mode,
        |_| {
            println!("Process is no longer running (query_id={query_id}).");
        },
    );
}

pub fn insert(
    client: &ApiClient,
    project: &str,
    organization: Option<&str>,
    table: &str,
    data: Option<&str>,
    file: Option<&str>,
    url: Option<&str>,
    json_mode: bool,
) -> Result<()> {
    let api_path = org::project_scoped_path(project, &format!("/tables/{table}"), organization);

    // Small inline data — send in one request
    if let Some(raw) = data {
        let json_data: Value = serde_json::from_str(raw).context("invalid JSON in --data")?;
        let resp: InsertResponse = client.post(&api_path, &json_data)?;
        print_inserted(resp.inserted, json_mode);
        return Ok(());
    }

    if let Some(raw_url) = url {
        return insert_from_url(client, project, organization, table, raw_url, json_mode);
    }

    let path =
        file.ok_or_else(|| anyhow::anyhow!("provide exactly one of --data, --file, or --url"))?;

    if is_jsonl(path) {
        insert_jsonl_streaming(client, project, organization, table, path, json_mode)?;
    } else {
        // Non-JSONL files: parse entire file (assumed to be a JSON array or object)
        let contents =
            fs::read_to_string(path).with_context(|| format!("failed to read file '{}'", path))?;
        let json_data: Value = serde_json::from_str(&contents).context("invalid JSON in file")?;
        let resp: InsertResponse = client.post(&api_path, &json_data)?;
        print_inserted(resp.inserted, json_mode);
    }

    Ok(())
}

fn insert_from_url(
    client: &ApiClient,
    project: &str,
    organization: Option<&str>,
    table: &str,
    url: &str,
    json_mode: bool,
) -> Result<()> {
    let path = build_url_insert_path(project, organization, table, url);
    let resp: InsertResponse = client.post_empty(&path)?;

    // Backward-compatibility path for older servers that still return synchronous counts.
    let query_id = match resp.query_id {
        Some(id) => id,
        None => {
            print_inserted(resp.inserted, json_mode);
            return Ok(());
        }
    };

    if resp.status.as_deref() != Some("started") {
        bail!(
            "URL insert was not started by server (status={:?})",
            resp.status
        );
    }

    let status_path =
        org::project_scoped_path(project, &format!("/processes/{query_id}"), organization);
    let deadline = Instant::now() + Duration::from_secs(120);
    let mut not_found_streak = 0u32;
    loop {
        let status = match client.get_optional::<ProcessStatusResponse>(&status_path)? {
            Some(status) => status,
            None => {
                not_found_streak += 1;
                if not_found_streak >= 3 {
                    print_process_not_running(&query_id, json_mode);
                    return Ok(());
                }
                if Instant::now() >= deadline {
                    bail!(
                        "Timed out waiting for URL insert process to appear/complete (query_id={})",
                        query_id
                    );
                }
                thread::sleep(Duration::from_millis(500));
                continue;
            }
        };
        not_found_streak = 0;
        match status.status.as_str() {
            "finished" => {
                let inserted = status.total_rows.unwrap_or(0);
                print_inserted(inserted, json_mode);
                return Ok(());
            }
            "failed" => {
                let details = status.error.unwrap_or_else(|| "Insert failed.".to_string());
                bail!(
                    "URL insert failed (query_id={}): {}",
                    status.query_id,
                    details
                );
            }
            "started" | "accepted" | "running" | "unknown" => {
                if Instant::now() >= deadline {
                    bail!(
                        "Timed out waiting for URL insert to finish (query_id={})",
                        status.query_id
                    );
                }
                thread::sleep(Duration::from_millis(500));
            }
            other => bail!(
                "Unexpected insert status '{}' for query_id={}",
                other,
                status.query_id
            ),
        }
    }
}

fn build_url_insert_path(
    project: &str,
    organization: Option<&str>,
    table: &str,
    url: &str,
) -> String {
    let encoded_url = urlencoding::encode(url);
    org::project_scoped_path(
        project,
        &format!("/tables/{table}?url={encoded_url}"),
        organization,
    )
}

#[cfg(test)]
fn build_process_status_path(project: &str, organization: Option<&str>, query_id: &str) -> String {
    org::project_scoped_path(project, &format!("/processes/{query_id}"), organization)
}

#[cfg(test)]
mod tests {
    use super::{build_process_status_path, build_url_insert_path};

    #[test]
    fn url_insert_path_uses_query_param_route() {
        let path =
            build_url_insert_path("events_2", None, "events", "https://example.com/a b.ndjson");

        assert_eq!(
            path,
            "/v1/events_2/tables/events?url=https%3A%2F%2Fexample.com%2Fa%20b.ndjson"
        );
    }

    #[test]
    fn process_status_path_uses_process_status_route() {
        let path = build_process_status_path("events_2", Some("org_1"), "abc-123");

        assert_eq!(path, "/v1/org_1/events_2/processes/abc-123");
    }
}

/// Stream JSONL: reader thread reads raw lines into batches, sender threads
/// build JSON by string concatenation, gzip-compress, and POST to server.
fn insert_jsonl_streaming(
    client: &ApiClient,
    project: &str,
    organization: Option<&str>,
    table: &str,
    path: &str,
    json_mode: bool,
) -> Result<()> {
    let file_size = fs::metadata(path)
        .with_context(|| format!("failed to stat file '{}'", path))?
        .len();

    let show_progress = std::io::stderr().is_terminal() && !json_mode;
    let pb = if show_progress {
        let pb = ProgressBar::new(file_size);
        pb.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})",
            )
            .unwrap()
            .progress_chars("#>-"),
        );
        pb
    } else {
        ProgressBar::hidden()
    };

    let senders = num_senders();
    let (tx, rx) = mpsc::sync_channel::<Vec<String>>(senders * 4);
    let rx = Arc::new(std::sync::Mutex::new(rx));
    let inserted = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(AtomicBool::new(false));
    let first_error: Arc<std::sync::Mutex<Option<String>>> = Arc::new(std::sync::Mutex::new(None));

    // Spawn sender threads — each reuses a body buffer, compresses, and POSTs
    let url = org::project_scoped_path(project, &format!("/tables/{table}"), organization);
    let mut handles = Vec::with_capacity(senders);
    for _ in 0..senders {
        let rx = Arc::clone(&rx);
        let inserted = Arc::clone(&inserted);
        let failed = Arc::clone(&failed);
        let first_error = Arc::clone(&first_error);
        let client = ApiClient::new(client.base_url.clone(), client.token.clone());
        let url = url.clone();

        handles.push(thread::spawn(move || {
            let mut body = String::with_capacity(512 * 1024);
            loop {
                let batch = {
                    let lock = rx.lock().unwrap();
                    lock.recv()
                };
                let batch = match batch {
                    Ok(b) => b,
                    Err(_) => break,
                };
                let batch_len = batch.len();
                build_body_into(&mut body, &batch);
                match client.post_compressed::<InsertResponse>(&url, &body) {
                    Ok(resp) => {
                        inserted.fetch_add(resp.inserted, Ordering::Relaxed);
                    }
                    Err(e) => {
                        failed.store(true, Ordering::Relaxed);
                        let mut err = first_error.lock().unwrap();
                        if err.is_none() {
                            *err = Some(format!("{:#}", e));
                        }
                    }
                }
                drop(batch);
                let _ = batch_len;
            }
        }));
    }

    // Reader: read raw lines into batches using read_line for buffer reuse
    let file = fs::File::open(path).with_context(|| format!("failed to read file '{}'", path))?;
    let mut reader = BufReader::with_capacity(READ_BUF_SIZE, file);
    let mut current_batch: Vec<String> = Vec::with_capacity(BATCH_SIZE);
    let mut line_buf = String::new();
    let mut has_rows = false;

    loop {
        if failed.load(Ordering::Relaxed) {
            break;
        }

        line_buf.clear();
        let bytes_read = reader
            .read_line(&mut line_buf)
            .context("failed to read line")?;
        if bytes_read == 0 {
            break; // EOF
        }

        pb.inc(bytes_read as u64);

        let trimmed = line_buf.trim_end();
        if trimmed.is_empty() {
            continue;
        }

        has_rows = true;
        current_batch.push(trimmed.to_string());

        if current_batch.len() >= BATCH_SIZE {
            let batch = std::mem::replace(&mut current_batch, Vec::with_capacity(BATCH_SIZE));
            if tx.send(batch).is_err() {
                break;
            }
        }
    }

    if !current_batch.is_empty() {
        let _ = tx.send(current_batch);
    }

    if !has_rows {
        drop(tx);
        for h in handles {
            let _ = h.join();
        }
        bail!("JSONL file is empty");
    }

    drop(tx);
    for h in handles {
        let _ = h.join();
    }

    pb.finish_and_clear();

    let total_inserted = inserted.load(Ordering::Relaxed);
    print_inserted(total_inserted, json_mode);

    let err = first_error.lock().unwrap();
    if let Some(ref msg) = *err {
        bail!("Failed to insert some batches: {}", msg);
    }

    Ok(())
}
