use anyhow::{Context, Result};
use flate2::write::GzEncoder;
use flate2::Compression;
use reqwest::blocking::RequestBuilder;
use reqwest::blocking::{Client, Response};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::io::Write;

const RAWTREE_CLIENT_HEADER: &str = "x-rawtree-client";
const RAWTREE_CLIENT_VALUE: &str = "cli";
const RAWTREE_CLIENT_VERSION_HEADER: &str = "x-rawtree-client-version";
const RAWTREE_CLIENT_VERSION_VALUE: &str = env!("CARGO_PKG_VERSION");
const RAWTREE_USER_AGENT: &str = concat!("rawtree-cli/", env!("CARGO_PKG_VERSION"));

pub struct ApiClient {
    pub base_url: String,
    pub token: Option<String>,
    client: Client,
}

impl ApiClient {
    pub fn new(base_url: String, token: Option<String>) -> Self {
        Self {
            base_url,
            token,
            client: Client::new(),
        }
    }

    pub fn post<T: DeserializeOwned>(&self, path: &str, body: &Value) -> Result<T> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = with_client_header(self.client.post(&url)).json(body);
        if let Some(ref token) = self.token {
            req = req.bearer_auth(token);
        }
        let resp = req.send().context("failed to connect to server")?;
        handle_response(resp)
    }

    /// POST without a request body.
    pub fn post_empty<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        self.post_empty_with_query_id(path).map(|(body, _)| body)
    }

    /// POST without a body, preserving the native ClickHouse query ID.
    pub fn post_empty_with_query_id<T: DeserializeOwned>(
        &self,
        path: &str,
    ) -> Result<(T, Option<String>)> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = with_client_header(self.client.post(&url));
        if let Some(ref token) = self.token {
            req = req.bearer_auth(token);
        }
        let resp = req.send().context("failed to connect to server")?;
        let query_id = resp
            .headers()
            .get("x-clickhouse-query-id")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        Ok((handle_response(resp)?, query_id))
    }

    pub fn patch<T: DeserializeOwned>(&self, path: &str, body: &Value) -> Result<T> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = with_client_header(self.client.patch(&url)).json(body);
        if let Some(ref token) = self.token {
            req = req.bearer_auth(token);
        }
        let resp = req.send().context("failed to connect to server")?;
        handle_response(resp)
    }

    pub fn delete<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = with_client_header(self.client.delete(&url));
        if let Some(ref token) = self.token {
            req = req.bearer_auth(token);
        }
        let resp = req.send().context("failed to connect to server")?;
        handle_response(resp)
    }

    pub fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = with_client_header(self.client.get(&url));
        if let Some(ref token) = self.token {
            req = req.bearer_auth(token);
        }
        let resp = req.send().context("failed to connect to server")?;
        handle_response(resp)
    }

    /// POST with a pre-serialized JSON string body, gzip-compressed.
    pub fn post_compressed<T: DeserializeOwned>(&self, path: &str, body: &str) -> Result<T> {
        let url = format!("{}{}", self.base_url, path);
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder
            .write_all(body.as_bytes())
            .context("gzip compression failed")?;
        let compressed = encoder.finish().context("gzip finalization failed")?;
        let mut req = self
            .client
            .post(&url)
            .header("content-type", "application/json")
            .header("content-encoding", "gzip")
            .body(compressed);
        req = with_client_header(req);
        if let Some(ref token) = self.token {
            req = req.bearer_auth(token);
        }
        let resp = req.send().context("failed to connect to server")?;
        handle_response(resp)
    }

    /// POST that returns raw text (for queries that return ClickHouse results directly).
    pub fn post_raw(&self, path: &str, body: &Value) -> Result<String> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = with_client_header(self.client.post(&url)).json(body);
        if let Some(ref token) = self.token {
            req = req.bearer_auth(token);
        }
        let resp = req.send().context("failed to connect to server")?;
        let status = resp.status();
        let text = resp.text().context("failed to read response body")?;
        if !status.is_success() {
            return Err(format_server_error(&text, status.as_u16()));
        }
        Ok(text)
    }

    /// GET that returns raw text.
    pub fn get_raw(&self, path: &str) -> Result<String> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = with_client_header(self.client.get(&url));
        if let Some(ref token) = self.token {
            req = req.bearer_auth(token);
        }
        let resp = req.send().context("failed to connect to server")?;
        let status = resp.status();
        let text = resp.text().context("failed to read response body")?;
        if !status.is_success() {
            return Err(format_server_error(&text, status.as_u16()));
        }
        Ok(text)
    }
}

fn with_client_header(req: RequestBuilder) -> RequestBuilder {
    req.header(reqwest::header::USER_AGENT, RAWTREE_USER_AGENT)
        .header(RAWTREE_CLIENT_HEADER, RAWTREE_CLIENT_VALUE)
        .header(RAWTREE_CLIENT_VERSION_HEADER, RAWTREE_CLIENT_VERSION_VALUE)
}

fn handle_response<T: DeserializeOwned>(resp: reqwest::blocking::Response) -> Result<T> {
    let status = resp.status();
    let text = resp.text().context("failed to read response body")?;
    if !status.is_success() {
        return Err(format_server_error(&text, status.as_u16()));
    }
    serde_json::from_str(&text).context("failed to parse server response")
}

fn format_server_error(body: &str, status: u16) -> anyhow::Error {
    if let Ok(json) = serde_json::from_str::<Value>(body) {
        let message = json["message"].as_str().unwrap_or("Unknown error");
        let hint = json["hint"].as_str().unwrap_or("");
        if hint.is_empty() {
            anyhow::anyhow!("Server error ({}): {}", status, message)
        } else {
            anyhow::anyhow!("Server error ({}): {}\nHint: {}", status, message, hint)
        }
    } else {
        anyhow::anyhow!("Server error ({}): {}", status, body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_post_reads_completion_json_and_query_id_or_http_error() {
        use std::io::{BufRead, BufReader, Write};
        use std::net::TcpListener;

        for (status, body) in [
            ("200 OK", r#"{"inserted":12}"#),
            ("200 OK", r#"{"inserted":null}"#),
            ("400 Bad Request", r#"{"message":"Import failed"}"#),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                let mut reader = BufReader::new(socket.try_clone().unwrap());
                let mut request = String::new();
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                    request.push_str(&line);
                }
                assert!(request.starts_with("POST /v1/tables/events?url="));
                assert!(request
                    .to_lowercase()
                    .contains("authorization: bearer test-token"));
                assert!(!request.contains("application/x-ndjson"));
                write!(socket, "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nX-ClickHouse-Query-Id: import-123\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
            });
            let client = ApiClient::new(format!("http://{address}"), Some("test-token".into()));
            let result = client.post_empty_with_query_id::<Value>(
                "/v1/tables/events?url=https%3A%2F%2Fexample.com%2Fdata.json",
            );
            if status.starts_with("200") {
                let (value, query_id) = result.unwrap();
                assert_eq!(value, serde_json::from_str::<Value>(body).unwrap());
                assert_eq!(query_id.as_deref(), Some("import-123"));
            } else {
                assert!(result.unwrap_err().to_string().contains("Import failed"));
            }
            server.join().unwrap();
        }
    }

    #[test]
    fn marks_cli_requests_with_rawtree_client_header() {
        let api_client = ApiClient::new("https://api.rawtree.local".to_string(), None);
        let request = with_client_header(
            api_client
                .client
                .get("https://api.rawtree.local/v1/databases"),
        )
        .build()
        .expect("request should build");

        assert_eq!(
            request
                .headers()
                .get(reqwest::header::USER_AGENT)
                .and_then(|value| value.to_str().ok()),
            Some(RAWTREE_USER_AGENT)
        );

        assert_eq!(
            request
                .headers()
                .get(RAWTREE_CLIENT_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some(RAWTREE_CLIENT_VALUE)
        );
        assert_eq!(
            request
                .headers()
                .get(RAWTREE_CLIENT_VERSION_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some(RAWTREE_CLIENT_VERSION_VALUE)
        );
    }
}
