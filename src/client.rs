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

    /// POST without a body and return a streaming response.
    pub fn post_empty_stream(&self, path: &str) -> Result<Response> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self
            .client
            .post(&url)
            .header("accept", "application/x-ndjson");
        req = with_client_header(req);
        if let Some(ref token) = self.token {
            req = req.bearer_auth(token);
        }
        let resp = req.send().context("failed to connect to server")?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().context("failed to read response body")?;
            return Err(format_server_error(&text, status.as_u16()));
        }
        Ok(resp)
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
    req.header(RAWTREE_CLIENT_HEADER, RAWTREE_CLIENT_VALUE)
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
    fn marks_cli_requests_with_rawtree_client_header() {
        let client = Client::new();
        let request = with_client_header(client.get("https://api.rawtree.local/v1/projects"))
            .build()
            .expect("request should build");

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
