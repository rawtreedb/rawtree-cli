use anyhow::{Context, Result};
use flate2::write::GzEncoder;
use flate2::Compression;
use reqwest::blocking::Client;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::io::Write;

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
        let mut req = self.client.post(&url).json(body);
        if let Some(ref token) = self.token {
            req = req.bearer_auth(token);
        }
        let resp = req.send().context("failed to connect to server")?;
        handle_response(resp)
    }

    pub fn post_empty<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self.client.post(&url);
        if let Some(ref token) = self.token {
            req = req.bearer_auth(token);
        }
        let resp = req.send().context("failed to connect to server")?;
        handle_response(resp)
    }

    pub fn patch<T: DeserializeOwned>(&self, path: &str, body: &Value) -> Result<T> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self.client.patch(&url).json(body);
        if let Some(ref token) = self.token {
            req = req.bearer_auth(token);
        }
        let resp = req.send().context("failed to connect to server")?;
        handle_response(resp)
    }

    pub fn delete<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self.client.delete(&url);
        if let Some(ref token) = self.token {
            req = req.bearer_auth(token);
        }
        let resp = req.send().context("failed to connect to server")?;
        handle_response(resp)
    }

    pub fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self.client.get(&url);
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
        if let Some(ref token) = self.token {
            req = req.bearer_auth(token);
        }
        let resp = req.send().context("failed to connect to server")?;
        handle_response(resp)
    }

    /// POST that returns raw text (for queries that return ClickHouse results directly).
    pub fn post_raw(&self, path: &str, body: &Value) -> Result<String> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self.client.post(&url).json(body);
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
        let mut req = self.client.get(&url);
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
