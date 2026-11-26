mod discovery;
pub(crate) use discovery::UnavailableRegion;
mod regions;
mod routing;
#[cfg(test)]
mod routing_tests;

use anyhow::{Context, Result};
use flate2::write::GzEncoder;
use flate2::Compression;
use reqwest::blocking::RequestBuilder;
use reqwest::blocking::{Client, Response};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::cell::RefCell;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

const RAWTREE_CLIENT_HEADER: &str = "x-rawtree-client";
const RAWTREE_CLIENT_VALUE: &str = "cli";
const RAWTREE_CLIENT_VERSION_HEADER: &str = "x-rawtree-client-version";
const RAWTREE_CLIENT_VERSION_VALUE: &str = env!("CARGO_PKG_VERSION");
const ORGANIZATION_ID_HEADER: &str = "x-rawtree-organization-id";
const CLUSTER_ID_HEADER: &str = "x-rawtree-cluster-id";
const RAWTREE_USER_AGENT: &str = concat!("rawtree-cli/", env!("CARGO_PKG_VERSION"));

#[derive(Clone)]
enum CredentialSource {
    Explicit(Option<String>),
    Saved(RefCell<Option<SavedCredential>>),
}

#[derive(Clone)]
struct SavedCredential {
    token: Option<String>,
    oauth_client_id: Option<String>,
}

/// How a request names an organization or cluster.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Selector {
    Id(String),
    Name(String),
}

impl Selector {
    /// UUIDs select by ID; anything else is a name.
    pub fn parse(value: &str) -> Self {
        if routing::is_uuid(value) {
            Self::Id(value.to_owned())
        } else {
            Self::Name(value.to_owned())
        }
    }
}

/// The organization and cluster a request acts on. The client resolves them
/// to the ID headers the API expects.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Scope {
    pub organization: Option<Selector>,
    pub cluster: Option<Selector>,
}

impl Scope {
    pub fn new(organization: Option<&str>, cluster: Option<&str>) -> Self {
        Self {
            organization: organization.map(Selector::parse),
            cluster: cluster.map(Selector::parse),
        }
    }

    pub fn organization(organization: Option<&str>) -> Self {
        Self::new(organization, None)
    }
}

pub struct ApiClient {
    pub base_url: String,
    credentials: CredentialSource,
    client: Client,
    routing: Arc<routing::RoutingCache>,
    deployment: Option<(String, String)>,
    scope: Scope,
}

impl ApiClient {
    pub fn new(base_url: String, token: Option<String>) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_owned(),
            credentials: CredentialSource::Explicit(token),
            client: Client::new(),
            routing: Arc::default(),
            deployment: None,
            scope: Scope::default(),
        }
    }

    pub fn scope(&self) -> &Scope {
        &self.scope
    }

    pub fn has_api_key(&self) -> Result<bool> {
        Ok(self.token()?.is_some_and(|token| token.starts_with("rt_")))
    }

    /// A view of this client whose requests act on `scope`; it shares the routing cache.
    pub fn with_scope(&self, scope: Scope) -> Self {
        // Settle the saved identity first, so every view of this client shares it.
        let _ = self.token();
        Self {
            base_url: self.base_url.clone(),
            credentials: self.credentials.clone(),
            client: self.client.clone(),
            routing: self.routing.clone(),
            deployment: self.deployment.clone(),
            scope,
        }
    }

    pub fn from_saved_config(base_url: String) -> Self {
        let mut client = Self::new(base_url, None);
        client.credentials = CredentialSource::Saved(RefCell::new(None));
        client
    }

    fn token(&self) -> Result<Option<String>> {
        match &self.credentials {
            CredentialSource::Explicit(token) => Ok(token.clone()),
            CredentialSource::Saved(saved) => {
                // Capture the identity on first use. Local-only commands need no
                // credentials; later requests must not silently switch accounts.
                let mut saved = saved.borrow_mut();
                if saved.is_none() {
                    let cfg = crate::config::load()?;
                    *saved = Some(SavedCredential {
                        token: cfg.token,
                        oauth_client_id: cfg.oauth.map(|credentials| credentials.client_id),
                    });
                }
                let saved = saved.as_ref().context("Missing saved credential")?;
                match &saved.oauth_client_id {
                    Some(client_id) => crate::oauth::saved_token(&self.base_url, client_id),
                    None => Ok(saved.token.clone()),
                }
            }
        }
    }

    pub fn fork(&self) -> Result<Self> {
        // Every sender must retain the same identity, even before its first request.
        self.token()?;
        Ok(Self {
            base_url: self.base_url.clone(),
            credentials: self.credentials.clone(),
            client: self.client.clone(),
            routing: Arc::default(),
            deployment: self.deployment.clone(),
            scope: self.scope.clone(),
        })
    }

    fn unrouted_request(&self, method: reqwest::Method, path: &str) -> Result<RequestBuilder> {
        self.deployment_request(method, path, self.deployment.as_ref())
    }

    fn deployment_request(
        &self,
        method: reqwest::Method,
        path: &str,
        deployment: Option<&(String, String)>,
    ) -> Result<RequestBuilder> {
        let url = routing::deployment_url(&format!("{}{}", self.base_url, path), deployment);
        let request = with_client_header(self.client.request(method, url));
        let request = match deployment {
            Some((deployment, region)) => request
                .header("X-RawTree-Deployment-Id", deployment)
                .header("X-RawTree-Region", region),
            None => request,
        };
        Ok(match self.token()? {
            Some(token) => request.bearer_auth(token),
            None => request,
        })
    }

    pub fn post<T: DeserializeOwned>(&self, path: &str, body: &Value) -> Result<T> {
        self.post_with_timeout(path, body, Duration::from_secs(30))
    }

    pub fn post_with_timeout<T: DeserializeOwned>(
        &self,
        path: &str,
        body: &Value,
        timeout: Duration,
    ) -> Result<T> {
        let req = self
            .routed_request_with_timeout(reqwest::Method::POST, path, timeout)?
            .json(body);
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
        let req = self.routed_request(reqwest::Method::POST, path)?;
        let resp = req.send().context("failed to connect to server")?;
        let query_id = resp
            .headers()
            .get("x-clickhouse-query-id")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        Ok((handle_response(resp)?, query_id))
    }

    /// POST without a body, sending an optional Idempotency-Key header.
    pub fn post_empty_idempotent<T: DeserializeOwned>(
        &self,
        path: &str,
        idempotency_key: Option<&str>,
    ) -> Result<T> {
        let mut req = self.routed_request(reqwest::Method::POST, path)?;
        if let Some(key) = idempotency_key {
            req = req.header("idempotency-key", key);
        }
        let resp = req.send().context("failed to connect to server")?;
        handle_response(resp)
    }

    /// POST without a body to an endpoint that replies without content.
    pub fn post_no_content(&self, path: &str) -> Result<()> {
        let req = self.routed_request(reqwest::Method::POST, path)?;
        let resp = req.send().context("failed to connect to server")?;
        check_status(resp)
    }

    /// DELETE to an endpoint that replies without content.
    pub fn delete_no_content(&self, path: &str) -> Result<()> {
        let req = self.routed_request(reqwest::Method::DELETE, path)?;
        let resp = req.send().context("failed to connect to server")?;
        check_status(resp)
    }

    pub fn put<T: DeserializeOwned>(&self, path: &str, body: &Value) -> Result<T> {
        let req = self.routed_request(reqwest::Method::PUT, path)?.json(body);
        let resp = req.send().context("failed to connect to server")?;
        handle_response(resp)
    }

    pub fn patch<T: DeserializeOwned>(&self, path: &str, body: &Value) -> Result<T> {
        let req = self
            .routed_request(reqwest::Method::PATCH, path)?
            .json(body);
        let resp = req.send().context("failed to connect to server")?;
        handle_response(resp)
    }

    pub fn delete<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let req = self.routed_request(reqwest::Method::DELETE, path)?;
        let resp = req.send().context("failed to connect to server")?;
        handle_response(resp)
    }

    pub fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let value: Value =
            if path == "/v1/clusters" && self.has_user_credential()? && self.deployment.is_none() {
                self.discover_clusters(self.scope.organization.as_ref(), None)?
            } else {
                let req = self.routed_request(reqwest::Method::GET, path)?;
                handle_response(req.send().context("failed to connect to server")?)?
            };
        self.remember_routing_metadata(path, &value);
        serde_json::from_value(value).context("failed to parse server response")
    }

    /// POST with a pre-serialized JSON string body, gzip-compressed.
    pub fn post_compressed<T: DeserializeOwned>(&self, path: &str, body: &str) -> Result<T> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder
            .write_all(body.as_bytes())
            .context("gzip compression failed")?;
        let compressed = encoder.finish().context("gzip finalization failed")?;
        let req = self
            .routed_request(reqwest::Method::POST, path)?
            .header("content-type", "application/json")
            .header("content-encoding", "gzip")
            .body(compressed);
        let resp = req.send().context("failed to connect to server")?;
        handle_response(resp)
    }

    /// POST that returns raw text (for queries that return ClickHouse results directly).
    pub fn post_raw(&self, path: &str, body: &Value) -> Result<String> {
        let req = self.routed_request(reqwest::Method::POST, path)?.json(body);
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
        let req = self.routed_request(reqwest::Method::GET, path)?;
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

fn handle_response<T: DeserializeOwned>(resp: Response) -> Result<T> {
    let status = resp.status();
    let text = resp.text().context("failed to read response body")?;
    if !status.is_success() {
        return Err(format_server_error(&text, status.as_u16()));
    }
    serde_json::from_str(&text).context("failed to parse server response")
}

fn check_status(resp: Response) -> Result<()> {
    let status = resp.status();
    if status.is_success() {
        return Ok(());
    }
    let text = resp.text().context("failed to read response body")?;
    Err(format_server_error(&text, status.as_u16()))
}

/// A non-success response from the API.
#[derive(Debug)]
pub struct ServerError {
    pub status: u16,
    message: String,
}

impl ServerError {
    /// The server error behind `error`, if one caused it.
    pub fn of(error: &anyhow::Error) -> Option<&Self> {
        error.chain().find_map(|cause| cause.downcast_ref())
    }
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Server error ({}): {}", self.status, self.message)
    }
}

impl std::error::Error for ServerError {}

fn format_server_error(body: &str, status: u16) -> anyhow::Error {
    let message = match serde_json::from_str::<Value>(body) {
        Ok(json) => {
            let message = json["message"].as_str().unwrap_or("Unknown error");
            match json["hint"].as_str().unwrap_or("") {
                "" => message.to_owned(),
                hint => format!("{message}\nHint: {hint}"),
            }
        }
        Err(_) => body.to_owned(),
    };
    ServerError { status, message }.into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forks_capture_one_identity_before_their_first_requests() {
        // Isolate HOME in a subprocess so parallel unit tests cannot read this config.
        if std::env::var_os("RAWTREE_FORK_IDENTITY_TEST").is_none() {
            let home = tempfile::tempdir().unwrap();
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "client::tests::forks_capture_one_identity_before_their_first_requests",
                    "--nocapture",
                ])
                .env("HOME", home.path())
                .env("RAWTREE_FORK_IDENTITY_TEST", "1")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }
        let base_url = "https://api.example.test";
        let mut cfg: crate::config::Config = serde_json::from_value(serde_json::json!({
            "token":"original-access", "oauth": {
                "api_url":base_url, "resource":base_url, "client_id":"original-client",
                "refresh_token":"original-refresh", "token_endpoint":format!("{base_url}/oauth/token"),
                "revocation_endpoint":format!("{base_url}/oauth/revoke"), "expires_at":i64::MAX
            }
        })).unwrap();
        crate::config::save(&cfg).unwrap();
        let client = ApiClient::from_saved_config(base_url.into());
        let first = client.fork().unwrap();
        let second = client.fork().unwrap();
        assert_eq!(first.token().unwrap().as_deref(), Some("original-access"));
        cfg.oauth.as_mut().unwrap().client_id = "replacement-client".into();
        cfg.token = Some("replacement-access".into());
        crate::config::save(&cfg).unwrap();
        assert!(second
            .token()
            .unwrap_err()
            .to_string()
            .contains("login changed"));
        assert!(client
            .token()
            .unwrap_err()
            .to_string()
            .contains("login changed"));
    }

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
