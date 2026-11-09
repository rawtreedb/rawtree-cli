use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use reqwest::blocking::{Client, Response};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::config;

#[derive(Debug, Serialize, Deserialize)]
pub struct Credentials {
    pub api_url: String,
    pub client_id: String,
    pub resource: String,
    pub token_endpoint: String,
    pub revocation_endpoint: String,
    pub refresh_token: String,
    pub expires_at: i64,
}

pub struct Login {
    pub access_token: String,
    pub email: String,
    pub credentials: Credentials,
}

#[derive(Deserialize)]
struct Metadata {
    issuer: String,
    authorization_endpoint: String,
    registration_endpoint: String,
    token_endpoint: String,
    revocation_endpoint: String,
}

#[derive(Deserialize)]
struct Registration {
    client_id: String,
}

#[derive(Deserialize)]
struct Tokens {
    access_token: String,
    refresh_token: String,
    token_type: String,
    expires_in: u32,
}

fn http_client() -> Result<Client> {
    Ok(Client::builder()
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .user_agent(concat!("rawtree-cli/", env!("CARGO_PKG_VERSION")))
        .build()?)
}

fn secure_url(value: &str) -> Result<Url> {
    let url = Url::parse(value).context("Invalid OAuth endpoint URL")?;
    let loopback = url.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    if !(url.scheme() == "https" || url.scheme() == "http" && loopback)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || url.query().is_some()
    {
        bail!(
            "OAuth requires HTTPS (or HTTP loopback) without URL credentials, query, or fragment"
        );
    }
    Ok(url)
}

fn endpoint(value: &str, issuer: &Url) -> Result<Url> {
    let url = secure_url(value)?;
    if url.origin() != issuer.origin() {
        bail!("OAuth endpoint does not match the authorization server origin");
    }
    Ok(url)
}

fn check_response(response: Response) -> Result<Response> {
    if !response.status().is_success() {
        // Never echo server bodies: they may contain authorization codes or tokens.
        bail!(
            "OAuth request failed (HTTP {}). Run `rtree login` to authorize again.",
            response.status().as_u16()
        );
    }
    Ok(response)
}

fn read_tokens(response: Response) -> Result<Tokens> {
    let tokens: Tokens = check_response(response)?
        .json()
        .map_err(|_| anyhow::anyhow!("Invalid OAuth token response"))?;
    if !tokens.token_type.eq_ignore_ascii_case("Bearer")
        || tokens.access_token.is_empty()
        || tokens.refresh_token.is_empty()
        || tokens.expires_in == 0
    {
        bail!("Invalid OAuth token response");
    }
    Ok(tokens)
}

fn random_secret() -> Result<String> {
    let mut bytes = [0u8; 32];
    getrandom::getrandom(&mut bytes)
        .map_err(|_| anyhow::anyhow!("Failed to obtain secure randomness"))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

pub fn authorize(base_url: &str, no_browser: bool, timeout: u64, json_mode: bool) -> Result<Login> {
    let base_url = base_url.trim_end_matches('/');
    secure_url(base_url)?;
    let http = http_client()?;
    let metadata: Metadata = check_response(
        http.get(format!("{base_url}/.well-known/oauth-authorization-server"))
            .send()?,
    )?
    .json()
    .context("Invalid OAuth discovery response")?;
    // A regional API may advertise the canonical authorization server.
    let issuer = secure_url(&metadata.issuer)?;
    let mut authorization = endpoint(&metadata.authorization_endpoint, &issuer)?;
    let registration = endpoint(&metadata.registration_endpoint, &issuer)?;
    endpoint(&metadata.token_endpoint, &issuer)?;
    endpoint(&metadata.revocation_endpoint, &issuer)?;

    let listener = TcpListener::bind("127.0.0.1:0").context(
        "Cannot listen for OAuth callback. Use `rtree login --device-auth` on remote terminals",
    )?;
    listener.set_nonblocking(true)?;
    let redirect_uri = format!("http://{}/oauth/callback", listener.local_addr()?);
    // A new client per login keeps failed/incomplete logins from revoking saved grants.
    let registered: Registration = check_response(
        http.post(registration)
            .json(&json!({
                "client_name": "RawTree CLI",
                "application_type": "native",
                "redirect_uris": [redirect_uri],
                "grant_types": ["authorization_code", "refresh_token"],
                "response_types": ["code"],
                "token_endpoint_auth_method": "none",
                "scope": "full_access"
            }))
            .send()?,
    )?
    .json()
    .context("Invalid OAuth registration response")?;
    if registered.client_id.is_empty() {
        bail!("Invalid OAuth client registration");
    }
    let state = random_secret()?;
    let verifier = random_secret()?;
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    authorization.query_pairs_mut().extend_pairs([
        ("response_type", "code"),
        ("client_id", registered.client_id.as_str()),
        ("redirect_uri", redirect_uri.as_str()),
        ("state", state.as_str()),
        ("code_challenge", challenge.as_str()),
        ("code_challenge_method", "S256"),
        ("scope", "full_access"),
        ("resource", metadata.issuer.as_str()),
    ]);
    let timeout = if timeout == 0 { 600 } else { timeout.min(600) };
    announce_authorization(
        &authorization,
        no_browser,
        timeout,
        json_mode,
        || webbrowser::open(authorization.as_str()).is_ok(),
        &mut std::io::stdout().lock(),
        &mut std::io::stderr().lock(),
    )?;
    let code = wait_for_code(
        || listener.accept().map(|(stream, _)| stream),
        &redirect_uri,
        &state,
        Duration::from_secs(timeout),
    )?;
    let tokens = read_tokens(
        http.post(&metadata.token_endpoint)
            .form(&[
                ("grant_type", "authorization_code"),
                ("client_id", registered.client_id.as_str()),
                ("redirect_uri", redirect_uri.as_str()),
                ("code", code.as_str()),
                ("code_verifier", verifier.as_str()),
                ("resource", metadata.issuer.as_str()),
            ])
            .send()?,
    )?;
    // This email is display-only. The token came directly from the trusted token
    // endpoint; API authorization always verifies the JWT server-side.
    #[derive(Deserialize)]
    struct DisplayClaims {
        email: String,
    }
    let payload = tokens
        .access_token
        .split('.')
        .nth(1)
        .context("Invalid OAuth access token")?;
    let claims: DisplayClaims = serde_json::from_slice(
        &URL_SAFE_NO_PAD
            .decode(payload)
            .context("Invalid OAuth access token")?,
    )
    .context("Missing email in OAuth access token")?;
    Ok(Login {
        access_token: tokens.access_token,
        email: claims.email,
        credentials: Credentials {
            api_url: base_url.to_owned(),
            client_id: registered.client_id,
            resource: metadata.issuer,
            token_endpoint: metadata.token_endpoint,
            revocation_endpoint: metadata.revocation_endpoint,
            refresh_token: tokens.refresh_token,
            expires_at: chrono::Utc::now().timestamp() + i64::from(tokens.expires_in),
        },
    })
}

fn announce_authorization(
    authorization: &Url,
    no_browser: bool,
    timeout: u64,
    json_mode: bool,
    open: impl FnOnce() -> bool,
    stdout: &mut impl Write,
    stderr: &mut impl Write,
) -> Result<()> {
    if json_mode {
        writeln!(
            stderr,
            "{}",
            json!({"event": "oauth_authorization_required", "authorization_url": authorization.as_str(), "expires_in": timeout})
        )?;
        stderr.flush()?;
    } else {
        writeln!(
            stdout,
            "Open this URL in a browser on this machine to log in:\n{authorization}"
        )?;
        if !no_browser && !open() {
            writeln!(stderr, "Could not open your browser. Open the URL above, or use `rtree login --device-auth` from a remote terminal.")?;
        }
        writeln!(stdout, "Waiting for authorization...")?;
    }
    Ok(())
}

fn callback_result(target: &str, redirect_uri: &str, state: &str) -> Result<Option<String>> {
    // Accept only an origin-form request for our exact callback path.
    if !target.starts_with("/oauth/callback?") {
        return Ok(None);
    }
    let url = Url::parse(redirect_uri)?.join(target)?;
    let pairs: Vec<_> = url.query_pairs().collect();
    let values = |name: &str| {
        pairs
            .iter()
            .filter(|(key, _)| key == name)
            .map(|(_, value)| value.as_ref())
            .collect::<Vec<_>>()
    };
    if values("state") != [state] {
        return Ok(None);
    }
    let codes = values("code");
    let errors = values("error");
    match (codes.as_slice(), errors.as_slice()) {
        ([code], []) if !code.is_empty() => Ok(Some((*code).to_owned())),
        ([], ["access_denied"]) => {
            bail!("OAuth authorization denied. Run `rtree login` to try again.")
        }
        ([], [_]) => bail!("OAuth authorization failed. Run `rtree login` to try again."),
        _ => Ok(None),
    }
}

fn read_callback(stream: &mut TcpStream, deadline: Instant) -> Result<String> {
    let mut bytes = Vec::new();
    // Bound the complete HTTP header read, not just each socket read. A client
    // trickling bytes must not extend the authorization deadline.
    while bytes.len() < 8192 {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .context("Callback request timed out")?;
        stream.set_read_timeout(Some(remaining))?;
        let mut chunk = [0; 1024];
        let capacity = chunk.len().min(8192 - bytes.len());
        let count = stream.read(&mut chunk[..capacity])?;
        if count == 0 {
            break;
        }
        bytes.extend_from_slice(&chunk[..count]);
        if bytes.windows(4).any(|part| part == b"\r\n\r\n") {
            break;
        }
    }
    let request = std::str::from_utf8(&bytes).context("Invalid callback request")?;
    if !request.contains("\r\n\r\n") {
        bail!("Incomplete callback request");
    }
    let line = request
        .split_inclusive('\n')
        .next()
        .context("Invalid callback request")?;
    let parts: Vec<_> = line.split_whitespace().collect();
    match parts.as_slice() {
        ["GET", target, "HTTP/1.1" | "HTTP/1.0"] if line.ends_with("\r\n") => {
            Ok((*target).to_owned())
        }
        _ => bail!("Invalid callback request"),
    }
}

fn wait_for_code(
    mut accept: impl FnMut() -> std::io::Result<TcpStream>,
    redirect_uri: &str,
    state: &str,
    timeout: Duration,
) -> Result<String> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        match accept() {
            Ok(mut stream) => {
                stream.set_nonblocking(false)?;
                let remaining = deadline
                    .saturating_duration_since(Instant::now())
                    .max(Duration::from_millis(1));
                stream.set_write_timeout(Some(Duration::from_secs(1)))?;
                let result = read_callback(
                    &mut stream,
                    Instant::now() + remaining.min(Duration::from_secs(2)),
                )
                .ok()
                .map(|target| callback_result(&target, redirect_uri, state));
                let accepted = matches!(&result, Some(Ok(Some(_))));
                let (status, text) = if accepted {
                    (
                        "200 OK",
                        "Authorization received. Return to your terminal to finish logging in.",
                    )
                } else {
                    (
                        "400 Bad Request",
                        "Authorization was not accepted. Return to your terminal.",
                    )
                };
                let _ = write!(stream, "HTTP/1.1 {status}\r\nContent-Type: text/plain; charset=utf-8\r\nCache-Control: no-store\r\nContent-Security-Policy: default-src 'none'\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{text}", text.len());
                if let Some(result) = result {
                    if let Some(code) = result? {
                        return Ok(code);
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(25))
            }
            Err(error) => return Err(error.into()),
        }
    }
    bail!("OAuth login timed out. Run `rtree login` again, or use `rtree login --device-auth` on a remote terminal.")
}

pub fn saved_token(base_url: &str, client_id: &str) -> Result<Option<String>> {
    let _lock = config::lock()?;
    let mut cfg = config::load()?;
    let credentials = cfg
        .oauth
        .as_mut()
        .filter(|credentials| credentials.client_id == client_id)
        .context("Saved OAuth login changed during this command. Run the command again.")?;
    if base_url.trim_end_matches('/') != credentials.api_url {
        bail!("Saved OAuth credentials belong to a different API URL. Run `rtree login` for this endpoint or pass --api-key.");
    }
    if credentials.expires_at <= chrono::Utc::now().timestamp() + 60 {
        let issuer = secure_url(&credentials.resource)?;
        let token_endpoint = endpoint(&credentials.token_endpoint, &issuer)?;
        let tokens = read_tokens(
            http_client()?
                .post(token_endpoint)
                .form(&[
                    ("grant_type", "refresh_token"),
                    ("client_id", credentials.client_id.as_str()),
                    ("refresh_token", credentials.refresh_token.as_str()),
                    ("resource", credentials.resource.as_str()),
                ])
                .send()?,
        )?;
        credentials.refresh_token = tokens.refresh_token;
        credentials.expires_at = chrono::Utc::now().timestamp() + i64::from(tokens.expires_in);
        cfg.token = Some(tokens.access_token);
        config::save(&cfg)?;
    }
    Ok(cfg.token)
}

pub fn revoke(credentials: &Credentials) -> Result<()> {
    let issuer = secure_url(&credentials.resource)?;
    let revocation = endpoint(&credentials.revocation_endpoint, &issuer)?;
    check_response(
        http_client()?
            .post(revocation)
            .form(&[
                ("client_id", credentials.client_id.as_str()),
                ("token", credentials.refresh_token.as_str()),
                ("token_type_hint", "refresh_token"),
            ])
            .send()?,
    )
    .context("OAuth logout failed; credentials were retained so revocation can be retried")?;
    Ok(())
}

#[cfg(test)]
mod tests;
