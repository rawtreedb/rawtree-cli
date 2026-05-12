use std::io::{self, IsTerminal, Write};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use reqwest::blocking::Client as HttpClient;
use serde::Deserialize;
use serde_json::json;

use crate::client::ApiClient;
use crate::config;
use crate::constants::DEFAULT_API_URL;
use crate::org;
use crate::output;

#[derive(Deserialize)]
struct AuthResponse {
    token: String,
    email: String,
}

#[derive(Deserialize)]
struct CliDeviceStartResponse {
    device_code: String,
    user_code: String,
    verification_uri: String,
    verification_uri_complete: String,
    expires_in: u64,
    interval: u64,
}

#[derive(Deserialize)]
struct CliDeviceTokenResponse {
    token: String,
    user_id: String,
    email: String,
}

#[derive(Deserialize)]
struct ApiErrorResponse {
    error: String,
    message: String,
    hint: Option<String>,
}

enum CliDeviceTokenPoll {
    Pending,
    Approved(CliDeviceTokenResponse),
}

#[derive(Clone, Debug, Default)]
struct AuthSelection {
    organization: Option<String>,
    project: Option<String>,
}

#[derive(Deserialize)]
struct ProjectItem {
    name: String,
}

#[derive(Deserialize)]
struct ListProjectsResponse {
    projects: Vec<ProjectItem>,
}

#[derive(Deserialize)]
struct ProjectContextResponse {
    project: Option<ProjectContextRef>,
    organization: Option<OrganizationContextRef>,
}

#[derive(Deserialize)]
struct ProjectContextRef {
    name: String,
}

#[derive(Deserialize)]
struct OrganizationContextRef {
    name: String,
}

fn apply_auth_config(
    cfg: &mut config::Config,
    base_url: &str,
    resp: &AuthResponse,
    selection: &AuthSelection,
) {
    cfg.token = Some(resp.token.clone());
    cfg.email = Some(resp.email.clone());
    cfg.last_claim_token = None;
    cfg.default_organization = selection.organization.clone();
    cfg.default_project = selection.project.clone();
    if cfg.url.is_none() && base_url != DEFAULT_API_URL {
        cfg.url = Some(base_url.to_string());
    }
}

fn organization_by_name<'a>(
    organizations: &'a [org::OrganizationItem],
    name: &str,
) -> Option<&'a org::OrganizationItem> {
    organizations.iter().find(|item| item.name == name)
}

fn select_organization(
    organizations: &[org::OrganizationItem],
    cli_org: Option<&str>,
    env_org: Option<&str>,
    cfg_org: Option<&str>,
) -> Result<Option<org::OrganizationItem>> {
    if let Some(name) = cli_org {
        return organization_by_name(organizations, name)
            .cloned()
            .map(Some)
            .ok_or_else(|| anyhow::anyhow!("Organization '{}' not found for current user.", name));
    }

    if let Some(name) = env_org {
        if let Some(found) = organization_by_name(organizations, name) {
            return Ok(Some(found.clone()));
        }
    }

    if let Some(name) = cfg_org {
        if let Some(found) = organization_by_name(organizations, name) {
            return Ok(Some(found.clone()));
        }
    }

    Ok(organizations.first().cloned())
}

fn select_project(
    project_names: &[String],
    selected_org: &str,
    cli_project: Option<&str>,
) -> Result<Option<String>> {
    if let Some(name) = cli_project {
        return project_names
            .iter()
            .find(|project| project.as_str() == name)
            .cloned()
            .map(Some)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Project '{}' not found in organization '{}'.",
                    name,
                    selected_org
                )
            });
    }

    Ok(project_names.first().cloned())
}

fn prompt_for_selection(label: &str, names: &[String], json_mode: bool) -> Result<Option<String>> {
    if json_mode || !io::stdin().is_terminal() {
        let flag_name = if label == "organization" {
            "org"
        } else {
            label
        };
        anyhow::bail!(
            "No {} specified. Run this command interactively or pass --{} <name>.",
            label,
            flag_name
        );
    }

    println!("Select {}:", label);
    for (index, name) in names.iter().enumerate() {
        println!("  {}. {}", index + 1, name);
    }

    loop {
        print!("{}: ", label);
        io::stdout().flush()?;

        let input = read_selection_input(label)?;
        if input.is_empty() {
            eprintln!("Enter a {} name or number.", label);
            continue;
        }

        if let Some(index) = parse_selection_number(&input, names.len()) {
            if let Some(name) = names.get(index) {
                return Ok(Some(name.clone()));
            }
        }

        if let Some(name) = names.iter().find(|name| name.as_str() == input.as_str()) {
            return Ok(Some(name.clone()));
        }

        eprintln!("{} '{}' was not found in the list.", label, input);
    }
}

fn select_single_or_prompt(
    label: &str,
    names: &[String],
    json_mode: bool,
) -> Result<Option<String>> {
    match names {
        [] => Ok(None),
        [name] => Ok(Some(name.clone())),
        _ => prompt_for_selection(label, names, json_mode),
    }
}

fn read_selection_input(label: &str) -> Result<String> {
    let mut input = String::new();
    let bytes_read = io::stdin().read_line(&mut input)?;
    if bytes_read == 0 {
        anyhow::bail!("No {} selected: input closed.", label);
    }
    Ok(input.trim().to_string())
}

fn parse_selection_number(input: &str, item_count: usize) -> Option<usize> {
    let selected = input.parse::<usize>().ok()?;
    if selected == 0 || selected > item_count {
        return None;
    }
    Some(selected - 1)
}

fn prompt_for_organization(
    organizations: &[org::OrganizationItem],
    json_mode: bool,
) -> Result<Option<org::OrganizationItem>> {
    let names = organizations
        .iter()
        .map(|organization| organization.name.clone())
        .collect::<Vec<_>>();
    let selected_name = select_single_or_prompt("organization", &names, json_mode)?;
    Ok(selected_name.and_then(|name| organization_by_name(organizations, &name).cloned()))
}

fn select_or_prompt_organization(
    organizations: &[org::OrganizationItem],
    cli_org: Option<&str>,
    json_mode: bool,
) -> Result<Option<org::OrganizationItem>> {
    if cli_org.is_some() {
        return select_organization(organizations, cli_org, None, None);
    }

    prompt_for_organization(organizations, json_mode)
}

fn select_or_prompt_project(
    project_names: &[String],
    selected_org: &str,
    cli_project: Option<&str>,
    json_mode: bool,
) -> Result<Option<String>> {
    if cli_project.is_some() {
        return select_project(project_names, selected_org, cli_project);
    }

    select_single_or_prompt("project", project_names, json_mode)
}

fn resolve_selected_project(
    project_names_result: Result<Vec<String>>,
    selected_org: &str,
    cli_project: Option<&str>,
) -> Result<Option<String>> {
    match project_names_result {
        Ok(project_names) => select_project(&project_names, selected_org, cli_project),
        Err(err) if cli_project.is_some() => Err(err),
        Err(_err) => Ok(None),
    }
}

fn resolve_selected_browser_project(
    project_names_result: Result<Vec<String>>,
    selected_org: &str,
    cli_project: Option<&str>,
    json_mode: bool,
) -> Result<Option<String>> {
    match project_names_result {
        Ok(project_names) => {
            select_or_prompt_project(&project_names, selected_org, cli_project, json_mode)
        }
        Err(err) if cli_project.is_some() => Err(err),
        Err(_err) => Ok(None),
    }
}

fn list_projects_for_organization(
    client: &ApiClient,
    organization_name: &str,
) -> Result<Vec<String>> {
    let path = org::projects_collection_path(Some(organization_name));
    let resp: ListProjectsResponse = client.get(&path)?;
    Ok(resp.projects.into_iter().map(|item| item.name).collect())
}

fn resolve_browser_auth_selection(
    base_url: &str,
    token: &str,
    cli_org: Option<&str>,
    cli_project: Option<&str>,
    json_mode: bool,
) -> Result<AuthSelection> {
    let authed_client = ApiClient::new(base_url.to_string(), Some(token.to_string()));
    let organizations = match org::list_organizations(&authed_client) {
        Ok(items) => items,
        Err(err) if cli_org.is_some() || cli_project.is_some() => {
            return Err(err.context("failed to list organizations for auth-time selection"));
        }
        Err(_err) => return Ok(AuthSelection::default()),
    };

    let selected_org = select_or_prompt_organization(&organizations, cli_org, json_mode)?;
    let selected_org = match selected_org {
        Some(item) => item,
        None => {
            if let Some(project_name) = cli_project {
                anyhow::bail!(
                    "Cannot select project '{}' because no organization is available.",
                    project_name
                );
            }
            return Ok(AuthSelection::default());
        }
    };

    let selected_project = resolve_selected_browser_project(
        list_projects_for_organization(&authed_client, &selected_org.name).with_context(|| {
            format!(
                "failed to list projects for organization '{}'",
                selected_org.name
            )
        }),
        &selected_org.name,
        cli_project,
        json_mode,
    )?;

    Ok(AuthSelection {
        organization: Some(selected_org.name),
        project: selected_project,
    })
}

fn resolve_auth_selection(
    base_url: &str,
    token: &str,
    cli_org: Option<&str>,
    cli_project: Option<&str>,
    env_org: Option<&str>,
    cfg_org: Option<&str>,
) -> Result<AuthSelection> {
    let authed_client = ApiClient::new(base_url.to_string(), Some(token.to_string()));
    let organizations = match org::list_organizations(&authed_client) {
        Ok(items) => items,
        Err(err) if cli_org.is_some() || cli_project.is_some() => {
            return Err(err.context("failed to list organizations for auth-time selection"));
        }
        Err(_err) => return Ok(AuthSelection::default()),
    };

    let selected_org = select_organization(&organizations, cli_org, env_org, cfg_org)?;
    let selected_org = match selected_org {
        Some(item) => item,
        None => {
            if let Some(project_name) = cli_project {
                anyhow::bail!(
                    "Cannot select project '{}' because no organization is available.",
                    project_name
                );
            }
            return Ok(AuthSelection::default());
        }
    };

    let selected_project = resolve_selected_project(
        list_projects_for_organization(&authed_client, &selected_org.name).with_context(|| {
            format!(
                "failed to list projects for organization '{}'",
                selected_org.name
            )
        }),
        &selected_org.name,
        cli_project,
    )?;

    Ok(AuthSelection {
        organization: Some(selected_org.name),
        project: selected_project,
    })
}

fn auth_selection_from_project_context(
    context: ProjectContextResponse,
    cli_org: Option<&str>,
    cli_project: Option<&str>,
) -> Result<AuthSelection> {
    let organization = context
        .organization
        .map(|org| org.name)
        .ok_or_else(|| anyhow::anyhow!("server response did not include an organization"))?;
    let project = context
        .project
        .map(|project| project.name)
        .ok_or_else(|| anyhow::anyhow!("server response did not include a project"))?;

    if let Some(requested_org) = cli_org {
        if requested_org != organization {
            anyhow::bail!(
                "API key belongs to organization '{}', not '{}'.",
                organization,
                requested_org
            );
        }
    }
    if let Some(requested_project) = cli_project {
        if requested_project != project {
            anyhow::bail!(
                "API key belongs to project '{}', not '{}'.",
                project,
                requested_project
            );
        }
    }

    Ok(AuthSelection {
        organization: Some(organization),
        project: Some(project),
    })
}

fn resolve_api_key_auth_selection(
    base_url: &str,
    token: &str,
    cli_org: Option<&str>,
    cli_project: Option<&str>,
) -> Result<AuthSelection> {
    let authed_client = ApiClient::new(base_url.to_string(), Some(token.to_string()));

    match authed_client.get::<ProjectContextResponse>("/v1/keys") {
        Ok(context) => return auth_selection_from_project_context(context, cli_org, cli_project),
        Err(keys_err) => match authed_client.get::<ProjectContextResponse>("/v1/tables") {
            Ok(context) => auth_selection_from_project_context(context, cli_org, cli_project),
            Err(tables_err) => Err(anyhow::anyhow!(
                "failed to resolve API key project context: {}; fallback /v1/tables failed: {}",
                keys_err,
                tables_err
            )),
        },
    }
}

fn map_validation_error(err: anyhow::Error) -> anyhow::Error {
    output::coded_error("validation_failed", format!("{:#}", err), 1)
}

fn map_write_error(err: anyhow::Error) -> anyhow::Error {
    output::coded_error("write_failed", format!("{:#}", err), 1)
}

fn map_config_read_error(err: anyhow::Error) -> anyhow::Error {
    output::coded_error("config_read_failed", format!("{:#}", err), 1)
}

fn update_and_save_config(
    client: &ApiClient,
    resp: &AuthResponse,
    cli_org: Option<&str>,
    cli_project: Option<&str>,
) -> Result<AuthSelection> {
    let mut cfg = config::load()?;
    let env_org = std::env::var("RAWTREE_ORG").ok();
    let selection = resolve_auth_selection(
        &client.base_url,
        &resp.token,
        cli_org,
        cli_project,
        env_org.as_deref(),
        cfg.default_organization.as_deref(),
    )?;
    apply_auth_config(&mut cfg, &client.base_url, resp, &selection);
    config::save(&cfg)?;
    Ok(selection)
}

fn update_and_save_browser_config(
    client: &ApiClient,
    resp: &AuthResponse,
    cli_org: Option<&str>,
    cli_project: Option<&str>,
    json_mode: bool,
) -> Result<AuthSelection> {
    let mut cfg = config::load()?;
    let selection = resolve_browser_auth_selection(
        &client.base_url,
        &resp.token,
        cli_org,
        cli_project,
        json_mode,
    )?;
    apply_auth_config(&mut cfg, &client.base_url, resp, &selection);
    config::save(&cfg)?;
    Ok(selection)
}

fn print_selected_context(selection: &AuthSelection) {
    match &selection.organization {
        Some(org_name) => println!("Selected organization: {}", org_name),
        None => println!("Selected organization: none"),
    }
    match &selection.project {
        Some(project_name) => println!("Selected project: {}", project_name),
        None => {
            println!("Selected project: none");
            eprintln!(
                "Warning: No default project selected. Create one with `rtree project create <name>`."
            );
        }
    }
}

fn clear_auth_config(cfg: &mut config::Config) {
    *cfg = config::Config::default();
}

pub fn register(
    client: &ApiClient,
    email: &str,
    password: &str,
    organization: Option<String>,
    project: Option<String>,
    json_mode: bool,
) -> Result<()> {
    let resp: AuthResponse = client.post(
        "/v1/auth/register",
        &json!({"email": email, "password": password}),
    )?;

    let selection =
        update_and_save_config(client, &resp, organization.as_deref(), project.as_deref())?;
    let selected_organization = selection.organization.clone();
    let selected_project = selection.project.clone();

    output::print_result(
        &json!({
            "email": resp.email,
            "status": "registered",
            "selected_organization": selected_organization,
            "selected_project": selected_project,
        }),
        json_mode,
        |_| {
            println!("Registered and logged in as {}.", resp.email);
            print_selected_context(&selection);
        },
    );
    Ok(())
}

pub fn login(
    client: &ApiClient,
    email: &str,
    password: &str,
    organization: Option<String>,
    project: Option<String>,
    json_mode: bool,
) -> Result<()> {
    let resp: AuthResponse = client.post(
        "/v1/auth/login",
        &json!({"email": email, "password": password}),
    )?;

    let selection =
        update_and_save_config(client, &resp, organization.as_deref(), project.as_deref())?;
    let selected_organization = selection.organization.clone();
    let selected_project = selection.project.clone();

    output::print_result(
        &json!({
            "email": resp.email,
            "status": "logged_in",
            "selected_organization": selected_organization,
            "selected_project": selected_project,
        }),
        json_mode,
        |_| {
            println!("Logged in as {}.", resp.email);
            print_selected_context(&selection);
        },
    );
    Ok(())
}

pub fn login_with_api_key(
    client: &ApiClient,
    api_key: &str,
    organization: Option<String>,
    project: Option<String>,
    json_mode: bool,
) -> Result<()> {
    if api_key.is_empty() {
        return Err(output::coded_error(
            "missing_api_key",
            "API key is required. Pass --api-key or provide it interactively.",
            1,
        ));
    }

    if api_key.chars().any(char::is_whitespace) {
        return Err(output::coded_error(
            "invalid_api_key_format",
            "Invalid API key format. API key must not contain whitespace.",
            1,
        ));
    }

    if !api_key.starts_with("rw_") {
        return Err(output::coded_error(
            "invalid_api_key_format",
            "Invalid API key format. Expected an API key starting with 'rw_'.",
            1,
        ));
    }

    let mut cfg = config::load().map_err(map_config_read_error)?;
    let selection = resolve_api_key_auth_selection(
        &client.base_url,
        api_key,
        organization.as_deref(),
        project.as_deref(),
    )
    .map_err(map_validation_error)?;

    cfg.token = Some(api_key.to_string());
    cfg.email = None;
    cfg.last_claim_token = None;
    cfg.default_organization = selection.organization.clone();
    cfg.default_project = selection.project.clone();
    if cfg.url.is_none() && client.base_url != DEFAULT_API_URL {
        cfg.url = Some(client.base_url.clone());
    }

    config::save(&cfg).map_err(map_write_error)?;
    let config_path = config::path().map_err(map_write_error)?;
    let config_path = config_path.display().to_string();
    let selected_organization = selection.organization.clone();
    let selected_project = selection.project.clone();

    output::print_result(
        &json!({
            "success": true,
            "config_path": config_path,
            "project": selected_project,
            "organization": selected_organization,
        }),
        json_mode,
        |_| {
            println!("API key saved to {}.", config_path);
            print_selected_context(&selection);
        },
    );
    Ok(())
}

fn effective_timeout_seconds(requested_timeout_seconds: u64, expires_in: u64) -> u64 {
    if requested_timeout_seconds == 0 {
        return expires_in;
    }
    requested_timeout_seconds.min(expires_in)
}

fn format_api_error(status: u16, body: &str) -> anyhow::Error {
    if let Ok(parsed) = serde_json::from_str::<ApiErrorResponse>(body) {
        if let Some(hint) = parsed.hint.as_deref() {
            if !hint.is_empty() {
                return anyhow::anyhow!(
                    "Server error ({}): {}\nHint: {}",
                    status,
                    parsed.message,
                    hint
                );
            }
        }
        return anyhow::anyhow!("Server error ({}): {}", status, parsed.message);
    }
    anyhow::anyhow!("Server error ({}): {}", status, body)
}

fn poll_cli_device_token(base_url: &str, device_code: &str) -> Result<CliDeviceTokenPoll> {
    let url = format!("{}{}", base_url, "/v1/auth/cli/device/token");
    let response = HttpClient::new()
        .post(&url)
        .json(&json!({"device_code": device_code}))
        .send()
        .context("failed to connect to server")?;

    let status = response.status();
    let status_code = status.as_u16();
    let body = response.text().context("failed to read response body")?;

    if status.is_success() {
        let parsed = serde_json::from_str::<CliDeviceTokenResponse>(&body)
            .context("failed to parse server response")?;
        return Ok(CliDeviceTokenPoll::Approved(parsed));
    }

    if status_code == 428 {
        return Ok(CliDeviceTokenPoll::Pending);
    }

    if let Ok(parsed) = serde_json::from_str::<ApiErrorResponse>(&body) {
        if parsed.error == "authorization_pending" {
            return Ok(CliDeviceTokenPoll::Pending);
        }
    }

    Err(format_api_error(status_code, &body))
}

pub fn login_with_browser(
    client: &ApiClient,
    no_browser: bool,
    timeout_seconds: u64,
    organization: Option<String>,
    project: Option<String>,
    json_mode: bool,
) -> Result<()> {
    let start: CliDeviceStartResponse = client.post("/v1/auth/cli/device/start", &json!({}))?;
    let total_timeout_seconds = effective_timeout_seconds(timeout_seconds, start.expires_in);
    let poll_interval_seconds = start.interval.max(1);

    if !json_mode {
        println!("CLI login code: {}", start.user_code);
        if no_browser {
            println!(
                "Open this URL to continue login: {}",
                start.verification_uri_complete
            );
        } else if let Err(error) = webbrowser::open(&start.verification_uri_complete) {
            eprintln!("Warning: failed to open browser automatically ({}).", error);
            println!(
                "Open this URL to continue login: {}",
                start.verification_uri_complete
            );
        } else {
            println!("Opened browser for login: {}", start.verification_uri);
            println!(
                "If it did not open correctly, visit: {}",
                start.verification_uri_complete
            );
        }
        println!("Waiting for approval...");
    }

    let deadline = Instant::now() + Duration::from_secs(total_timeout_seconds);
    loop {
        match poll_cli_device_token(&client.base_url, &start.device_code)? {
            CliDeviceTokenPoll::Approved(resp) => {
                let CliDeviceTokenResponse {
                    token,
                    user_id: _user_id,
                    email,
                } = resp;
                let auth = AuthResponse { token, email };
                let selection = update_and_save_browser_config(
                    client,
                    &auth,
                    organization.as_deref(),
                    project.as_deref(),
                    json_mode,
                )?;
                let selected_organization = selection.organization.clone();
                let selected_project = selection.project.clone();
                output::print_result(
                    &json!({
                        "email": auth.email,
                        "status": "logged_in",
                        "method": "browser",
                        "selected_organization": selected_organization,
                        "selected_project": selected_project,
                    }),
                    json_mode,
                    |_| {
                        println!("Logged in as {}.", auth.email);
                        print_selected_context(&selection);
                    },
                );
                return Ok(());
            }
            CliDeviceTokenPoll::Pending => {
                if Instant::now() >= deadline {
                    anyhow::bail!(
                        "Browser login timed out after {} seconds. Run `rtree login` to try again.",
                        total_timeout_seconds
                    );
                }
                thread::sleep(Duration::from_secs(poll_interval_seconds));
            }
        }
    }
}

pub fn logout(json_mode: bool) -> Result<()> {
    let mut cfg = config::load()?;
    clear_auth_config(&mut cfg);
    config::save(&cfg)?;

    output::print_result(&json!({"status": "logged_out"}), json_mode, |_| {
        println!("Logged out. Local config reset to defaults.");
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        apply_auth_config, auth_selection_from_project_context, clear_auth_config,
        effective_timeout_seconds, parse_selection_number, prompt_for_selection,
        resolve_selected_project, select_or_prompt_organization, select_or_prompt_project,
        select_organization, select_project, AuthResponse, AuthSelection, ProjectContextResponse,
    };
    use crate::config::Config;
    use crate::org::OrganizationItem;

    fn sample_auth_response() -> AuthResponse {
        AuthResponse {
            token: "jwt".to_string(),
            email: "user@example.com".to_string(),
        }
    }

    fn sample_org(name: &str) -> OrganizationItem {
        OrganizationItem {
            name: name.to_string(),
            role: "owner".to_string(),
        }
    }

    #[test]
    fn apply_auth_config_sets_default_organization_and_project() {
        let mut cfg = Config::default();
        let resp = sample_auth_response();
        let selection = AuthSelection {
            organization: Some("team_alpha".to_string()),
            project: Some("analytics".to_string()),
        };
        apply_auth_config(&mut cfg, "https://api.rawtree.com", &resp, &selection);

        assert_eq!(cfg.token.as_deref(), Some("jwt"));
        assert_eq!(cfg.email.as_deref(), Some("user@example.com"));
        assert_eq!(cfg.default_organization.as_deref(), Some("team_alpha"));
        assert_eq!(cfg.default_project.as_deref(), Some("analytics"));
        assert_eq!(cfg.url, None);
    }

    #[test]
    fn apply_auth_config_sets_url_when_using_non_default_api_url() {
        let mut cfg = Config::default();
        let resp = sample_auth_response();
        let selection = AuthSelection::default();
        apply_auth_config(&mut cfg, "https://staging.rawtree.dev", &resp, &selection);

        assert_eq!(cfg.url.as_deref(), Some("https://staging.rawtree.dev"));
    }

    #[test]
    fn apply_auth_config_clears_default_selection_when_missing() {
        let mut cfg = Config {
            default_project: Some("old_project".to_string()),
            default_organization: Some("old_team".to_string()),
            ..Config::default()
        };
        let resp = sample_auth_response();
        let selection = AuthSelection::default();
        apply_auth_config(&mut cfg, "https://api.rawtree.com", &resp, &selection);

        assert_eq!(cfg.default_organization, None);
        assert_eq!(cfg.default_project, None);
    }

    #[test]
    fn apply_auth_config_clears_last_claim_token() {
        let mut cfg = Config {
            last_claim_token: Some("stale_claim".to_string()),
            ..Config::default()
        };
        let resp = sample_auth_response();
        let selection = AuthSelection::default();
        apply_auth_config(&mut cfg, "https://api.rawtree.com", &resp, &selection);

        assert_eq!(cfg.last_claim_token, None);
    }

    #[test]
    fn select_organization_uses_cli_when_present() {
        let organizations = vec![sample_org("team_alpha"), sample_org("team_beta")];
        let selected = select_organization(
            &organizations,
            Some("team_beta"),
            Some("team_alpha"),
            Some("team_alpha"),
        )
        .expect("selection should succeed")
        .expect("organization should be selected");

        assert_eq!(selected.name, "team_beta");
    }

    #[test]
    fn select_organization_errors_for_unknown_cli_org() {
        let organizations = vec![sample_org("team_alpha")];
        let result = select_organization(&organizations, Some("missing"), None, None);
        assert!(result.is_err(), "unknown CLI org should fail");
    }

    #[test]
    fn select_organization_uses_env_then_cfg_then_first() {
        let organizations = vec![sample_org("team_alpha"), sample_org("team_beta")];

        let env_selected = select_organization(&organizations, None, Some("team_beta"), None)
            .expect("env selection should succeed")
            .expect("organization should exist");
        assert_eq!(env_selected.name, "team_beta");

        let cfg_selected =
            select_organization(&organizations, None, Some("missing"), Some("team_beta"))
                .expect("cfg selection should succeed")
                .expect("organization should exist");
        assert_eq!(cfg_selected.name, "team_beta");

        let first_selected =
            select_organization(&organizations, None, Some("missing"), Some("also_missing"))
                .expect("fallback selection should succeed")
                .expect("organization should exist");
        assert_eq!(first_selected.name, "team_alpha");
    }

    #[test]
    fn select_project_prefers_cli_and_fails_when_unknown() {
        let projects = vec!["analytics".to_string(), "billing".to_string()];

        let selected = select_project(&projects, "team_alpha", Some("billing"))
            .expect("selection should succeed")
            .expect("project should exist");
        assert_eq!(selected, "billing");

        let err = select_project(&projects, "team_alpha", Some("missing"));
        assert!(err.is_err(), "unknown CLI project should fail");
    }

    #[test]
    fn select_project_defaults_to_first_when_cli_missing() {
        let projects = vec!["analytics".to_string(), "billing".to_string()];
        let selected = select_project(&projects, "team_alpha", None)
            .expect("selection should succeed")
            .expect("first project should be selected");
        assert_eq!(selected, "analytics");
    }

    #[test]
    fn browser_project_selection_prefers_cli_and_fails_when_unknown() {
        let projects = vec!["analytics".to_string(), "billing".to_string()];

        let selected = select_or_prompt_project(&projects, "team_alpha", Some("billing"), true)
            .expect("selection should succeed")
            .expect("project should exist");
        assert_eq!(selected, "billing");

        let err = select_or_prompt_project(&projects, "team_alpha", Some("missing"), true);
        assert!(err.is_err(), "unknown CLI project should fail");
    }

    #[test]
    fn browser_project_selection_uses_only_project_without_prompt() {
        let projects = vec!["analytics".to_string()];

        let selected = select_or_prompt_project(&projects, "team_alpha", None, true)
            .expect("selection should succeed")
            .expect("project should exist");
        assert_eq!(selected, "analytics");
    }

    #[test]
    fn browser_organization_selection_uses_only_org_without_prompt() {
        let organizations = vec![sample_org("team_alpha")];

        let selected = select_or_prompt_organization(&organizations, None, true)
            .expect("selection should succeed")
            .expect("organization should exist");
        assert_eq!(selected.name, "team_alpha");
    }

    #[test]
    fn browser_selection_requires_prompt_when_json_mode_and_missing() {
        let projects = vec!["analytics".to_string(), "billing".to_string()];

        let err = prompt_for_selection("project", &projects, true)
            .expect_err("json browser login should require an explicit project");
        assert!(
            err.to_string().contains("No project specified"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn selection_number_is_one_based() {
        assert_eq!(parse_selection_number("1", 2), Some(0));
        assert_eq!(parse_selection_number("2", 2), Some(1));
        assert_eq!(parse_selection_number("0", 2), None);
        assert_eq!(parse_selection_number("3", 2), None);
        assert_eq!(parse_selection_number("analytics", 2), None);
    }

    #[test]
    fn resolve_selected_project_tolerates_fetch_errors_when_cli_project_missing() {
        let result = resolve_selected_project(
            Err(anyhow::anyhow!("failed to list projects")),
            "team_alpha",
            None,
        )
        .expect("implicit selection should not fail");
        assert_eq!(result, None);
    }

    #[test]
    fn resolve_selected_project_fails_on_fetch_errors_when_cli_project_provided() {
        let result = resolve_selected_project(
            Err(anyhow::anyhow!("failed to list projects")),
            "team_alpha",
            Some("analytics"),
        );
        assert!(result.is_err(), "explicit project should remain strict");
    }

    #[test]
    fn api_key_auth_selection_uses_server_project_context() {
        let context: ProjectContextResponse = serde_json::from_str(
            r#"{
                "project": {"name": "analytics"},
                "organization": {"name": "team_alpha"}
            }"#,
        )
        .expect("valid context");
        let selection = auth_selection_from_project_context(context, None, None)
            .expect("context should select");

        assert_eq!(selection.organization.as_deref(), Some("team_alpha"));
        assert_eq!(selection.project.as_deref(), Some("analytics"));
    }

    #[test]
    fn api_key_auth_selection_rejects_conflicting_project_context() {
        let context: ProjectContextResponse = serde_json::from_str(
            r#"{
                "project": {"name": "analytics"},
                "organization": {"name": "team_alpha"}
            }"#,
        )
        .expect("valid context");
        let err = auth_selection_from_project_context(context, Some("team_alpha"), Some("billing"))
            .expect_err("conflicting project should fail");

        assert!(
            err.to_string().contains("API key belongs to project"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn clear_auth_config_resets_auth_state_and_saved_url() {
        let mut cfg = Config {
            token: Some("rw_temp".to_string()),
            email: Some("user@example.com".to_string()),
            url: Some("https://api.rawtree.com".to_string()),
            default_project: Some("analytics".to_string()),
            default_organization: Some("team_alpha".to_string()),
            last_claim_token: Some("claim_abc".to_string()),
            ..Config::default()
        };

        clear_auth_config(&mut cfg);

        assert_eq!(cfg.token, None);
        assert_eq!(cfg.email, None);
        assert_eq!(cfg.url, None);
        assert_eq!(cfg.default_project, None);
        assert_eq!(cfg.default_organization, None);
        assert_eq!(cfg.last_claim_token, None);
    }

    #[test]
    fn timeout_uses_smaller_of_requested_and_expiry() {
        assert_eq!(effective_timeout_seconds(300, 600), 300);
        assert_eq!(effective_timeout_seconds(900, 600), 600);
    }

    #[test]
    fn timeout_uses_expiry_when_requested_is_zero() {
        assert_eq!(effective_timeout_seconds(0, 600), 600);
    }
}
