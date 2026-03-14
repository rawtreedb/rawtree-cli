mod cli;
mod client;
mod commands;
mod config;
mod org;
mod output;

use std::io::{self, IsTerminal, Read};

use anyhow::Result;
use clap::{CommandFactory, Parser};
use clap_complete::generate;

use cli::{
    Cli, Command, KeysCommand, OrganizationCommand, ProjectCommand, ShellType, TableCommand,
};
use client::ApiClient;

fn resolve_url(cli_url: Option<&str>) -> String {
    if let Some(url) = cli_url {
        return url.to_string();
    }
    if let Ok(url) = std::env::var("RAWTREE_URL") {
        return url;
    }
    if let Ok(cfg) = config::load() {
        if let Some(url) = cfg.url {
            return url;
        }
    }
    "https://api.rawtree.dev".to_string()
}

fn resolve_token() -> Option<String> {
    if let Ok(token) = std::env::var("RAWTREE_TOKEN") {
        return Some(token);
    }
    config::load().ok().and_then(|c| c.token)
}

fn resolve_project_from_sources(
    cli_project: Option<String>,
    env_project: Option<String>,
    cfg_project: Option<String>,
) -> Option<String> {
    cli_project.or(env_project).or(cfg_project)
}

fn resolve_optional_project(cli_project: Option<String>) -> Option<String> {
    let env_project = std::env::var("RAWTREE_PROJECT").ok();
    let cfg_project = config::load().ok().and_then(|c| c.default_project);
    resolve_project_from_sources(cli_project, env_project, cfg_project)
}

fn resolve_project(cli_project: Option<String>) -> Result<String> {
    resolve_optional_project(cli_project).ok_or_else(|| {
        anyhow::anyhow!(
            "No project specified. Use --project, RAWTREE_PROJECT env, or `rtree project use <name>`"
        )
    })
}

fn resolve_org_from_sources(
    cli_org: Option<String>,
    env_org: Option<String>,
    cfg_org: Option<String>,
) -> Option<String> {
    cli_org.or(env_org).or(cfg_org)
}

fn resolve_explicit_org(cli_org: Option<String>) -> Option<String> {
    let env_org = std::env::var("RAWTREE_ORG").ok();
    let cfg_org = config::load().ok().and_then(|c| c.default_organization);
    resolve_org_from_sources(cli_org, env_org, cfg_org)
}

fn resolve_effective_org_with<F>(
    explicit_org: Option<String>,
    fetch_default_org: F,
) -> Option<String>
where
    F: FnOnce() -> Option<String>,
{
    explicit_org.or_else(fetch_default_org)
}

fn resolve_effective_org(client: &ApiClient, cli_org: Option<String>) -> Option<String> {
    let explicit_org = resolve_explicit_org(cli_org);
    resolve_effective_org_with(explicit_org, || org::first_organization_name(client))
}

fn token_looks_like_jwt(token: &str) -> bool {
    let mut parts = token.split('.');
    parts.next().is_some()
        && parts.next().is_some()
        && parts.next().is_some()
        && parts.next().is_none()
}

fn should_resolve_org_for_project_create(token: Option<&str>) -> bool {
    token.map(token_looks_like_jwt).unwrap_or(false)
}

fn resolve_effective_org_for_project_create(
    client: &ApiClient,
    cli_org: Option<String>,
) -> Option<String> {
    if !should_resolve_org_for_project_create(client.token.as_deref()) {
        return None;
    }
    resolve_effective_org(client, cli_org)
}

fn resolve_saved_claim_token(cfg: &config::Config) -> Result<String> {
    if let Some(claim_token) = cfg.last_claim_token.clone() {
        return Ok(claim_token);
    }

    Err(anyhow::anyhow!(
        "No claim token found. Create an anonymous project first and try `rtree open` again."
    ))
}

fn build_claim_dashboard_url(base_url: &str, claim_token: &str) -> String {
    format!(
        "{}/claim/{}/dashboard",
        base_url.trim_end_matches('/'),
        urlencoding::encode(claim_token)
    )
}

fn should_open_claim_dashboard_by_default(token: Option<&str>, claim_token: Option<&str>) -> bool {
    token.map(|t| !token_looks_like_jwt(t)).unwrap_or(false) && claim_token.is_some()
}

fn should_bootstrap_anonymous_project_for_insert(
    has_jwt_auth: bool,
    token_present: bool,
    cli_project: Option<&str>,
    resolved_project: Option<&str>,
) -> bool {
    if has_jwt_auth {
        return false;
    }
    if !token_present {
        return true;
    }
    cli_project.is_some() || resolved_project.is_none()
}

fn create_anonymous_project_for_insert(
    client: &ApiClient,
    requested_project: Option<String>,
    json_mode: bool,
) -> Result<commands::project::CreatedProjectInfo> {
    if !json_mode {
        println!("Not logged in. Creating an anonymous project for this ingest...");
    }

    let initial_attempt =
        commands::project::create_for_insert(client, requested_project.as_deref());
    let created = match initial_attempt {
        Ok(info) => info,
        Err(err)
            if requested_project.is_some() && format!("{:#}", err).contains("already exists") =>
        {
            if !json_mode {
                println!(
                    "Requested project name already exists. Creating a random anonymous project instead..."
                );
            }
            commands::project::create_for_insert(client, None)?
        }
        Err(err) => return Err(err),
    };

    if !json_mode {
        println!("Using anonymous project '{}'.", created.project);
        if let Some(ref claim_token) = created.claim_token {
            let claim_url =
                build_claim_dashboard_url(&commands::open::resolve_ui_base_url(), claim_token);
            println!("Claim URL: {}", claim_url);
        }
    }

    Ok(created)
}

fn read_stdin() -> Result<String> {
    let mut buf = String::new();
    io::stdin().read_to_string(&mut buf)?;
    let trimmed = buf.trim().to_string();
    if trimmed.is_empty() {
        anyhow::bail!("no SQL provided on stdin");
    }
    Ok(trimmed)
}

fn resolve_sql(positional: Option<String>, flag: Option<String>) -> Result<String> {
    let raw = positional.or(flag);
    match raw {
        Some(s) if s == "-" => read_stdin(),
        Some(s) => Ok(s),
        None => {
            if !io::stdin().is_terminal() {
                read_stdin()
            } else {
                anyhow::bail!("SQL query is required. Provide it as a positional argument, with --query, or pipe via stdin")
            }
        }
    }
}

fn main() {
    let cli = Cli::parse();
    let json_mode = cli.json;
    if let Err(e) = run(cli) {
        let code = output::print_error(&e, json_mode);
        std::process::exit(code);
    }
}

fn prompt_password_if_missing(password: Option<String>) -> Result<String> {
    match password {
        Some(p) => Ok(p),
        None => {
            let p = rpassword::prompt_password("Password: ")?;
            if p.is_empty() {
                anyhow::bail!("password cannot be empty");
            }
            Ok(p)
        }
    }
}

fn run(cli: Cli) -> Result<()> {
    let Cli {
        api_url: cli_url,
        json,
        org: cli_org,
        command,
    } = cli;

    let url = resolve_url(cli_url.as_deref());
    let token = resolve_token();
    let client = ApiClient::new(url.clone(), token);

    match command {
        Command::Register { email, password } => {
            let password = prompt_password_if_missing(password)?;
            commands::auth::register(&client, &email, &password, json)
        }
        Command::Login {
            email,
            password,
            no_browser,
            timeout_seconds,
        } => {
            if let Some(email) = email {
                let password = prompt_password_if_missing(password)?;
                commands::auth::login(&client, &email, &password, json)
            } else {
                commands::auth::login_with_browser(&client, no_browser, timeout_seconds, json)
            }
        }
        Command::Logout => commands::auth::logout(json),
        Command::Project { action } => match action {
            ProjectCommand::List => {
                let effective_org = resolve_effective_org(&client, cli_org.clone());
                commands::project::list(&client, effective_org.as_deref(), json)
            }
            ProjectCommand::Create { name } => {
                let effective_org =
                    resolve_effective_org_for_project_create(&client, cli_org.clone());
                commands::project::create(&client, &name, effective_org.as_deref(), json)
            }
            ProjectCommand::Use { name } => commands::project::use_project(&name, json),
            ProjectCommand::Rename { old, new_name } => {
                let effective_org = resolve_effective_org(&client, cli_org.clone());
                commands::project::rename(&client, &old, &new_name, effective_org.as_deref(), json)
            }
            ProjectCommand::Delete { name } => {
                let effective_org = resolve_effective_org(&client, cli_org.clone());
                commands::project::delete(&client, &name, effective_org.as_deref(), json)
            }
        },
        Command::Organization { action } => match action {
            OrganizationCommand::List => commands::organization::list(&client, json),
            OrganizationCommand::Create { name } => {
                commands::organization::create(&client, &name, json)
            }
            OrganizationCommand::Use { name } => {
                commands::organization::use_organization(&name, json)
            }
            OrganizationCommand::Rename { old, new_name } => {
                commands::organization::rename(&client, &old, &new_name, json)
            }
            OrganizationCommand::Delete { name } => {
                commands::organization::delete(&client, &name, json)
            }
        },
        Command::Keys { action } => {
            let effective_org = resolve_effective_org(&client, cli_org.clone());
            match action {
                KeysCommand::List { project } => {
                    let project = resolve_project(project)?;
                    commands::keys::list(&client, &project, effective_org.as_deref(), json)
                }
                KeysCommand::Create {
                    project,
                    label,
                    permission,
                } => {
                    let project = resolve_project(project)?;
                    commands::keys::create(
                        &client,
                        &project,
                        effective_org.as_deref(),
                        &label,
                        &permission,
                        json,
                    )
                }
                KeysCommand::Delete { project, key_id } => {
                    let project = resolve_project(project)?;
                    commands::keys::delete(
                        &client,
                        &project,
                        effective_org.as_deref(),
                        &key_id,
                        json,
                    )
                }
            }
        }
        Command::Table { action } => {
            let effective_org = resolve_effective_org(&client, cli_org.clone());
            match action {
                TableCommand::List { project } => {
                    let project = resolve_project(project)?;
                    commands::table::list(&client, &project, effective_org.as_deref(), json)
                }
                TableCommand::Describe { project, table } => {
                    let project = resolve_project(project)?;
                    commands::table::describe(
                        &client,
                        &project,
                        effective_org.as_deref(),
                        &table,
                        json,
                    )
                }
            }
        }
        Command::Query {
            project,
            sql,
            query,
            format,
            limit,
        } => {
            let effective_org = resolve_effective_org(&client, cli_org.clone());
            let project = resolve_project(project)?;
            let sql = resolve_sql(sql, query)?;
            commands::query::query(
                &client,
                &project,
                effective_org.as_deref(),
                &sql,
                format.as_deref(),
                limit,
            )
        }
        Command::Insert {
            project,
            table,
            data,
            file,
            source_url,
        } => {
            let has_jwt_auth = should_resolve_org_for_project_create(client.token.as_deref());
            let cli_project = project.clone();
            let resolved_project = resolve_optional_project(project);
            let token_present = client.token.is_some();

            let mut insert_client = ApiClient::new(client.base_url.clone(), client.token.clone());
            let (project, effective_org) = if has_jwt_auth {
                (
                    resolve_project(cli_project)?,
                    resolve_effective_org(&client, cli_org.clone()),
                )
            } else {
                let should_bootstrap = should_bootstrap_anonymous_project_for_insert(
                    has_jwt_auth,
                    token_present,
                    cli_project.as_deref(),
                    resolved_project.as_deref(),
                );
                if should_bootstrap {
                    let requested_project = if token_present {
                        cli_project.clone()
                    } else {
                        cli_project.clone().or(resolved_project.clone())
                    };
                    let created =
                        create_anonymous_project_for_insert(&client, requested_project, json)?;
                    insert_client = ApiClient::new(client.base_url.clone(), Some(created.api_key));
                    (created.project, None)
                } else {
                    (
                        resolved_project.ok_or_else(|| {
                            anyhow::anyhow!(
                                "No project specified. Use --project, RAWTREE_PROJECT env, or `rtree project use <name>`"
                            )
                        })?,
                        None,
                    )
                }
            };

            commands::insert::insert(
                &insert_client,
                &project,
                effective_org.as_deref(),
                &table,
                data.as_deref(),
                file.as_deref(),
                source_url.as_deref(),
                json,
            )
        }
        Command::Sample {
            project,
            table,
            limit,
            format,
        } => {
            let effective_org = resolve_effective_org(&client, cli_org.clone());
            let project = resolve_project(project)?;
            commands::sample::sample(
                &client,
                &project,
                effective_org.as_deref(),
                &table,
                limit,
                format.as_deref(),
            )
        }
        Command::Export {
            project,
            sql,
            query,
            output,
            format,
        } => {
            let effective_org = resolve_effective_org(&client, cli_org);
            let project = resolve_project(project)?;
            let sql = resolve_sql(sql, query)?;
            commands::export::export(
                &client,
                &project,
                effective_org.as_deref(),
                &sql,
                &output,
                format.as_deref(),
            )
        }
        Command::Ping => commands::ping::ping(&client, json),
        Command::Docs => commands::docs::docs(&client),
        Command::Whoami => commands::whoami::whoami(&url, json),
        Command::Status => commands::status::status(&url, json),
        Command::Open { project } => {
            let ui_base_url = commands::open::resolve_ui_base_url();
            let cfg = config::load()?;

            if should_open_claim_dashboard_by_default(
                client.token.as_deref(),
                cfg.last_claim_token.as_deref(),
            ) {
                if let Ok(claim_token) = resolve_saved_claim_token(&cfg) {
                    let claim_dashboard_url = build_claim_dashboard_url(&ui_base_url, &claim_token);
                    return commands::open::open_url(&claim_dashboard_url, json);
                }
            }

            let effective_org = resolve_effective_org(&client, cli_org);
            let project = resolve_optional_project(project);
            commands::open::open(
                &ui_base_url,
                effective_org.as_deref(),
                project.as_deref(),
                json,
            )
        }
        Command::Completions { shell } => {
            let shell = match shell {
                ShellType::Bash => clap_complete::Shell::Bash,
                ShellType::Zsh => clap_complete::Shell::Zsh,
                ShellType::Fish => clap_complete::Shell::Fish,
            };
            generate(shell, &mut Cli::command(), "rtree", &mut io::stdout());
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::Config;

    use super::{
        build_claim_dashboard_url, resolve_effective_org_with, resolve_org_from_sources,
        resolve_project_from_sources, resolve_saved_claim_token,
        should_bootstrap_anonymous_project_for_insert, should_open_claim_dashboard_by_default,
        should_resolve_org_for_project_create, token_looks_like_jwt,
    };

    #[test]
    fn resolve_org_uses_cli_first() {
        let resolved = resolve_org_from_sources(
            Some("cli-org".to_string()),
            Some("env-org".to_string()),
            Some("cfg-org".to_string()),
        );
        assert_eq!(resolved.as_deref(), Some("cli-org"));
    }

    #[test]
    fn resolve_org_uses_env_when_cli_missing() {
        let resolved = resolve_org_from_sources(
            None,
            Some("env-org".to_string()),
            Some("cfg-org".to_string()),
        );
        assert_eq!(resolved.as_deref(), Some("env-org"));
    }

    #[test]
    fn resolve_org_uses_config_when_cli_and_env_missing() {
        let resolved = resolve_org_from_sources(None, None, Some("cfg-org".to_string()));
        assert_eq!(resolved.as_deref(), Some("cfg-org"));
    }

    #[test]
    fn resolve_org_returns_none_when_no_sources() {
        let resolved = resolve_org_from_sources(None, None, None);
        assert_eq!(resolved, None);
    }

    #[test]
    fn explicit_org_wins_over_auto_default_fetch() {
        let resolved = resolve_effective_org_with(Some("explicit-org".to_string()), || {
            Some("fetched-org".to_string())
        });
        assert_eq!(resolved.as_deref(), Some("explicit-org"));
    }

    #[test]
    fn auto_default_org_is_used_when_explicit_is_missing() {
        let resolved = resolve_effective_org_with(None, || Some("fetched-org".to_string()));
        assert_eq!(resolved.as_deref(), Some("fetched-org"));
    }

    #[test]
    fn auto_default_org_can_fall_back_to_none() {
        let resolved = resolve_effective_org_with(None, || None);
        assert_eq!(resolved, None);
    }

    #[test]
    fn token_looks_like_jwt_detects_jwt_shape() {
        assert!(token_looks_like_jwt("a.b.c"));
        assert!(!token_looks_like_jwt("rw_key"));
        assert!(!token_looks_like_jwt("a.b"));
    }

    #[test]
    fn project_create_org_resolution_requires_jwt_token() {
        assert!(should_resolve_org_for_project_create(Some("a.b.c")));
        assert!(!should_resolve_org_for_project_create(Some("rw_key")));
        assert!(!should_resolve_org_for_project_create(None));
    }

    #[test]
    fn resolve_project_uses_cli_first() {
        let resolved = resolve_project_from_sources(
            Some("cli-project".to_string()),
            Some("env-project".to_string()),
            Some("cfg-project".to_string()),
        );
        assert_eq!(resolved.as_deref(), Some("cli-project"));
    }

    #[test]
    fn resolve_project_uses_env_when_cli_missing() {
        let resolved = resolve_project_from_sources(
            None,
            Some("env-project".to_string()),
            Some("cfg-project".to_string()),
        );
        assert_eq!(resolved.as_deref(), Some("env-project"));
    }

    #[test]
    fn resolve_project_uses_config_when_cli_and_env_missing() {
        let resolved = resolve_project_from_sources(None, None, Some("cfg-project".to_string()));
        assert_eq!(resolved.as_deref(), Some("cfg-project"));
    }

    #[test]
    fn insert_bootstrap_skips_when_authenticated_with_jwt() {
        let should_bootstrap =
            should_bootstrap_anonymous_project_for_insert(true, true, Some("analytics"), None);
        assert!(!should_bootstrap);
    }

    #[test]
    fn insert_bootstrap_runs_when_no_token() {
        let should_bootstrap =
            should_bootstrap_anonymous_project_for_insert(false, false, None, Some("analytics"));
        assert!(should_bootstrap);
    }

    #[test]
    fn insert_bootstrap_runs_for_cli_project_with_api_key_token() {
        let should_bootstrap =
            should_bootstrap_anonymous_project_for_insert(false, true, Some("analytics"), None);
        assert!(should_bootstrap);
    }

    #[test]
    fn insert_bootstrap_skips_for_resolved_project_with_api_key_token() {
        let should_bootstrap =
            should_bootstrap_anonymous_project_for_insert(false, true, None, Some("analytics"));
        assert!(!should_bootstrap);
    }

    #[test]
    fn resolve_saved_claim_token_returns_value_when_present() {
        let cfg = Config {
            last_claim_token: Some("abc".to_string()),
            ..Config::default()
        };
        let claim_token = resolve_saved_claim_token(&cfg).expect("claim token should resolve");
        assert_eq!(claim_token, "abc");
    }

    #[test]
    fn resolve_saved_claim_token_errors_when_missing_token() {
        let cfg = Config::default();
        let err =
            resolve_saved_claim_token(&cfg).expect_err("missing claim token should produce error");
        assert!(
            format!("{:#}", err).contains("No claim token found"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn build_claim_dashboard_url_appends_dashboard_route() {
        let url = build_claim_dashboard_url("https://rawtree.com/", "a/b");
        assert_eq!(url, "https://rawtree.com/claim/a%2Fb/dashboard");
    }

    #[test]
    fn open_claim_dashboard_by_default_requires_temporary_auth_and_claim_token() {
        assert!(should_open_claim_dashboard_by_default(
            Some("rw_temp"),
            Some("claim_abc")
        ));
        assert!(!should_open_claim_dashboard_by_default(
            Some("a.b.c"),
            Some("claim_abc")
        ));
        assert!(!should_open_claim_dashboard_by_default(
            Some("rw_temp"),
            None
        ));
        assert!(!should_open_claim_dashboard_by_default(
            None,
            Some("claim_abc")
        ));
    }
}
