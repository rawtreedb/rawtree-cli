mod cli;
mod client;
mod commands;
mod config;
mod constants;
mod org;
mod output;

use std::io::{self, IsTerminal, Read};

use anyhow::Result;
use clap::{CommandFactory, Parser};
use clap_complete::generate;

use cli::{Cli, Command, KeyCommand, OrganizationCommand, ProjectCommand, ShellType, TableCommand};
use client::ApiClient;
use constants::DEFAULT_API_URL;

fn resolve_url(cli_url: Option<&str>) -> String {
    if let Some(url) = cli_url {
        return url.to_string();
    }
    if let Ok(url) = std::env::var("RAWTREE_API_URL") {
        return url;
    }
    if let Ok(url) = std::env::var("RAWTREE_URL") {
        return url;
    }
    if let Ok(cfg) = config::load() {
        if let Some(url) = cfg.url {
            return url;
        }
    }
    DEFAULT_API_URL.to_string()
}

fn resolve_token_from_sources(
    cli_api_key: Option<String>,
    env_api_key: Option<String>,
    legacy_env_token: Option<String>,
    cfg_token: Option<String>,
) -> Option<String> {
    cli_api_key
        .or(env_api_key)
        .or(legacy_env_token)
        .or(cfg_token)
}

fn resolve_token(cli_api_key: Option<String>) -> Option<String> {
    let env_api_key = std::env::var("RAWTREE_API_KEY").ok();
    let legacy_env_token = std::env::var("RAWTREE_TOKEN").ok();
    let cfg_token = config::load().ok().and_then(|c| c.token);
    resolve_token_from_sources(cli_api_key, env_api_key, legacy_env_token, cfg_token)
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

pub(crate) fn token_looks_like_jwt(token: &str) -> bool {
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
                anyhow::bail!("SQL query is required. Provide it as a positional argument, with --sql, or pipe via stdin")
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
        api_key: cli_api_key,
        api_url: cli_url,
        json,
        org: cli_org,
        command,
    } = cli;

    let url = resolve_url(cli_url.as_deref());
    let token = resolve_token(cli_api_key.clone());
    let client = ApiClient::new(url.clone(), token);

    match command {
        Command::Register {
            email,
            password,
            project,
        } => {
            let password = prompt_password_if_missing(password)?;
            commands::auth::register(&client, &email, &password, cli_org.clone(), project, json)
        }
        Command::Login {
            email,
            password,
            no_browser,
            timeout_seconds,
            project,
        } => {
            if let Some(api_key) = cli_api_key {
                if email.is_some() || password.is_some() {
                    anyhow::bail!(
                        "--api-key cannot be used with --email or --password during login"
                    );
                }
                commands::auth::login_with_api_key(
                    &client,
                    &api_key,
                    cli_org.clone(),
                    project,
                    json,
                )
            } else if let Some(email) = email {
                let password = prompt_password_if_missing(password)?;
                commands::auth::login(&client, &email, &password, cli_org.clone(), project, json)
            } else {
                commands::auth::login_with_browser(
                    &client,
                    no_browser,
                    timeout_seconds,
                    cli_org.clone(),
                    project,
                    json,
                )
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
        Command::Key { action } => {
            let effective_org = resolve_effective_org(&client, cli_org.clone());
            match action {
                KeyCommand::List { project } => {
                    let project = resolve_project(project)?;
                    commands::keys::list(&client, &project, effective_org.as_deref(), json)
                }
                KeyCommand::Create {
                    project,
                    name,
                    permission,
                } => {
                    let project = resolve_project(project)?;
                    commands::keys::create(
                        &client,
                        &project,
                        effective_org.as_deref(),
                        &name,
                        &permission,
                        json,
                    )
                }
                KeyCommand::Delete {
                    project,
                    id_or_token,
                } => {
                    let project = resolve_project(project)?;
                    commands::keys::delete(
                        &client,
                        &project,
                        effective_org.as_deref(),
                        &id_or_token,
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
        Command::Logs {
            project,
            r#type,
            table,
            status,
            limit,
            offset,
            since,
            until,
            start_time,
            end_time,
        } => {
            let effective_org = resolve_effective_org(&client, cli_org.clone());
            let project = resolve_project(project)?;
            commands::logs::logs(
                &client,
                &project,
                effective_org.as_deref(),
                r#type.as_deref(),
                &table,
                status.as_deref(),
                limit,
                offset,
                since.as_deref(),
                until.as_deref(),
                start_time.as_deref(),
                end_time.as_deref(),
                json,
            )
        }
        Command::Query {
            project,
            sql_positional,
            sql,
            limit,
        } => {
            let effective_org = resolve_effective_org(&client, cli_org.clone());
            let project = resolve_project(project)?;
            let sql = resolve_sql(sql_positional, sql)?;
            commands::query::query(
                &client,
                &project,
                effective_org.as_deref(),
                &sql,
                limit,
                json,
            )
        }
        Command::Insert {
            project,
            table,
            data,
            file,
            url,
            transform,
        } => {
            let effective_org = resolve_effective_org(&client, cli_org.clone());
            let project = resolve_project(project)?;

            commands::insert::insert(
                &client,
                &project,
                effective_org.as_deref(),
                &table,
                data.as_deref(),
                file.as_deref(),
                url.as_deref(),
                transform.as_deref(),
                json,
            )
        }
        Command::Ping => commands::ping::ping(&client, json),
        Command::Docs => commands::docs::docs(&client),
        Command::Status => commands::status::status(&url, json),
        Command::Open { project } => {
            let ui_base_url = commands::open::resolve_ui_base_url();
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
    use super::{
        resolve_effective_org_with, resolve_org_from_sources, resolve_project_from_sources,
        resolve_token_from_sources, resolve_url, should_resolve_org_for_project_create,
        token_looks_like_jwt,
    };

    struct EnvVarGuard {
        name: &'static str,
        original: Option<String>,
    }

    impl EnvVarGuard {
        fn set(name: &'static str, value: &str) -> Self {
            let original = std::env::var(name).ok();
            std::env::set_var(name, value);
            Self { name, original }
        }

        fn remove(name: &'static str) -> Self {
            let original = std::env::var(name).ok();
            std::env::remove_var(name);
            Self { name, original }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match &self.original {
                Some(value) => std::env::set_var(self.name, value),
                None => std::env::remove_var(self.name),
            }
        }
    }

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
    fn resolve_url_supports_legacy_rawtree_url_fallback() {
        let _api_url_guard = EnvVarGuard::remove("RAWTREE_API_URL");
        let _legacy_url_guard = EnvVarGuard::set("RAWTREE_URL", "https://legacy.example.com");

        assert_eq!(resolve_url(None), "https://legacy.example.com");
    }

    #[test]
    fn resolve_url_prefers_api_url_over_legacy_rawtree_url() {
        let _api_url_guard = EnvVarGuard::set("RAWTREE_API_URL", "https://api.example.com");
        let _legacy_url_guard = EnvVarGuard::set("RAWTREE_URL", "https://legacy.example.com");

        assert_eq!(resolve_url(None), "https://api.example.com");
    }

    #[test]
    fn resolve_token_uses_cli_first() {
        let resolved = resolve_token_from_sources(
            Some("cli-key".to_string()),
            Some("env-key".to_string()),
            Some("legacy-env-token".to_string()),
            Some("cfg-token".to_string()),
        );
        assert_eq!(resolved.as_deref(), Some("cli-key"));
    }

    #[test]
    fn resolve_token_uses_env_when_cli_missing() {
        let resolved = resolve_token_from_sources(
            None,
            Some("env-key".to_string()),
            Some("legacy-env-token".to_string()),
            Some("cfg-token".to_string()),
        );
        assert_eq!(resolved.as_deref(), Some("env-key"));
    }

    #[test]
    fn resolve_token_uses_legacy_env_when_api_key_env_missing() {
        let resolved = resolve_token_from_sources(
            None,
            None,
            Some("legacy-env-token".to_string()),
            Some("cfg-token".to_string()),
        );
        assert_eq!(resolved.as_deref(), Some("legacy-env-token"));
    }

    #[test]
    fn resolve_token_uses_config_when_cli_and_env_missing() {
        let resolved = resolve_token_from_sources(None, None, None, Some("cfg-token".to_string()));
        assert_eq!(resolved.as_deref(), Some("cfg-token"));
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
}
