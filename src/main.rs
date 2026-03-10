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

fn resolve_project(cli_project: Option<String>) -> Result<String> {
    if let Some(p) = cli_project {
        return Ok(p);
    }
    if let Ok(p) = std::env::var("RAWTREE_PROJECT") {
        return Ok(p);
    }
    if let Ok(cfg) = config::load() {
        if let Some(p) = cfg.default_project {
            return Ok(p);
        }
    }
    anyhow::bail!(
        "No project specified. Use --project, RAWTREE_PROJECT env, or `rtree project use <name>`"
    )
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
        url: cli_url,
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
        Command::Login { email, password } => {
            let password = prompt_password_if_missing(password)?;
            commands::auth::login(&client, &email, &password, json)
        }
        Command::Project { action } => match action {
            ProjectCommand::List => {
                let effective_org = resolve_effective_org(&client, cli_org.clone());
                commands::project::list(&client, effective_org.as_deref(), json)
            }
            ProjectCommand::Create { name } => {
                let effective_org = resolve_effective_org(&client, cli_org.clone());
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
            OrganizationCommand::Members { organization } => {
                commands::organization::members(&client, &organization, json)
            }
            OrganizationCommand::AddMember {
                organization,
                email,
            } => commands::organization::add_member(&client, &organization, &email, json),
            OrganizationCommand::UpdateMember {
                organization,
                user_id,
                role,
            } => commands::organization::update_member(
                &client,
                &organization,
                &user_id,
                role.as_str(),
                json,
            ),
            OrganizationCommand::RemoveMember {
                organization,
                user_id,
            } => commands::organization::remove_member(&client, &organization, &user_id, json),
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
    use super::{resolve_effective_org_with, resolve_org_from_sources};

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
}
