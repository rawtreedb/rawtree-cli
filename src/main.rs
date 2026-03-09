mod cli;
mod client;
mod commands;
mod config;
mod output;

use std::io::{self, IsTerminal, Read};

use anyhow::Result;
use clap::{CommandFactory, Parser};
use clap_complete::generate;

use cli::{Cli, Command, KeysCommand, ProjectCommand, ShellType, TableCommand};
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
    let url = resolve_url(cli.url.as_deref());
    let token = resolve_token();
    let client = ApiClient::new(url.clone(), token);
    let json = cli.json;

    match cli.command {
        Command::Register { email, password } => {
            let password = prompt_password_if_missing(password)?;
            commands::auth::register(&client, &email, &password, json)
        }
        Command::Login { email, password } => {
            let password = prompt_password_if_missing(password)?;
            commands::auth::login(&client, &email, &password, json)
        }
        Command::Project { action } => match action {
            ProjectCommand::List => commands::project::list(&client, json),
            ProjectCommand::Create { name } => commands::project::create(&client, &name, json),
            ProjectCommand::Use { name } => commands::project::use_project(&name, json),
            ProjectCommand::Rename { old, new_name } => {
                commands::project::rename(&client, &old, &new_name, json)
            }
            ProjectCommand::Delete { name } => commands::project::delete(&client, &name, json),
        },
        Command::Keys { action } => match action {
            KeysCommand::List { project } => {
                let project = resolve_project(project)?;
                commands::keys::list(&client, &project, json)
            }
            KeysCommand::Create {
                project,
                label,
                permission,
            } => {
                let project = resolve_project(project)?;
                commands::keys::create(&client, &project, &label, &permission, json)
            }
            KeysCommand::Delete { project, key_id } => {
                let project = resolve_project(project)?;
                commands::keys::delete(&client, &project, &key_id, json)
            }
        },
        Command::Table { action } => match action {
            TableCommand::List { project } => {
                let project = resolve_project(project)?;
                commands::table::list(&client, &project, json)
            }
            TableCommand::Describe { project, table } => {
                let project = resolve_project(project)?;
                commands::table::describe(&client, &project, &table, json)
            }
        },
        Command::Query {
            project,
            sql,
            query,
            format,
            limit,
        } => {
            let project = resolve_project(project)?;
            let sql = resolve_sql(sql, query)?;
            commands::query::query(&client, &project, &sql, format.as_deref(), limit)
        }
        Command::Insert {
            project,
            table,
            data,
            file,
        } => {
            let project = resolve_project(project)?;
            commands::insert::insert(
                &client,
                &project,
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
            let project = resolve_project(project)?;
            commands::sample::sample(&client, &project, &table, limit, format.as_deref())
        }
        Command::Export {
            project,
            sql,
            query,
            output,
            format,
        } => {
            let project = resolve_project(project)?;
            let sql = resolve_sql(sql, query)?;
            commands::export::export(&client, &project, &sql, &output, format.as_deref())
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
