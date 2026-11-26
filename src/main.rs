mod cli;
mod client;
mod cluster_identity;
mod commands;
mod config;
mod constants;
mod oauth;
mod org;
mod output;
mod s3_storage;

use std::cell::Cell;
use std::io::{self, IsTerminal, Read};
use std::process::ExitCode;

use anyhow::Result;
use clap::{CommandFactory, Parser};
use clap_complete::generate;

use cli::{
    ApiKeyCommand, Cli, ClusterCommand, Command, DatabaseCommand, IntegrationCommand,
    IntegrationKind, OrganizationCommand, ServiceAccountCommand, ShellType, TableCommand,
    WorkflowCommand, WorkflowSinkCommand,
};
use client::{ApiClient, Scope, Selector, ServerError};
use constants::DEFAULT_API_URL;

fn resolve_url_from_sources(
    cli_url: Option<&str>,
    env_api_url: Option<String>,
    cfg_url: Option<String>,
) -> String {
    cli_url
        .map(str::to_string)
        .or(env_api_url)
        .or(cfg_url)
        .unwrap_or_else(|| DEFAULT_API_URL.to_string())
}

fn resolve_url(cli_url: Option<&str>) -> String {
    let env_api_url = std::env::var("RAWTREE_API_URL").ok();
    let cfg_url = config::load().ok().and_then(|c| c.url);
    resolve_url_from_sources(cli_url, env_api_url, cfg_url)
}

fn resolve_database_from_sources(
    cli_database: Option<String>,
    env_database: Option<String>,
    cfg_database: Option<String>,
) -> Option<String> {
    cli_database.or(env_database).or(cfg_database)
}

fn resolve_optional_database(cli_database: Option<String>) -> Option<String> {
    let env_database = std::env::var("RAWTREE_DATABASE").ok();
    let cfg_database = config::load().ok().and_then(|c| c.default_database);
    resolve_database_from_sources(cli_database, env_database, cfg_database)
}

fn resolve_database(cli_database: Option<String>) -> Result<String> {
    resolve_optional_database(cli_database).ok_or_else(|| {
        anyhow::anyhow!(
            "No database specified. Use --database, RAWTREE_DATABASE env, or `rtree database use <name>`"
        )
    })
}

fn resolve_login_cluster_from_sources(
    cli_cluster: Option<String>,
    env_cluster: Option<String>,
) -> Option<String> {
    cli_cluster.or(env_cluster)
}

fn resolve_login_cluster(cli_cluster: Option<String>) -> Option<String> {
    let env_cluster = std::env::var("RAWTREE_CLUSTER").ok();
    // Login replaces saved credentials and defaults, so stale saved cluster
    // context must not constrain the new authentication selection.
    resolve_login_cluster_from_sources(cli_cluster, env_cluster)
}

/// An organization or cluster chosen by flag, environment or config. Only a
/// config choice carries the ID saved next to its name.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Choice {
    name: String,
    saved_id: Option<String>,
}

impl Choice {
    fn name(&self) -> &str {
        &self.name
    }
}

fn choose(
    explicit: Option<String>,
    configured: Option<String>,
    saved_id: Option<String>,
) -> Option<Choice> {
    match explicit {
        Some(name) => Some(Choice {
            name,
            saved_id: None,
        }),
        None => configured.map(|name| Choice { name, saved_id }),
    }
}

/// The organization and cluster commands act on unless they name their own.
/// Remembers whether a command used an ID saved in the config, because a
/// rejected saved ID is never replaced by a name lookup.
struct Selection {
    organization: Option<Choice>,
    cluster: Option<Choice>,
    used_saved_ids: Cell<bool>,
}

impl Selection {
    fn from_sources(
        cli_org: Option<String>,
        env_org: Option<String>,
        cli_cluster: Option<String>,
        env_cluster: Option<String>,
        cfg: config::Config,
    ) -> Self {
        let explicit_org = cli_org.or(env_org);
        // A saved cluster ID belongs to the saved organization.
        let cluster_id = cfg.default_cluster_id.filter(|_| explicit_org.is_none());
        Self {
            organization: choose(
                explicit_org,
                cfg.default_organization,
                cfg.default_organization_id,
            ),
            cluster: choose(cli_cluster.or(env_cluster), cfg.default_cluster, cluster_id),
            used_saved_ids: Cell::new(false),
        }
    }

    fn load(cli_org: Option<String>, cli_cluster: Option<String>) -> Self {
        Self::from_sources(
            cli_org,
            std::env::var("RAWTREE_ORG").ok(),
            cli_cluster,
            std::env::var("RAWTREE_CLUSTER").ok(),
            config::load().unwrap_or_default(),
        )
    }

    /// The chosen organization, or else the first one the credential can see.
    fn effective_organization(&self, client: &ApiClient) -> Option<Choice> {
        self.organization.clone().or_else(|| {
            org::first_organization_name(client).map(|name| Choice {
                name,
                saved_id: None,
            })
        })
    }

    fn selector(&self, choice: &Choice) -> Selector {
        match &choice.saved_id {
            Some(id) => {
                self.used_saved_ids.set(true);
                Selector::Id(id.clone())
            }
            None => Selector::parse(&choice.name),
        }
    }

    fn organization_selector(&self) -> Option<Selector> {
        self.organization
            .as_ref()
            .map(|choice| self.selector(choice))
    }

    fn scope(&self, organization: Option<&Choice>, cluster: Option<&Choice>) -> Scope {
        Scope {
            organization: organization.map(|choice| self.selector(choice)),
            cluster: cluster.map(|choice| self.selector(choice)),
        }
    }

    /// A client acting on the effective organization and the chosen cluster.
    fn scoped(&self, client: &ApiClient) -> ApiClient {
        let organization = self.effective_organization(client);
        client.with_scope(self.scope(organization.as_ref(), self.cluster.as_ref()))
    }

    /// A rejected saved ID is never replaced by a name lookup, so when the
    /// server refuses a command that used one, check whether the ID is still
    /// visible and say how to refresh it if not.
    fn explain_rejected_saved_ids(
        &self,
        client: &ApiClient,
        error: anyhow::Error,
    ) -> anyhow::Error {
        let refused =
            ServerError::of(&error).is_some_and(|error| (400..500).contains(&error.status));
        if !(self.used_saved_ids.get() && refused) {
            return error;
        }
        let organization = self.effective_organization(client);
        let saved = Scope {
            organization: organization.as_ref().map(|choice| self.selector(choice)),
            cluster: self
                .cluster
                .as_ref()
                .and_then(|choice| choice.saved_id.clone())
                .map(Selector::Id),
        };
        if !matches!(client.can_access(&saved), Ok(false)) {
            return error;
        }
        anyhow::anyhow!(
            "{error:#}\nThe saved organization or cluster ID was rejected; it may have been deleted or your access may have changed. Run `rtree organization use <name>` and `rtree cluster use <name>`, or `rtree login` again."
        )
    }
}

fn read_stdin() -> Result<String> {
    let mut buf = String::new();
    io::stdin().read_to_string(&mut buf)?;
    if buf.trim().is_empty() {
        anyhow::bail!("no SQL provided on stdin");
    }
    Ok(buf)
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
                anyhow::bail!("SQL query is required. Provide it as a positional argument, with --sql / --query / -q, or pipe via stdin")
            }
        }
    }
}

fn read_sql_arg(sql: String) -> Result<String> {
    if sql == "-" {
        read_stdin()
    } else {
        Ok(sql)
    }
}

fn ensure_single_stdin_reader(sql: Option<&str>, sinks: &[String]) -> Result<()> {
    if sql == Some("-") && sinks.iter().any(|sink| sink == "-") {
        return Err(output::coded_error(
            "validation_error",
            "--sql - and --sink - both read stdin; pass the sinks with --sink @<path> instead",
            2,
        ));
    }
    Ok(())
}

fn require_organization<T>(organization: Option<T>) -> Result<T> {
    organization.ok_or_else(|| {
        anyhow::anyhow!(
            "No organization specified. Use --org, RAWTREE_ORG env, or `rtree organization use <name>`"
        )
    })
}

/// API keys are bound to a cluster, so the backend takes their tenant from the
/// key; user credentials must select one.
fn require_workflow_scope(client: &ApiClient, scope: Scope) -> Result<Scope> {
    if client.has_api_key()? {
        return Ok(scope);
    }
    if !matches!(scope.cluster, Some(Selector::Id(_))) {
        require_organization(scope.organization.as_ref())?;
    }
    anyhow::ensure!(
        scope.cluster.is_some(),
        "No cluster specified. Use --cluster, RAWTREE_CLUSTER env, or `rtree cluster use <name>`"
    );
    Ok(scope)
}

fn main() -> ExitCode {
    let mut cli = Cli::parse();
    cli.json = effective_json_mode(&cli, io::stdin().is_terminal());
    let json_mode = cli.json;
    match run(cli) {
        Ok(code) => code,
        Err(error) => ExitCode::from(output::print_error(&error, json_mode) as u8),
    }
}

fn effective_json_mode(cli: &Cli, stdin_is_terminal: bool) -> bool {
    cli.json || (matches!(cli.command, Command::Login { .. }) && !stdin_is_terminal)
}

fn run(cli: Cli) -> Result<ExitCode> {
    let Cli {
        api_key: cli_api_key,
        api_url: cli_url,
        json,
        org: cli_org,
        cluster: cli_cluster,
        command,
    } = cli;

    let url = resolve_url(cli_url.as_deref());
    let client = if let Some(key) = cli_api_key
        .clone()
        .or_else(|| std::env::var("RAWTREE_API_KEY").ok())
    {
        ApiClient::new(url.clone(), Some(key))
    } else {
        ApiClient::from_saved_config(url.clone())
    };
    let selection = Selection::load(cli_org.clone(), cli_cluster.clone());
    let login_cluster = resolve_login_cluster(cli_cluster);

    let result = (|| -> Result<ExitCode> {
        match command {
            Command::Login {
                device_auth,
                no_browser,
                timeout_seconds,
                database,
            } => {
                if device_auth && cli_api_key.is_some() {
                    anyhow::bail!("--device-auth cannot be used with --api-key");
                }
                let client = ApiClient::new(url.clone(), None);
                if let Some(api_key) = cli_api_key.or_else(|| {
                    std::env::var("RAWTREE_API_KEY")
                        .ok()
                        .filter(|_| !device_auth)
                }) {
                    commands::auth::login_with_api_key(
                        &client,
                        &api_key,
                        cli_org.clone(),
                        login_cluster.clone(),
                        database,
                        json,
                    )
                } else {
                    return commands::auth::login_with_browser(
                        &client,
                        commands::auth::BrowserLoginOptions {
                            device_auth,
                            no_browser,
                            timeout_seconds,
                        },
                        cli_org.clone(),
                        login_cluster.clone(),
                        database,
                        json,
                    );
                }
            }
            Command::Logout => commands::auth::logout(json),
            Command::Database { action } => match action {
                DatabaseCommand::List => commands::database::list(&selection.scoped(&client), json),
                DatabaseCommand::Create { name, s3_storage } => {
                    let organization = selection.effective_organization(&client);
                    let cluster = selection.cluster.as_ref();
                    commands::database::create(
                        &client.with_scope(selection.scope(organization.as_ref(), cluster)),
                        &name,
                        organization.as_ref().map(Choice::name),
                        cluster.map(Choice::name),
                        s3_storage,
                        json,
                    )
                }
                DatabaseCommand::Use { name } => commands::database::use_database(&name, json),
                DatabaseCommand::Delete { name } => {
                    commands::database::delete(&selection.scoped(&client), &name, json)
                }
            },
            Command::FederationIssuer { action } => commands::federation_issuer::run(
                &client,
                selection.organization_selector().as_ref(),
                action,
                json,
            ),
            Command::FederationRule { action } => {
                let scope =
                    selection.scope(selection.organization.as_ref(), selection.cluster.as_ref());
                commands::federation_rule::run(&client.with_scope(scope), action, json)
            }
            Command::ServiceAccount { action } => {
                let organization = selection.organization_selector();
                match action {
                    ServiceAccountCommand::List => {
                        commands::service_account::list(&client, organization.as_ref(), json)
                    }
                    ServiceAccountCommand::Create {
                        name,
                        description,
                        role,
                    } => commands::service_account::create(
                        &client,
                        organization.as_ref(),
                        commands::service_account::CreateServiceAccount {
                            name,
                            description,
                            role,
                        },
                        json,
                    ),
                }
            }
            Command::Organization { action } => match action {
                OrganizationCommand::List => commands::organization::list(&client, json),
                OrganizationCommand::Create { name } => {
                    commands::organization::create(&client, &name, json)
                }
                OrganizationCommand::Use { name } => {
                    commands::organization::use_organization(&client, &name, json)
                }
                OrganizationCommand::Rename { old, new_name } => {
                    commands::organization::rename(&client, &old, &new_name, json)
                }
                OrganizationCommand::Delete { name } => {
                    commands::organization::delete(&client, &name, json)
                }
            },
            Command::Cluster { action } => {
                let organization = match &action {
                    ClusterCommand::Use { .. } | ClusterCommand::Sizes => None,
                    _ => selection.effective_organization(&client),
                };
                let scoped = client.with_scope(selection.scope(organization.as_ref(), None));
                match action {
                    ClusterCommand::Use { name } => commands::cluster::use_cluster(
                        &client,
                        &name,
                        selection.organization.as_ref().map(Choice::name),
                        json,
                    ),
                    ClusterCommand::List => commands::cluster::list(
                        &scoped,
                        organization.as_ref().map(Choice::name),
                        json,
                    ),
                    ClusterCommand::Regions => commands::cluster::regions(&scoped, json),
                    ClusterCommand::Sizes => commands::cluster::sizes(&client, json),
                    ClusterCommand::Create {
                        region_id,
                        name,
                        replicas,
                        min_size,
                        max_size,
                        idle_timeout_minutes,
                        s3_storage,
                        identity,
                    } => commands::cluster::create(
                        &scoped,
                        commands::cluster::ClusterCreateOptions {
                            region_id: &region_id,
                            name: &name,
                            replicas,
                            min_size,
                            max_size,
                            idle_timeout_minutes,
                            s3_storage,
                            identity,
                        },
                        json,
                    ),
                    ClusterCommand::Status { name_or_id } => {
                        commands::cluster::status(&scoped, &name_or_id, json)
                    }
                    ClusterCommand::Update {
                        name_or_id,
                        name,
                        idle_timeout_minutes,
                    } => commands::cluster::update(
                        &scoped,
                        &name_or_id,
                        name.as_deref(),
                        idle_timeout_minutes,
                        json,
                    ),
                    ClusterCommand::Stop { name_or_id } => {
                        commands::cluster::stop(&scoped, &name_or_id, json)
                    }
                    ClusterCommand::Resume { name_or_id } => {
                        commands::cluster::resume(&scoped, &name_or_id, json)
                    }
                    ClusterCommand::Delete { name_or_id } => {
                        commands::cluster::delete(&scoped, &name_or_id, json)
                    }
                }
            }
            Command::ApiKey { action } => {
                let client = selection.scoped(&client);
                match action {
                    ApiKeyCommand::List => commands::keys::list(&client, json),
                    ApiKeyCommand::Create {
                        database,
                        name,
                        permission,
                        expires_at,
                    } => {
                        let database = resolve_optional_database(database);
                        commands::keys::create(
                            &client,
                            database.as_deref(),
                            commands::keys::CreateApiKeyRequest {
                                name: &name,
                                permission: &permission,
                                expires_at: expires_at.as_deref(),
                            },
                            json,
                        )
                    }
                    ApiKeyCommand::Delete { id } => commands::keys::delete(&client, id, json),
                }
            }
            Command::Table { action } => {
                let client = selection.scoped(&client);
                match action {
                    TableCommand::List { database } => {
                        commands::table::list(&client, &resolve_database(database)?, json)
                    }
                    TableCommand::Describe { database, table } => commands::table::describe(
                        &client,
                        &resolve_database(database)?,
                        &table,
                        json,
                    ),
                    TableCommand::Create {
                        database,
                        table,
                        sorting_key,
                    } => commands::table::create(
                        &client,
                        &resolve_database(database)?,
                        &table,
                        sorting_key.as_deref(),
                        json,
                    ),
                    TableCommand::Update {
                        database,
                        table,
                        sorting_key,
                    } => commands::table::update(
                        &client,
                        &resolve_database(database)?,
                        &table,
                        &sorting_key,
                        json,
                    ),
                }
            }
            Command::Integration { action } => match action {
                IntegrationCommand::List => commands::integration::list(
                    &client,
                    selection.organization_selector().as_ref(),
                    json,
                ),
                IntegrationCommand::Add {
                    kind: IntegrationKind::Slack,
                } => {
                    let organization = selection.effective_organization(&client);
                    commands::integration::add_slack(
                        &commands::open::resolve_ui_base_url(&url),
                        require_organization(organization.as_ref())?.name(),
                        json,
                    )
                }
            },
            Command::Workflow { action } => {
                let organization = if client.has_api_key()? {
                    selection.organization.clone()
                } else {
                    selection.effective_organization(&client)
                };
                let scope = require_workflow_scope(
                    &client,
                    selection.scope(organization.as_ref(), selection.cluster.as_ref()),
                )?;
                let client = client.with_scope(scope);
                match action {
                    WorkflowCommand::List => commands::workflow::list(&client, json),
                    WorkflowCommand::Get { id } => commands::workflow::get(&client, &id, json),
                    WorkflowCommand::Create {
                        name,
                        database,
                        sql,
                        schedule_cron,
                        schedule_enabled,
                        sinks,
                    } => {
                        ensure_single_stdin_reader(Some(&sql), &sinks)?;
                        commands::workflow::create(
                            &client,
                            commands::workflow::CreateWorkflow {
                                name,
                                database: resolve_database(database)?,
                                sql: read_sql_arg(sql)?,
                                cron: schedule_cron.and_then(|cron| cron.0),
                                enabled: schedule_enabled,
                                sinks,
                            },
                            json,
                        )
                    }
                    WorkflowCommand::Update {
                        id,
                        name,
                        database,
                        sql,
                        schedule_cron,
                        schedule_enabled,
                        sinks,
                        clear_sinks,
                    } => {
                        ensure_single_stdin_reader(sql.as_deref(), &sinks)?;
                        commands::workflow::update(
                            &client,
                            &id,
                            commands::workflow::UpdateWorkflow {
                                name,
                                database,
                                sql: sql.map(read_sql_arg).transpose()?,
                                cron: schedule_cron.map(|cron| cron.0),
                                enabled: schedule_enabled,
                                sinks: (clear_sinks || !sinks.is_empty()).then_some(sinks),
                            },
                            json,
                        )
                    }
                    WorkflowCommand::Delete { id } => {
                        commands::workflow::delete(&client, &id, json)
                    }
                    WorkflowCommand::Run {
                        id,
                        idempotency_key,
                    } => commands::workflow::run(&client, &id, idempotency_key.as_deref(), json),
                    WorkflowCommand::Runs { id, limit, cursor } => {
                        commands::workflow::runs(&client, &id, limit, cursor.as_deref(), json)
                    }
                    WorkflowCommand::Cancel { id, run_id } => {
                        commands::workflow::cancel(&client, &id, &run_id, json)
                    }
                    WorkflowCommand::Logs {
                        id,
                        range,
                        limit,
                        cursor,
                    } => commands::workflow::logs(
                        &client,
                        &id,
                        &range,
                        limit,
                        cursor.as_deref(),
                        json,
                    ),
                    WorkflowCommand::Sink { action } => match action {
                        WorkflowSinkCommand::Create { workflow_id, sink } => {
                            commands::workflow::sink::create(&client, &workflow_id, &sink, json)
                        }
                        WorkflowSinkCommand::Update {
                            workflow_id,
                            sink_id,
                            changes,
                        } => commands::workflow::sink::update(
                            &client,
                            &workflow_id,
                            &sink_id,
                            &changes,
                            json,
                        ),
                        WorkflowSinkCommand::Delete {
                            workflow_id,
                            sink_id,
                        } => {
                            commands::workflow::sink::delete(&client, &workflow_id, &sink_id, json)
                        }
                    },
                }
            }
            Command::Logs {
                search,
                methods,
                status_codes,
                levels,
                sources,
                user_agent,
                host,
                paths,
                min_duration_ms,
                max_duration_ms,
                limit,
                offset,
                since,
                until,
                start_time,
                end_time,
            } => commands::logs::logs(
                &selection.scoped(&client),
                search.as_deref(),
                &methods,
                &status_codes,
                &levels,
                &sources,
                user_agent.as_deref(),
                host.as_deref(),
                &paths,
                min_duration_ms,
                max_duration_ms,
                limit,
                offset,
                since.as_deref(),
                until.as_deref(),
                start_time.as_deref(),
                end_time.as_deref(),
                json,
            ),
            Command::Query {
                database,
                sql_positional,
                sql,
                limit,
            } => {
                let client = selection.scoped(&client);
                let database = resolve_database(database)?;
                let sql = resolve_sql(sql_positional, sql)?;
                commands::query::query(&client, &database, &sql, limit, json)
            }
            Command::Format(args) => {
                let database = resolve_optional_database(args.database);
                let sql = resolve_sql(args.sql_positional, args.sql)?;
                commands::format::format(
                    &selection.scoped(&client),
                    database.as_deref(),
                    &sql,
                    json,
                )
            }
            Command::Insert {
                database,
                table,
                data,
                file,
                url,
                transform,
            } => {
                let client = selection.scoped(&client);
                commands::insert::insert(
                    &client,
                    &resolve_database(database)?,
                    &table,
                    data.as_deref(),
                    file.as_deref(),
                    url.as_deref(),
                    transform.as_deref(),
                    json,
                )
            }
            Command::Feedback(args) => commands::feedback::submit(&client, args, json),
            Command::Ping => commands::ping::ping(&client, json),
            Command::Docs => commands::docs::docs(&client),
            Command::Status => commands::status::status(&url, json),
            Command::Update => commands::update::update(json),
            Command::Open { database } => {
                let ui_base_url = commands::open::resolve_ui_base_url(&url);
                let organization = selection.effective_organization(&client);
                let database = resolve_optional_database(database);
                commands::open::open(
                    &ui_base_url,
                    organization.as_ref().map(Choice::name),
                    selection.cluster.as_ref().map(Choice::name),
                    database.as_deref(),
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
        }?;
        Ok(ExitCode::SUCCESS)
    })();
    result.map_err(|error| selection.explain_rejected_saved_ids(&client, error))
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::constants::DEFAULT_API_URL;
    use super::{
        effective_json_mode, resolve_database_from_sources, resolve_login_cluster_from_sources,
        resolve_url_from_sources, Choice, Cli, Selection,
    };
    use crate::client::{Scope, Selector};

    fn config_with_ids() -> super::config::Config {
        super::config::Config {
            default_organization: Some("team".into()),
            default_organization_id: Some("org-id".into()),
            default_cluster: Some("prod".into()),
            default_cluster_id: Some("cluster-id".into()),
            ..Default::default()
        }
    }

    fn choice(name: &str, saved_id: Option<&str>) -> Option<Choice> {
        Some(Choice {
            name: name.into(),
            saved_id: saved_id.map(str::to_owned),
        })
    }

    fn select(
        cli_org: Option<&str>,
        env_org: Option<&str>,
        cli_cluster: Option<&str>,
        env_cluster: Option<&str>,
        cfg: super::config::Config,
    ) -> Selection {
        Selection::from_sources(
            cli_org.map(str::to_owned),
            env_org.map(str::to_owned),
            cli_cluster.map(str::to_owned),
            env_cluster.map(str::to_owned),
            cfg,
        )
    }

    #[test]
    fn flags_win_over_environment_which_wins_over_config() {
        let selection = select(
            Some("cli"),
            Some("env"),
            Some("cli"),
            Some("env"),
            config_with_ids(),
        );
        assert_eq!(selection.organization, choice("cli", None));
        assert_eq!(selection.cluster, choice("cli", None));

        let selection = select(None, Some("env"), None, Some("env"), config_with_ids());
        assert_eq!(selection.organization, choice("env", None));
        assert_eq!(selection.cluster, choice("env", None));

        let selection = select(None, None, None, None, Default::default());
        assert_eq!(selection.organization, None);
        assert_eq!(selection.cluster, None);
    }

    #[test]
    fn saved_ids_apply_only_to_config_selections() {
        let selection = select(None, None, None, None, config_with_ids());
        assert_eq!(selection.organization, choice("team", Some("org-id")));
        assert_eq!(selection.cluster, choice("prod", Some("cluster-id")));
        assert!(!selection.used_saved_ids.get());
        assert_eq!(
            selection.scope(selection.organization.as_ref(), selection.cluster.as_ref()),
            Scope {
                organization: Some(Selector::Id("org-id".into())),
                cluster: Some(Selector::Id("cluster-id".into())),
            }
        );
        assert!(selection.used_saved_ids.get());

        let selection = select(None, None, None, Some("prod"), config_with_ids());
        assert_eq!(selection.organization, choice("team", Some("org-id")));
        assert_eq!(
            selection.cluster,
            choice("prod", None),
            "RAWTREE_CLUSTER is resolved by name"
        );
    }

    #[test]
    fn overriding_the_organization_drops_the_saved_cluster_id() {
        for (cli_org, env_org) in [(Some("other"), None), (None, Some("other"))] {
            let selection = select(cli_org, env_org, None, None, config_with_ids());
            assert_eq!(selection.organization, choice("other", None));
            assert_eq!(selection.cluster, choice("prod", None));
        }
    }

    #[test]
    fn configs_without_ids_select_by_name() {
        let cfg = super::config::Config {
            default_organization: Some("team".into()),
            default_cluster: Some("prod".into()),
            ..Default::default()
        };
        let selection = select(None, None, None, None, cfg);
        assert_eq!(
            selection.scope(selection.organization.as_ref(), selection.cluster.as_ref()),
            Scope::new(Some("team"), Some("prod"))
        );
        assert!(!selection.used_saved_ids.get());
    }

    #[test]
    fn json_is_automatic_only_for_non_terminal_login() {
        for (args, terminal, expected) in [
            (vec!["rtree", "login"], false, true),
            (vec!["rtree", "login"], true, false),
            (vec!["rtree", "login", "--json"], true, true),
            (vec!["rtree", "status"], false, false),
            (vec!["rtree", "status", "--json"], false, true),
        ] {
            let cli = Cli::try_parse_from(args).unwrap();
            assert_eq!(effective_json_mode(&cli, terminal), expected);
        }
    }

    #[test]
    fn login_cluster_uses_cli_then_env_without_saved_config() {
        let cli = resolve_login_cluster_from_sources(
            Some("cli-cluster".to_string()),
            Some("env-cluster".to_string()),
        );
        assert_eq!(cli.as_deref(), Some("cli-cluster"));

        let env = resolve_login_cluster_from_sources(None, Some("env-cluster".to_string()));
        assert_eq!(env.as_deref(), Some("env-cluster"));

        assert_eq!(resolve_login_cluster_from_sources(None, None), None);
    }

    #[test]
    fn resolve_url_uses_cli_first() {
        let resolved = resolve_url_from_sources(
            Some("https://cli.example.com"),
            Some("https://api.example.com".to_string()),
            Some("https://cfg.example.com".to_string()),
        );
        assert_eq!(resolved, "https://cli.example.com");
    }

    #[test]
    fn resolve_url_uses_env_when_cli_missing() {
        let resolved = resolve_url_from_sources(
            None,
            Some("https://api.example.com".to_string()),
            Some("https://cfg.example.com".to_string()),
        );
        assert_eq!(resolved, "https://api.example.com");
    }

    #[test]
    fn resolve_url_uses_config_when_cli_and_env_missing() {
        let resolved =
            resolve_url_from_sources(None, None, Some("https://cfg.example.com".to_string()));
        assert_eq!(resolved, "https://cfg.example.com");
    }

    #[test]
    fn resolve_url_defaults_when_no_sources() {
        let resolved = resolve_url_from_sources(None, None, None);
        assert_eq!(resolved, DEFAULT_API_URL);
    }

    #[test]
    fn resolve_database_uses_cli_first() {
        let resolved = resolve_database_from_sources(
            Some("cli-database".to_string()),
            Some("env-database".to_string()),
            Some("cfg-database".to_string()),
        );
        assert_eq!(resolved.as_deref(), Some("cli-database"));
    }

    #[test]
    fn resolve_database_uses_env_when_cli_missing() {
        let resolved = resolve_database_from_sources(
            None,
            Some("env-database".to_string()),
            Some("cfg-database".to_string()),
        );
        assert_eq!(resolved.as_deref(), Some("env-database"));
    }

    #[test]
    fn resolve_database_uses_config_when_cli_and_env_missing() {
        let resolved = resolve_database_from_sources(None, None, Some("cfg-database".to_string()));
        assert_eq!(resolved.as_deref(), Some("cfg-database"));
    }
}
