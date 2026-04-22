use clap::{ArgAction, Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(
    name = "rtree",
    version,
    disable_version_flag = true,
    arg(
        clap::Arg::new("version")
            .short('v')
            .long("version")
            .help("Output the current version")
            .action(clap::ArgAction::Version)
    ),
    about = "CLI for the RawTree analytics platform"
)]
pub struct Cli {
    /// API URL (overrides RAWTREE_URL env and config file)
    #[arg(long, global = true)]
    pub api_url: Option<String>,

    /// Output results as JSON (for scripting and agents)
    #[arg(long, global = true)]
    pub json: bool,

    /// Organization name (overrides RAWTREE_ORG env and config file)
    #[arg(long, global = true)]
    pub org: Option<String>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Register a new account
    Register {
        #[arg(long)]
        email: String,
        /// Password (prompted interactively if omitted)
        #[arg(long)]
        password: Option<String>,
        /// Project name to set as default after authentication
        #[arg(long)]
        project: Option<String>,
    },
    /// Log in and save token
    Login {
        #[arg(long)]
        email: Option<String>,
        /// Password (prompted interactively if omitted)
        #[arg(long, requires = "email")]
        password: Option<String>,
        /// Do not try to open the browser automatically
        #[arg(long, default_value_t = false)]
        no_browser: bool,
        /// Max seconds to wait for browser login approval
        #[arg(long, default_value_t = 300)]
        timeout_seconds: u64,
        /// Project name to set as default after authentication
        #[arg(long)]
        project: Option<String>,
    },
    /// Log out and remove saved local credentials
    Logout,
    /// Manage projects
    Project {
        #[command(subcommand)]
        action: ProjectCommand,
    },
    /// Manage API keys
    #[command(name = "key")]
    Key {
        #[command(subcommand)]
        action: KeyCommand,
    },
    /// Manage organizations
    Organization {
        #[command(subcommand)]
        action: OrganizationCommand,
    },
    /// Inspect tables
    Table {
        #[command(subcommand)]
        action: TableCommand,
    },
    /// View query logs for a project
    Logs {
        #[arg(long)]
        project: Option<String>,
        /// Filter by query type: select or insert
        #[arg(long)]
        r#type: Option<String>,
        /// Filter by table name. Repeat to include multiple tables.
        #[arg(long, action = ArgAction::Append)]
        table: Vec<String>,
        /// Filter by status: success or error
        #[arg(long, value_parser = ["success", "error"])]
        status: Option<String>,
        /// Maximum number of log entries to return (default: 50, max: 200)
        #[arg(long, default_value = "50", value_parser = clap::value_parser!(u64).range(1..=200))]
        limit: u64,
        /// Offset for pagination
        #[arg(long, default_value = "0")]
        offset: u64,
        /// Show logs from the last duration (e.g., 1h, 30m, 7d, 2w)
        #[arg(long, conflicts_with_all = ["start_time", "end_time"])]
        since: Option<String>,
        /// Show logs until this duration ago (e.g., 30m)
        #[arg(long, conflicts_with_all = ["start_time", "end_time"])]
        until: Option<String>,
        /// Start time in UTC (e.g., "2026-03-28T18:00:00Z")
        #[arg(long, conflicts_with_all = ["since", "until"])]
        start_time: Option<String>,
        /// End time in UTC (e.g., "2026-03-28T19:00:00Z")
        #[arg(long, conflicts_with_all = ["since", "until"])]
        end_time: Option<String>,
    },
    /// Execute a SQL query against a project
    Query {
        #[arg(long)]
        project: Option<String>,
        /// SQL query to execute (positional or --sql). Use "-" to read from stdin.
        #[arg(value_name = "SQL", conflicts_with = "sql")]
        sql_positional: Option<String>,
        /// SQL query to execute
        #[arg(long)]
        sql: Option<String>,
        /// Append LIMIT N to the query
        #[arg(long)]
        limit: Option<u64>,
    },
    /// Insert data into a table
    Insert {
        #[arg(long)]
        project: Option<String>,
        #[arg(long)]
        table: String,
        /// Inline JSON data
        #[arg(long, conflicts_with = "file")]
        data: Option<String>,
        /// Path to a JSON or JSONL file
        #[arg(long, conflicts_with = "data")]
        file: Option<String>,
        /// Public URL to JSON or JSONL content
        #[arg(long, conflicts_with_all = ["data", "file"])]
        url: Option<String>,
        /// Apply a predefined transform (e.g., otlp-traces, otlp-logs, otlp-metrics)
        #[arg(long)]
        transform: Option<String>,
    },
    /// Check server connectivity
    Ping,
    /// Fetch and display API documentation from the server
    Docs,
    /// Show current auth state and API URL
    Status,
    /// Open Rawtree UI in your browser
    Open {
        /// Project name (defaults to --project/RAWTREE_PROJECT/config default)
        #[arg(long)]
        project: Option<String>,
    },
    /// Generate shell completions
    Completions {
        /// Shell to generate completions for
        #[arg(value_enum)]
        shell: ShellType,
    },
}

#[derive(Clone, ValueEnum)]
pub enum ShellType {
    Bash,
    Zsh,
    Fish,
}

#[derive(Subcommand)]
pub enum ProjectCommand {
    /// List all projects
    List,
    /// Create a new project
    Create {
        /// Project name
        name: String,
    },
    /// Set the default project
    Use {
        /// Project name
        name: String,
    },
    /// Rename a project
    Rename {
        /// Current project name
        old: String,
        /// New project name
        new_name: String,
    },
    /// Delete a project and all its data
    Delete {
        /// Project name
        name: String,
    },
}

#[derive(Subcommand)]
pub enum OrganizationCommand {
    /// List all organizations
    List,
    /// Create a new organization
    Create {
        /// Organization name
        name: String,
    },
    /// Set the default organization
    Use {
        /// Organization name
        name: String,
    },
    /// Rename an organization
    Rename {
        /// Current organization name
        old: String,
        /// New organization name
        new_name: String,
    },
    /// Delete an organization
    Delete {
        /// Organization name
        name: String,
    },
}

#[derive(Subcommand)]
pub enum KeyCommand {
    /// List API keys for a project
    List {
        #[arg(long)]
        project: Option<String>,
    },
    /// Create a new API key
    Create {
        #[arg(long)]
        project: Option<String>,
        /// Name for the key
        #[arg(long)]
        name: String,
        /// Permission level: admin, read_write, write_only, read_only
        #[arg(long)]
        permission: String,
    },
    /// Delete an API key
    Delete {
        #[arg(long)]
        project: Option<String>,
        /// Key ID or full API key token to delete
        id_or_token: String,
    },
}

#[derive(Subcommand)]
pub enum TableCommand {
    /// List tables in a project
    List {
        #[arg(long)]
        project: Option<String>,
    },
    /// Describe a table
    Describe {
        #[arg(long)]
        project: Option<String>,
        /// Table name
        table: String,
    },
}

#[cfg(test)]
mod tests {
    use super::{Cli, Command, KeyCommand};
    use clap::{error::ErrorKind, CommandFactory, Parser};

    #[test]
    fn root_command_exposes_version_flag() {
        let command = Cli::command();
        assert_eq!(command.get_version(), Some(env!("CARGO_PKG_VERSION")));
    }

    #[test]
    fn lowercase_v_triggers_version_output() {
        let err = match Cli::try_parse_from(["rtree", "-v"]) {
            Ok(_) => panic!("-v should print version"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), ErrorKind::DisplayVersion);
        assert!(err.to_string().contains(env!("CARGO_PKG_VERSION")));
    }

    #[test]
    fn login_without_email_is_allowed_for_browser_flow() {
        let cli = Cli::try_parse_from(["rtree", "login"]).expect("login should parse");
        match cli.command {
            Command::Login { email, .. } => assert!(email.is_none()),
            _ => panic!("expected login command"),
        }
    }

    #[test]
    fn login_with_password_requires_email() {
        let result = Cli::try_parse_from(["rtree", "login", "--password", "secret123"]);
        assert!(result.is_err(), "password without email should fail");
    }

    #[test]
    fn login_with_project_without_email_is_allowed_for_browser_flow() {
        let cli = Cli::try_parse_from(["rtree", "login", "--project", "analytics"])
            .expect("login with --project should parse");
        match cli.command {
            Command::Login { email, project, .. } => {
                assert!(email.is_none());
                assert_eq!(project.as_deref(), Some("analytics"));
            }
            _ => panic!("expected login command"),
        }
    }

    #[test]
    fn register_with_project_parses() {
        let cli = Cli::try_parse_from([
            "rtree",
            "register",
            "--email",
            "user@example.com",
            "--password",
            "secret123",
            "--project",
            "analytics",
        ])
        .expect("register with --project should parse");

        match cli.command {
            Command::Register { project, .. } => {
                assert_eq!(project.as_deref(), Some("analytics"));
            }
            _ => panic!("expected register command"),
        }
    }

    #[test]
    fn insert_with_url_is_allowed() {
        let cli = Cli::try_parse_from([
            "rtree",
            "insert",
            "--project",
            "analytics",
            "--table",
            "events",
            "--url",
            "https://example.com/events.jsonl",
        ])
        .expect("insert --url should parse");

        match cli.command {
            Command::Insert { url, .. } => {
                assert_eq!(url.as_deref(), Some("https://example.com/events.jsonl"))
            }
            _ => panic!("expected insert command"),
        }
    }

    #[test]
    fn insert_url_conflicts_with_data() {
        let result = Cli::try_parse_from([
            "rtree",
            "insert",
            "--project",
            "analytics",
            "--table",
            "events",
            "--url",
            "https://example.com/events.jsonl",
            "--data",
            r#"{"id":1}"#,
        ]);
        assert!(result.is_err(), "insert --url should conflict with --data");
    }

    #[test]
    fn api_url_and_insert_url_can_both_be_provided() {
        let cli = Cli::try_parse_from([
            "rtree",
            "--api-url",
            "https://api.rawtree.com",
            "insert",
            "--project",
            "analytics",
            "--table",
            "events",
            "--url",
            "https://example.com/events.jsonl",
        ])
        .expect("--api-url and insert --url should parse");

        assert_eq!(
            cli.api_url.as_deref(),
            Some("https://api.rawtree.com")
        );
        match cli.command {
            Command::Insert { url, .. } => {
                assert_eq!(url.as_deref(), Some("https://example.com/events.jsonl"))
            }
            _ => panic!("expected insert command"),
        }
    }

    #[test]
    fn api_url_can_be_passed_before_subcommand() {
        let cli = Cli::try_parse_from([
            "rtree",
            "--api-url",
            "https://api.rawtree.com",
            "query",
            "--project",
            "analytics",
            "--sql",
            "SELECT 1",
        ])
        .expect("--api-url should parse before subcommand");

        assert_eq!(
            cli.api_url.as_deref(),
            Some("https://api.rawtree.com")
        );
    }

    #[test]
    fn query_format_flag_is_rejected() {
        let result = Cli::try_parse_from([
            "rtree",
            "query",
            "--project",
            "analytics",
            "--sql",
            "SELECT 1",
            "--format",
            "csv",
        ]);
        assert!(result.is_err(), "query --format should not be supported");
    }

    #[test]
    fn query_named_query_flag_is_rejected() {
        let result = Cli::try_parse_from([
            "rtree",
            "query",
            "--project",
            "analytics",
            "--query",
            "SELECT 1",
        ]);
        assert!(result.is_err(), "query --query should not be supported");
    }

    #[test]
    fn key_command_is_singular() {
        let cli =
            Cli::try_parse_from(["rtree", "key", "list", "--project", "analytics"]).unwrap();

        match cli.command {
            Command::Key { action } => match action {
                KeyCommand::List { project } => {
                    assert_eq!(project.as_deref(), Some("analytics"));
                }
                _ => panic!("expected key list command"),
            },
            _ => panic!("expected key command"),
        }
    }

    #[test]
    fn keys_command_is_rejected() {
        let result = Cli::try_parse_from(["rtree", "keys", "list", "--project", "analytics"]);
        assert!(result.is_err(), "keys should not be accepted as a command");
    }

    #[test]
    fn key_create_uses_name_flag() {
        let cli = Cli::try_parse_from([
            "rtree",
            "key",
            "create",
            "--project",
            "analytics",
            "--name",
            "ci",
            "--permission",
            "read_write",
        ])
        .expect("key create with --name should parse");

        match cli.command {
            Command::Key { action } => match action {
                KeyCommand::Create { name, .. } => {
                    assert_eq!(name, "ci");
                }
                _ => panic!("expected key create command"),
            },
            _ => panic!("expected key command"),
        }
    }

    #[test]
    fn key_create_label_flag_is_rejected() {
        let result = Cli::try_parse_from([
            "rtree",
            "key",
            "create",
            "--project",
            "analytics",
            "--label",
            "ci",
            "--permission",
            "read_write",
        ]);
        assert!(result.is_err(), "key create should use --name, not --label");
    }

    #[test]
    fn logs_table_filter_can_be_repeated() {
        let cli = Cli::try_parse_from([
            "rtree",
            "logs",
            "--project",
            "analytics",
            "--table",
            "events",
            "--table",
            "audit",
        ])
        .expect("repeated --table should parse");

        match cli.command {
            Command::Logs { table, .. } => {
                assert_eq!(table, vec!["events".to_string(), "audit".to_string()]);
            }
            _ => panic!("expected logs command"),
        }
    }

    #[test]
    fn logs_status_only_accepts_success_or_error() {
        let ok = Cli::try_parse_from(["rtree", "logs", "--status", "success"]);
        assert!(ok.is_ok(), "success should parse");

        let err = Cli::try_parse_from(["rtree", "logs", "--status", "ok"]);
        assert!(err.is_err(), "ok should not be accepted as a logs status");
    }
}
