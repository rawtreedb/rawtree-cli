use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(name = "rtree", about = "CLI for the RawTree analytics platform")]
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
    },
    /// Log out and remove saved local credentials
    Logout,
    /// Manage projects
    Project {
        #[command(subcommand)]
        action: ProjectCommand,
    },
    /// Manage API keys
    Keys {
        #[command(subcommand)]
        action: KeysCommand,
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
    /// Execute a SQL query against a project
    Query {
        #[arg(long)]
        project: Option<String>,
        /// SQL query to execute (positional or --query). Use "-" to read from stdin.
        #[arg(conflicts_with = "query")]
        sql: Option<String>,
        /// SQL query to execute
        #[arg(long)]
        query: Option<String>,
        /// Output format: json (default) or csv
        #[arg(long)]
        format: Option<String>,
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
    /// Preview rows from a table
    Sample {
        #[arg(long)]
        project: Option<String>,
        #[arg(long)]
        table: String,
        /// Number of rows to return (default: 10)
        #[arg(long, default_value = "10")]
        limit: u64,
        /// Output format: json (default) or csv
        #[arg(long)]
        format: Option<String>,
    },
    /// Export query results to a file
    Export {
        #[arg(long)]
        project: Option<String>,
        /// SQL query to execute (positional or --query)
        #[arg(conflicts_with = "query")]
        sql: Option<String>,
        /// SQL query to execute
        #[arg(long)]
        query: Option<String>,
        /// Output file path
        #[arg(long, short)]
        output: String,
        /// Output format: json (default) or csv
        #[arg(long)]
        format: Option<String>,
    },
    /// Check server connectivity
    Ping,
    /// Fetch and display API documentation from the server
    Docs,
    /// Show current authenticated user and server
    Whoami,
    /// Show current auth state and server URL
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
pub enum KeysCommand {
    /// List API keys for a project
    List {
        #[arg(long)]
        project: Option<String>,
    },
    /// Create a new API key
    Create {
        #[arg(long)]
        project: Option<String>,
        /// Label for the key
        #[arg(long)]
        label: String,
        /// Permission level: admin, read_write, write_only, read_only
        #[arg(long)]
        permission: String,
    },
    /// Delete an API key
    Delete {
        #[arg(long)]
        project: Option<String>,
        /// Key ID to delete
        key_id: String,
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
    use super::{Cli, Command};
    use clap::Parser;

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
            "https://api.us-east-1.aws.rawtree.com",
            "insert",
            "--project",
            "analytics",
            "--table",
            "events",
            "--url",
            "https://example.com/events.jsonl",
        ])
        .expect("--api-url and insert --url should parse");

        assert_eq!(cli.api_url.as_deref(), Some("https://api.us-east-1.aws.rawtree.com"));
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
            "https://api.us-east-1.aws.rawtree.com",
            "query",
            "--project",
            "analytics",
            "--query",
            "SELECT 1",
        ])
        .expect("--api-url should parse before subcommand");

        assert_eq!(cli.api_url.as_deref(), Some("https://api.us-east-1.aws.rawtree.com"));
    }
}
