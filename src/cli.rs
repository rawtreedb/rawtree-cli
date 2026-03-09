use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(name = "rtree", about = "CLI for the RawTree analytics platform")]
pub struct Cli {
    /// Server URL (overrides RAWTREE_URL env and config file)
    #[arg(long, global = true)]
    pub url: Option<String>,

    /// Output results as JSON (for scripting and agents)
    #[arg(long, global = true)]
    pub json: bool,

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
        email: String,
        /// Password (prompted interactively if omitted)
        #[arg(long)]
        password: Option<String>,
    },
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
    /// Describe a table and show total row count
    Describe {
        #[arg(long)]
        project: Option<String>,
        #[arg(long)]
        table: String,
    },
}
