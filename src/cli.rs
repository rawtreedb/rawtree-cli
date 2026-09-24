use std::{fmt, str::FromStr};

use clap::{Parser, Subcommand, ValueEnum};

use crate::s3_storage::{DatabaseS3AccessArgs, S3StorageArgs};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ClusterSizeArg {
    pub(crate) cpu_cores: u32,
    pub(crate) memory_gib: u32,
}

impl FromStr for ClusterSizeArg {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let Some((cpu_cores, memory_gib)) = value.split_once(':') else {
            return Err(
                "expected CPU_CORES:MEMORY_GIB with positive integers (for example, 2:8)"
                    .to_string(),
            );
        };
        if memory_gib.contains(':') {
            return Err(
                "expected CPU_CORES:MEMORY_GIB with positive integers (for example, 2:8)"
                    .to_string(),
            );
        }

        let cpu_cores = cpu_cores.parse::<u32>().map_err(|_| {
            "expected CPU_CORES:MEMORY_GIB with positive integers (for example, 2:8)".to_string()
        })?;
        let memory_gib = memory_gib.parse::<u32>().map_err(|_| {
            "expected CPU_CORES:MEMORY_GIB with positive integers (for example, 2:8)".to_string()
        })?;
        if cpu_cores == 0 || memory_gib == 0 {
            return Err(
                "expected CPU_CORES:MEMORY_GIB with positive integers (for example, 2:8)"
                    .to_string(),
            );
        }

        Ok(Self {
            cpu_cores,
            memory_gib,
        })
    }
}

impl fmt::Display for ClusterSizeArg {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.cpu_cores, self.memory_gib)
    }
}

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
    /// API key (overrides RAWTREE_API_KEY env and config file token)
    #[arg(long, global = true)]
    pub api_key: Option<String>,

    /// API URL (overrides RAWTREE_API_URL env and config file)
    #[arg(long, global = true)]
    pub api_url: Option<String>,

    /// Output results as JSON (for scripting and agents)
    #[arg(long, global = true)]
    pub json: bool,

    /// Organization name (overrides RAWTREE_ORG env and config file)
    #[arg(long, global = true)]
    pub org: Option<String>,

    /// Cluster name (overrides RAWTREE_CLUSTER env and config file)
    #[arg(long, global = true)]
    pub cluster: Option<String>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Log in and save credentials
    #[command(
        after_help = "API key mode:\n  --api-key saves an API key directly without browser authentication.\n\nAPI key output (--json):\n  {\"success\":true,\"config_path\":\"<path>\",\"database\":\"<name>\",\"organization\":\"<name>\",\"cluster\":\"<name>\"}"
    )]
    Login {
        #[arg(long, hide = true)]
        email: Option<String>,
        /// Password (prompted interactively if omitted)
        #[arg(long, requires = "email", hide = true)]
        password: Option<String>,
        /// Do not try to open the browser automatically
        #[arg(long, default_value_t = false)]
        no_browser: bool,
        /// Max seconds to wait for browser login approval
        #[arg(long, default_value_t = 300)]
        timeout_seconds: u64,
        /// Database name to set as default after authentication
        #[arg(long)]
        database: Option<String>,
    },
    /// Log out and remove saved local credentials
    Logout,
    /// Manage databases
    Database {
        #[command(subcommand)]
        action: DatabaseCommand,
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
    /// Inspect dedicated clusters
    Cluster {
        #[command(subcommand)]
        action: ClusterCommand,
    },
    /// Inspect tables
    Table {
        #[command(subcommand)]
        action: TableCommand,
    },
    /// View API request logs for a cluster
    Logs {
        #[arg(long)]
        search: Option<String>,
        /// Comma-separated HTTP methods (for example GET,POST)
        #[arg(long, value_delimiter = ',')]
        methods: Vec<String>,
        /// Comma-separated HTTP status codes (for example 200,404,500)
        #[arg(long, value_delimiter = ',', value_parser = clap::value_parser!(u16).range(100..=599))]
        status_codes: Vec<u16>,
        /// Comma-separated request levels: success, warning, or error
        #[arg(long, value_delimiter = ',')]
        levels: Vec<String>,
        /// Comma-separated request sources: ui, cli, or api
        #[arg(long, value_delimiter = ',')]
        sources: Vec<String>,
        /// Exact user-agent value
        #[arg(long)]
        user_agent: Option<String>,
        /// Case-insensitive substring of the request host
        #[arg(long)]
        host: Option<String>,
        /// Comma-separated exact request paths
        #[arg(long, value_delimiter = ',')]
        paths: Vec<String>,
        /// Minimum request duration in milliseconds
        #[arg(long)]
        min_duration_ms: Option<u64>,
        /// Maximum request duration in milliseconds
        #[arg(long)]
        max_duration_ms: Option<u64>,
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
    /// Execute a SQL query against a database
    Query {
        #[arg(long)]
        database: Option<String>,
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
        database: Option<String>,
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
        /// Database name (defaults to --database/RAWTREE_DATABASE/config default)
        #[arg(long)]
        database: Option<String>,
    },
    /// Update rtree to the latest release
    Update,
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
pub enum DatabaseCommand {
    /// List all databases
    List,
    /// Create a new database
    Create {
        /// Database name
        name: String,
        #[command(flatten)]
        s3_storage: S3StorageArgs,
    },
    /// Set the default database
    Use {
        /// Database name
        name: String,
    },
    /// Delete a database and all its data
    Delete {
        /// Database name
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
pub enum ClusterCommand {
    /// List dedicated clusters
    List,
    /// List the CPU and memory pairs available for dedicated clusters
    Sizes,
    /// Create a dedicated cluster
    #[command(
        after_help = "Sizes use CPU_CORES:MEMORY_GIB (for example, 2:8). Run `rtree cluster sizes` to list the available sizes."
    )]
    Create {
        /// Cluster name
        #[arg(long)]
        name: String,
        /// Number of cluster replicas
        #[arg(long)]
        replicas: u32,
        /// Minimum size per replica as CPU cores:memory GiB
        #[arg(long, value_name = "CPU_CORES:MEMORY_GIB")]
        min_size: ClusterSizeArg,
        /// Maximum size per replica as CPU cores:memory GiB; defaults to the minimum
        #[arg(long, value_name = "CPU_CORES:MEMORY_GIB")]
        max_size: Option<ClusterSizeArg>,
        /// Minutes without activity before automatically pausing; 0 disables idling
        #[arg(long)]
        idle_timeout_minutes: Option<u64>,
        #[command(flatten)]
        s3_storage: S3StorageArgs,
        #[command(flatten)]
        database_s3_access: DatabaseS3AccessArgs,
    },
    /// Set the default cluster
    Use {
        /// Cluster name
        name: String,
    },
    /// Show the current state of a dedicated cluster
    Status {
        /// Cluster name or ID
        name_or_id: String,
    },
    /// Update dedicated cluster settings
    Update {
        /// Cluster name or ID
        name_or_id: String,
        /// New cluster name
        #[arg(long)]
        name: Option<String>,
        /// Minutes without activity before automatically pausing; 0 disables idling
        #[arg(long)]
        idle_timeout_minutes: Option<u64>,
    },
    /// Request that a dedicated cluster stop
    Stop {
        /// Cluster name or ID
        name_or_id: String,
    },
    /// Request that a stopped dedicated cluster resume
    Resume {
        /// Cluster name or ID
        name_or_id: String,
    },
    /// Request deletion of a dedicated cluster and its data
    Delete {
        /// Cluster name or ID
        name_or_id: String,
    },
}

#[derive(Subcommand)]
pub enum KeyCommand {
    /// List API keys for a cluster
    List,
    /// Create a new API key
    Create {
        /// Default database for the new key (server default when no database is selected)
        #[arg(long)]
        database: Option<String>,
        /// Name for the key
        #[arg(long)]
        name: String,
        /// Permission level: admin, read_write, write_only, read_only
        #[arg(long)]
        permission: String,
    },
    /// Delete an API key
    Delete {
        /// Key ID or full API key token to delete
        id_or_token: String,
    },
}

#[derive(Subcommand)]
pub enum TableCommand {
    /// List tables in a database
    List {
        #[arg(long)]
        database: Option<String>,
    },
    /// Describe a table
    Describe {
        #[arg(long)]
        database: Option<String>,
        /// Table name
        table: String,
    },
    /// Create an empty table, optionally with a custom sorting key
    Create {
        #[arg(long)]
        database: Option<String>,
        /// Table name
        table: String,
        /// Comma-separated SQL sorting expressions (quote the argument in your shell)
        #[arg(long)]
        sorting_key: Option<String>,
    },
    /// Change a table's sorting key for new parts and later merges
    Update {
        #[arg(long)]
        database: Option<String>,
        /// Table name
        table: String,
        /// Comma-separated SQL sorting expressions (quote the argument in your shell)
        #[arg(long)]
        sorting_key: String,
    },
}

#[cfg(test)]
mod tests;
