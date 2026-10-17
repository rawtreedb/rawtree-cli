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
        after_help = "API key mode:\n  --api-key saves an API key directly without browser authentication.\n\nAPI key output (--json):\n  {\"success\":true,\"config_path\":\"<path>\",\"database\":\"<name>\",\"organization\":\"<name>\",\"cluster\":\"<name>\"}\n\nNon-interactive login:\n  JSON is automatic when stdin is not a terminal. Read the device approval link\n  from a JSON event on stderr while login is running. Missing selections return\n  JSON choices on stdout and exit code 2. Rerun with --org, --cluster, or\n  --database; each retry requires approval again."
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
    /// Manage scheduled SQL workflows
    Workflow {
        #[command(subcommand)]
        action: WorkflowCommand,
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
        /// SQL to execute. Use "-" or omit SQL to read from piped stdin.
        #[arg(value_name = "SQL", conflicts_with = "sql")]
        sql_positional: Option<String>,
        /// SQL to execute. Use "-" to read from stdin.
        #[arg(long, short = 'q', visible_alias = "query")]
        sql: Option<String>,
        /// Append LIMIT N to the query
        #[arg(long)]
        limit: Option<u64>,
    },
    /// Format SQL without executing it
    Format(crate::commands::format::FormatArgs),
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
        /// Expiration as an RFC 3339 timestamp with timezone; omit to never expire
        #[arg(long)]
        expires_at: Option<String>,
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

const SINK_HELP: &str = "Sinks are JSON objects with a type and its settings:\n  {\"type\":\"table\",\"settings\":{\"database\":\"analytics\",\"table\":\"alerts\"}}\n  {\"type\":\"http\",\"settings\":{\"url\":\"https://example.com/hook\",\"headers\":{\"Authorization\":\"Bearer ...\"}}}";

const SINK_UPDATE_HELP: &str = r#"Sinks are JSON objects with a type and its settings. --sink replaces the whole
list; include the "id" of existing sinks to keep them. An existing HTTP sink may
omit "settings", or its "url" or "headers", to retain them, and a null header
value keeps the stored value:
  {"type":"table","id":"<sink-id>","settings":{"database":"analytics","table":"alerts"}}
  {"type":"http","id":"<sink-id>"}
  {"type":"http","settings":{"url":"https://example.com/hook"}}

Example: keep an HTTP sink unchanged and point a table sink at another table.
Find the sink IDs with `rtree workflow get <id>`, then pass every sink to keep:
  rtree workflow update <id> \
    --sink '{"type":"http","id":"<http-sink-id>"}' \
    --sink '{"type":"table","id":"<table-sink-id>","settings":{"database":"analytics","table":"alerts_v2"}}'"#;

#[derive(Subcommand)]
pub enum WorkflowCommand {
    /// List workflows in the selected cluster
    List,
    /// Show a workflow
    Get {
        /// Workflow ID
        id: String,
    },
    /// Create a workflow that runs SQL on demand or on a schedule
    #[command(after_help = SINK_HELP)]
    Create {
        /// Workflow name (letters, digits, '-' and '_')
        #[arg(long)]
        name: String,
        /// Database the SQL runs in (defaults to RAWTREE_DATABASE/config default)
        #[arg(long)]
        database: Option<String>,
        /// SQL to run: a SELECT or INSERT INTO ... SELECT. Use "-" to read from stdin.
        #[arg(long)]
        sql: String,
        /// Seconds between automatic runs (1–86400); null or omitted means manual-only
        #[arg(long, value_name = "SECONDS|null")]
        interval_seconds: Option<WorkflowIntervalArg>,
        /// Create the workflow paused
        #[arg(long)]
        disabled: bool,
        /// Sink as a JSON object; repeat for up to five sinks
        #[arg(long = "sink", value_name = "JSON")]
        sinks: Vec<String>,
    },
    /// Update a workflow; omitted fields are left unchanged
    #[command(after_help = SINK_UPDATE_HELP)]
    Update {
        /// Workflow ID
        id: String,
        /// New workflow name
        #[arg(long)]
        name: Option<String>,
        /// Database the SQL runs in
        #[arg(long)]
        database: Option<String>,
        /// New SQL. Use "-" to read from stdin.
        #[arg(long)]
        sql: Option<String>,
        /// Seconds between automatic runs (1–86400); null removes the schedule
        #[arg(long, value_name = "SECONDS|null")]
        interval_seconds: Option<WorkflowIntervalArg>,
        /// Resume scheduled runs
        #[arg(long, conflicts_with = "disable")]
        enable: bool,
        /// Pause scheduled runs
        #[arg(long)]
        disable: bool,
        /// Sink as a JSON object; replaces all sinks. Repeat for up to five.
        #[arg(long = "sink", value_name = "JSON", conflicts_with = "clear_sinks")]
        sinks: Vec<String>,
        /// Remove all sinks
        #[arg(long)]
        clear_sinks: bool,
    },
    /// Delete a workflow and stop its schedule
    Delete {
        /// Workflow ID
        id: String,
    },
    /// Run a workflow once, independently of its schedule
    Run {
        /// Workflow ID
        id: String,
        /// Retry key; reusing it returns the same run instead of starting another
        #[arg(long)]
        idempotency_key: Option<String>,
    },
    /// List manual and scheduled runs of a workflow
    Runs {
        /// Workflow ID
        id: String,
        /// Maximum runs to return (default: 20, max: 100)
        #[arg(long, value_parser = clap::value_parser!(u16).range(1..=100))]
        limit: Option<u16>,
        /// Cursor from a previous page
        #[arg(long)]
        cursor: Option<String>,
    },
    /// Request cancellation of a workflow run
    Cancel {
        /// Workflow ID
        id: String,
        /// Run ID
        run_id: String,
    },
    /// Show workflow execution and sink delivery logs
    Logs {
        /// Workflow ID
        id: String,
        #[command(flatten)]
        range: WorkflowTimeRange,
        /// Maximum log entries to return (default: 100, max: 200)
        #[arg(long, value_parser = clap::value_parser!(u16).range(1..=200))]
        limit: Option<u16>,
        /// Cursor from a previous page
        #[arg(long)]
        cursor: Option<String>,
    },
    /// Show workflow execution, match, delivery, and error counts
    Metrics {
        /// Workflow ID
        id: String,
        #[command(flatten)]
        range: WorkflowTimeRange,
    },
}

#[derive(Clone, Copy, Debug)]
pub struct WorkflowIntervalArg(pub Option<u32>);

impl FromStr for WorkflowIntervalArg {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value == "null" {
            return Ok(Self(None));
        }
        match value.parse::<u32>() {
            Ok(seconds) if (1..=86400).contains(&seconds) => Ok(Self(Some(seconds))),
            _ => Err("expected a whole number from 1 to 86400, or null".into()),
        }
    }
}

/// Time window for workflow logs and metrics. Defaults to the last 24 hours; at most 7 days.
#[derive(clap::Args, Debug, Default)]
pub struct WorkflowTimeRange {
    /// Start this long ago (e.g., 1h, 30m, 7d)
    #[arg(long, conflicts_with_all = ["start_time", "end_time"])]
    pub since: Option<String>,
    /// End this long ago (e.g., 30m)
    #[arg(long, conflicts_with_all = ["start_time", "end_time"])]
    pub until: Option<String>,
    /// Start time as RFC 3339 (e.g., "2026-03-28T18:00:00Z")
    #[arg(long)]
    pub start_time: Option<String>,
    /// End time as RFC 3339 (e.g., "2026-03-28T19:00:00Z")
    #[arg(long)]
    pub end_time: Option<String>,
}

#[cfg(test)]
mod tests;
