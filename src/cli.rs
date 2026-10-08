use std::{fmt, str::FromStr};

use clap::{Parser, Subcommand, ValueEnum};

use crate::cluster_identity::ClusterIdentityArgs;
use crate::s3_storage::S3StorageArgs;

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
    /// Create and list organization service accounts (requires an organization admin)
    ServiceAccount {
        #[command(subcommand)]
        action: ServiceAccountCommand,
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
pub enum ServiceAccountCommand {
    /// List service accounts in the selected organization
    List,
    /// Create a service account; its name cannot be changed later
    Create {
        /// Lowercase letters and digits separated by single hyphens (1–128 characters)
        #[arg(value_parser = crate::commands::service_account::parse_name)]
        name: String,
        /// Description (at most 2000 UTF-8 bytes)
        #[arg(long, value_parser = crate::commands::service_account::parse_description)]
        description: Option<String>,
        /// Stored organization role; API key permissions are configured separately
        #[arg(long, value_enum, default_value = "member")]
        role: crate::commands::service_account::ServiceAccountRole,
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
    Sizes {
        /// Region ID from `rtree cluster regions`; omit for the default backend
        #[arg(long)]
        region_id: Option<String>,
    },
    /// List regions available to the selected organization
    Regions,
    /// Create a dedicated cluster
    #[command(
        after_help = "Sizes use CPU_CORES:MEMORY_GIB (for example, 2:8). Run `rtree cluster sizes` to list the available sizes."
    )]
    Create {
        /// Region ID from `rtree cluster regions`; omit for the default backend
        #[arg(long)]
        region_id: Option<String>,
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
        identity: ClusterIdentityArgs,
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

const SINK_HELP: &str = r#"Sinks:
  --sink-table and --sink-http cover the common cases. --sink takes JSON for
  anything else: an object, an array of objects, @<path> to read a file, or -
  to read stdin. A workflow has at most five sinks in total.

  rtree workflow create --name alerts --sql - --sink-table analytics.alerts < alerts.sql
  rtree workflow create --name hook --sql 'SELECT 1' \
    --sink-http https://example.com/hook --sink-header 'Authorization: Bearer ...'
  rtree workflow create --name alerts --sql 'SELECT 1' --sink @sinks.json

Run `rtree workflow sink schema` for each sink type's fields."#;

const SINK_UPDATE_HELP: &str = r#"Sinks:
  To add, change, or remove a single sink, use `rtree workflow sink`.

  --sink replaces the whole list. It takes a JSON object, an array of objects,
  @<path> to read a file, or - to read stdin. Include the "id" of existing sinks
  to keep them; an existing HTTP sink may omit "settings", or its "url" or
  "headers", to retain them:
    rtree workflow update <id> \
      --sink '{"type":"http","id":"<http-sink-id>"}' \
      --sink '{"type":"table","id":"<table-sink-id>","settings":{"database":"analytics","table":"alerts_v2"}}'

Run `rtree workflow sink schema` for each sink type's fields."#;

const SINK_COMMAND_HELP: &str = r#"Each change reads the workflow's current sinks and writes back the full list,
so a concurrent edit to the same workflow's sinks can be overwritten.

Examples:
  rtree workflow sink list <workflow-id>
  rtree workflow sink create <workflow-id> --table analytics.alerts
  rtree workflow sink create <workflow-id> --http https://example.com/hook \
    --header 'Authorization: Bearer ...'
  rtree workflow sink update <workflow-id> <sink-id> --header 'Authorization: Bearer ...'
  rtree workflow sink delete <workflow-id> <sink-id>
  rtree workflow sink schema --type http"#;

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
        /// UTC cron expression (5 fields, or 6 with seconds); null or omitted means manual-only
        #[arg(long, value_name = "EXPRESSION|null")]
        schedule_cron: Option<WorkflowCronArg>,
        /// Enable or pause the schedule (true or false); requires a cron expression
        #[arg(long, action = clap::ArgAction::Set, value_name = "BOOL")]
        schedule_enabled: Option<bool>,
        /// Table sink as [DATABASE.]TABLE; the database defaults to the workflow's
        #[arg(long = "sink-table", value_name = "[DATABASE.]TABLE")]
        sink_tables: Vec<String>,
        /// HTTP sink URL
        #[arg(long = "sink-http", value_name = "URL")]
        sink_urls: Vec<String>,
        /// Header for the HTTP sink, as 'Name: value'; requires exactly one --sink-http
        #[arg(long = "sink-header", value_name = "HEADER", requires = "sink_urls")]
        sink_headers: Vec<String>,
        /// Sink as JSON (object or array), @<path>, or - for stdin
        #[arg(long = "sink", value_name = "JSON|@PATH|-")]
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
        /// UTC cron expression (5 fields, or 6 with seconds); null removes the schedule
        #[arg(long, value_name = "EXPRESSION|null")]
        schedule_cron: Option<WorkflowCronArg>,
        /// Enable or pause scheduled runs (true or false); omit to preserve the state
        #[arg(long, action = clap::ArgAction::Set, value_name = "BOOL")]
        schedule_enabled: Option<bool>,
        /// Sink as JSON (object or array), @<path>, or - for stdin; replaces all sinks
        #[arg(
            long = "sink",
            value_name = "JSON|@PATH|-",
            conflicts_with = "clear_sinks"
        )]
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
    /// List, add, change, or remove a workflow's sinks
    #[command(after_help = SINK_COMMAND_HELP)]
    Sink {
        #[command(subcommand)]
        action: WorkflowSinkCommand,
    },
}

#[derive(Subcommand)]
pub enum WorkflowSinkCommand {
    /// List a workflow's sinks
    List {
        /// Workflow ID
        workflow_id: String,
    },
    /// Add a sink to a workflow
    #[command(group(clap::ArgGroup::new("target").required(true).args(["table", "http"])))]
    Create {
        /// Workflow ID
        workflow_id: String,
        /// Write results to a table, as [DATABASE.]TABLE; the database defaults to the workflow's
        #[arg(long, value_name = "[DATABASE.]TABLE")]
        table: Option<String>,
        /// Send results to this webhook URL
        #[arg(long, value_name = "URL")]
        http: Option<String>,
        /// HTTP header as 'Name: value'; repeat for more headers
        #[arg(
            long = "header",
            value_name = "HEADER",
            requires = "http",
            conflicts_with = "table"
        )]
        headers: Vec<String>,
    },
    /// Change one sink; everything not passed is left unchanged
    Update {
        /// Workflow ID
        workflow_id: String,
        /// Sink ID, from `rtree workflow sink list`
        sink_id: String,
        /// Table sinks: new target, as [DATABASE.]TABLE; the database defaults to the sink's current one
        #[arg(long, value_name = "[DATABASE.]TABLE", conflicts_with_all = ["url", "headers", "remove_headers"])]
        table: Option<String>,
        /// HTTP sinks: new URL
        #[arg(long, value_name = "URL")]
        url: Option<String>,
        /// HTTP sinks: add or replace a header, as 'Name: value'; other headers are kept
        #[arg(long = "header", value_name = "HEADER")]
        headers: Vec<String>,
        /// HTTP sinks: remove a header by name
        #[arg(long = "remove-header", value_name = "NAME")]
        remove_headers: Vec<String>,
    },
    /// Remove a sink from a workflow
    Delete {
        /// Workflow ID
        workflow_id: String,
        /// Sink ID, from `rtree workflow sink list`
        sink_id: String,
    },
    /// Show each sink type's fields, rules, and examples
    Schema {
        /// Sink type to describe; omit to describe every type
        #[arg(long = "type", value_enum)]
        sink_type: Option<SinkTypeArg>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum SinkTypeArg {
    Table,
    Http,
}

#[derive(Clone, Debug)]
pub struct WorkflowCronArg(pub Option<String>);

impl FromStr for WorkflowCronArg {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value == "null" {
            return Ok(Self(None));
        }
        let value = value.trim();
        if value.is_empty() {
            return Err("expected a UTC cron expression or null".into());
        }
        Ok(Self(Some(value.to_string())))
    }
}

/// Time window for workflow logs. Defaults to the last 24 hours; at most 7 days.
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
