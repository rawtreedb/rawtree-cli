//! Shared sink documentation for the commands that accept sink JSON.

const NEW_SINKS: &str = r##"New sinks:
  Each sink requires a type and settings. Omit id when adding a sink.

  table: settings.database and settings.table are required strings.
    {"type":"table","settings":{"database":"analytics","table":"alerts"}}

  http: settings.url is required; settings.headers is an optional object of
  header names to string values. Header values are write-only.
    {"type":"http","settings":{"url":"https://example.com/hook","headers":{"Authorization":"Bearer ..."}}}

  slack: settings.integration_id is the UUID of a connected Slack integration
  in the workflow's organization. Set exactly one of settings.channel_id or
  settings.channel_name. Channel names are resolved to IDs when saved.
    {"type":"slack","settings":{"integration_id":"<integration-id>","channel_id":"C0123456789"}}
    {"type":"slack","settings":{"integration_id":"<integration-id>","channel_name":"#alerts"}}"##;

const SINK_RULES: &str = r##"Sink rules:
  A workflow has at most five sinks. The API validates their settings.
  Table database/table names start with a letter and contain only letters,
  digits, or underscores, up to 64 characters. Table targets must be distinct.
  HTTP URLs use HTTP or HTTPS, without user info or fragments, up to 2048 bytes.
  Destinations must use a public address or an origin allowed by the server.
  URLs and header values cannot contain {{, %, or ${; percent-encoded URLs
  are not supported. Use at most 20 headers, 8192 bytes total, with names up to
  128 bytes and values up to 4096 bytes. Names must be valid HTTP tokens and
  distinct ignoring case; values cannot contain newlines or invalid controls.
  Transport and RawTree delivery headers cannot be overridden.
  Slack channel names have 1 to 80 characters, with an optional leading #.
  The RawTree app must be a member of private Slack channels. Slack targets
  must use distinct workspace/channel pairs.
  Prefer @files for HTTP credentials to keep them out of shell history."##;

const HTTP_HEADER_UPDATES: &str = r##"HTTP header updates:
  Omit settings.headers to keep all stored headers. A supplied headers object
  replaces the header list: omitted names are removed, string values set or
  replace a header, and null keeps the stored value for the same name (ignoring
  case). Use {} to remove all headers. null requires an existing header.
  Responses show header_names only; do not copy those back as header values.
  This settings patch keeps Authorization, sets X-Region, and removes every
  other header. With workflow update --sink, include type and id as well:
    {"settings":{"headers":{"Authorization":null,"X-Region":"eu"}}}"##;

pub(super) fn workflow_create() -> String {
    [
        r##"Create a workflow:
  --database selects the database for the SQL; sink databases are independent.
  Omit --schedule-cron for manual runs, or pass a UTC cron expression to schedule
  runs. Use --schedule-enabled false to create a paused schedule.
  --sink accepts a JSON object, an array of objects, @<path>, or - for stdin.
  Repeat --sink to combine inputs. Omit it to create a workflow without sinks.
  --sql - and --sink - cannot both read stdin."##,
        NEW_SINKS,
        SINK_RULES,
        r##"Examples:
  rtree workflow create --name alerts --database analytics --sql 'SELECT 1 AS value' \
    --sink '{"type":"table","settings":{"database":"analytics","table":"alerts"}}'
  rtree workflow create --name alerts --database analytics --sql - \
    --schedule-cron '*/5 * * * *' --sink @sinks.json < alerts.sql
  rtree workflow create --name alerts --database analytics --sql 'SELECT 1 AS value' \
    --sink '{"type":"slack","settings":{"integration_id":"<integration-id>","channel_name":"#alerts"}}'
  rtree workflow run <workflow-id>"##,
    ]
    .join("\n\n")
}

pub(super) fn workflow_update() -> String {
    [
        r##"Update a workflow:
  Omitted flags keep their stored values. --schedule-cron null removes the
  schedule; --schedule-enabled false pauses it and true resumes it.
  Omit --sink to keep all sinks. --clear-sinks removes every sink.
  --sink accepts a JSON object, an array of objects, @<path>, or - for stdin,
  and can be repeated. --sql - and --sink - cannot both read stdin.
  --sink replaces the whole list: sinks left out are removed. Include each
  existing sink's id and type to keep it; omit id to add a new sink.
  Find sink IDs with `rtree workflow get <workflow-id>`.

Keep existing sinks when replacing the list:
  table requires full settings, including database and table.
    {"type":"table","id":"<sink-id>","settings":{"database":"analytics","table":"alerts"}}
  http may omit settings, url, or headers to keep their stored values.
    {"type":"http","id":"<sink-id>"}
  slack requires integration_id and exactly one channel_id or channel_name.
  Use the stored channel_id to keep the same channel; do not send both channel
  fields returned by workflow get.
    {"type":"slack","id":"<sink-id>","settings":{"integration_id":"<integration-id>","channel_id":"C0123456789"}}"##,
        NEW_SINKS,
        HTTP_HEADER_UPDATES,
        SINK_RULES,
        r##"Examples:
  rtree workflow update <workflow-id> --sink @sinks.json
  rtree workflow update <workflow-id> --clear-sinks
  To change just one sink without resending the others:
    rtree workflow sink update <workflow-id> <sink-id> '{"settings":{"table":"alerts_v2"}}'
  See `rtree workflow sink create --help` and `rtree workflow sink update --help`."##,
    ]
    .join("\n\n")
}

pub(super) const SINK_COMMAND_HELP: &str = r##"Change one sink without resending or removing the others:
  rtree workflow get <workflow-id>
  rtree workflow sink create <workflow-id> @sink.json
  rtree workflow sink update <workflow-id> <sink-id> @changes.json
  rtree workflow sink delete <workflow-id> <sink-id>

Table, HTTP, and Slack fields and examples:
  rtree workflow sink create --help
  rtree workflow sink update --help

To replace the entire list, see `rtree workflow update --help`."##;

pub(super) fn sink_create() -> String {
    [
        r##"Add one sink:
  Takes one JSON object, @<path> to read a file, or - to read stdin.
  Arrays are not accepted. Other sinks remain unchanged."##,
        NEW_SINKS,
        SINK_RULES,
        r##"Examples:
  rtree workflow sink create <workflow-id> @sink.json
  rtree workflow sink create <workflow-id> \
    '{"type":"slack","settings":{"integration_id":"<integration-id>","channel_name":"#alerts"}}'
  rtree workflow get <workflow-id>"##,
    ]
    .join("\n\n")
}

pub(super) fn sink_update() -> String {
    [
        r##"Change one sink:
  Find its ID with `rtree workflow get <workflow-id>`.
  Takes one JSON object, @<path> to read a file, or - to read stdin.
  Arrays are not accepted. Supply a nonempty settings object with only the
  fields to change; omitted settings fields keep their stored values, except
  for names within a supplied HTTP headers object (see below).
  Other sinks remain unchanged. Do not put id in the JSON. type is optional
  and cannot change; delete the sink and create a new one to change its type.

Settings by sink type:
  table: change database, table, or both; either omitted field is preserved.
    {"settings":{"table":"alerts_v2"}}
  http: change url, headers, or both; either omitted field is preserved.
    {"settings":{"url":"https://example.com/new-hook"}}
  slack: change integration_id, channel_id, or channel_name. integration_id is
  a connected Slack integration's UUID in the workflow's organization.
  Omit integration_id to keep it. Omit both channel fields to keep the channel,
  or set exactly one to replace it; channel names are resolved to IDs on save.
    {"settings":{"channel_name":"#alerts-v2"}}
    {"settings":{"integration_id":"<integration-id>","channel_id":"C0123456789"}}"##,
        HTTP_HEADER_UPDATES,
        SINK_RULES,
        r##"Examples:
  rtree workflow sink update <workflow-id> <sink-id> @changes.json
  rtree workflow sink update <workflow-id> <sink-id> '{"settings":{"table":"alerts_v2"}}'
  rtree workflow sink update <workflow-id> <sink-id> '{"settings":{"channel_name":"#alerts-v2"}}'
  rtree workflow sink update <workflow-id> <sink-id> \
    '{"settings":{"headers":{"Authorization":null,"X-Region":"eu"}}}'"##,
    ]
    .join("\n\n")
}

pub(super) const SINK_DELETE_HELP: &str = r##"Remove only the selected sink; other sinks remain unchanged.
Find sink IDs with `rtree workflow get <workflow-id>`.

  rtree workflow sink delete <workflow-id> <sink-id>

To remove every sink, use `rtree workflow update <workflow-id> --clear-sinks`."##;
