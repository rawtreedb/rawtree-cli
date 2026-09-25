# RawTree CLI

The official CLI for the RawTree analytics platform.

Built for humans, AI agents, and CI/CD pipelines.

The package/repo name is `rawtree-cli`, and the command you run is `rtree`.

## Install

### GitHub Releases (recommended)

```sh
curl --proto '=https' --tlsv1.2 -LsSf \
  https://github.com/rawtreedb/rawtree-cli/releases/latest/download/rawtree-cli-installer.sh | sh
```

### Cargo (from source)

```sh
git clone https://github.com/rawtreedb/rawtree-cli.git
cd rawtree-cli
cargo install --path .
```

### Build locally

```sh
git clone https://github.com/rawtreedb/rawtree-cli.git
cd rawtree-cli
cargo build --release
./target/release/rtree --help
```

## Update

If you installed with the GitHub Releases installer, update in place:

```sh
rtree update
```

`rtree update --json` prints `{"updated":true,"previous_version":"<old>","version":"<new>"}`,
or `{"updated":false,"version":"<current>"}` when already on the latest release.
Source installs aren't managed by the installer; update them with `git pull && cargo install --path .`.

## Quick Start

```sh
# Authenticate (browser flow by default)
rtree login

# Create and select a database
rtree database create analytics
rtree database use analytics

# Insert a JSON row
rtree insert --table events --data '{"event":"signup","user_id":1}'

# Run a query
rtree query --sql "SELECT count(*) FROM events"

# Open the UI for the current database
rtree open
```

## Authentication

### Login modes

- Interactive login selection: `rtree login`
- Direct API key save: `rtree login --api-key rt_123`
- Select a database during auth in a specific cluster: `rtree login --org team-alpha --cluster production --database analytics`

Interactive login offers browser-based Rawtree authentication or securely prompts
for an existing API key. Non-interactive and `--json` login continue to use
browser-based authentication unless `--api-key` is provided.

When using `--api-key`, the CLI validates the key and any supplied organization/cluster
selectors with the server before saving it. Explicit selections are saved as defaults.
If the server omits organization/database metadata and no selection was supplied,
those settings remain unset. Commands that require a database still need `--database`,
`RAWTREE_DATABASE`, or a saved default from `rtree database use <name>`.
With `--json`, API key login returns (unset selections are `null`):

```json
{"success":true,"config_path":"<path>","database":"<name>","organization":"<name>","cluster":"<name>"}
```

### Token resolution

1. `--api-key` flag
2. `RAWTREE_API_KEY` environment variable
3. Local config file

### Logout

```sh
rtree logout
```

Logout clears the local config, including any saved API URL, so the next run uses
the default `https://api.rawtree.com` endpoint unless an override is provided.

## Configuration

Config file location:

- Unix: `~/.config/rtree/config.json`

Resolution priority by setting:

- API KEY: `--api-key` -> `RAWTREE_API_KEY` -> config file token
- API URL: `--api-url` -> `RAWTREE_API_URL` -> config file -> `https://api.rawtree.com`
- Database: `--database` -> `RAWTREE_DATABASE` -> config file default database
- Organization: `--org` -> `RAWTREE_ORG` -> config file default organization
- Cluster: `--cluster` -> `RAWTREE_CLUSTER` -> config file default cluster

API keys remain restricted to their bound cluster regardless of which selector source is used.

## Commands

Top-level commands:

- `login`, `logout`
- `database`, `organization`, `cluster`, `key`, `table`
- `query`, `insert`
- `ping`, `docs`, `status`, `open`, `completions`

Global flags:

- `--api-url <URL>`
- `--org <ORG>`
- `--cluster <CLUSTER>`
- `--json`

## Common Workflows

### Databases and organizations

```sh
rtree organization list
rtree organization create team-alpha
rtree organization use team-alpha

rtree cluster list
rtree cluster use production

rtree database list
rtree database create analytics
rtree database use analytics
```

Database creation saves the selected organization, cluster, and new database locally.
With `--json`, creation returns `{"database":{"name":"analytics"}}`; listing returns
`{"databases":[{"name":"analytics","s3_storage":null}]}`. Database output no longer
adds organization metadata that is absent from the API response.

### Querying

```sh
# Positional SQL
rtree query "SELECT * FROM events LIMIT 10"

# SQL from stdin
cat query.sql | rtree query -

# JSON output
rtree query --json --sql "SELECT * FROM events LIMIT 10"
```

### Data ingestion

```sh
# Inline JSON
rtree insert --table events --data '{"event":"page_view"}'

# JSON/JSONL file
rtree insert --table events --file ./events.jsonl

# Public URL to JSON/JSONL
rtree insert --table events --url https://example.com/events.jsonl
```

URL imports wait for completion and print the inserted row count and query ID.
With `--json`, the result is
`{"inserted":1000}` (or `{"inserted":null}` when the count is unavailable).

### Keys and tables

```sh
rtree key list
rtree key create --name ci --permission read_write
rtree key create --database analytics --name analytics-ci --permission read_write

rtree table list --database analytics
rtree table describe --database analytics events
rtree table create --database analytics events --sorting-key 'region, ifNull(cityHash64(host, instanceId), 0)'
rtree table update --database analytics events --sorting-key 'region, toStartOfHour(timestamp)'
```

Omit `--sorting-key` when creating a table to choose a key automatically per part. `table describe` shows the current sorting key as a string. Updating the key affects new parts and later merges; existing parts may keep their previous key until they are merged. A custom sorting key cannot be reset to automatic.

API keys belong to a cluster. `key list` and `key delete` do not accept `--database`;
listing includes each key's default database. `key create --database` selects the
new key's default database, using `RAWTREE_DATABASE` or the saved selection when
omitted. If none is selected, the server uses `default`.

### Request logs

```sh
rtree --org team-alpha --cluster production logs --since 1h --status-codes 500
```

Logs cover the selected cluster. The obsolete `--database` and `--log-databases`
flags are rejected because the API does not apply database filters.

### Clusters

```sh
rtree cluster list
rtree cluster list --json
rtree cluster sizes
rtree cluster create \
  --name production \
  --replicas 2 \
  --min-size 2:8 \
  --max-size 64:256 \
  --idle-timeout-minutes 30
rtree cluster use production
rtree cluster status production
rtree cluster update production --idle-timeout-minutes 60
rtree cluster update production --idle-timeout-minutes 0
rtree cluster stop production
rtree cluster resume production
rtree cluster delete production
```

`--min-size` is required and uses `CPU_CORES:MEMORY_GIB` format. `--max-size`
is optional; omitting it uses the minimum size for both bounds and disables
vertical autoscaling. Sizes are validated against the server's cluster size
catalog. Run `rtree cluster sizes` to see the currently available sizes; add
`--json` for a machine-readable response.
`--idle-timeout-minutes` is optional on create and update; omit it to use the
server default on create, and pass `0` to disable automatic idling.

Cluster lifecycle and provisioning changes are asynchronous. The `create`,
`stop`, `resume`, and `delete` commands return as soon as the API accepts the
request; they do not wait for the infrastructure operation to finish. After
creating, stopping, or resuming a cluster, run `rtree cluster status
<name-or-id>` to follow its current state.

## Shell Completions

```sh
# Bash
rtree completions bash > ~/.rtree-completion.bash

# Zsh
rtree completions zsh > ~/.rtree-completion.zsh

# Fish
rtree completions fish > ~/.config/fish/completions/rtree.fish
```

## Local Development

Prerequisites:

- Rust (stable)

Setup:

```sh
git clone https://github.com/rawtreedb/rawtree-cli.git
cd rawtree-cli
cargo check
cargo fmt --all -- --check
cargo test --locked
```

The `Tests` workflow runs the CLI's unit and mock-API contract tests on pushes
and pull requests. It also runs `tests/live_platform.rs` against the full
Platform Docker Compose stack on same-repository changes. That test uses the
Platform launcher to create a local organization and cluster, then checks CLI
database creation, insertion, querying, API key login, and deletion through the real API.
The Platform repository continues to test API endpoints directly.

The Docker job checks out private `rawtreedb/rawtree-platform` at `main`. It
requires a read-only `PLATFORM_REPO_READ_TOKEN` secret in this repository's
GitHub Actions settings. Fork pull requests run the unit and mock-API tests;
GitHub does not pass the private checkout secret to those runs.

To run the real test locally, start Platform from its checkout with
`bash scripts/codex/run-local-compose.sh 18087`, then set
`RAWTREE_LIVE_API_URL=http://localhost:18087` and the
`RAWTREE_LIVE_SESSION_TOKEN`, `RAWTREE_LIVE_ORGANIZATION`, and
`RAWTREE_LIVE_CLUSTER` values from that checkout's ignored `.env.local`.
Run `cargo test --locked --test live_platform -- --ignored` in this checkout.

Run locally:

```sh
cargo run -- --help
```

## Release Notes

- Repository/package name: `rawtree-cli`
- Executable name: `rtree`
