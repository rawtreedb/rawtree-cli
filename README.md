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

- Browser-based (default): `rtree login`
- Email/password: `rtree login --email you@example.com --password '***'`
- Direct API key save: `rtree login --api-key rt_123`
- Select defaults during auth: `rtree login --org team-alpha --database analytics`

When using `--api-key`, the CLI stores the API key directly and resolves organization/database defaults from that key.
With `--json`, API key login returns:

```json
{"success":true,"config_path":"<path>","database":"<name>","organization":"<name>"}
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

When `--api-key` or `RAWTREE_API_KEY` overrides saved authentication, the saved
cluster is not applied. Pass `--cluster` or `RAWTREE_CLUSTER` explicitly when
the override key is scoped to a named cluster.

## Commands

Top-level commands:

- `register`, `login`, `logout`
- `database`, `organization`, `cluster`, `key`, `table`
- `query`, `insert`
- `ping`, `docs`, `status`, `open`, `completions`

Global flags:

- `--api-url <URL>`
- `--org <ORG>`
- `--cluster <CLUSTER>`
- `--json`

## Common Workflows

### Organizations, clusters, and databases

```sh
rtree organization list
rtree organization create team-alpha
rtree organization use team-alpha

rtree cluster list
rtree cluster create production
rtree cluster use production

rtree database list
rtree database create analytics
rtree database use analytics
```

Cluster management requires a user login. `cluster create` uses the platform's
default size. Deleting a cluster also deletes its databases and cluster-scoped
API keys, so interactive deletion asks for confirmation and non-interactive or
JSON usage requires `--yes`:

```sh
rtree cluster delete production --yes
```

Use `--cluster` or `RAWTREE_CLUSTER` for a one-command override without changing
the saved cluster:

```sh
rtree database list --cluster production
RAWTREE_CLUSTER=production rtree query --database analytics "SELECT 1"
```

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
rtree insert --database analytics --table events --data '{"event":"page_view"}'

# JSON/JSONL file
rtree insert --database analytics --table events --file ./events.jsonl

# Public URL to JSON/JSONL
rtree insert --database analytics --table events --url https://example.com/events.jsonl
```

### Keys and tables

```sh
rtree key list --database analytics
rtree key create --database analytics --name ci --permission read_write

rtree table list --database analytics
rtree table describe --database analytics events
```

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
cargo test
```

Run locally:

```sh
cargo run -- --help
```

## Release Notes

- Repository/package name: `rawtree-cli`
- Executable name: `rtree`
