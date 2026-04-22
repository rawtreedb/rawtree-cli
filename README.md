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

# Create and select a project
rtree project create analytics
rtree project use analytics

# Insert a JSON row
rtree insert --table events --data '{"event":"signup","user_id":1}'

# Run a query
rtree query --sql "SELECT count(*) FROM events"

# Open the UI for the current project
rtree open
```

## Authentication

### Login modes

- Browser-based (default): `rtree login`
- Email/password: `rtree login --email you@example.com --password '***'`
- Select defaults during auth: `rtree login --org team-alpha --project analytics`

After `login` or `register`, the CLI prints the selected organization and project.
If `--project` is omitted, it selects the first project in the selected organization.

### Token resolution

1. `RAWTREE_TOKEN` environment variable
2. Local config file

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

- API URL: `--api-url` -> `RAWTREE_URL` -> config file -> `https://api.rawtree.com`
- Project: `--project` -> `RAWTREE_PROJECT` -> config file default project
- Organization: `--org` -> `RAWTREE_ORG` -> config file default organization

## Commands

Top-level commands:

- `register`, `login`, `logout`
- `project`, `organization`, `key`, `table`
- `query`, `insert`
- `ping`, `docs`, `status`, `open`, `completions`

Global flags:

- `--api-url <URL>`
- `--org <ORG>`
- `--json`

## Common Workflows

### Projects and organizations

```sh
rtree organization list
rtree organization create team-alpha
rtree organization use team-alpha

rtree project list
rtree project create analytics
rtree project use analytics
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
rtree insert --project analytics --table events --data '{"event":"page_view"}'

# JSON/JSONL file
rtree insert --project analytics --table events --file ./events.jsonl

# Public URL to JSON/JSONL
rtree insert --project analytics --table events --url https://example.com/events.jsonl
```

If you are not logged in with a JWT, `rtree insert` can bootstrap an anonymous ingest project automatically.

### Keys and tables

```sh
rtree key list --project analytics
rtree key create --project analytics --name ci --permission read_write

rtree table list --project analytics
rtree table describe --project analytics events
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
