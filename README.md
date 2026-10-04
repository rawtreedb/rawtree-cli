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

When stdin is not a terminal, `rtree login` uses JSON automatically. Other
commands require `--json` for JSON output. Use `--json` to select JSON output in
a terminal.

Browser login emits a `device_approval_required` event on stderr before
approval. Read stderr as newline-delimited JSON while the command runs. Open the
`verification_uri_complete` link in a browser on any device. JSON mode does not
open a browser automatically. Interactive terminals can use `--no-browser` to
show the link without a browser launch.

```json
{"event":"device_approval_required","verification_uri":"<approval page>","verification_uri_complete":"<approval link>","user_code":"ABCD-EFGH","expires_in":600}
```

The CLI saves authentication immediately after approval. Bare JSON login leaves
resource defaults unset, writes this result to stdout, and exits with code `0`:

```json
{"email":"user@example.com","status":"logged_in","method":"browser","selected_organization":null,"selected_cluster":null,"selected_database":null}
```

Use separate commands to list resources and save defaults. This sequence
requires only one approval:

```sh
rtree login
rtree organization list --json
rtree organization use team-alpha --json
rtree cluster list --json
rtree cluster use production --json
rtree database list --json
rtree database use analytics --json
```

You can also select defaults during login:

```sh
rtree login --org team-alpha
rtree login --org team-alpha --cluster production --database analytics
```

The CLI checks requested selectors before it saves defaults. JSON login leaves
unrequested child defaults unset. If a requested child has no parent selector,
the CLI uses a parent only when exactly one choice exists. Otherwise, it returns
a selection error with the choices and exit code `2`.

 Selection and discovery errors after approval retain the saved session. Their
JSON errors appear on stderr with `authentication_saved: true`. Use the resource
commands to continue without another login. Approval failures return a JSON
error on stderr and a nonzero exit. They retain the previous config.

API-key login skips device approval.

Interactive login retains its selection prompts. If a prompt stops or discovery
fails after approval, the session remains saved.

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

Changing the saved organization clears the cluster and database defaults.
Changing the saved cluster clears the database default. Selecting the same
parent retains its child defaults. Ordinary flags override environment variables
and saved defaults for one command. They do not change the config. A different
parent override prevents the command from reusing saved child defaults.

A child `use` command rejects parent overrides that differ from the saved
defaults. Set the saved parent first. The `use` commands change local defaults.
They do not check whether resources exist.

With user credentials, commands can use the only available organization or
cluster without saving it. If context is missing or ambiguous, the CLI returns a
structured error on stderr with exit code `2`. The error includes `needs`,
available parent choices, and instructions to select a default or pass a flag.
API-key commands retain the scope and defaults supplied by the server.

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
rtree key create --name temporary-ci --permission read_only --expires-at 2027-01-01T00:00:00Z
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

`key create --expires-at` accepts a future RFC 3339 timestamp with a timezone.
The server validates and normalizes it to UTC. Omit the flag for a key that never
expires. Create/list output includes expiration (`never` in text, `null` in JSON);
older servers that omit the field are also supported. Expiration is fixed at
creation; create a replacement key to change it. There is no `key update` command.

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
to `main` and pull requests. New PR updates cancel obsolete runs. It also runs `tests/live_platform.rs` against the full
Platform Docker Compose stack on same-repository changes. That test uses the
Platform launcher to create a local organization and cluster, then checks CLI
database creation, insertion, querying, API key login, and deletion through the real API.
The Platform repository continues to test API endpoints directly.

The Docker job checks out private `rawtreedb/rawtree-platform` at `main`. It
requires a read-only deploy key on the Platform repository, with its private SSH
key stored as `PLATFORM_REPO_SSH_KEY` in this repository's GitHub Actions secrets.
The job also requires `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` secrets to pull
the private RawTree server, keeper, and backend images. Use a read-only Docker Hub
token with access to those repositories.

The integration job uses an ARM64 runner and reuses the published backend image
only when its source revision is an ancestor of the checked-out Platform `main`
and its backend release inputs are unchanged. It pulls the verified image by
digest; if the inputs differ or revision metadata is missing, it builds from
source. The frontend is built for the local test URL. Compose still starts the
full stack, waits for healthy services, and bootstraps the test identity. Build
and startup timings appear as separate CI steps. Private Platform layers are
never stored in the public CLI repository's Actions cache.
Fork pull requests run the unit and mock-API tests;
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
