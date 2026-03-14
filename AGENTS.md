# AGENTS.md

## Cursor Cloud specific instructions

This is a **Rust CLI project** (`rtree`) — a client for the RawTree analytics platform. There is no local backend server; the binary talks to a remote API at `https://api.rawtree.dev`.

### Build / Test / Lint / Run

Standard Cargo commands — see `Cargo.toml` for details:

- **Build:** `cargo build`
- **Test:** `cargo test` (47 unit tests, all self-contained with no network)
- **Lint:** `cargo clippy -- -D warnings`
- **Format check:** `cargo fmt --check`
- **Run:** `cargo run -- <command>` (e.g. `cargo run -- --help`, `cargo run -- status`)

### Non-obvious notes

- The Rust toolchain must be **1.85+** (the `comfy-table` 7.2.2 dependency requires `edition2024` support). The update script ensures stable is current via `rustup update stable`.
- `libssl-dev` and `pkg-config` are required system dependencies for building (`openssl-sys` crate). These are installed once during initial VM setup and persist across sessions.
- All existing tests are pure unit tests that require **no network access**. Commands that hit the remote API (e.g. `ping`, `login`, `query`) will fail in sandboxed environments without internet egress to `api.rawtree.dev`.
- The `--json` flag on any command switches output to JSON, useful for scripting and testing.
