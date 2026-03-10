# Task Plan - 2026-03-10

## Objective
Implement CLI Organization Support v1 with explicit org context, client auto-default org selection, org management commands, and org-aware routing while preserving legacy unscoped behavior.

## Checklist
- [x] Add CLI/config plumbing for organization context (`--org`, env, config default)
- [x] Add shared org utilities (list orgs, resolve org id, scoped project path)
- [x] Implement `organization` command group (`list|create|use|rename|delete|members|add-member|update-member|remove-member`)
- [x] Update existing project/data/key commands for optional org-aware routing
- [x] Update status/whoami output to include `default_organization`
- [x] Add tests for precedence + scoped path behavior + auto-default selection logic
- [x] Run formatting/tests and validate behavior
- [x] Write implementation review notes in this file

## Review
- Added global organization context support:
  - CLI flag: `--org`
  - Env fallback: `RAWTREE_ORG`
  - Config field: `default_organization`
  - Explicit precedence: `--org` -> env -> config
- Added client auto-default org behavior in runtime command dispatch:
  - If explicit org missing, CLI calls `/v1/organizations` and picks the first org name.
  - If fetch fails or is empty, CLI falls back to unscoped legacy routes.
- Added new `organization` subcommands in CLI and implementation module:
  - `list`, `create`, `use`, `rename`, `delete`
  - `members`, `add-member`, `update-member`, `remove-member`
- Introduced shared org utility module `src/org.rs`:
  - `list_organizations`
  - `first_organization_name`
  - `resolve_organization_id`
  - `project_scoped_path`
- Updated routing behavior for existing commands:
  - Data/key commands now use `/v1/{org}/{project}/...` when org is available; otherwise `/v1/{project}/...`.
  - Project list/create now resolve org id and call `/v1/projects?organization_id=...` when org is available.
  - Project rename/delete now use scoped `/v1/{org}/{project}` when org is available; otherwise legacy `/v1/projects/{project}`.
- Extended status outputs:
  - `whoami` JSON now includes `default_organization`.
  - `status` JSON/human output now includes `default_organization`.
- Verification performed:
  - `cargo fmt`
  - `cargo test` (13 passed)
- Compatibility note:
  - Existing unscoped behavior remains intact when no org can be resolved.
