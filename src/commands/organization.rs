use anyhow::Result;
use serde::Deserialize;
use serde_json::json;

use crate::client::{ApiClient, SavedIds, Scope};
use crate::config::{self, Config};
use crate::org;
use crate::output;

#[derive(Deserialize)]
struct CreateOrganizationResponse {
    name: String,
}

#[derive(Deserialize)]
struct RenameOrganizationResponse {
    name: String,
}

#[derive(Deserialize)]
struct DeleteOrganizationResponse {
    deleted: bool,
}

fn is_default_org(cfg: &Config, name: &str, id: &str) -> bool {
    cfg.default_organization_id.as_deref() == Some(id)
        || matches!(cfg.default_organization.as_deref(), Some(current) if current == name || current == id)
}

fn rename_default_org(cfg: &mut Config, old_name: &str, id: &str, new_name: &str) {
    if is_default_org(cfg, old_name, id) {
        cfg.set_default_organization(Some(new_name.to_string()), Some(id.to_string()));
    }
}

/// The saved cluster belongs to the previous default organization.
fn default_org_after_delete(
    cfg: &mut Config,
    deleted_name: &str,
    id: &str,
    next_available_organization: Option<(String, String)>,
) {
    if is_default_org(cfg, deleted_name, id) {
        let (name, id) = next_available_organization.unzip();
        cfg.set_default_organization(name, id);
        cfg.default_cluster_id = None;
    }
}

pub fn list(client: &ApiClient, json_mode: bool) -> Result<()> {
    let organizations = org::list_organizations(client)?;
    output::print_result(
        &json!({
            "organizations": organizations,
        }),
        json_mode,
        |_| {
            if organizations.is_empty() {
                println!(
                    "No organizations yet. Create one with `rtree organization create <name>`."
                );
            } else {
                for item in &organizations {
                    println!(
                        "{:<20} {:<8}",
                        item.name,
                        item.role.as_deref().unwrap_or("-")
                    );
                }
            }
        },
    );
    Ok(())
}

pub fn create(client: &ApiClient, name: &str, json_mode: bool) -> Result<()> {
    let resp: CreateOrganizationResponse =
        client.post("/v1/organizations", &json!({"organization_name": name}))?;
    output::print_result(
        &json!({
            "name": resp.name,
        }),
        json_mode,
        |_| {
            println!("Organization '{}' created.", resp.name);
        },
    );
    Ok(())
}

pub fn use_organization(client: &ApiClient, name: &str, json_mode: bool) -> Result<()> {
    // Re-resolve by name: this command is how a rejected saved ID gets refreshed.
    let id = client
        .with_scope(Scope::default())
        .with_saved_ids(SavedIds::default())
        .organization_id(name)
        .ok();
    let _config_lock = config::lock()?;
    let mut cfg = config::load()?;
    if cfg.default_organization_id != id {
        cfg.default_cluster_id = None;
    }
    cfg.set_default_organization(Some(name.to_string()), id);
    config::save(&cfg)?;

    output::print_result(&json!({"default_organization": name}), json_mode, |_| {
        println!("Default organization set to '{}'.", name)
    });
    Ok(())
}

pub fn rename(client: &ApiClient, old: &str, new_name: &str, json_mode: bool) -> Result<()> {
    let id = client.organization_id(old)?;
    let resp: RenameOrganizationResponse = client.patch(
        &format!("/v1/organizations/{}", urlencoding::encode(&id)),
        &json!({"organization_name": new_name}),
    )?;
    let _config_lock = config::lock()?;
    let mut cfg = config::load()?;
    rename_default_org(&mut cfg, old, &id, &resp.name);
    config::save(&cfg)?;

    output::print_result(
        &json!({"old_name": old, "name": resp.name}),
        json_mode,
        |_| {
            println!("Organization '{}' renamed to '{}'.", old, resp.name);
        },
    );
    Ok(())
}

pub fn delete(client: &ApiClient, name: &str, json_mode: bool) -> Result<()> {
    let id = client.organization_id(name)?;
    let resp: DeleteOrganizationResponse =
        client.delete(&format!("/v1/organizations/{}", urlencoding::encode(&id)))?;
    if resp.deleted && is_default_org(&config::load()?, name, &id) {
        let next = org::list_organizations(client)?
            .into_iter()
            .next()
            .map(|item| (item.name, item.organization_id));
        let _config_lock = config::lock()?;
        let mut cfg = config::load()?;
        if is_default_org(&cfg, name, &id) {
            default_org_after_delete(&mut cfg, name, &id, next);
            config::save(&cfg)?;
        }
    }

    output::print_result(
        &json!({"deleted": resp.deleted, "organization": name}),
        json_mode,
        |_| {
            if resp.deleted {
                println!("Organization '{}' deleted.", name);
            }
        },
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{default_org_after_delete, rename_default_org};
    use crate::config::Config;

    fn config_with(name: &str, id: Option<&str>) -> Config {
        Config {
            default_organization: Some(name.to_string()),
            default_organization_id: id.map(str::to_string),
            default_cluster_id: Some("cluster-id".to_string()),
            ..Config::default()
        }
    }

    #[test]
    fn renamed_default_org_updates_the_name_and_keeps_the_id() {
        for (current, id) in [
            ("team_old", None),
            ("org-id", None),
            ("stale_name", Some("org-id")),
        ] {
            let mut cfg = config_with(current, id);
            rename_default_org(&mut cfg, "team_old", "org-id", "team_new");
            assert_eq!(cfg.default_organization.as_deref(), Some("team_new"));
            assert_eq!(cfg.default_organization_id.as_deref(), Some("org-id"));
        }
    }

    #[test]
    fn renamed_default_org_preserves_non_matching_default() {
        let mut cfg = config_with("team_other", Some("other-id"));
        rename_default_org(&mut cfg, "team_old", "org-id", "team_new");
        assert_eq!(cfg.default_organization.as_deref(), Some("team_other"));
        assert_eq!(cfg.default_organization_id.as_deref(), Some("other-id"));
    }

    #[test]
    fn default_org_after_delete_promotes_next_org_with_its_id() {
        let mut cfg = config_with("team_old", Some("org-id"));
        let next = Some(("team_next".to_string(), "next-id".to_string()));
        default_org_after_delete(&mut cfg, "team_old", "org-id", next);
        assert_eq!(cfg.default_organization.as_deref(), Some("team_next"));
        assert_eq!(cfg.default_organization_id.as_deref(), Some("next-id"));
        assert_eq!(cfg.default_cluster_id, None);
    }

    #[test]
    fn default_org_after_delete_keeps_non_matching_default() {
        let mut cfg = config_with("team_other", Some("other-id"));
        let next = Some(("team_next".to_string(), "next-id".to_string()));
        default_org_after_delete(&mut cfg, "team_old", "org-id", next);
        assert_eq!(cfg.default_organization.as_deref(), Some("team_other"));
        assert_eq!(cfg.default_organization_id.as_deref(), Some("other-id"));
        assert_eq!(cfg.default_cluster_id.as_deref(), Some("cluster-id"));
    }

    #[test]
    fn default_org_after_delete_clears_name_and_id_when_no_next_org() {
        let mut cfg = config_with("team_old", Some("org-id"));
        default_org_after_delete(&mut cfg, "team_old", "org-id", None);
        assert_eq!(cfg.default_organization, None);
        assert_eq!(cfg.default_organization_id, None);
    }
}
