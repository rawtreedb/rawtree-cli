use anyhow::Result;
use serde::Deserialize;
use serde_json::json;

use crate::client::ApiClient;
use crate::config;
use crate::org;
use crate::output;

#[derive(Deserialize)]
struct CreateOrganizationResponse {
    organization_id: String,
    organization_name: String,
}

#[derive(Deserialize)]
struct RenameOrganizationResponse {
    organization_name: String,
}

#[derive(Deserialize)]
struct DeleteOrganizationResponse {
    deleted: bool,
}

#[derive(Deserialize)]
struct OrganizationMemberItem {
    user_id: String,
    email: String,
    role: String,
    created_at: String,
}

#[derive(Deserialize)]
struct ListOrganizationMembersResponse {
    members: Vec<OrganizationMemberItem>,
}

#[derive(Deserialize)]
struct AddOrganizationMemberResponse {
    added: bool,
}

#[derive(Deserialize)]
struct UpdateOrganizationMemberRoleResponse {
    role: String,
}

#[derive(Deserialize)]
struct RemoveOrganizationMemberResponse {
    removed: bool,
}

fn renamed_default_org(
    current_default_organization: Option<&str>,
    old_name: &str,
    new_name: &str,
) -> Option<String> {
    match current_default_organization {
        Some(current) if current == old_name => Some(new_name.to_string()),
        Some(current) => Some(current.to_string()),
        None => None,
    }
}

fn default_org_after_delete(
    current_default_organization: Option<&str>,
    deleted_name: &str,
    next_available_organization: Option<String>,
) -> Option<String> {
    match current_default_organization {
        Some(current) if current == deleted_name => next_available_organization,
        Some(current) => Some(current.to_string()),
        None => None,
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
                        "{:<20} {:<8} id={} created={}",
                        item.organization_name, item.role, item.organization_id, item.created_at
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
            "organization_id": resp.organization_id,
            "organization_name": resp.organization_name,
        }),
        json_mode,
        |_| {
            println!(
                "Organization '{}' created (id={}).",
                resp.organization_name, resp.organization_id
            );
        },
    );
    Ok(())
}

pub fn use_organization(name: &str, json_mode: bool) -> Result<()> {
    let mut cfg = config::load()?;
    cfg.default_organization = Some(name.to_string());
    config::save(&cfg)?;

    output::print_result(&json!({"default_organization": name}), json_mode, |_| {
        println!("Default organization set to '{}'.", name)
    });
    Ok(())
}

pub fn rename(client: &ApiClient, old: &str, new_name: &str, json_mode: bool) -> Result<()> {
    let resp: RenameOrganizationResponse = client.patch(
        &format!("/v1/organizations/{old}"),
        &json!({"organization_name": new_name}),
    )?;
    let mut cfg = config::load()?;
    cfg.default_organization = renamed_default_org(
        cfg.default_organization.as_deref(),
        old,
        &resp.organization_name,
    );
    config::save(&cfg)?;

    output::print_result(
        &json!({"old_name": old, "organization_name": resp.organization_name}),
        json_mode,
        |_| {
            println!(
                "Organization '{}' renamed to '{}'.",
                old, resp.organization_name
            );
        },
    );
    Ok(())
}

pub fn delete(client: &ApiClient, name: &str, json_mode: bool) -> Result<()> {
    let resp: DeleteOrganizationResponse = client.delete(&format!("/v1/organizations/{name}"))?;
    if resp.deleted {
        let mut cfg = config::load()?;
        if cfg.default_organization.as_deref() == Some(name) {
            let next = org::list_organizations(client)?
                .into_iter()
                .next()
                .map(|item| item.organization_name);
            cfg.default_organization =
                default_org_after_delete(cfg.default_organization.as_deref(), name, next);
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

pub fn members(client: &ApiClient, organization: &str, json_mode: bool) -> Result<()> {
    let resp: ListOrganizationMembersResponse =
        client.get(&format!("/v1/organizations/{organization}/members"))?;
    output::print_result(
        &json!({
            "organization": organization,
            "members": resp.members.iter().map(|m| json!({
                "user_id": m.user_id,
                "email": m.email,
                "role": m.role,
                "created_at": m.created_at,
            })).collect::<Vec<_>>()
        }),
        json_mode,
        |_| {
            if resp.members.is_empty() {
                println!("No members in organization '{}'.", organization);
            } else {
                for member in &resp.members {
                    println!(
                        "{:<36} {:<28} {:<8} created={}",
                        member.user_id, member.email, member.role, member.created_at
                    );
                }
            }
        },
    );
    Ok(())
}

pub fn add_member(
    client: &ApiClient,
    organization: &str,
    email: &str,
    json_mode: bool,
) -> Result<()> {
    let resp: AddOrganizationMemberResponse = client.post(
        &format!("/v1/organizations/{organization}/members"),
        &json!({"email": email}),
    )?;
    output::print_result(
        &json!({"organization": organization, "email": email, "added": resp.added}),
        json_mode,
        |_| {
            if resp.added {
                println!("Added '{}' to organization '{}'.", email, organization);
            }
        },
    );
    Ok(())
}

pub fn update_member(
    client: &ApiClient,
    organization: &str,
    user_id: &str,
    role: &str,
    json_mode: bool,
) -> Result<()> {
    let resp: UpdateOrganizationMemberRoleResponse = client.patch(
        &format!("/v1/organizations/{organization}/members/{user_id}"),
        &json!({"role": role}),
    )?;
    output::print_result(
        &json!({
            "organization": organization,
            "user_id": user_id,
            "role": resp.role,
        }),
        json_mode,
        |_| {
            println!(
                "Updated '{}' role to '{}' in '{}'.",
                user_id, resp.role, organization
            )
        },
    );
    Ok(())
}

pub fn remove_member(
    client: &ApiClient,
    organization: &str,
    user_id: &str,
    json_mode: bool,
) -> Result<()> {
    let resp: RemoveOrganizationMemberResponse = client.delete(&format!(
        "/v1/organizations/{organization}/members/{user_id}"
    ))?;
    output::print_result(
        &json!({"organization": organization, "user_id": user_id, "removed": resp.removed}),
        json_mode,
        |_| {
            if resp.removed {
                println!(
                    "Removed '{}' from organization '{}'.",
                    user_id, organization
                );
            }
        },
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{default_org_after_delete, renamed_default_org};

    #[test]
    fn renamed_default_org_updates_matching_default() {
        let updated = renamed_default_org(Some("team_old"), "team_old", "team_new");
        assert_eq!(updated.as_deref(), Some("team_new"));
    }

    #[test]
    fn renamed_default_org_preserves_non_matching_default() {
        let updated = renamed_default_org(Some("team_other"), "team_old", "team_new");
        assert_eq!(updated.as_deref(), Some("team_other"));
    }

    #[test]
    fn default_org_after_delete_promotes_next_org() {
        let updated =
            default_org_after_delete(Some("team_old"), "team_old", Some("team_next".to_string()));
        assert_eq!(updated.as_deref(), Some("team_next"));
    }

    #[test]
    fn default_org_after_delete_keeps_non_matching_default() {
        let updated = default_org_after_delete(
            Some("team_other"),
            "team_old",
            Some("team_next".to_string()),
        );
        assert_eq!(updated.as_deref(), Some("team_other"));
    }

    #[test]
    fn default_org_after_delete_clears_when_no_next_org() {
        let updated = default_org_after_delete(Some("team_old"), "team_old", None);
        assert_eq!(updated, None);
    }
}
