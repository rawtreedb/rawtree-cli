use anyhow::Result;
use clap::Args;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Args, Clone, Debug, Default)]
pub(crate) struct S3StorageArgs {
    /// Customer-owned S3 bucket for database data
    #[arg(long, value_name = "BUCKET")]
    pub(crate) s3_data_bucket: Option<String>,
    /// Optional path prefix within the data bucket
    #[arg(long, value_name = "PATH")]
    pub(crate) s3_data_path: Option<String>,
    /// Customer-owned S3 bucket for backups
    #[arg(long, value_name = "BUCKET")]
    pub(crate) s3_backups_bucket: Option<String>,
    /// Optional path prefix within the backups bucket
    #[arg(long, value_name = "PATH")]
    pub(crate) s3_backups_path: Option<String>,
    /// IAM role ARN RawTree should assume
    #[arg(long, value_name = "ARN")]
    pub(crate) s3_role_arn: Option<String>,
    /// External ID configured in the customer IAM role trust policy
    #[arg(long, value_name = "ID")]
    pub(crate) s3_external_id: Option<String>,
}

#[derive(Args, Clone, Debug, Default)]
pub(crate) struct DatabaseS3AccessArgs {
    /// External ID configured in the customer IAM role trust policy for database buckets
    #[arg(long, value_name = "ID")]
    pub(crate) database_s3_external_id: Option<String>,
    /// Immutable tag required on customer-owned database buckets and IAM roles
    #[arg(long, value_name = "TAG")]
    pub(crate) database_bucket_tag: Option<String>,
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct S3StorageMetadata {
    pub(crate) data: S3StorageDestination,
    pub(crate) backups: S3StorageDestination,
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct S3StorageDestination {
    pub(crate) bucket: String,
    pub(crate) path: String,
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct DatabaseS3AccessMetadata {
    pub(crate) external_id: String,
    pub(crate) database_bucket_tag: String,
}

impl S3StorageArgs {
    pub(crate) fn to_json(&self) -> Result<Option<Value>> {
        let configured = [
            &self.s3_data_bucket,
            &self.s3_data_path,
            &self.s3_backups_bucket,
            &self.s3_backups_path,
            &self.s3_role_arn,
            &self.s3_external_id,
        ]
        .iter()
        .any(|value| value.is_some());
        if !configured {
            return Ok(None);
        }

        let data_bucket = required_s3_value(&self.s3_data_bucket, "--s3-data-bucket")?;
        let backups_bucket = required_s3_value(&self.s3_backups_bucket, "--s3-backups-bucket")?;
        let role_arn = required_s3_value(&self.s3_role_arn, "--s3-role-arn")?;
        let external_id = required_s3_value(&self.s3_external_id, "--s3-external-id")?;

        Ok(Some(json!({
            "data": {
                "bucket": data_bucket,
                "path": self.s3_data_path.as_deref().unwrap_or(""),
            },
            "backups": {
                "bucket": backups_bucket,
                "path": self.s3_backups_path.as_deref().unwrap_or(""),
            },
            "role_arn": role_arn,
            "external_id": external_id,
        })))
    }
}

impl DatabaseS3AccessArgs {
    pub(crate) fn to_json(&self) -> Result<Option<Value>> {
        let configured =
            self.database_s3_external_id.is_some() || self.database_bucket_tag.is_some();
        if !configured {
            return Ok(None);
        }

        let external_id = self
            .database_s3_external_id
            .as_deref()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Database S3 access is incomplete. Provide --database-s3-external-id and --database-bucket-tag. Missing --database-s3-external-id."
                )
            })?;
        let database_bucket_tag = self.database_bucket_tag.as_deref().ok_or_else(|| {
            anyhow::anyhow!(
                "Database S3 access is incomplete. Provide --database-s3-external-id and --database-bucket-tag. Missing --database-bucket-tag."
            )
        })?;
        validate_database_s3_external_id(external_id)?;
        validate_database_bucket_tag(database_bucket_tag)?;

        Ok(Some(json!({
            "external_id": external_id,
            "database_bucket_tag": database_bucket_tag,
        })))
    }
}

fn validate_database_s3_external_id(value: &str) -> Result<()> {
    let value = value.trim();
    if !(2..=1224).contains(&value.len())
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'_' | b'+' | b'=' | b',' | b'.' | b'@' | b':' | b'/' | b'-'
                )
        })
    {
        anyhow::bail!(
            "Invalid database S3 access External ID. Use 2-1224 letters, numbers, or the characters _ + = , . @ : / -."
        );
    }
    Ok(())
}

fn validate_database_bucket_tag(value: &str) -> Result<()> {
    let value = value.trim();
    if value.is_empty()
        || value.len() > 256
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        || !value
            .as_bytes()
            .first()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
    {
        anyhow::bail!(
            "Invalid database S3 bucket tag. Use 1-256 lowercase letters, numbers, or hyphens, starting with a letter or number."
        );
    }
    Ok(())
}

fn required_s3_value<'a>(value: &'a Option<String>, flag: &str) -> Result<&'a str> {
    value.as_deref().ok_or_else(|| {
        anyhow::anyhow!(
            "S3 storage is incomplete. Provide --s3-data-bucket, --s3-backups-bucket, --s3-role-arn, and --s3-external-id. Missing {flag}."
        )
    })
}

pub(crate) fn validate_matching_s3_external_ids(
    s3_storage: &S3StorageArgs,
    database_s3_access: &DatabaseS3AccessArgs,
) -> Result<()> {
    if let (Some(storage_external_id), Some(database_external_id)) = (
        s3_storage.s3_external_id.as_deref(),
        database_s3_access.database_s3_external_id.as_deref(),
    ) {
        if storage_external_id.trim() != database_external_id.trim() {
            anyhow::bail!(
                "S3 External IDs do not match. Use the same value for --s3-external-id and --database-s3-external-id."
            );
        }
    }
    Ok(())
}

pub(crate) fn format_storage(storage: Option<&S3StorageMetadata>) -> &'static str {
    if storage.is_some() {
        "customer-owned S3"
    } else {
        "cluster default"
    }
}

pub(crate) fn format_database_s3_access(access: Option<&DatabaseS3AccessMetadata>) -> String {
    access
        .map(|access| format!("configured ({})", access.database_bucket_tag))
        .unwrap_or_else(|| "not configured".to_string())
}

#[cfg(test)]
mod tests {
    use super::{DatabaseS3AccessArgs, S3StorageArgs};
    use crate::cli::{Cli, ClusterCommand, Command};
    use clap::Parser;

    #[test]
    fn cluster_create_parses_s3_storage_options() {
        let cli = Cli::try_parse_from([
            "rtree",
            "cluster",
            "create",
            "--name",
            "production",
            "--replicas",
            "1",
            "--min-size",
            "2:8",
            "--s3-data-bucket",
            "customer-data",
            "--s3-data-path",
            "rawtree/data",
            "--s3-backups-bucket",
            "customer-backups",
            "--s3-backups-path",
            "rawtree/backups",
            "--s3-role-arn",
            "arn:aws:iam::123456789012:role/RawTreeS3Access",
            "--s3-external-id",
            "rawtree-example",
        ])
        .expect("cluster create S3 options should parse");

        let Command::Cluster {
            action: ClusterCommand::Create { s3_storage, .. },
        } = cli.command
        else {
            panic!("expected cluster create command");
        };

        assert_eq!(
            s3_storage
                .to_json()
                .expect("complete S3 options should be valid"),
            Some(serde_json::json!({
                "data": {"bucket": "customer-data", "path": "rawtree/data"},
                "backups": {"bucket": "customer-backups", "path": "rawtree/backups"},
                "role_arn": "arn:aws:iam::123456789012:role/RawTreeS3Access",
                "external_id": "rawtree-example"
            }))
        );
    }

    #[test]
    fn partial_s3_storage_options_are_rejected_before_request() {
        let args = S3StorageArgs {
            s3_data_bucket: Some("customer-data".to_string()),
            ..S3StorageArgs::default()
        };

        let error = args
            .to_json()
            .expect_err("partial S3 options should be rejected");
        assert!(error.to_string().contains("--s3-backups-bucket"));
    }

    #[test]
    fn cluster_create_parses_independent_database_s3_access_options() {
        let cli = Cli::try_parse_from([
            "rtree",
            "cluster",
            "create",
            "--name",
            "production",
            "--replicas",
            "1",
            "--min-size",
            "2:8",
            "--database-s3-external-id",
            "rawtree-database-access",
            "--database-bucket-tag",
            "rawtree-customer-database",
        ])
        .expect("cluster create database S3 options should parse");

        let Command::Cluster {
            action:
                ClusterCommand::Create {
                    database_s3_access,
                    s3_storage,
                    ..
                },
        } = cli.command
        else {
            panic!("expected cluster create command");
        };

        assert_eq!(
            s3_storage.to_json().expect("storage should be omitted"),
            None
        );
        assert_eq!(
            database_s3_access
                .to_json()
                .expect("complete database S3 access should be valid"),
            Some(serde_json::json!({
                "external_id": "rawtree-database-access",
                "database_bucket_tag": "rawtree-customer-database"
            }))
        );
    }

    #[test]
    fn partial_database_s3_access_options_are_rejected_before_request() {
        let args = DatabaseS3AccessArgs {
            database_s3_external_id: Some("rawtree-database-access".to_string()),
            ..DatabaseS3AccessArgs::default()
        };

        let error = args
            .to_json()
            .expect_err("partial database S3 access should be rejected");
        assert!(error.to_string().contains("--database-bucket-tag"));
    }

    #[test]
    fn invalid_database_s3_access_values_are_rejected_before_request() {
        let invalid_external_id = DatabaseS3AccessArgs {
            database_s3_external_id: Some("?".to_string()),
            database_bucket_tag: Some("rawtree-customer-database".to_string()),
        };
        assert!(invalid_external_id.to_json().is_err());

        let invalid_bucket_tag = DatabaseS3AccessArgs {
            database_s3_external_id: Some("rawtree-database-access".to_string()),
            database_bucket_tag: Some("RawTree_Invalid".to_string()),
        };
        assert!(invalid_bucket_tag.to_json().is_err());
    }
}
