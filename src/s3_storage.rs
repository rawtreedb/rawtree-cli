use anyhow::Result;
use clap::Args;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Args, Clone, Debug, Default)]
pub(crate) struct S3StorageArgs {
    /// Customer-owned S3 data bucket; requires --s3-backup-bucket and --s3-role-arn
    #[arg(long, value_name = "BUCKET")]
    pub(crate) s3_data_bucket: Option<String>,
    /// Optional path prefix within the data bucket
    #[arg(long, value_name = "PATH")]
    pub(crate) s3_data_path: Option<String>,
    /// Customer-owned S3 bucket for backups
    #[arg(long, value_name = "BUCKET")]
    pub(crate) s3_backup_bucket: Option<String>,
    /// Optional path prefix within the backups bucket
    #[arg(long, value_name = "PATH")]
    pub(crate) s3_backup_path: Option<String>,
    /// IAM role ARN RawTree should assume
    #[arg(long, value_name = "ARN")]
    pub(crate) s3_role_arn: Option<String>,
}

impl S3StorageArgs {
    pub(crate) fn to_json(&self) -> Result<Option<Value>> {
        let configured = [
            &self.s3_data_bucket,
            &self.s3_data_path,
            &self.s3_backup_bucket,
            &self.s3_backup_path,
            &self.s3_role_arn,
        ]
        .iter()
        .any(|value| value.is_some());
        if !configured {
            return Ok(None);
        }

        let data_bucket = required_s3_value(&self.s3_data_bucket, "--s3-data-bucket")?;
        let backups_bucket = required_s3_value(&self.s3_backup_bucket, "--s3-backup-bucket")?;
        let role_arn = required_s3_value(&self.s3_role_arn, "--s3-role-arn")?;

        Ok(Some(json!({
            "data": { "bucket": data_bucket, "path": self.s3_data_path.as_deref().unwrap_or("") },
            "backups": { "bucket": backups_bucket, "path": self.s3_backup_path.as_deref().unwrap_or("") },
            "role_arn": role_arn,
        })))
    }
}

pub(crate) fn format_storage(
    storage: Option<&S3StorageMetadata>,
    default: &'static str,
) -> &'static str {
    if storage.is_some() {
        "customer-owned S3"
    } else {
        default
    }
}

fn required_s3_value<'a>(value: &'a Option<String>, flag: &str) -> Result<&'a str> {
    value.as_deref().map(str::trim).filter(|value| !value.is_empty()).ok_or_else(|| {
        anyhow::anyhow!("S3 storage is incomplete. Provide --s3-data-bucket, --s3-backup-bucket, and --s3-role-arn. Missing {flag}.")
    })
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
