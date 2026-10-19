use clap::Args;

#[derive(Args, Clone, Debug, Default)]
pub(crate) struct ClusterIdentityArgs {
    /// Immutable External ID for IAM trust policies; API generates it for managed storage when omitted
    #[arg(long = "s3-storage-external-id", value_name = "ID")]
    pub(crate) external_id: Option<String>,
    /// Immutable rawtree.com/cluster AWS resource tag value; API generates it when omitted
    #[arg(long = "s3-storage-resource-tag", value_name = "TAG")]
    pub(crate) resource_tag: Option<String>,
}
