use super::LiveCli;
use serde_json::Value;

#[test]
#[ignore = "requires a bootstrapped Platform stack with the new cluster identity API"]
fn cluster_identity_is_generated_persisted_and_read_back() {
    let cli = LiveCli::from_environment();
    let catalog = cli.json(&["cluster", "sizes"]);
    let size = &catalog["sizes"][0];
    let size = format!("{}:{}", size["cpu_cores"], size["memory_gib"]);
    let replicas = catalog["min_number_of_replicas"]
        .as_u64()
        .expect("replica limit")
        .to_string();
    let name = format!("cli-identity-{}", uuid::Uuid::new_v4().simple());
    let created = cli.json(&[
        "cluster",
        "create",
        "--name",
        &name,
        "--replicas",
        &replicas,
        "--min-size",
        &size,
    ]);
    // Capture readback before cleanup, but assert afterward so failed assertions do not leak a cluster.
    let readback = cli.execute(&["cluster", "status", &name], Some(&cli.token));
    cli.json(&["cluster", "delete", &name]);
    assert!(readback.status.success(), "cluster status failed");
    let readback: Value = serde_json::from_slice(&readback.stdout).expect("status JSON");
    for field in ["external_id", "resource_tag"] {
        let value = created[field].as_str().expect("saved identity");
        let id =
            uuid::Uuid::parse_str(value.strip_prefix("rawtree-").expect("prefix")).expect("UUID");
        assert_eq!(id.get_version_num(), 4);
        assert_eq!(readback[field], created[field]);
    }
    assert_ne!(created["external_id"], created["resource_tag"]);
    assert!(created["s3_storage"].is_null());
}
