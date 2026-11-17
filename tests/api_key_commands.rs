mod common;

use serde_json::json;

#[test]
fn delete_rejects_tokens_and_malformed_ids_before_authentication() {
    for id in [
        "rt_secret_token",
        "key-1",
        "",
        "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
    ] {
        let (output, config, bodies) = common::run_cli(&[], &["api-key", "delete", id], &json!({}));
        assert_eq!(output.status.code(), Some(2));
        let error = String::from_utf8(output.stderr).unwrap();
        assert!(error.contains("invalid value"), "{error}");
        assert!(error.contains("<ID>"), "{error}");
        assert!(output.stdout.is_empty());
        assert_eq!(config, json!({}));
        assert!(bodies.is_empty());
    }
}

#[test]
fn delete_help_documents_uuid_input() {
    let (output, _, _) = common::run_cli(&[], &["api-key", "delete", "--help"], &json!({}));
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    assert!(
        help.contains("rtree api-key delete [OPTIONS] <ID>"),
        "{help}"
    );
    assert!(help.contains("API key UUID to delete"), "{help}");
    assert!(!help.contains("ID_OR_TOKEN"), "{help}");
}
