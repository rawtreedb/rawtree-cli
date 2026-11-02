use std::process::Command;

fn help(command: &[&str], flag: &str) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_rtree"))
        .args(command)
        .arg(flag)
        .output()
        .unwrap();
    assert!(output.status.success(), "{command:?}: {output:?}");
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn every_sink_input_command_explains_all_types_in_its_own_help() {
    for command in [
        vec!["workflow", "create"],
        vec!["workflow", "update"],
        vec!["workflow", "sink", "create"],
        vec!["workflow", "sink", "update"],
    ] {
        for flag in ["--help", "-h"] {
            let text = help(&command, flag);
            for expected in [
                "table:",
                "http:",
                "slack:",
                "integration_id",
                "channel_id",
                "channel_name",
                "private Slack channels",
                "@<path>",
                "stdin",
                r#""settings":{"#,
            ] {
                assert!(
                    text.contains(expected),
                    "{command:?} {flag} lacks {expected}"
                );
            }
            assert!(!text.contains("sink schema"), "{text}");
        }
    }
}

#[test]
fn update_help_distinguishes_replacement_from_partial_changes() {
    for command in [
        vec!["workflow", "update"],
        vec!["workflow", "sink", "update"],
    ] {
        let text = help(&command, "--help");
        assert!(text.contains("omitted names are removed"), "{text}");
        assert!(text.contains("null keeps the stored value"), "{text}");
        assert!(text.contains("Use {} to remove all headers"), "{text}");
    }
    let workflow = help(&["workflow", "update"], "--help");
    assert!(workflow.contains("sinks left out are removed"));
    assert!(workflow.contains("table requires full settings"));
    assert!(workflow.contains("do not send both channel"));

    let sink = help(&["workflow", "sink", "update"], "--help");
    assert!(sink.contains("Other sinks remain unchanged"));
    assert!(sink.contains("Omit both channel fields to keep the channel"));
    assert!(sink.contains("Do not put id in the JSON"));
}

#[test]
fn sink_help_links_to_field_documentation_and_scopes_deletion() {
    let sink = help(&["workflow", "sink"], "--help");
    assert!(sink.contains("rtree workflow sink create --help"));
    assert!(sink.contains("rtree workflow sink update --help"));
    assert!(!sink.contains("schema"));

    let delete = help(&["workflow", "sink", "delete"], "--help");
    assert!(delete.contains("Remove only the selected sink"));
    assert!(delete.contains("other sinks remain unchanged"));
    assert!(delete.contains("--clear-sinks"));
}
