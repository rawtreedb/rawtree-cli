use super::{Cli, ClusterCommand, ClusterSizeArg, Command, KeyCommand, TableCommand};
use clap::{error::ErrorKind, CommandFactory, Parser};

#[test]
fn root_command_exposes_version_flag() {
    let command = Cli::command();
    assert_eq!(command.get_version(), Some(env!("CARGO_PKG_VERSION")));
}

#[test]
fn table_sorting_key_accepts_one_sql_expression_string() {
    let expression = "region, ifNull(cityHash64(host, instanceId), 0)";
    let cli = Cli::try_parse_from([
        "rtree",
        "table",
        "create",
        "events",
        "--database",
        "analytics",
        "--sorting-key",
        expression,
    ])
    .expect("table create should parse");
    assert!(matches!(
        cli.command,
        Command::Table {
            action: TableCommand::Create { sorting_key: Some(key), .. }
        } if key == expression
    ));

    let cli = Cli::try_parse_from([
        "rtree",
        "table",
        "update",
        "events",
        "--sorting-key",
        expression,
    ])
    .expect("table update should parse");
    assert!(matches!(
        cli.command,
        Command::Table {
            action: TableCommand::Update { sorting_key, .. }
        } if sorting_key == expression
    ));

    let err = Cli::try_parse_from(["rtree", "table", "update", "events"])
        .err()
        .expect("update requires a sorting key");
    assert_eq!(err.kind(), ErrorKind::MissingRequiredArgument);
}

#[test]
fn lowercase_v_triggers_version_output() {
    let err = match Cli::try_parse_from(["rtree", "-v"]) {
        Ok(_) => panic!("-v should print version"),
        Err(err) => err,
    };
    assert_eq!(err.kind(), ErrorKind::DisplayVersion);
    assert!(err.to_string().contains(env!("CARGO_PKG_VERSION")));
}

#[test]
fn login_without_api_key_is_allowed_for_interactive_flow() {
    let cli = Cli::try_parse_from(["rtree", "login"]).expect("login should parse");
    assert!(cli.api_key.is_none());
}

#[test]
fn login_with_api_key_parses() {
    let cli = Cli::try_parse_from(["rtree", "login", "--api-key", "rt_abc123"])
        .expect("login with --api-key should parse");
    assert_eq!(cli.api_key.as_deref(), Some("rt_abc123"));
}

#[test]
fn login_help_hides_internal_email_and_password_flags() {
    let err = match Cli::try_parse_from(["rtree", "login", "--help"]) {
        Ok(_) => panic!("--help should return display output"),
        Err(err) => err,
    };
    assert_eq!(err.kind(), ErrorKind::DisplayHelp);
    let help = err.to_string();
    assert!(!help.contains("--email"));
    assert!(!help.contains("--password"));
    assert!(!help.to_ascii_lowercase().contains("email"));
}

#[test]
fn global_api_key_parses_before_subcommand() {
    let cli = Cli::try_parse_from(["rtree", "--api-key", "rt_abc123", "database", "list"])
        .expect("global --api-key should parse before subcommand");
    assert_eq!(cli.api_key.as_deref(), Some("rt_abc123"));
}

#[test]
fn cluster_list_parses() {
    let cli = Cli::try_parse_from(["rtree", "cluster", "list"]).expect("cluster list should parse");
    assert!(matches!(
        cli.command,
        Command::Cluster {
            action: ClusterCommand::List
        }
    ));
}

#[test]
fn cluster_sizes_parses() {
    let cli =
        Cli::try_parse_from(["rtree", "cluster", "sizes"]).expect("cluster sizes should parse");
    assert!(matches!(
        cli.command,
        Command::Cluster {
            action: ClusterCommand::Sizes
        }
    ));
}

#[test]
fn cluster_create_parses_explicit_name_and_idle_timeout() {
    let cli = Cli::try_parse_from([
        "rtree",
        "cluster",
        "create",
        "--name",
        "production",
        "--replicas",
        "2",
        "--min-size",
        "2:8",
        "--max-size",
        "64:256",
        "--idle-timeout-minutes",
        "30",
    ])
    .expect("cluster create should parse");
    assert!(matches!(
        cli.command,
        Command::Cluster {
            action: ClusterCommand::Create {
                name,
                replicas,
                idle_timeout_minutes: Some(30),
                min_size,
                max_size: Some(max_size),
                ..
            }
        } if name == "production"
            && replicas == 2
            && min_size == ClusterSizeArg { cpu_cores: 2, memory_gib: 8 }
            && max_size == ClusterSizeArg { cpu_cores: 64, memory_gib: 256 }
    ));
}

#[test]
fn cluster_create_rejects_invalid_size_format() {
    let error = match Cli::try_parse_from([
        "rtree",
        "cluster",
        "create",
        "--name",
        "production",
        "--replicas",
        "2",
        "--min-size",
        "2x8",
    ]) {
        Ok(_) => panic!("cluster create should reject an invalid size"),
        Err(error) => error,
    };

    let message = error.to_string();
    assert!(message.contains("CPU_CORES:MEMORY_GIB"));
    assert!(message.contains("2:8"));
}

#[test]
fn cluster_update_parses_partial_settings() {
    let cli = Cli::try_parse_from([
        "rtree",
        "cluster",
        "update",
        "production",
        "--idle-timeout-minutes",
        "0",
    ])
    .expect("cluster update should parse");
    assert!(matches!(
        cli.command,
        Command::Cluster {
            action: ClusterCommand::Update {
                name_or_id,
                name: None,
                idle_timeout_minutes: Some(0),
            }
        } if name_or_id == "production"
    ));
}

#[test]
fn cluster_use_parses() {
    let cli = Cli::try_parse_from(["rtree", "cluster", "use", "production"])
        .expect("cluster use should parse");

    match cli.command {
        Command::Cluster {
            action: ClusterCommand::Use { name },
        } => assert_eq!(name, "production"),
        _ => panic!("expected cluster use command"),
    }
}

#[test]
fn cluster_lifecycle_commands_parse() {
    let status = Cli::try_parse_from(["rtree", "cluster", "status", "production"])
        .expect("cluster status should parse");
    assert!(matches!(
        status.command,
        Command::Cluster {
            action: ClusterCommand::Status { name_or_id }
        } if name_or_id == "production"
    ));

    let stop = Cli::try_parse_from(["rtree", "cluster", "stop", "production"])
        .expect("cluster stop should parse");
    assert!(matches!(
        stop.command,
        Command::Cluster {
            action: ClusterCommand::Stop { name_or_id }
        } if name_or_id == "production"
    ));

    let resume = Cli::try_parse_from(["rtree", "cluster", "resume", "production"])
        .expect("cluster resume should parse");
    assert!(matches!(
        resume.command,
        Command::Cluster {
            action: ClusterCommand::Resume { name_or_id }
        } if name_or_id == "production"
    ));

    let delete = Cli::try_parse_from(["rtree", "cluster", "delete", "production"])
        .expect("cluster delete should parse");
    assert!(matches!(
        delete.command,
        Command::Cluster {
            action: ClusterCommand::Delete { name_or_id }
        } if name_or_id == "production"
    ));
}

#[test]
fn login_with_token_flag_is_rejected() {
    let result = Cli::try_parse_from(["rtree", "login", "--token", "rt_abc123"]);
    assert!(result.is_err(), "--token should not be accepted");
}

#[test]
fn login_with_password_requires_email() {
    let result = Cli::try_parse_from(["rtree", "login", "--password", "secret123"]);
    assert!(result.is_err(), "password without email should fail");
}

#[test]
fn login_with_api_key_conflicts_with_email() {
    let cli = Cli::try_parse_from([
        "rtree",
        "login",
        "--api-key",
        "rt_abc123",
        "--email",
        "user@example.com",
    ])
    .expect("global --api-key is parsed before runtime login validation");
    assert_eq!(cli.api_key.as_deref(), Some("rt_abc123"));
}

#[test]
fn login_with_database_without_email_is_allowed_for_interactive_flow() {
    let cli = Cli::try_parse_from(["rtree", "login", "--database", "analytics"])
        .expect("login with --database should parse");
    match cli.command {
        Command::Login {
            email, database, ..
        } => {
            assert!(email.is_none());
            assert_eq!(database.as_deref(), Some("analytics"));
        }
        _ => panic!("expected login command"),
    }
}

#[test]
fn register_command_is_rejected() {
    let result = Cli::try_parse_from([
        "rtree",
        "register",
        "--email",
        "user@example.com",
        "--password",
        "secret123",
    ]);
    assert!(result.is_err(), "register command should not be accepted");
}

#[test]
fn insert_with_url_is_allowed() {
    let cli = Cli::try_parse_from([
        "rtree",
        "insert",
        "--database",
        "analytics",
        "--table",
        "events",
        "--url",
        "https://example.com/events.jsonl",
    ])
    .expect("insert --url should parse");

    match cli.command {
        Command::Insert { url, .. } => {
            assert_eq!(url.as_deref(), Some("https://example.com/events.jsonl"))
        }
        _ => panic!("expected insert command"),
    }
}

#[test]
fn insert_url_conflicts_with_data() {
    let result = Cli::try_parse_from([
        "rtree",
        "insert",
        "--database",
        "analytics",
        "--table",
        "events",
        "--url",
        "https://example.com/events.jsonl",
        "--data",
        r#"{"id":1}"#,
    ]);
    assert!(result.is_err(), "insert --url should conflict with --data");
}

#[test]
fn api_url_and_insert_url_can_both_be_provided() {
    let cli = Cli::try_parse_from([
        "rtree",
        "--api-url",
        "https://api.rawtree.com",
        "insert",
        "--database",
        "analytics",
        "--table",
        "events",
        "--url",
        "https://example.com/events.jsonl",
    ])
    .expect("--api-url and insert --url should parse");

    assert_eq!(cli.api_url.as_deref(), Some("https://api.rawtree.com"));
    match cli.command {
        Command::Insert { url, .. } => {
            assert_eq!(url.as_deref(), Some("https://example.com/events.jsonl"))
        }
        _ => panic!("expected insert command"),
    }
}

#[test]
fn api_url_can_be_passed_before_subcommand() {
    let cli = Cli::try_parse_from([
        "rtree",
        "--api-url",
        "https://api.rawtree.com",
        "query",
        "--database",
        "analytics",
        "--sql",
        "SELECT 1",
    ])
    .expect("--api-url should parse before subcommand");

    assert_eq!(cli.api_url.as_deref(), Some("https://api.rawtree.com"));
}

#[test]
fn cluster_can_be_passed_after_subcommand() {
    let cli = Cli::try_parse_from([
        "rtree",
        "query",
        "--cluster",
        "production",
        "--database",
        "analytics",
        "--sql",
        "SELECT 1",
    ])
    .expect("global --cluster should parse after the subcommand");

    assert_eq!(cli.cluster.as_deref(), Some("production"));
}

#[test]
fn query_format_flag_is_rejected() {
    let result = Cli::try_parse_from([
        "rtree",
        "query",
        "--database",
        "analytics",
        "--sql",
        "SELECT 1",
        "--format",
        "csv",
    ]);
    assert!(result.is_err(), "query --format should not be supported");
}

#[test]
fn query_named_query_flag_is_rejected() {
    let result = Cli::try_parse_from([
        "rtree",
        "query",
        "--database",
        "analytics",
        "--query",
        "SELECT 1",
    ]);
    assert!(result.is_err(), "query --query should not be supported");
}

#[test]
fn key_command_is_singular() {
    let cli = Cli::try_parse_from(["rtree", "key", "list"]).unwrap();
    assert!(matches!(
        cli.command,
        Command::Key {
            action: KeyCommand::List
        }
    ));
}

#[test]
fn cluster_scoped_commands_reject_obsolete_database_flags() {
    for args in [
        vec!["rtree", "key", "list", "--database", "analytics"],
        vec![
            "rtree",
            "key",
            "delete",
            "key-id",
            "--database",
            "analytics",
        ],
        vec!["rtree", "logs", "--database", "analytics"],
        vec!["rtree", "logs", "--log-databases", "analytics"],
    ] {
        assert_eq!(
            Cli::try_parse_from(args)
                .err()
                .expect("obsolete flag must fail")
                .kind(),
            ErrorKind::UnknownArgument
        );
    }
}

#[test]
fn keys_command_is_rejected() {
    let result = Cli::try_parse_from(["rtree", "keys", "list", "--database", "analytics"]);
    assert!(result.is_err(), "keys should not be accepted as a command");
}

#[test]
fn key_create_uses_name_flag() {
    let cli = Cli::try_parse_from([
        "rtree",
        "key",
        "create",
        "--database",
        "analytics",
        "--name",
        "ci",
        "--permission",
        "read_write",
    ])
    .expect("key create with --name should parse");

    match cli.command {
        Command::Key { action } => match action {
            KeyCommand::Create { name, .. } => {
                assert_eq!(name, "ci");
            }
            _ => panic!("expected key create command"),
        },
        _ => panic!("expected key command"),
    }
}

#[test]
fn key_create_label_flag_is_rejected() {
    let result = Cli::try_parse_from([
        "rtree",
        "key",
        "create",
        "--database",
        "analytics",
        "--label",
        "ci",
        "--permission",
        "read_write",
    ]);
    assert!(result.is_err(), "key create should use --name, not --label");
}

#[test]
fn logs_request_filters_parse() {
    let cli = Cli::try_parse_from([
        "rtree",
        "logs",
        "--search",
        "request-123",
        "--methods",
        "GET,POST",
        "--status-codes",
        "200,500",
        "--levels",
        "success,error",
        "--sources",
        "cli",
        "--paths",
        "/v1/query,/v1/logs",
    ])
    .expect("request log filters should parse");

    match cli.command {
        Command::Logs {
            search,
            methods,
            status_codes,
            levels,
            sources,
            paths,
            ..
        } => {
            assert_eq!(search.as_deref(), Some("request-123"));
            assert_eq!(methods, vec!["GET", "POST"]);
            assert_eq!(status_codes, vec![200, 500]);
            assert_eq!(levels, vec!["success", "error"]);
            assert_eq!(sources, vec!["cli"]);
            assert_eq!(paths, vec!["/v1/query", "/v1/logs"]);
        }
        _ => panic!("expected logs command"),
    }
}

#[test]
fn logs_status_codes_must_be_http_statuses() {
    let result = Cli::try_parse_from(["rtree", "logs", "--status-codes", "99"]);
    assert!(result.is_err(), "status codes below 100 should be rejected");
}
