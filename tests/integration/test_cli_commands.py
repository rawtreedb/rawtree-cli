"""CLI end-to-end integration tests against a live backend."""

import json

from conftest import assert_ok, parse_stdout_json


def test_register_then_status_and_logout(rtree, id_suffix, base_url):
    email = f"cli_register_{id_suffix}@example.com"
    password = "securepassword123"

    register = rtree("register", "--email", email, "--password", password, "--json")
    assert_ok(register)
    register_payload = parse_stdout_json(register)
    assert register_payload["status"] == "registered"
    assert register_payload["email"] == email

    status_logged_in = rtree("status", "--json")
    assert_ok(status_logged_in)
    status_payload = parse_stdout_json(status_logged_in)
    assert status_payload["authenticated"] is True
    assert status_payload["user"] == email
    assert status_payload["api_url"] == base_url

    logout = rtree("logout", "--json")
    assert_ok(logout)

    status_logged_out = rtree("status", "--json")
    assert_ok(status_logged_out)
    after_logout = parse_stdout_json(status_logged_out)
    assert after_logout["authenticated"] is False


def test_login_wrong_password_fails(rtree, registered_user):
    result = rtree(
        "login",
        "--email",
        registered_user["email"],
        "--password",
        "wrongpassword123",
        "--json",
    )
    assert result.returncode != 0
    assert result.stderr.strip(), "expected JSON error payload on stderr"
    err = json.loads(result.stderr)
    assert err["exit_code"] == 1
    assert "401" in err["error"]


def test_ping_and_docs_commands(rtree):
    ping = rtree("ping", "--json")
    assert_ok(ping)
    ping_payload = parse_stdout_json(ping)
    assert ping_payload["status"] == "ok"

    docs = rtree("docs")
    assert_ok(docs)
    assert "openapi" in docs.stdout.lower()


def test_project_insert_query_table_flow(rtree, project_name):
    inserted = rtree(
        "insert",
        "--table",
        "events",
        "--data",
        '{"action":"signup","user":"alice","value":42}',
        "--json",
    )
    assert_ok(inserted)
    insert_payload = parse_stdout_json(inserted)
    assert insert_payload["inserted"] == 1

    query = rtree("query", "SELECT count() AS cnt FROM events", "--json")
    assert_ok(query)
    query_payload = parse_stdout_json(query)
    assert query_payload["rows"] == 1
    assert query_payload["data"][0]["cnt"] >= 1

    table_list = rtree("table", "list", "--json")
    assert_ok(table_list)
    tables_payload = parse_stdout_json(table_list)
    assert any(table["name"] == "events" for table in tables_payload["tables"])

    describe = rtree("table", "describe", "events", "--json")
    assert_ok(describe)
    describe_payload = parse_stdout_json(describe)
    assert describe_payload["table"] == "events"
    assert any(col["name"] == "action" for col in describe_payload["columns"])

    # Guard that project context is actually active by querying with an explicit project too.
    query_project = rtree("query", "SELECT 1 AS value", "--project", project_name, "--json")
    assert_ok(query_project)
    explicit_query_payload = parse_stdout_json(query_project)
    assert explicit_query_payload["rows"] == 1


def test_query_via_stdin(rtree, project_name):
    seed = rtree(
        "insert",
        "--table",
        "stdin_events",
        "--data",
        '{"kind":"stdin_test"}',
        "--project",
        project_name,
        "--json",
    )
    assert_ok(seed)

    result = rtree(
        "query",
        "-",
        "--project",
        project_name,
        "--json",
        input_text="SELECT count() AS cnt FROM stdin_events",
    )
    assert_ok(result)
    payload = parse_stdout_json(result)
    assert payload["rows"] == 1
    assert payload["data"][0]["cnt"] >= 1


def test_keys_create_list_delete(rtree, project_name, id_suffix):
    label = f"it-key-{id_suffix}"
    create = rtree(
        "keys",
        "create",
        "--project",
        project_name,
        "--label",
        label,
        "--permission",
        "read_only",
        "--json",
    )
    assert_ok(create)
    create_payload = parse_stdout_json(create)
    key_id = create_payload["key_id"]
    assert create_payload["label"] == label
    assert create_payload["permission"] == "read_only"
    assert create_payload["api_key"]

    list_before_delete = rtree("keys", "list", "--project", project_name, "--json")
    assert_ok(list_before_delete)
    list_payload = parse_stdout_json(list_before_delete)
    assert any(item["key_id"] == key_id for item in list_payload["keys"])

    delete = rtree("keys", "delete", key_id, "--project", project_name, "--json")
    assert_ok(delete)
    delete_payload = parse_stdout_json(delete)
    assert delete_payload["deleted"] is True


def test_insert_with_transform(rtree, project_name, id_suffix):
    table = f"otlp_traces_{id_suffix}"
    payload = {
        "resource": {
            "attributes": [
                {"key": "service.name", "value": {"stringValue": "my-service"}},
            ]
        },
        "scopeSpans": [
            {
                "scope": {"name": "my-scope"},
                "spans": [
                    {"spanId": "aaa", "name": "op1", "kind": 1},
                    {"spanId": "bbb", "name": "op2", "kind": 2},
                ],
            }
        ],
    }

    insert = rtree(
        "insert",
        "--project",
        project_name,
        "--table",
        table,
        "--data",
        json.dumps(payload),
        "--transform",
        "otlp-traces",
        "--json",
    )
    assert_ok(insert)
    insert_payload = parse_stdout_json(insert)
    assert insert_payload["inserted"] == 2

    query = rtree(
        "query",
        "--project",
        project_name,
        "--json",
        f"SELECT count() AS cnt FROM {table}",
    )
    assert_ok(query)
    query_payload = parse_stdout_json(query)
    assert query_payload["data"][0]["cnt"] == 2
