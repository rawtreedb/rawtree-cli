use serde::Serialize;
use serde_json::json;

/// Print a value: as JSON when json_mode is true, otherwise run the human formatter.
pub fn print_result<T: Serialize, F: FnOnce(&T)>(value: &T, json_mode: bool, human: F) {
    if json_mode {
        println!("{}", serde_json::to_string(value).unwrap());
    } else {
        human(value);
    }
}

/// Print an error. In JSON mode, outputs structured JSON to stderr.
/// Returns an appropriate exit code based on the error message.
pub fn print_error(err: &anyhow::Error, json_mode: bool) -> i32 {
    let msg = format!("{:#}", err);
    let code = exit_code_for(&msg);

    if json_mode {
        eprintln!("{}", json!({"error": msg, "exit_code": code}));
    } else {
        eprintln!("Error: {}", msg);
    }

    code
}

/// Map error messages to specific exit codes:
///   1 = authentication/authorization error
///   2 = validation/bad request error
///   3 = server/connection error
///   4 = not found
///   5 = general error
fn exit_code_for(msg: &str) -> i32 {
    if msg.contains("(401)") || msg.contains("(403)") || msg.contains("Not logged in") {
        1
    } else if msg.contains("(400)") || msg.contains("invalid") || msg.contains("required") {
        2
    } else if msg.contains("(404)") || msg.contains("not found") {
        4
    } else if msg.contains("(5") || msg.contains("failed to connect") {
        3
    } else {
        5
    }
}
