use serde::Serialize;
use serde_json::json;
use std::fmt;

#[derive(Debug)]
pub struct CliError {
    code: &'static str,
    message: String,
    exit_code: i32,
}

impl CliError {
    pub fn new(code: &'static str, message: impl Into<String>, exit_code: i32) -> Self {
        Self {
            code,
            message: message.into(),
            exit_code,
        }
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn exit_code(&self) -> i32 {
        self.exit_code
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for CliError {}

pub fn coded_error(
    code: &'static str,
    message: impl Into<String>,
    exit_code: i32,
) -> anyhow::Error {
    CliError::new(code, message, exit_code).into()
}

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
    if let Some(cli_err) = err.downcast_ref::<CliError>() {
        if json_mode {
            eprintln!(
                "{}",
                json!({"error": {"message": cli_err.message(), "code": cli_err.code()}})
            );
        } else {
            eprintln!("Error: {}", cli_err.message());
        }
        return cli_err.exit_code();
    }

    let msg = format!("{:#}", err);
    let code = exit_code_for(&msg);

    if json_mode {
        eprintln!(
            "{}",
            json!({
                "error": {
                    "message": msg,
                    "code": error_code_for(code),
                },
                "exit_code": code
            })
        );
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

fn error_code_for(exit_code: i32) -> &'static str {
    match exit_code {
        1 => "auth_error",
        2 => "validation_error",
        3 => "server_error",
        4 => "not_found",
        _ => "general_error",
    }
}
