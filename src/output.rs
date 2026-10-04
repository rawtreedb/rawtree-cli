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

#[derive(Debug, Serialize)]
#[serde(tag = "needs", rename_all = "lowercase")]
pub enum SelectionRequired {
    Org {
        orgs: Vec<String>,
    },
    Cluster {
        organization: String,
        clusters: Vec<String>,
    },
    Database,
}

impl fmt::Display for SelectionRequired {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::Org { .. } => "Select an organization. List choices with `rtree organization list`. Save a default with `rtree organization use <name>`, or pass --org <name>.",
            Self::Cluster { .. } => "Select a cluster. List choices with `rtree cluster list`. Save a default with `rtree cluster use <name>`, or pass --cluster <name>.",
            Self::Database => "Select a database. List choices with `rtree database list`. Save a default with `rtree database use <name>`, or pass --database <name>.",
        };
        f.write_str(message)
    }
}

impl std::error::Error for SelectionRequired {}

#[derive(Debug)]
pub struct AuthenticationSavedError(pub anyhow::Error);

impl fmt::Display for AuthenticationSavedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "The CLI saved authentication. {:#}", self.0)
    }
}

impl std::error::Error for AuthenticationSavedError {}

fn error_result(err: &anyhow::Error) -> (serde_json::Value, i32) {
    if let Some(saved) = err.downcast_ref::<AuthenticationSavedError>() {
        let (mut result, code) = error_result(&saved.0);
        result["authentication_saved"] = json!(true);
        result["hint"] = json!("The CLI saved authentication. Use the resource commands to select defaults without another login.");
        return (result, code);
    }
    if let Some(selection) = err.downcast_ref::<SelectionRequired>() {
        let mut result = serde_json::to_value(selection).unwrap();
        result["error"] = json!({"code": "selection_required", "message": selection.to_string()});
        result["exit_code"] = json!(2);
        return (result, 2);
    }
    if let Some(cli_err) = err.downcast_ref::<CliError>() {
        return (
            json!({"error": {"message": cli_err.message(), "code": cli_err.code()}}),
            cli_err.exit_code(),
        );
    }
    let msg = format!("{:#}", err);
    let code = exit_code_for(&msg);
    (
        json!({"error": {"message": msg, "code": error_code_for(code)}, "exit_code": code}),
        code,
    )
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
    let (result, code) = error_result(err);
    if json_mode {
        eprintln!("{}", result);
    } else {
        eprintln!("Error: {:#}", err);
        if let Some(hint) = result.get("hint").and_then(|value| value.as_str()) {
            eprintln!("{}", hint);
        }
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
