use std::sync::Arc;

use duckdb::*;
use rust_extensions::TaskCompletion;

use crate::app_ctx::AppContext;

pub async fn execute(app: &Arc<AppContext>, sql: String) -> Result<usize, String> {
    let app = app.clone();

    let mut result = TaskCompletion::new();

    let awaiter = result.get_awaiter();
    std::thread::spawn(move || {
        let conn = app.connection.lock().unwrap();

        let execute_result = match conn.execute(&sql, []) {
            Ok(execute_result) => execute_result,
            Err(err) => {
                result.set_error(format!("Error executing statement. {:?}", err));
                return;
            }
        };

        result.set_ok(execute_result);
    });

    awaiter.get_result().await
}

pub struct DuckDbRow {
    pub columns: Vec<DuckDbValue>,
}

impl DuckDbRow {
    pub fn new(row: &Row) -> Self {
        let mut columns = Vec::new();
        for i in 0..999 {
            println!("{:?}", row.get_ref(i));
            match row.get_ref(i) {
                Ok(value) => match value.as_str() {
                    Ok(value) => {
                        columns.push(DuckDbValue::from_str(value));
                    }
                    Err(value) => {
                        columns.push(DuckDbValue::String(format!("{:?}", value)));
                    }
                },
                Err(_) => break,
            }
        }

        Self { columns }
    }
}

pub enum DuckDbValue {
    Null,
    Undefined,
    NaN,
    String(String),
    Number(i64),
    Double(f64),
    Bool(bool),
}

impl DuckDbValue {
    pub fn from_str(value: &str) -> Self {
        if value.eq_ignore_ascii_case("true") {
            return Self::Bool(true);
        }

        if value.eq_ignore_ascii_case("false") {
            return Self::Bool(false);
        }

        if value.eq_ignore_ascii_case("null") {
            return Self::Null;
        }

        if value.eq_ignore_ascii_case("undefined") {
            return Self::Undefined;
        }

        match my_json::json_utils::is_number(value.as_bytes()) {
            my_json::json_utils::NumberType::NaN => return Self::NaN,
            my_json::json_utils::NumberType::Number => {
                if let Ok(value) = value.parse() {
                    return Self::Double(value);
                }
            }
            my_json::json_utils::NumberType::Double => {
                if let Ok(value) = value.parse() {
                    return Self::Double(value);
                }
            }
        }

        DuckDbValue::String(value.to_string())
    }
}
