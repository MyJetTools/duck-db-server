use duckdb::Row;
use my_json::json_writer::*;

use super::DuckDbValue;

pub struct DuckDbRow {
    pub columns: Vec<(String, DuckDbValue)>,
}

impl DuckDbRow {
    pub fn new(row: &Row, names: &[String]) -> Self {
        let mut columns = Vec::new();

        for (i, name) in names.iter().enumerate() {
            match row.get_ref(i) {
                Ok(value) => {
                    let value = DuckDbValue::from_value_ref(value);
                    columns.push((name.to_string(), value));
                }
                Err(_) => break,
            }
        }

        Self { columns }
    }

    pub fn as_json_object(&self) -> my_json::json_writer::JsonObjectWriter {
        let mut result = my_json::json_writer::JsonObjectWriter::new();

        for column in &self.columns {
            match &column.1 {
                DuckDbValue::Null => {
                    result.write(column.0.as_str(), JsonNullValue);
                }
                DuckDbValue::String(value) => {
                    result.write(column.0.as_str(), value.as_str());
                }
                DuckDbValue::Number(value) => {
                    result.write(column.0.as_str(), *value);
                }
                DuckDbValue::Double(value) => {
                    result.write(column.0.as_str(), *value);
                }
                DuckDbValue::Bool(value) => {
                    result.write(column.0.as_str(), *value);
                }

                DuckDbValue::Json(value) => {
                    result.write(column.0.as_str(), RawJsonObject::AsStr(value));
                }
            }
        }

        result
    }
}
