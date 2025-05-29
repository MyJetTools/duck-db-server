use std::sync::Arc;

use duckdb::{types::ValueRef, *};
use my_json::json_writer::JsonNullValue;
use rust_extensions::TaskCompletion;

use crate::app_ctx::AppContext;

pub async fn execute_select(app: &Arc<AppContext>, sql: String) -> Result<Vec<DuckDbRow>, String> {
    let app = app.clone();

    let mut result = TaskCompletion::new();

    let awaiter = result.get_awaiter();
    std::thread::spawn(move || {
        let conn = app.connection.lock().unwrap();

        let mut stmt = match conn.prepare(&sql) {
            Ok(stmt) => stmt,
            Err(err) => {
                result.set_error(format!("Error preparing statement. {:?}", err));
                return;
            }
        };

        let mut rows = match stmt.query([]) {
            Ok(rows) => rows,
            Err(err) => {
                result.set_error(format!("Error querying statement. {:?}", err));
                return;
            }
        };

        let names = rows.as_ref().unwrap().column_names();

        let mut result_data_set: Vec<DuckDbRow> = Vec::new();

        loop {
            let next = rows.next();

            let row = match next {
                Ok(row) => row,
                Err(err) => {
                    result.set_error(format!("Error fetching data. {:?}", err));
                    return;
                }
            };

            let Some(row) = row else {
                break;
            };

            let db_row = DuckDbRow::new(row, names.as_slice());

            result_data_set.push(db_row);
        }

        result.set_ok(result_data_set);
    });

    awaiter.get_result().await
}

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

        println!("{:?}", columns);
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
                DuckDbValue::Decimal(value) => {
                    result.write(column.0.as_str(), *value);
                }
                DuckDbValue::Bool(value) => {
                    result.write(column.0.as_str(), *value);
                }
            }
        }

        result
    }
}

#[derive(Debug)]
pub enum DuckDbValue {
    Null,
    String(String),
    Number(i64),
    Decimal(rust_decimal::Decimal),
    Bool(bool),
}

impl DuckDbValue {
    pub fn from_value_ref(value: ValueRef) -> Self {
        match value {
            ValueRef::Null => Self::Null,
            ValueRef::Boolean(value) => Self::Bool(value),
            ValueRef::TinyInt(value) => Self::Number(value as i64),
            ValueRef::SmallInt(value) => Self::Number(value as i64),
            ValueRef::Int(value) => Self::Number(value as i64),
            ValueRef::BigInt(value) => Self::Number(value as i64),
            ValueRef::HugeInt(value) => Self::Number(value as i64),
            ValueRef::UTinyInt(value) => Self::Number(value as i64),
            ValueRef::USmallInt(value) => Self::Number(value as i64),
            ValueRef::UInt(value) => Self::Number(value as i64),
            ValueRef::UBigInt(value) => Self::Number(value as i64),
            ValueRef::Float(value) => Self::Number(value as i64),
            ValueRef::Double(value) => Self::Number(value as i64),
            ValueRef::Decimal(decimal) => Self::Decimal(decimal),

            ValueRef::Timestamp(time_unit, _) => {
                todo!("Not supported type time_unit");
            }
            ValueRef::Text(items) => Self::String(std::str::from_utf8(items).unwrap().to_string()),
            ValueRef::Blob(items) => Self::String(std::str::from_utf8(items).unwrap().to_string()),
            ValueRef::Date32(dt) => Self::Number(dt as i64),
            ValueRef::Time64(time_unit, _) => {
                todo!("Not supported time_unit")
            }
            ValueRef::Interval {
                months,
                days,
                nanos,
            } => {
                todo!("Not supported interval")
            }
            ValueRef::List(list_type, _) => {
                todo!("Not supported list")
            }
            ValueRef::Enum(enum_type, _) => {
                todo!("Not supported enum")
            }
            ValueRef::Struct(struct_array, _) => {
                todo!("Not supported struct_array")
            }
            ValueRef::Array(fixed_size_list_array, _) => {
                todo!("Not supported array")
            }
            ValueRef::Map(map_array, _) => {
                todo!("Not supported map")
            }
            ValueRef::Union(array, _) => {
                todo!("Not supported union")
            }
        }
    }
}
