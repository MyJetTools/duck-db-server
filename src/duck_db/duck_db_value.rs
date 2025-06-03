use duckdb::types::*;
use my_json::json_reader::{self, AsJsonSlice};
use rust_extensions::{base64::IntoBase64, date_time::DateTimeAsMicroseconds};
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub enum DuckDbValue {
    Null,
    String(String),
    Number(i64),
    Double(f64),
    Bool(bool),
    Json(String),
}

impl DuckDbValue {
    pub fn as_to_sql(&self) -> &(dyn ToSql + 'static) {
        match self {
            DuckDbValue::Null => {
                todo!("Null value")
            }
            DuckDbValue::String(value) => value,
            DuckDbValue::Json(value) => value,
            DuckDbValue::Number(value) => value,
            DuckDbValue::Double(value) => value,
            DuckDbValue::Bool(value) => value,
        }
    }

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
            ValueRef::Decimal(decimal) => {
                let value = decimal.to_string(); //todo!("Optimize it")
                let value = value.parse().unwrap();
                Self::Double(value)
            }

            ValueRef::Timestamp(ts, ts2) => match ts {
                TimeUnit::Second => {
                    Self::String(DateTimeAsMicroseconds::new(ts2 * 1000000).to_rfc3339())
                }
                TimeUnit::Millisecond => {
                    Self::String(DateTimeAsMicroseconds::new(ts2 * 1000).to_rfc3339())
                }
                TimeUnit::Microsecond => {
                    Self::String(DateTimeAsMicroseconds::new(ts2).to_rfc3339())
                }
                TimeUnit::Nanosecond => {
                    Self::String(DateTimeAsMicroseconds::new(ts2 / 1000).to_rfc3339())
                }
            },
            ValueRef::Text(items) => {
                let value = std::str::from_utf8(items).unwrap().to_string();

                println!("{}", value);

                if check_is_json_object(items) {
                    Self::Json(value)
                } else if check_is_json_array(items) {
                    Self::Json(value)
                } else {
                    Self::String(value)
                }
            }
            ValueRef::Blob(items) => Self::String(items.into_base64()),
            ValueRef::Date32(dt) => Self::Number(dt as i64),
            ValueRef::Time64(v1, v2) => match v1 {
                TimeUnit::Second => {
                    Self::String(format_time(DateTimeAsMicroseconds::new(v2 * 1000000)))
                }
                TimeUnit::Millisecond => {
                    Self::String(format_time(DateTimeAsMicroseconds::new(v2 * 1000)))
                }
                TimeUnit::Microsecond => Self::String(format_time(DateTimeAsMicroseconds::new(v2))),
                TimeUnit::Nanosecond => {
                    Self::String(DateTimeAsMicroseconds::new(v2 / 1000).to_rfc3339())
                }
            },
            ValueRef::Interval {
                months,
                days,
                nanos,
            } => {
                let model = IntervalJsonModel {
                    months,
                    days,
                    nanos,
                };
                let v = serde_json::to_string(&model).unwrap();
                Self::Json(v)
            }
            ValueRef::List(v, _) => Self::String(format!("{:?}", v)),
            ValueRef::Enum(e, _) => Self::String(format!("{:?}", e)),
            ValueRef::Struct(s, _) => Self::String(format!("{:?}", s)),
            ValueRef::Array(a, _) => Self::String(format!("{:?}", a)),
            ValueRef::Map(m, _) => Self::String(format!("{:?}", m)),
            ValueRef::Union(v, _) => Self::String(format!("{:?}", v)),
        }
    }
}

fn format_time(value: DateTimeAsMicroseconds) -> String {
    let mut value = value.to_rfc3339();
    value.drain(..11);

    let mut index = None;
    for (i, c) in value.chars().enumerate() {
        if c == '+' || c == '-' {
            index = Some(i);
            break;
        }
    }

    if let Some(index) = index {
        value.drain(index..);
    }

    value
}

fn check_is_json_object(value: &[u8]) -> bool {
    let first_line_parser = json_reader::JsonFirstLineIterator::new(value.as_slice());

    while let Some(value) = first_line_parser.get_next() {
        if value.is_err() {
            return false;
        }
    }

    true
}

fn check_is_json_array(value: &[u8]) -> bool {
    let first_line_parser = json_reader::JsonArrayIterator::new(value.as_slice());

    if first_line_parser.is_err() {
        return false;
    }

    let first_line_parser = first_line_parser.unwrap();

    while let Some(value) = first_line_parser.get_next() {
        if value.is_err() {
            return false;
        }
    }

    true
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IntervalJsonModel {
    pub months: i32,
    pub days: i32,
    pub nanos: i64,
}
