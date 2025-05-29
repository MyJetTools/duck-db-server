use duckdb::types::*;

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

            ValueRef::Timestamp(_, _) => {
                todo!("Not supported type time_unit");
            }
            ValueRef::Text(items) => Self::String(std::str::from_utf8(items).unwrap().to_string()),
            ValueRef::Blob(items) => Self::String(std::str::from_utf8(items).unwrap().to_string()),
            ValueRef::Date32(dt) => Self::Number(dt as i64),
            ValueRef::Time64(_, _) => {
                todo!("Not supported time_unit")
            }
            ValueRef::Interval {
                months: _,
                days: _,
                nanos: _,
            } => {
                todo!("Not supported interval")
            }
            ValueRef::List(_, _) => {
                todo!("Not supported list")
            }
            ValueRef::Enum(_, _) => {
                todo!("Not supported enum")
            }
            ValueRef::Struct(_, _) => {
                todo!("Not supported struct_array")
            }
            ValueRef::Array(_, _) => {
                todo!("Not supported array")
            }
            ValueRef::Map(_, _) => {
                todo!("Not supported map")
            }
            ValueRef::Union(_, _) => {
                todo!("Not supported union")
            }
        }
    }
}
