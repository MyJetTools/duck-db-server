use duckdb::types::*;

#[derive(Debug)]
pub enum DuckDbValue {
    Null,
    String(String),
    Number(i64),
    Double(f64),
    Bool(bool),
}

impl DuckDbValue {
    pub fn as_to_sql(&self) -> &(dyn ToSql + 'static) {
        match self {
            DuckDbValue::Null => {
                todo!("Null value")
            }
            DuckDbValue::String(value) => value,
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
