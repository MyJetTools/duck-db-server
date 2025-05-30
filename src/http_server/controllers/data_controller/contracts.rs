use my_http_server::{macros::*, types::RawDataTyped};
use serde::*;

use crate::duck_db::DuckDbValue;

#[derive(MyHttpInput)]
pub struct ExecuteHttpInput {
    #[http_body_raw(description = "Sql request data")]
    pub raw: RawDataTyped<ExecuteModel>,
}

impl ExecuteHttpInput {
    pub fn deserialize(&self) -> (String, Vec<DuckDbValue>) {
        let mut name = None;
        let mut params = None;

        let read = my_json::json_reader::JsonFirstLineIterator::new(self.raw.as_slice());

        while let Some(next) = read.get_next() {
            let next = match next {
                Ok(next) => next,
                Err(_) => {
                    let sql = std::str::from_utf8(self.raw.as_slice())
                        .unwrap()
                        .to_string();

                    return (sql, vec![]);
                }
            };

            match next.0.as_str().unwrap().as_str() {
                "sql" => {
                    name = next.1.as_str().map(|itm| itm.to_string());
                }
                "params" => {
                    let deserialized = crate::duck_db::deserialize_params(next.1.as_slice());
                    params = Some(deserialized);
                }
                _ => {}
            }
        }

        (name.unwrap(), params.unwrap_or_default())
    }
}

#[derive(Serialize, Deserialize, MyHttpObjectStructure)]
pub struct ExecuteModel {
    pub sql: String,
    pub params: Option<Vec<String>>,
}
