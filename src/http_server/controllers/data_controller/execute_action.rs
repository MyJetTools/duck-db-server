use my_http_server::{HttpContext, HttpFailResult, HttpOkResult};
use my_http_server::{HttpOutput, macros::*};
use std::sync::Arc;

use crate::app_ctx::AppContext;

#[http_route(
    method: "POST",
    route: "/data/execute",
    controller: "Data",
    description: "Execute Select Request",
    summary: "Execute Select Request",
    input_data: ExecuteModel,
    result:[
        {status_code: 200, description: "Single Row or array of rows"},
    ]
)]
pub struct ExecuteAction {
    app: Arc<AppContext>,
}

impl ExecuteAction {
    pub fn new(app: Arc<AppContext>) -> Self {
        Self { app }
    }
}

async fn handle_request(
    action: &ExecuteAction,
    input_data: ExecuteModel,
    _ctx: &mut HttpContext,
) -> Result<HttpOkResult, HttpFailResult> {
    let result = crate::flows::execute(&action.app, input_data.sql).await;

    let result = match result {
        Ok(ok) => ok,
        Err(err) => return HttpFailResult::as_fatal_error(err).into_err(),
    };

    /*
       let mut json_writer = my_json::json_writer::JsonArrayWriter::new();

       for db_row in result {
           let mut json_object_writer = my_json::json_writer::JsonObjectWriter::new();

           for db_column in db_row.columns {
               match db_column {
                   crate::flows::DuckDbValue::Null => {
                       json_object_writer.write(key, value);
                   }
                   crate::flows::DuckDbValue::Undefined => todo!(),
                   crate::flows::DuckDbValue::NaN => todo!(),
                   crate::flows::DuckDbValue::String(_) => todo!(),
                   crate::flows::DuckDbValue::Number(_) => todo!(),
                   crate::flows::DuckDbValue::Double(_) => todo!(),
                   crate::flows::DuckDbValue::Bool(_) => todo!(),
               }
           }
       }

       let result = json_writer.build();
    */
    let result = HttpOutput::as_text(result.to_string())
        .set_content_type(my_http_server::WebContentType::Json)
        .into_ok_result(false);

    result
}

#[derive(MyHttpInput)]
pub struct ExecuteModel {
    #[http_body(description:"Sql statement")]
    pub sql: String,
}
