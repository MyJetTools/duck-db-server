use my_http_server::types::RawDataTyped;
use my_http_server::{HttpContext, HttpFailResult, HttpOkResult};
use my_http_server::{HttpOutput, macros::*};
use std::sync::Arc;

use crate::app_ctx::AppContext;

#[http_route(
    method: "POST",
    route: "/data/select",
    controller: "Data",
    description: "Execute Select Request",
    summary: "Execute Select Request",
    input_data: ExecuteSelectModel,
    result:[
        {status_code: 200, description: "Single Row or array of rows"},
    ]
)]
pub struct SelectAction {
    app: Arc<AppContext>,
}

impl SelectAction {
    pub fn new(app: Arc<AppContext>) -> Self {
        Self { app }
    }
}

async fn handle_request(
    action: &SelectAction,
    input_data: ExecuteSelectModel,
    _ctx: &mut HttpContext,
) -> Result<HttpOkResult, HttpFailResult> {
    let data = input_data.params.as_slice();

    let params = crate::duck_db::deserialize_prams(data);

    let result = crate::scripts::execute_select(&action.app, input_data.sql, params).await;

    let result = match result {
        Ok(ok) => ok,
        Err(err) => return HttpFailResult::as_fatal_error(err).into_err(),
    };

    let mut json_writer = my_json::json_writer::JsonArrayWriter::new();

    for db_row in result {
        let json_object = db_row.as_json_object();
        json_writer.write(json_object);
    }

    let result = json_writer.build();

    let result = HttpOutput::as_text(result)
        .set_content_type(my_http_server::WebContentType::Json)
        .into_ok_result(false);

    result
}

#[derive(MyHttpInput)]
pub struct ExecuteSelectModel {
    #[http_body(description:"Sql statement")]
    pub sql: String,
    #[http_body(description = "Sql Parameters")]
    pub params: RawDataTyped<Vec<String>>,
}
