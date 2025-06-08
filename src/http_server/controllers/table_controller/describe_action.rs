use my_http_server::{HttpContext, HttpFailResult, HttpOkResult};
use my_http_server::{HttpOutput, macros::*};
use std::sync::Arc;

use crate::app_ctx::AppContext;

#[http_route(
    method: "GET",
    route: "/table/describe",
    controller: "Table",
    description: "Describe table schema",
    summary: "Describe table schema",
    input_data: DescribeTableInputData,
    result:[
        {status_code: 200, description: "Table description"},
    ]
)]
pub struct DescribeAction {
    app: Arc<AppContext>,
}

impl DescribeAction {
    pub fn new(app: Arc<AppContext>) -> Self {
        Self { app }
    }
}

async fn handle_request(
    action: &DescribeAction,
    input_data: DescribeTableInputData,
    _ctx: &mut HttpContext,
) -> Result<HttpOkResult, HttpFailResult> {
    let result =
        crate::scripts::get_table_schema_description(action.app.clone(), &input_data.table_name)
            .await;

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
pub struct DescribeTableInputData {
    #[http_query(description:"Table name")]
    pub table_name: String,
}
