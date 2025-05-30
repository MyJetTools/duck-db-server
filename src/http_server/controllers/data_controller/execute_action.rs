use my_http_server::{HttpContext, HttpFailResult, HttpOkResult};
use my_http_server::{HttpOutput, macros::*};
use std::sync::Arc;

use crate::app_ctx::AppContext;

use super::ExecuteHttpInput;

#[http_route(
    method: "POST",
    route: "/data/execute",
    controller: "Data",
    description: "Execute Select Request",
    summary: "Execute Select Request",
    input_data: ExecuteHttpInput,
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
    input_data: ExecuteHttpInput,
    _ctx: &mut HttpContext,
) -> Result<HttpOkResult, HttpFailResult> {
    let (sql, params) = input_data.deserialize();

    let result = crate::scripts::execute(&action.app, sql, params).await;

    let result = match result {
        Ok(ok) => ok,
        Err(err) => return HttpFailResult::as_fatal_error(err).into_err(),
    };

    let result = HttpOutput::as_text(result.to_string())
        .set_content_type(my_http_server::WebContentType::Json)
        .into_ok_result(false);

    result
}
