use std::sync::Arc;

use my_http_server::controllers::ControllersMiddleware;

use crate::app_ctx::AppContext;

pub fn build(app: &Arc<AppContext>) -> ControllersMiddleware {
    let mut result = ControllersMiddleware::new(None, None);

    result.register_post_action(Arc::new(super::data_controller::SelectAction::new(
        app.clone(),
    )));

    result.register_post_action(Arc::new(super::data_controller::ExecuteAction::new(
        app.clone(),
    )));
    result
}
