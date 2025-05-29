use std::{net::SocketAddr, sync::Arc};

use my_http_server::controllers::swagger::SwaggerMiddleware;
use my_http_server::{HttpConnectionsCounter, MyHttpServer};

use crate::app_ctx::AppContext;

pub fn setup_server(app: &Arc<AppContext>) -> HttpConnectionsCounter {
    let mut http_server = MyHttpServer::new(SocketAddr::from(([0, 0, 0, 0], 4000)));

    let controllers = Arc::new(crate::http_server::controllers::builder::build(app));

    let swagger_middleware = SwaggerMiddleware::new(
        controllers.clone(),
        crate::app_ctx::APP_NAME,
        crate::app_ctx::APP_VERSION,
    );

    http_server.add_middleware(Arc::new(swagger_middleware));
    http_server.add_middleware(controllers);

    http_server.add_middleware(Arc::new(my_http_server::StaticFilesMiddleware::new(
        None, None,
    )));
    http_server.start(app.app_states.clone(), my_logger::LOGGER.clone());

    http_server.get_http_connections_counter()
}
