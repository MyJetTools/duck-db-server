use std::sync::Arc;

use app_ctx::AppContext;

mod app_ctx;
mod flows;
mod http_server;
mod settings;

#[tokio::main]
async fn main() {
    let settings_reader = my_settings_reader::SettingsReader::new("~/.duck-db");

    let app_ctx = AppContext::new(settings_reader).await;
    let app_ctx = Arc::new(app_ctx);

    crate::http_server::setup_server(&app_ctx);

    app_ctx.app_states.wait_until_shutdown().await;
}
