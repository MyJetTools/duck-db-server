use std::sync::{Arc, Mutex};

use rust_extensions::AppStates;

pub const APP_NAME: &'static str = env!("CARGO_PKG_NAME");
pub const APP_VERSION: &'static str = env!("CARGO_PKG_VERSION");

pub struct AppContext {
    pub app_states: Arc<AppStates>,
    pub connection: Mutex<duckdb::Connection>,
}

impl AppContext {
    pub async fn new(
        settings_reader: my_settings_reader::SettingsReader<crate::settings::SettingsModel>,
    ) -> Self {
        let db_file_name = settings_reader.get(|itm| itm.db_file_path.clone()).await;

        let db_file_name = rust_extensions::file_utils::format_path(db_file_name.as_str());
        let connection = duckdb::Connection::open(db_file_name.to_string()).unwrap();
        Self {
            app_states: Arc::new(AppStates::create_initialized()),
            connection: Mutex::new(connection),
        }
    }
}
