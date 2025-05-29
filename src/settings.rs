use serde::*;

#[derive(Debug, Serialize, Deserialize)]
pub struct SettingsModel {
    pub db_file_path: String,
}
