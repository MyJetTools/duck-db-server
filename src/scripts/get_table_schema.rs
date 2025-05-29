use std::sync::Arc;

use crate::{app_ctx::AppContext, duck_db::DuckDbRow};

pub async fn get_table_schema_description(
    app: &Arc<AppContext>,
    table_name: &str,
) -> Result<Vec<DuckDbRow>, String> {
    return super::execute_select(app, format!("DESCRIBE {}", table_name)).await;
}
