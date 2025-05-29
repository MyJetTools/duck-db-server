use std::sync::Arc;

use duckdb::*;
use rust_extensions::TaskCompletion;

use crate::app_ctx::AppContext;

pub async fn execute(app: &Arc<AppContext>, sql: String) -> Result<usize, String> {
    let app = app.clone();

    let mut result = TaskCompletion::new();

    let awaiter = result.get_awaiter();
    std::thread::spawn(move || {
        let conn = app.connection.lock().unwrap();

        let execute_result = match conn.execute(&sql, []) {
            Ok(execute_result) => execute_result,
            Err(err) => {
                result.set_error(format!("Error executing statement. {:?}", err));
                return;
            }
        };

        result.set_ok(execute_result);
    });

    awaiter.get_result().await
}
