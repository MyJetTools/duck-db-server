use std::sync::Arc;

use duckdb::*;
use rust_extensions::TaskCompletion;

use crate::{app_ctx::AppContext, duck_db::DuckDbValue};

pub async fn execute(
    app: &Arc<AppContext>,
    sql: String,
    params: Vec<DuckDbValue>,
) -> Result<usize, String> {
    let app = app.clone();

    let mut result = TaskCompletion::new();

    let awaiter = result.get_awaiter();
    std::thread::spawn(move || {
        let mut params_to_invoke: Vec<&(dyn ToSql + 'static)> = vec![];

        for param in params.iter() {
            params_to_invoke.push(param.as_to_sql());
        }

        let params_to_invoke = params_to_invoke.as_slice();

        let conn = app.connection.lock().unwrap();

        let execute_result = match conn.execute(&sql, params_to_invoke) {
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
