use std::sync::Arc;

use duckdb::*;
use rust_extensions::StopWatch;

use crate::{app_ctx::AppContext, duck_db::DuckDbValue};

pub async fn execute(
    app: Arc<AppContext>,
    sql: String,
    params: Vec<DuckDbValue>,
) -> Result<usize, String> {
    let mut sw = StopWatch::new();
    sw.start();

    let sql_to_measure = sql.to_string();

    let result = tokio::task::spawn_blocking(move || execute_spawned(app, sql, params)).await;

    let Ok(result) = result else {
        return Err(format!("Panic during the execute_spawned"));
    };

    sw.pause();

    println!("[{}] is executed in {:?}", sql_to_measure, sw.duration());

    result
}

fn execute_spawned(
    app: Arc<AppContext>,
    sql: String,
    params: Vec<DuckDbValue>,
) -> Result<usize, String> {
    let mut params_to_invoke: Vec<&(dyn ToSql + 'static)> = vec![];

    for param in params.iter() {
        params_to_invoke.push(param.as_to_sql());
    }

    let params_to_invoke = params_to_invoke.as_slice();

    let conn = app.connection.lock().unwrap();

    let execute_result = match conn.execute(&sql, params_to_invoke) {
        Ok(execute_result) => execute_result,
        Err(err) => {
            return Err(format!("Error executing statement. {:?}", err));
        }
    };

    Ok(execute_result)
}
