use std::sync::Arc;

use duckdb::*;
use rust_extensions::StopWatch;

use crate::{app_ctx::AppContext, duck_db::*};

pub async fn execute_select(
    app: Arc<AppContext>,
    sql: String,
    params: Vec<DuckDbValue>,
) -> Result<Vec<DuckDbRow>, String> {
    let debug_sql = app.get_debug_sql_value().await;

    let debug_sql = if debug_sql {
        let mut sw = StopWatch::new();
        sw.start();

        let sql_to_monitor = sql.to_string();

        Some((sw, sql_to_monitor))
    } else {
        None
    };

    let result =
        tokio::task::spawn_blocking(move || execute_select_spawned(app, sql, params)).await;

    if let Some((mut sw, sql_to_monitor)) = debug_sql {
        sw.pause();
        println!("[{}] is executed in {:?}", sql_to_monitor, sw.duration());
    }

    let Ok(result) = result else {
        return Err(format!("Panic during the execute_select_spawned"));
    };

    result
}

fn execute_select_spawned(
    app: Arc<AppContext>,
    sql: String,
    params: Vec<DuckDbValue>,
) -> Result<Vec<DuckDbRow>, String> {
    let mut params_to_invoke: Vec<&(dyn ToSql + 'static)> = vec![];

    for param in params.iter() {
        params_to_invoke.push(param.as_to_sql());
    }

    let params_to_invoke = params_to_invoke.as_slice();

    let conn = app.connection.lock().unwrap();

    let mut stmt = match conn.prepare(&sql) {
        Ok(stmt) => stmt,
        Err(err) => {
            return Err(format!("Error preparing statement. {:?}", err));
        }
    };

    let mut rows = match stmt.query(params_to_invoke) {
        Ok(rows) => rows,
        Err(err) => {
            return Err(format!("Error querying statement. {:?}", err));
        }
    };

    let names = rows.as_ref().unwrap().column_names();

    let mut result_data_set: Vec<DuckDbRow> = Vec::new();

    loop {
        let next = rows.next();

        let row = match next {
            Ok(row) => row,
            Err(err) => {
                return Err(format!("Error fetching data. {:?}", err));
            }
        };

        let Some(row) = row else {
            break;
        };

        let db_row = DuckDbRow::new(row, names.as_slice());

        result_data_set.push(db_row);
    }

    Ok(result_data_set)
}
