use std::sync::Arc;

use duckdb::*;
use rust_extensions::TaskCompletion;

use crate::{app_ctx::AppContext, duck_db::*};

pub async fn execute_select(app: &Arc<AppContext>, sql: String) -> Result<Vec<DuckDbRow>, String> {
    let app = app.clone();
    let mut result = TaskCompletion::new();

    let awaiter = result.get_awaiter();
    std::thread::spawn(move || {
        let conn = app.connection.lock().unwrap();

        let mut stmt = match conn.prepare(&sql) {
            Ok(stmt) => stmt,
            Err(err) => {
                result.set_error(format!("Error preparing statement. {:?}", err));
                return;
            }
        };

        let mut rows = match stmt.query([]) {
            Ok(rows) => rows,
            Err(err) => {
                result.set_error(format!("Error querying statement. {:?}", err));
                return;
            }
        };

        let names = rows.as_ref().unwrap().column_names();

        let mut result_data_set: Vec<DuckDbRow> = Vec::new();

        loop {
            let next = rows.next();

            let row = match next {
                Ok(row) => row,
                Err(err) => {
                    result.set_error(format!("Error fetching data. {:?}", err));
                    return;
                }
            };

            let Some(row) = row else {
                break;
            };

            let db_row = DuckDbRow::new(row, names.as_slice());

            result_data_set.push(db_row);
        }

        result.set_ok(result_data_set);
    });

    awaiter.get_result().await
}
