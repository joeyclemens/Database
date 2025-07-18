use axum::{routing::get, Router, response::{Html, IntoResponse}, Json};
use serde::Serialize;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use odbc_api::{Environment, Error, Cursor, buffers::TextRowSet, ResultSetMetadata};
use axum::extract::Path;
use serde::Deserialize;
use std::ffi::CString;
use once_cell::sync::Lazy;
use tokio::sync::RwLock;
use std::time::{Duration, Instant};
use bb8::{Pool, ManageConnection};
use axum::extract::Extension;
use odbc_api::Connection;
use async_trait::async_trait;
use dashmap::DashMap;
use std::sync::Arc;
use webbrowser;

#[derive(Serialize)]
struct DatabaseList {
    databases: Vec<String>,
}

#[derive(Serialize)]
struct TableData {
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
}

#[derive(Deserialize)]
struct UpdateCell {
    parent_area: String,
    column: String,
    value: String,
}

#[derive(Deserialize)]
struct DeleteRow {
    parent_area: String,
}

#[derive(Deserialize)]
struct UpdateAreaslevel2Cell {
    arealevel_id: String,
    column: String,
    value: String,
}

#[derive(Deserialize)]
struct DeleteAreaslevel2Row {
    arealevel_id: String,
}

#[derive(Deserialize)]
struct UpdateAreaslevel1Cell {
    arealevel_id: String,
    column: String,
    value: String,
}

#[derive(Deserialize)]
struct DeleteAreaslevel1Row {
    arealevel_id: String,
}

#[derive(Deserialize)]
struct UpdateRoomScheduleCell {
    room_id: String,
    column: String,
    value: String,
}

#[derive(Deserialize)]
struct DeleteRoomScheduleRow {
    room_id: String,
}

// Struct for Item_Schedule update payload
#[derive(serde::Deserialize)]
struct UpdateItemScheduleCell {
    item_schedule_id: String,
    column: String,
    value: String,
}

#[derive(Deserialize)]
struct AddItemScheduleRow {
    Item_Ref: String,
    Room_Code: String,
    Ignore_flag: String,
    Qty_New: i64,
    Qty_Trans: i64,
    Notes: String,
    instance_variant: i64,
}

#[derive(serde::Deserialize)]
struct DeleteItemScheduleRow {
    item_schedule_id: i64,
}

async fn add_item_schedule_row(
    Path(db_name): Path<String>,
    Json(payload): Json<AddItemScheduleRow>
) -> axum::response::Response {
    let pool = get_or_create_pool(&db_name).await;
    let result = tokio::task::spawn_blocking({
        let pool = pool.clone();
        let payload = payload;
        move || {
            let mut conn = match tokio::runtime::Handle::current().block_on(pool.get()) {
                Ok(conn) => conn,
                Err(e) => return Err(format!("{:?}", e)),
            };
            let sql = "INSERT INTO Item_Schedule ([Item_Ref], [Room_Code], [Ignore_flag], [Qty_New], [Qty_Trans], [Notes], [instance_variant]) VALUES (?, ?, ?, ?, ?, ?, ?)";
            let qty_new = payload.Qty_New.to_string();
            let qty_trans = payload.Qty_Trans.to_string();
            let instance_variant = payload.instance_variant.to_string();
            let item_ref_cstr = CString::new(payload.Item_Ref).map_err(|e| e.to_string())?;
            let room_code_cstr = CString::new(payload.Room_Code).map_err(|e| e.to_string())?;
            let ignore_flag_cstr = CString::new(payload.Ignore_flag).map_err(|e| e.to_string())?;
            let notes_cstr = CString::new(payload.Notes).map_err(|e| e.to_string())?;
            let qty_new_cstr = CString::new(qty_new).map_err(|e| e.to_string())?;
            let qty_trans_cstr = CString::new(qty_trans).map_err(|e| e.to_string())?;
            let instance_variant_cstr = CString::new(instance_variant).map_err(|e| e.to_string())?;
            let exec_result = conn.execute(
                sql,
                (
                    &item_ref_cstr,
                    &room_code_cstr,
                    &ignore_flag_cstr,
                    &qty_new_cstr,
                    &qty_trans_cstr,
                    &notes_cstr,
                    &instance_variant_cstr,
                ),
            );
            match exec_result {
                Ok(_) => Ok(()),
                Err(e) => Err(e.to_string()),
            }
        }
    }).await;
    match result {
        Ok(Ok(())) => axum::response::Response::new("OK".into()),
        Ok(Err(e)) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn delete_item_schedule_row(
    Path(db_name): Path<String>,
    Json(payload): Json<DeleteItemScheduleRow>
) -> axum::response::Response {
    let pool = get_or_create_pool(&db_name).await;
    let result = tokio::task::spawn_blocking({
        let pool = pool.clone();
        let item_schedule_id = payload.item_schedule_id;
        move || {
            let mut conn = match tokio::runtime::Handle::current().block_on(pool.get()) {
                Ok(conn) => conn,
                Err(e) => return Err(format!("{:?}", e)),
            };
            let sql = "DELETE FROM Item_Schedule WHERE [Item_schedule_id] = ?";
            let id_cstr = CString::new(item_schedule_id.to_string()).map_err(|e| e.to_string())?;
            let exec_result = conn.execute(sql, (&id_cstr,));
            match exec_result {
                Ok(_) => Ok(()),
                Err(e) => Err(e.to_string()),
            }
        }
    }).await;
    match result {
        Ok(Ok(())) => axum::response::Response::new("OK".into()),
        Ok(Err(e)) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

struct CachedDatabases {
    data: Vec<String>,
    last_updated: Instant,
}

static DATABASE_CACHE: Lazy<RwLock<Option<CachedDatabases>>> = Lazy::new(|| RwLock::new(None));
static POOL_CACHE: once_cell::sync::Lazy<DashMap<String, Arc<Pool<OdbcManager>>>> = once_cell::sync::Lazy::new(|| DashMap::new());

// Custom ODBC connection manager for bb8
#[derive(Clone, Debug)]
struct OdbcManager {
    conn_str: String,
}

#[async_trait]
impl ManageConnection for OdbcManager {
    type Connection = Connection<'static>;
    type Error = String;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        static ENV: Lazy<odbc_api::Environment> = Lazy::new(|| {
            unsafe { odbc_api::Environment::new().expect("Failed to create ODBC Environment") }
        });
        ENV.connect_with_connection_string(&self.conn_str, odbc_api::ConnectionOptions::default())
            .map_err(|e| e.to_string())
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.execute("SELECT 1", ()).map(|_| ()).map_err(|e| e.to_string())
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}

type DbPool = Pool<OdbcManager>;

async fn index() -> impl IntoResponse {
    Html(include_str!("../static/index.html"))
}

async fn get_databases() -> axum::response::Response {
    // Check cache first
    {
        let cache = DATABASE_CACHE.read().await;
        if let Some(cached) = &*cache {
            if cached.last_updated.elapsed() < Duration::from_secs(60) {
                let dbs = DatabaseList { databases: cached.data.clone() };
                return Json(dbs).into_response();
            }
        }
    }
    // Use pooled connection
    let result = tokio::task::spawn_blocking({
        let pool = POOL_CACHE.get("master").unwrap().clone(); // Assuming 'master' is the default or first DB
        move || {
            let mut conn = match tokio::runtime::Handle::current().block_on(pool.get()) {
                Ok(conn) => conn,
                Err(e) => return Err(format!("{:?}", e)),
            };
            let query = "SELECT name FROM sys.databases";
            let mut dbs = Vec::new();
            let cursor = match conn.execute(query, ()) {
                Ok(cursor) => cursor,
                Err(e) => return Err(e.to_string()),
            };
            if let Some(mut cursor) = cursor {
                let batch_size = 32;
                let mut buffers = match TextRowSet::for_cursor(batch_size, &mut cursor, Some(4096)) {
                    Ok(buffers) => buffers,
                    Err(e) => return Err(e.to_string()),
                };
                let mut row_set_cursor = match cursor.bind_buffer(&mut buffers) {
                    Ok(rsc) => rsc,
                    Err(e) => return Err(e.to_string()),
                };
                while let Some(batch) = match row_set_cursor.fetch() {
                    Ok(batch) => batch,
                    Err(e) => return Err(e.to_string()),
                } {
                    for row_idx in 0..batch.num_rows() {
                        if let Some(name) = batch.at(0, row_idx) {
                            if let Ok(name_str) = std::str::from_utf8(name) {
                                dbs.push(name_str.to_string());
                            }
                        }
                    }
                }
            }
            Ok(dbs)
        }
    }).await;
    match result {
        Ok(Ok(dbs)) => {
            // Update cache
            {
                let mut cache = DATABASE_CACHE.write().await;
                *cache = Some(CachedDatabases {
                    data: dbs.clone(),
                    last_updated: Instant::now(),
                });
            }
            let dbs = DatabaseList { databases: dbs };
            Json(dbs).into_response()
        }
        Ok(Err(e)) => {
            let msg = format!("Failed to fetch databases: {e}");
            (axum::http::StatusCode::INTERNAL_SERVER_ERROR, msg).into_response()
        }
        Err(e) => {
            let msg = format!("Task join error: {e}");
            (axum::http::StatusCode::INTERNAL_SERVER_ERROR, msg).into_response()
        }
    }
}

async fn db_control_space(Path(db_name): Path<String>) -> impl IntoResponse {
    Html(include_str!("../static/db.html"))
}

async fn get_areaslevel3(Path(db_name): Path<String>) -> axum::response::Response {
    let pool = get_or_create_pool(&db_name).await;
    let result = tokio::task::spawn_blocking({
        let pool = pool.clone();
        move || {
            let mut conn = match tokio::runtime::Handle::current().block_on(pool.get()) {
                Ok(conn) => conn,
                Err(e) => return Err(format!("{:?}", e)),
            };
            let query = r#"
                SELECT a3.[ArealevelID], a3.[ParentArea], a3.[AreaDescription],
                    FLOOR(ISNULL((
                        SELECT SUM(CAST(r.[Area] AS FLOAT))
                        FROM Room_Schedule r
                        WHERE r.[ParentArea] IN (
                            SELECT al1.[ArealevelID]
                            FROM Areaslevel1 al1
                            WHERE al1.[ParentArea] IN (
                                SELECT a2.[ArealevelID]
                                FROM Areaslevel2 a2
                                WHERE a2.[ParentArea] = a3.[ArealevelID]
                            )
                        )
                    ), 0)) AS [Area]
                FROM Areaslevel3 a3
            "#;
            let mut columns = Vec::new();
            let mut rows = Vec::new();
            let cursor = match conn.execute(query, ()) {
                Ok(cursor) => cursor,
                Err(e) => return Err(e.to_string()),
            };
            if let Some(mut cursor) = cursor {
                columns = match cursor.column_names() {
                    Ok(names) => names.map(|n| n.unwrap_or_default()).collect(),
                    Err(_) => vec![],
                };
                let batch_size = 32;
                let mut buffers = match TextRowSet::for_cursor(batch_size, &mut cursor, Some(4096)) {
                    Ok(buffers) => buffers,
                    Err(e) => return Err(e.to_string()),
                };
                let mut row_set_cursor = match cursor.bind_buffer(&mut buffers) {
                    Ok(rsc) => rsc,
                    Err(e) => return Err(e.to_string()),
                };
                while let Some(batch) = match row_set_cursor.fetch() {
                    Ok(batch) => batch,
                    Err(e) => return Err(e.to_string()),
                } {
                    for row_idx in 0..batch.num_rows() {
                        let mut row = Vec::new();
                        for col_idx in 0..batch.num_cols() {
                            let cell = batch.at(col_idx, row_idx)
                                .and_then(|bytes| std::str::from_utf8(bytes).ok())
                                .unwrap_or("").to_string();
                            row.push(cell);
                        }
                        rows.push(row);
                    }
                }
            }
            Ok(TableData { columns, rows })
        }
    }).await;
    match result {
        Ok(Ok(table)) => Json(table).into_response(),
        Ok(Err(e)) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn update_areaslevel3_cell(Path(db_name): Path<String>, Json(payload): Json<UpdateCell>) -> axum::response::Response {
    let UpdateCell { parent_area, column, value } = payload;
    let pool = get_or_create_pool(&db_name).await;
    let result = tokio::task::spawn_blocking({
        let pool = pool.clone();
        move || {
            let mut conn = match tokio::runtime::Handle::current().block_on(pool.get()) {
                Ok(conn) => conn,
                Err(e) => return Err(format!("{:?}", e)),
            };
            let value_cstr = match CString::new(value) {
                Ok(s) => s,
                Err(e) => return Err(format!("Invalid value: {e}")),
            };
            let parent_area_cstr = match CString::new(parent_area) {
                Ok(s) => s,
                Err(e) => return Err(format!("Invalid parent_area: {e}")),
            };
            let sql = format!("UPDATE Areaslevel3 SET [{}] = ? WHERE [ParentArea] = ?", column.replace('"', ""));
            let exec_result = conn.execute(&sql, (&value_cstr, &parent_area_cstr));
            match exec_result {
                Ok(_) => Ok(()),
                Err(e) => Err(e.to_string()),
            }
        }
    }).await;
    match result {
        Ok(Ok(())) => axum::response::Response::new("OK".into()),
        Ok(Err(e)) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn delete_areaslevel3_row(Path(db_name): Path<String>, Json(payload): Json<DeleteRow>) -> axum::response::Response {
    let DeleteRow { parent_area } = payload;
    let pool = get_or_create_pool(&db_name).await;
    let result = tokio::task::spawn_blocking({
        let pool = pool.clone();
        move || {
            let mut conn = match tokio::runtime::Handle::current().block_on(pool.get()) {
                Ok(conn) => conn,
                Err(e) => return Err(format!("{:?}", e)),
            };
            let parent_area_cstr = match CString::new(parent_area) {
                Ok(s) => s,
                Err(e) => return Err(format!("Invalid parent_area: {e}")),
            };
            let sql = "DELETE FROM Areaslevel3 WHERE [ParentArea] = ?";
            let exec_result = conn.execute(sql, (&parent_area_cstr,));
            match exec_result {
                Ok(_) => Ok(()),
                Err(e) => Err(e.to_string()),
            }
        }
    }).await;
    match result {
        Ok(Ok(())) => axum::response::Response::new("OK".into()),
        Ok(Err(e)) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn add_areaslevel3_row(Path(db_name): Path<String>, Json(payload): Json<serde_json::Value>) -> axum::response::Response {
    let pool = get_or_create_pool(&db_name).await;
    let result = tokio::task::spawn_blocking({
        let pool = pool.clone();
        move || {
            let mut conn = match tokio::runtime::Handle::current().block_on(pool.get()) {
                Ok(conn) => conn,
                Err(e) => return Err(format!("{:?}", e)),
            };
            let obj = payload.as_object().ok_or("Invalid row data")?;
            let columns: Vec<_> = obj.keys().map(|k| format!("[{}]", k.replace('"', ""))).collect();
            let placeholders: Vec<_> = (0..columns.len()).map(|_| "?").collect();
            let values: Result<Vec<CString>, _> = obj.values().map(|v| CString::new(v.as_str().unwrap_or("")).map_err(|e| e.to_string())).collect();
            let values = match values {
                Ok(v) => v,
                Err(e) => return Err(e),
            };
            let sql = format!("INSERT INTO Areaslevel3 ({}) VALUES ({})", columns.join(", "), placeholders.join(", "));
            let exec_result = conn.execute(&sql, values.as_slice());
            match exec_result {
                Ok(_) => Ok(()),
                Err(e) => Err(e.to_string()),
            }
        }
    }).await;
    match result {
        Ok(Ok(())) => axum::response::Response::new("OK".into()),
        Ok(Err(e)) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn areaslevel2_page(Path((db_name, parent_id)): Path<(String, String)>) -> impl IntoResponse {
    Html(include_str!("../static/areaslevel2.html"))
}

async fn get_areaslevel2(Path((db_name, parent_id)): Path<(String, String)>) -> axum::response::Response {
    let pool = get_or_create_pool(&db_name).await;
    let result = tokio::task::spawn_blocking({
        let pool = pool.clone();
        move || {
            let mut conn = match tokio::runtime::Handle::current().block_on(pool.get()) {
                Ok(conn) => conn,
                Err(e) => return Err(format!("{:?}", e)),
            };
            use std::ffi::CString;
            let parent_area_cstr = match CString::new(parent_id) {
                Ok(s) => s,
                Err(e) => return Err(format!("Invalid parent_id: {e}")),
            };
            let query = r#"
                SELECT a2.[ArealevelID], a2.[ParentArea], a2.[AreaDescription],
                    FLOOR(ISNULL((
                        SELECT SUM(CAST(r.[Area] AS FLOAT))
                        FROM Room_Schedule r
                        WHERE r.[ParentArea] IN (
                            SELECT al1.[ArealevelID]
                            FROM Areaslevel1 al1
                            WHERE al1.[ParentArea] = a2.[ArealevelID]
                        )
                    ), 0)) AS [Area]
                FROM Areaslevel2 a2
                WHERE a2.[ParentArea] = ?
            "#;
            let mut columns = Vec::new();
            let mut rows = Vec::new();
            let cursor = match conn.execute(query, (&parent_area_cstr,)) {
                Ok(cursor) => cursor,
                Err(e) => return Err(e.to_string()),
            };
            if let Some(mut cursor) = cursor {
                columns = match cursor.column_names() {
                    Ok(names) => names.map(|n| n.unwrap_or_default()).collect(),
                    Err(_) => vec![],
                };
                let batch_size = 32;
                let mut buffers = match TextRowSet::for_cursor(batch_size, &mut cursor, Some(4096)) {
                    Ok(buffers) => buffers,
                    Err(e) => return Err(e.to_string()),
                };
                let mut row_set_cursor = match cursor.bind_buffer(&mut buffers) {
                    Ok(rsc) => rsc,
                    Err(e) => return Err(e.to_string()),
                };
                while let Some(batch) = match row_set_cursor.fetch() {
                    Ok(batch) => batch,
                    Err(e) => return Err(e.to_string()),
                } {
                    for row_idx in 0..batch.num_rows() {
                        let mut row = Vec::new();
                        for col_idx in 0..batch.num_cols() {
                            let cell = batch.at(col_idx, row_idx)
                                .and_then(|bytes| std::str::from_utf8(bytes).ok())
                                .unwrap_or("").to_string();
                            row.push(cell);
                        }
                        rows.push(row);
                    }
                }
            }
            Ok(TableData { columns, rows })
        }
    }).await;
    match result {
        Ok(Ok(table)) => Json(table).into_response(),
        Ok(Err(e)) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn update_areaslevel2_cell(Path(db_name): Path<String>, Json(payload): Json<UpdateAreaslevel2Cell>) -> axum::response::Response {
    let UpdateAreaslevel2Cell { arealevel_id, column, value } = payload;
    let pool = get_or_create_pool(&db_name).await;
    let result = tokio::task::spawn_blocking({
        let pool = pool.clone();
        move || {
            let mut conn = match tokio::runtime::Handle::current().block_on(pool.get()) {
                Ok(conn) => conn,
                Err(e) => return Err(format!("{:?}", e)),
            };
            let value_cstr = match CString::new(value) {
                Ok(s) => s,
                Err(e) => return Err(format!("Invalid value: {e}")),
            };
            let arealevel_id_cstr = match CString::new(arealevel_id) {
                Ok(s) => s,
                Err(e) => return Err(format!("Invalid arealevel_id: {e}")),
            };
            let sql = format!("UPDATE Areaslevel2 SET [{}] = ? WHERE [ArealevelID] = ?", column.replace('"', ""));
            let exec_result = conn.execute(&sql, (&value_cstr, &arealevel_id_cstr));
            match exec_result {
                Ok(_) => Ok(()),
                Err(e) => Err(e.to_string()),
            }
        }
    }).await;
    match result {
        Ok(Ok(())) => axum::response::Response::new("OK".into()),
        Ok(Err(e)) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn add_areaslevel2_row(Path(db_name): Path<String>, Json(payload): Json<serde_json::Value>) -> axum::response::Response {
    let pool = get_or_create_pool(&db_name).await;
    let result = tokio::task::spawn_blocking({
        let pool = pool.clone();
        move || {
            let mut conn = match tokio::runtime::Handle::current().block_on(pool.get()) {
                Ok(conn) => conn,
                Err(e) => return Err(format!("{:?}", e)),
            };
            let obj = payload.as_object().ok_or("Invalid row data")?;
            let columns: Vec<_> = obj.keys().map(|k| format!("[{}]", k.replace('"', ""))).collect();
            let placeholders: Vec<_> = (0..columns.len()).map(|_| "?").collect();
            let values: Result<Vec<CString>, _> = obj.values().map(|v| CString::new(v.as_str().unwrap_or("")).map_err(|e| e.to_string())).collect();
            let values = match values {
                Ok(v) => v,
                Err(e) => return Err(e),
            };
            let sql = format!("INSERT INTO Areaslevel2 ({}) VALUES ({})", columns.join(", "), placeholders.join(", "));
            let exec_result = conn.execute(&sql, values.as_slice());
            match exec_result {
                Ok(_) => Ok(()),
                Err(e) => Err(e.to_string()),
            }
        }
    }).await;
    match result {
        Ok(Ok(())) => axum::response::Response::new("OK".into()),
        Ok(Err(e)) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn delete_areaslevel2_row(Path(db_name): Path<String>, Json(payload): Json<DeleteAreaslevel2Row>) -> axum::response::Response {
    let DeleteAreaslevel2Row { arealevel_id } = payload;
    let pool = get_or_create_pool(&db_name).await;
    let result = tokio::task::spawn_blocking({
        let pool = pool.clone();
        move || {
            let mut conn = match tokio::runtime::Handle::current().block_on(pool.get()) {
                Ok(conn) => conn,
                Err(e) => return Err(format!("{:?}", e)),
            };
            let arealevel_id_cstr = match CString::new(arealevel_id) {
                Ok(s) => s,
                Err(e) => return Err(format!("Invalid arealevel_id: {e}")),
            };
            let sql = "DELETE FROM Areaslevel2 WHERE [ArealevelID] = ?";
            let exec_result = conn.execute(sql, (&arealevel_id_cstr,));
            match exec_result {
                Ok(_) => Ok(()),
                Err(e) => Err(e.to_string()),
            }
        }
    }).await;
    match result {
        Ok(Ok(())) => axum::response::Response::new("OK".into()),
        Ok(Err(e)) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn areaslevel1_page(Path((db_name, parent_id)): Path<(String, String)>) -> impl IntoResponse {
    Html(include_str!("../static/areaslevel1.html"))
}

async fn get_areaslevel1(Path((db_name, parent_id)): Path<(String, String)>) -> axum::response::Response {
    let pool = get_or_create_pool(&db_name).await;
    let result = tokio::task::spawn_blocking({
        let pool = pool.clone();
        move || {
            let mut conn = match tokio::runtime::Handle::current().block_on(pool.get()) {
                Ok(conn) => conn,
                Err(e) => return Err(format!("{:?}", e)),
            };
            use std::ffi::CString;
            let parent_area_cstr = match CString::new(parent_id) {
                Ok(s) => s,
                Err(e) => return Err(format!("Invalid parent_id: {e}")),
            };
            let query = r#"
                SELECT a.[ArealevelID], a.[ParentArea], a.[AreaDescription],
                    FLOOR(ISNULL(SUM(CAST(r.[Area] AS FLOAT)), 0)) AS [Area]
                FROM Areaslevel1 a
                LEFT JOIN Room_Schedule r ON r.[ParentArea] = a.[ArealevelID]
                WHERE a.[ParentArea] = ?
                GROUP BY a.[ArealevelID], a.[ParentArea], a.[AreaDescription]
            "#;
            let mut columns = Vec::new();
            let mut rows = Vec::new();
            let cursor = match conn.execute(query, (&parent_area_cstr,)) {
                Ok(cursor) => cursor,
                Err(e) => return Err(e.to_string()),
            };
            if let Some(mut cursor) = cursor {
                columns = match cursor.column_names() {
                    Ok(names) => names.map(|n| n.unwrap_or_default()).collect(),
                    Err(_) => vec![],
                };
                let batch_size = 32;
                let mut buffers = match TextRowSet::for_cursor(batch_size, &mut cursor, Some(4096)) {
                    Ok(buffers) => buffers,
                    Err(e) => return Err(e.to_string()),
                };
                let mut row_set_cursor = match cursor.bind_buffer(&mut buffers) {
                    Ok(rsc) => rsc,
                    Err(e) => return Err(e.to_string()),
                };
                while let Some(batch) = match row_set_cursor.fetch() {
                    Ok(batch) => batch,
                    Err(e) => return Err(e.to_string()),
                } {
                    for row_idx in 0..batch.num_rows() {
                        let mut row = Vec::new();
                        for col_idx in 0..batch.num_cols() {
                            let cell = batch.at(col_idx, row_idx)
                                .and_then(|bytes| std::str::from_utf8(bytes).ok())
                                .unwrap_or("").to_string();
                            row.push(cell);
                        }
                        rows.push(row);
                    }
                }
            }
            Ok(TableData { columns, rows })
        }
    }).await;
    match result {
        Ok(Ok(table)) => Json(table).into_response(),
        Ok(Err(e)) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn update_areaslevel1_cell(Path(db_name): Path<String>, Json(payload): Json<UpdateAreaslevel1Cell>) -> axum::response::Response {
    let UpdateAreaslevel1Cell { arealevel_id, column, value } = payload;
    let pool = get_or_create_pool(&db_name).await;
    let result = tokio::task::spawn_blocking({
        let pool = pool.clone();
        move || {
            let mut conn = match tokio::runtime::Handle::current().block_on(pool.get()) {
                Ok(conn) => conn,
                Err(e) => return Err(format!("{:?}", e)),
            };
            let value_cstr = match CString::new(value) {
                Ok(s) => s,
                Err(e) => return Err(format!("Invalid value: {e}")),
            };
            let arealevel_id_cstr = match CString::new(arealevel_id) {
                Ok(s) => s,
                Err(e) => return Err(format!("Invalid arealevel_id: {e}")),
            };
            let sql = format!("UPDATE Areaslevel1 SET [{}] = ? WHERE [ArealevelID] = ?", column.replace('"', ""));
            let exec_result = conn.execute(&sql, (&value_cstr, &arealevel_id_cstr));
            match exec_result {
                Ok(_) => Ok(()),
                Err(e) => Err(e.to_string()),
            }
        }
    }).await;
    match result {
        Ok(Ok(())) => axum::response::Response::new("OK".into()),
        Ok(Err(e)) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn add_areaslevel1_row(Path(db_name): Path<String>, Json(payload): Json<serde_json::Value>) -> axum::response::Response {
    let pool = get_or_create_pool(&db_name).await;
    let result = tokio::task::spawn_blocking({
        let pool = pool.clone();
        move || {
            let mut conn = match tokio::runtime::Handle::current().block_on(pool.get()) {
                Ok(conn) => conn,
                Err(e) => return Err(format!("{:?}", e)),
            };
            let obj = payload.as_object().ok_or("Invalid row data")?;
            let columns: Vec<_> = obj.keys().map(|k| format!("[{}]", k.replace('"', ""))).collect();
            let placeholders: Vec<_> = (0..columns.len()).map(|_| "?").collect();
            let values: Result<Vec<CString>, _> = obj.values().map(|v| CString::new(v.as_str().unwrap_or("")).map_err(|e| e.to_string())).collect();
            let values = match values {
                Ok(v) => v,
                Err(e) => return Err(e),
            };
            let sql = format!("INSERT INTO Areaslevel1 ({}) VALUES ({})", columns.join(", "), placeholders.join(", "));
            let exec_result = conn.execute(&sql, values.as_slice());
            match exec_result {
                Ok(_) => Ok(()),
                Err(e) => Err(e.to_string()),
            }
        }
    }).await;
    match result {
        Ok(Ok(())) => axum::response::Response::new("OK".into()),
        Ok(Err(e)) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn delete_areaslevel1_row(Path(db_name): Path<String>, Json(payload): Json<DeleteAreaslevel1Row>) -> axum::response::Response {
    let DeleteAreaslevel1Row { arealevel_id } = payload;
    let pool = get_or_create_pool(&db_name).await;
    let result = tokio::task::spawn_blocking({
        let pool = pool.clone();
        move || {
            let mut conn = match tokio::runtime::Handle::current().block_on(pool.get()) {
                Ok(conn) => conn,
                Err(e) => return Err(format!("{:?}", e)),
            };
            let arealevel_id_cstr = match CString::new(arealevel_id) {
                Ok(s) => s,
                Err(e) => return Err(format!("Invalid arealevel_id: {e}")),
            };
            let sql = "DELETE FROM Areaslevel1 WHERE [ArealevelID] = ?";
            let exec_result = conn.execute(sql, (&arealevel_id_cstr,));
            match exec_result {
                Ok(_) => Ok(()),
                Err(e) => Err(e.to_string()),
            }
        }
    }).await;
    match result {
        Ok(Ok(())) => axum::response::Response::new("OK".into()),
        Ok(Err(e)) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn room_schedule_page(Path((db_name, parent_id)): Path<(String, String)>) -> impl IntoResponse {
    Html(include_str!("../static/room_schedule.html"))
}

async fn get_room_schedule(Path((db_name, parent_id)): Path<(String, String)>) -> axum::response::Response {
    let pool = get_or_create_pool(&db_name).await;
    let result = tokio::task::spawn_blocking({
        let pool = pool.clone();
        move || {
            let mut conn = match tokio::runtime::Handle::current().block_on(pool.get()) {
                Ok(conn) => conn,
                Err(e) => return Err(format!("{:?}", e)),
            };
            use std::ffi::CString;
            let parent_area_cstr = match CString::new(parent_id) {
                Ok(s) => s,
                Err(e) => return Err(format!("Invalid parent_id: {e}")),
            };
            let query = "SELECT [ParentArea], [Room_Id], [Project_Room_Description], [Ignore_Flag], [Internal_Notes], [Room_Code], [Area] FROM Room_Schedule WHERE [ParentArea] = ?";
            let mut columns = Vec::new();
            let mut rows = Vec::new();
            let cursor = match conn.execute(query, (&parent_area_cstr,)) {
                Ok(cursor) => cursor,
                Err(e) => return Err(e.to_string()),
            };
            if let Some(mut cursor) = cursor {
                columns = match cursor.column_names() {
                    Ok(names) => names.map(|n| n.unwrap_or_default()).collect(),
                    Err(_) => vec![],
                };
                let batch_size = 32;
                let mut buffers = match odbc_api::buffers::TextRowSet::for_cursor(batch_size, &mut cursor, Some(4096)) {
                    Ok(buffers) => buffers,
                    Err(e) => return Err(e.to_string()),
                };
                let mut row_set_cursor = match cursor.bind_buffer(&mut buffers) {
                    Ok(rsc) => rsc,
                    Err(e) => return Err(e.to_string()),
                };
                while let Some(batch) = match row_set_cursor.fetch() {
                    Ok(batch) => batch,
                    Err(e) => return Err(e.to_string()),
                } {
                    for row_idx in 0..batch.num_rows() {
                        let mut row = Vec::new();
                        for col_idx in 0..batch.num_cols() {
                            let cell = batch.at(col_idx, row_idx)
                                .and_then(|bytes| std::str::from_utf8(bytes).ok())
                                .unwrap_or("").to_string();
                            row.push(cell);
                        }
                        rows.push(row);
                    }
                }
            }
            Ok(TableData { columns, rows })
        }
    }).await;
    match result {
        Ok(Ok(table)) => Json(table).into_response(),
        Ok(Err(e)) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn update_room_schedule_cell(Path(db_name): Path<String>, Json(payload): Json<UpdateRoomScheduleCell>) -> axum::response::Response {
    let UpdateRoomScheduleCell { room_id, column, value } = payload;
    let pool = get_or_create_pool(&db_name).await;
    let result = tokio::task::spawn_blocking({
        let pool = pool.clone();
        move || {
            let mut conn = match tokio::runtime::Handle::current().block_on(pool.get()) {
                Ok(conn) => conn,
                Err(e) => return Err(format!("{:?}", e)),
            };
            let value_cstr = match CString::new(value) {
                Ok(s) => s,
                Err(e) => return Err(format!("Invalid value: {e}")),
            };
            let room_id_cstr = match CString::new(room_id) {
                Ok(s) => s,
                Err(e) => return Err(format!("Invalid room_id: {e}")),
            };
            let sql = format!("UPDATE Room_Schedule SET [{}] = ? WHERE [Room_Id] = ?", column.replace('"', ""));
            let exec_result = conn.execute(&sql, (&value_cstr, &room_id_cstr));
            match exec_result {
                Ok(_) => Ok(()),
                Err(e) => Err(e.to_string()),
            }
        }
    }).await;
    match result {
        Ok(Ok(())) => axum::response::Response::new("OK".into()),
        Ok(Err(e)) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn add_room_schedule_row(Path(db_name): Path<String>, Json(payload): Json<serde_json::Value>) -> axum::response::Response {
    let pool = get_or_create_pool(&db_name).await;
    let result = tokio::task::spawn_blocking({
        let pool = pool.clone();
        move || {
            let mut conn = match tokio::runtime::Handle::current().block_on(pool.get()) {
                Ok(conn) => conn,
                Err(e) => return Err(format!("{:?}", e)),
            };
            let obj = payload.as_object().ok_or("Invalid row data")?;
            let columns: Vec<_> = obj.keys().map(|k| format!("[{}]", k.replace('"', ""))).collect();
            let placeholders: Vec<_> = (0..columns.len()).map(|_| "?").collect();
            let values: Result<Vec<CString>, _> = obj.values().map(|v| CString::new(v.as_str().unwrap_or("")).map_err(|e| e.to_string())).collect();
            let values = match values {
                Ok(v) => v,
                Err(e) => return Err(e),
            };
            let sql = format!("INSERT INTO Room_Schedule ({}) VALUES ({})", columns.join(", "), placeholders.join(", "));
            let exec_result = conn.execute(&sql, values.as_slice());
            match exec_result {
                Ok(_) => Ok(()),
                Err(e) => Err(e.to_string()),
            }
        }
    }).await;
    match result {
        Ok(Ok(())) => axum::response::Response::new("OK".into()),
        Ok(Err(e)) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn delete_room_schedule_row(Path(db_name): Path<String>, Json(payload): Json<DeleteRoomScheduleRow>) -> axum::response::Response {
    let DeleteRoomScheduleRow { room_id } = payload;
    let pool = get_or_create_pool(&db_name).await;
    let result = tokio::task::spawn_blocking({
        let pool = pool.clone();
        move || {
            let mut conn = match tokio::runtime::Handle::current().block_on(pool.get()) {
                Ok(conn) => conn,
                Err(e) => return Err(format!("{:?}", e)),
            };
            let room_id_cstr = match CString::new(room_id) {
                Ok(s) => s,
                Err(e) => return Err(format!("Invalid room_id: {e}")),
            };
            let sql = "DELETE FROM Room_Schedule WHERE [Room_Id] = ?";
            let exec_result = conn.execute(sql, (&room_id_cstr,));
            match exec_result {
                Ok(_) => Ok(()),
                Err(e) => Err(e.to_string()),
            }
        }
    }).await;
    match result {
        Ok(Ok(())) => axum::response::Response::new("OK".into()),
        Ok(Err(e)) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// Handler for the new Room_Schedule All page
async fn room_schedule_all_page(Path(db_name): Path<String>) -> impl IntoResponse {
    Html(include_str!("../static/room_schedule_all.html"))
}

// Handler to get all Room_Schedule rows for a db (no parent filter)
async fn get_room_schedule_all(Path(db_name): Path<String>) -> axum::response::Response {
    let pool = get_or_create_pool(&db_name).await;
    let result = tokio::task::spawn_blocking({
        let pool = pool.clone();
        move || {
            let mut conn = match tokio::runtime::Handle::current().block_on(pool.get()) {
                Ok(conn) => conn,
                Err(e) => return Err(format!("{:?}", e)),
            };
            let query = "SELECT [ParentArea], [Room_Id], [Project_Room_Description], [Ignore_Flag], [Internal_Notes], [Room_Code], [Area] FROM Room_Schedule";
            let mut columns = Vec::new();
            let mut rows = Vec::new();
            let cursor = match conn.execute(query, ()) {
                Ok(cursor) => cursor,
                Err(e) => return Err(e.to_string()),
            };
            if let Some(mut cursor) = cursor {
                columns = match cursor.column_names() {
                    Ok(names) => names.map(|n| n.unwrap_or_default()).collect(),
                    Err(_) => vec![],
                };
                let batch_size = 32;
                let mut buffers = match odbc_api::buffers::TextRowSet::for_cursor(batch_size, &mut cursor, Some(4096)) {
                    Ok(buffers) => buffers,
                    Err(e) => return Err(e.to_string()),
                };
                let mut row_set_cursor = match cursor.bind_buffer(&mut buffers) {
                    Ok(rsc) => rsc,
                    Err(e) => return Err(e.to_string()),
                };
                while let Some(batch) = match row_set_cursor.fetch() {
                    Ok(batch) => batch,
                    Err(e) => return Err(e.to_string()),
                } {
                    for row_idx in 0..batch.num_rows() {
                        let mut row = Vec::new();
                        for col_idx in 0..batch.num_cols() {
                            let cell = batch.at(col_idx, row_idx)
                                .and_then(|bytes| std::str::from_utf8(bytes).ok())
                                .unwrap_or("").to_string();
                            row.push(cell);
                        }
                        rows.push(row);
                    }
                }
            }
            Ok(TableData { columns, rows })
        }
    }).await;
    match result {
        Ok(Ok(table)) => Json(table).into_response(),
        Ok(Err(e)) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// Handler for the Item_Schedule page
async fn item_schedule_page(Path((db_name, room_code)): Path<(String, String)>) -> impl IntoResponse {
    Html(include_str!("../static/item_schedule.html"))
}

// Handler to get Item_Schedule rows filtered by Room_Code
async fn get_item_schedule(Path((db_name, room_code)): Path<(String, String)>) -> axum::response::Response {
    let pool = get_or_create_pool(&db_name).await;
    let result = tokio::task::spawn_blocking({
        let pool = pool.clone();
        let room_code = room_code.clone();
        move || {
            let mut conn = match tokio::runtime::Handle::current().block_on(pool.get()) {
                Ok(conn) => conn,
                Err(e) => return Err(format!("{:?}", e)),
            };
            let query = "SELECT s.[Item_schedule_id], s.[Item_Ref], d.[Item_Description], s.[Room_Code], s.[Ignore_flag], s.[Qty_New], s.[Qty_Trans], s.[Notes] FROM Item_Schedule s LEFT JOIN Item_descriptions d ON s.[Item_Ref] = d.[ADB_Ref] WHERE s.[Room_Code] = ?";
            let room_code_cstr = match CString::new(room_code) {
                Ok(s) => s,
                Err(e) => return Err(format!("Invalid room_code: {e}")),
            };
            let mut columns = Vec::new();
            let mut rows = Vec::new();
            let cursor = match conn.execute(query, (&room_code_cstr,)) {
                Ok(cursor) => cursor,
                Err(e) => return Err(e.to_string()),
            };
            if let Some(mut cursor) = cursor {
                columns = match cursor.column_names() {
                    Ok(names) => names.map(|n| n.unwrap_or_default()).collect(),
                    Err(_) => vec![],
                };
                let batch_size = 32;
                let mut buffers = match odbc_api::buffers::TextRowSet::for_cursor(batch_size, &mut cursor, Some(4096)) {
                    Ok(buffers) => buffers,
                    Err(e) => return Err(e.to_string()),
                };
                let mut row_set_cursor = match cursor.bind_buffer(&mut buffers) {
                    Ok(rsc) => rsc,
                    Err(e) => return Err(e.to_string()),
                };
                while let Some(batch) = match row_set_cursor.fetch() {
                    Ok(batch) => batch,
                    Err(e) => return Err(e.to_string()),
                } {
                    for row_idx in 0..batch.num_rows() {
                        let mut row = Vec::new();
                        for col_idx in 0..batch.num_cols() {
                            let cell = batch.at(col_idx, row_idx)
                                .and_then(|bytes| std::str::from_utf8(bytes).ok())
                                .unwrap_or("").to_string();
                            row.push(cell);
                        }
                        rows.push(row);
                    }
                }
            }
            Ok(TableData { columns, rows })
        }
    }).await;
    match result {
        Ok(Ok(table)) => Json(table).into_response(),
        Ok(Err(e)) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// Handler to update a cell in Item_Schedule
async fn update_item_schedule_cell(Path(db_name): Path<String>, Json(payload): Json<UpdateItemScheduleCell>) -> axum::response::Response {
    let UpdateItemScheduleCell { item_schedule_id, column, value } = payload;
    let pool = get_or_create_pool(&db_name).await;
    let result = tokio::task::spawn_blocking({
        let pool = pool.clone();
        let item_schedule_id = item_schedule_id.clone();
        let column = column.clone();
        let value = value.clone();
        move || {
            let mut conn = match tokio::runtime::Handle::current().block_on(pool.get()) {
                Ok(conn) => conn,
                Err(e) => return Err(format!("{:?}", e)),
            };
            let value_cstr = match CString::new(value) {
                Ok(s) => s,
                Err(e) => return Err(format!("Invalid value: {e}")),
            };
            let item_schedule_id_cstr = match CString::new(item_schedule_id) {
                Ok(s) => s,
                Err(e) => return Err(format!("Invalid item_schedule_id: {e}")),
            };
            let sql = format!("UPDATE Item_Schedule SET [{}] = ? WHERE [Item_schedule_id] = ?", column.replace('"', ""));
            let exec_result = conn.execute(&sql, (&value_cstr, &item_schedule_id_cstr));
            match exec_result {
                Ok(_) => Ok(()),
                Err(e) => Err(e.to_string()),
            }
        }
    }).await;
    match result {
        Ok(Ok(())) => axum::response::Response::new("OK".into()),
        Ok(Err(e)) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// Handler for the Item_Descriptions search page
async fn item_descriptions_search_page(Path(db_name): Path<String>) -> impl IntoResponse {
    Html(include_str!("../static/item_descriptions_search.html"))
}

// Handler to get all Item_descriptions rows for a db
async fn get_item_descriptions(Path(db_name): Path<String>) -> axum::response::Response {
    let pool = get_or_create_pool(&db_name).await;
    let result = tokio::task::spawn_blocking({
        let pool = pool.clone();
        move || {
            let mut conn = match tokio::runtime::Handle::current().block_on(pool.get()) {
                Ok(conn) => conn,
                Err(e) => return Err(format!("{:?}", e)),
            };
            let query = "SELECT d.[ADB_Ref], d.[Item_Description], d.[Unit_Cost], e.[Cat], e.[Group] FROM Item_descriptions d LEFT JOIN ERM e ON d.[ADB_Ref] = e.[ADB_Code]";
            let mut columns = Vec::new();
            let mut rows = Vec::new();
            let cursor = match conn.execute(query, ()) {
                Ok(cursor) => cursor,
                Err(e) => return Err(e.to_string()),
            };
            if let Some(mut cursor) = cursor {
                columns = match cursor.column_names() {
                    Ok(names) => names.map(|n| n.unwrap_or_default()).collect(),
                    Err(_) => vec![],
                };
                let batch_size = 32;
                let mut buffers = match odbc_api::buffers::TextRowSet::for_cursor(batch_size, &mut cursor, Some(4096)) {
                    Ok(buffers) => buffers,
                    Err(e) => return Err(e.to_string()),
                };
                let mut row_set_cursor = match cursor.bind_buffer(&mut buffers) {
                    Ok(rsc) => rsc,
                    Err(e) => return Err(e.to_string()),
                };
                while let Some(batch) = match row_set_cursor.fetch() {
                    Ok(batch) => batch,
                    Err(e) => return Err(e.to_string()),
                } {
                    for row_idx in 0..batch.num_rows() {
                        let mut row = Vec::new();
                        for col_idx in 0..batch.num_cols() {
                            let cell = batch.at(col_idx, row_idx)
                                .and_then(|bytes| std::str::from_utf8(bytes).ok())
                                .unwrap_or("").to_string();
                            row.push(cell);
                        }
                        rows.push(row);
                    }
                }
            }
            Ok(TableData { columns, rows })
        }
    }).await;
    match result {
        Ok(Ok(table)) => Json(table).into_response(),
        Ok(Err(e)) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// Handler for the Room_Types search page
async fn room_types_search_page(axum::extract::Path(db_name): axum::extract::Path<String>) -> impl axum::response::IntoResponse {
    axum::response::Html(include_str!("../static/room_types_search.html"))
}

// Handler to get all Room_Types rows for a db
async fn get_room_types(axum::extract::Path(db_name): axum::extract::Path<String>) -> axum::response::Response {
    let pool = get_or_create_pool(&db_name).await;
    let result = tokio::task::spawn_blocking({
        let pool = pool.clone();
        move || {
            let mut conn = match tokio::runtime::Handle::current().block_on(pool.get()) {
                Ok(conn) => conn,
                Err(e) => return Err(format!("{:?}", e)),
            };
            let query = "SELECT [Room_Code], [Room_Description], [Area] FROM Room_Types";
            let mut columns = Vec::new();
            let mut rows = Vec::new();
            let cursor = match conn.execute(query, ()) {
                Ok(cursor) => cursor,
                Err(e) => return Err(e.to_string()),
            };
            if let Some(mut cursor) = cursor {
                columns = match cursor.column_names() {
                    Ok(names) => names.map(|n| n.unwrap_or_default()).collect(),
                    Err(_) => vec![],
                };
                let batch_size = 32;
                let mut buffers = match odbc_api::buffers::TextRowSet::for_cursor(batch_size, &mut cursor, Some(4096)) {
                    Ok(buffers) => buffers,
                    Err(e) => return Err(e.to_string()),
                };
                let mut row_set_cursor = match cursor.bind_buffer(&mut buffers) {
                    Ok(rsc) => rsc,
                    Err(e) => return Err(e.to_string()),
                };
                while let Some(batch) = match row_set_cursor.fetch() {
                    Ok(batch) => batch,
                    Err(e) => return Err(e.to_string()),
                } {
                    for row_idx in 0..batch.num_rows() {
                        let mut row = Vec::new();
                        for col_idx in 0..batch.num_cols() {
                            let cell = batch.at(col_idx, row_idx)
                                .and_then(|bytes| std::str::from_utf8(bytes).ok())
                                .unwrap_or("").to_string();
                            row.push(cell);
                        }
                        rows.push(row);
                    }
                }
            }
            Ok(TableData { columns, rows })
        }
    }).await;
    match result {
        Ok(Ok(table)) => axum::Json(table).into_response(),
        Ok(Err(e)) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// Handler to update a cell in Room_Types
#[derive(serde::Deserialize)]
struct UpdateRoomTypeCell {
    room_code: String,
    column: String,
    value: String,
}

async fn update_room_type_cell(axum::extract::Path(db_name): axum::extract::Path<String>, axum::Json(payload): axum::Json<UpdateRoomTypeCell>) -> axum::response::Response {
    let UpdateRoomTypeCell { room_code, column, value } = payload;
    let pool = get_or_create_pool(&db_name).await;
    let result = tokio::task::spawn_blocking({
        let pool = pool.clone();
        let room_code = room_code.clone();
        let column = column.clone();
        let value = value.clone();
        move || {
            let mut conn = match tokio::runtime::Handle::current().block_on(pool.get()) {
                Ok(conn) => conn,
                Err(e) => return Err(format!("{:?}", e)),
            };
            let value_cstr = match std::ffi::CString::new(value) {
                Ok(s) => s,
                Err(e) => return Err(format!("Invalid value: {e}")),
            };
            let room_code_cstr = match std::ffi::CString::new(room_code) {
                Ok(s) => s,
                Err(e) => return Err(format!("Invalid room_code: {e}")),
            };
            let sql = format!("UPDATE Room_Types SET [{}] = ? WHERE [Room_Code] = ?", column.replace('"', ""));
            let exec_result = conn.execute(&sql, (&value_cstr, &room_code_cstr));
            match exec_result {
                Ok(_) => Ok(()),
                Err(e) => Err(e.to_string()),
            }
        }
    }).await;
    match result {
        Ok(Ok(())) => axum::response::Response::new("OK".into()),
        Ok(Err(e)) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// Handler to update a cell in Item_descriptions
#[derive(serde::Deserialize)]
struct UpdateItemDescriptionCell {
    adb_ref: String,
    column: String,
    value: String,
}

async fn update_item_description_cell(axum::extract::Path(db_name): axum::extract::Path<String>, axum::Json(payload): axum::Json<UpdateItemDescriptionCell>) -> axum::response::Response {
    let UpdateItemDescriptionCell { adb_ref, column, value } = payload;
    // Only allow updates to Item_Description and Unit_Cost
    if column != "Item_Description" && column != "Unit_Cost" {
        return (axum::http::StatusCode::BAD_REQUEST, "Only Item_Description and Unit_Cost can be updated").into_response();
    }
    let pool = get_or_create_pool(&db_name).await;
    let result = tokio::task::spawn_blocking({
        let pool = pool.clone();
        let adb_ref = adb_ref.clone();
        let column = column.clone();
        let value = value.clone();
        move || {
            let mut conn = match tokio::runtime::Handle::current().block_on(pool.get()) {
                Ok(conn) => conn,
                Err(e) => return Err(format!("{:?}", e)),
            };
            let value_cstr = match std::ffi::CString::new(value) {
                Ok(s) => s,
                Err(e) => return Err(format!("Invalid value: {e}")),
            };
            let adb_ref_cstr = match std::ffi::CString::new(adb_ref) {
                Ok(s) => s,
                Err(e) => return Err(format!("Invalid adb_ref: {e}")),
            };
            let sql = format!("UPDATE Item_descriptions SET [{}] = ? WHERE [ADB_Ref] = ?", column.replace('"', ""));
            let exec_result = conn.execute(&sql, (&value_cstr, &adb_ref_cstr));
            match exec_result {
                Ok(_) => Ok(()),
                Err(e) => Err(e.to_string()),
            }
        }
    }).await;
    match result {
        Ok(Ok(())) => axum::response::Response::new("OK".into()),
        Ok(Err(e)) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// Handler to update Cat or Group in ERM
#[derive(serde::Deserialize)]
struct UpdateErmCell {
    adb_ref: String,
    column: String,
    value: String,
}

async fn update_erm_cell(axum::extract::Path(db_name): axum::extract::Path<String>, axum::Json(payload): axum::Json<UpdateErmCell>) -> axum::response::Response {
    let UpdateErmCell { adb_ref, column, value } = payload;
    // Only allow updates to Cat and Group
    if column != "Cat" && column != "Group" {
        return (axum::http::StatusCode::BAD_REQUEST, "Only Cat and Group can be updated").into_response();
    }
    let pool = get_or_create_pool(&db_name).await;
    let result = tokio::task::spawn_blocking({
        let pool = pool.clone();
        let adb_ref = adb_ref.clone();
        let column = column.clone();
        let value = value.clone();
        move || {
            let mut conn = match tokio::runtime::Handle::current().block_on(pool.get()) {
                Ok(conn) => conn,
                Err(e) => return Err(format!("{:?}", e)),
            };
            let value_cstr = match std::ffi::CString::new(value) {
                Ok(s) => s,
                Err(e) => return Err(format!("Invalid value: {e}")),
            };
            let adb_ref_cstr = match std::ffi::CString::new(adb_ref) {
                Ok(s) => s,
                Err(e) => return Err(format!("Invalid adb_ref: {e}")),
            };
            let sql = format!("UPDATE ERM SET [{}] = ? WHERE [ADB_Code] = ?", column.replace('"', ""));
            let exec_result = conn.execute(&sql, (&value_cstr, &adb_ref_cstr));
            match exec_result {
                Ok(_) => Ok(()),
                Err(e) => Err(e.to_string()),
            }
        }
    }).await;
    match result {
        Ok(Ok(())) => axum::response::Response::new("OK".into()),
        Ok(Err(e)) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// Helper to get or create a pool for a given database
async fn get_or_create_pool(db_name: &str) -> Arc<Pool<OdbcManager>> {
    if let Some(pool) = POOL_CACHE.get(db_name) {
        return pool.clone();
    }
    let conn_str = format!("Driver={{ODBC Driver 17 for SQL Server}};Server=mjm-sql01;Database={};Trusted_Connection=Yes;", db_name);
    let manager = OdbcManager { conn_str };
    let pool = Pool::builder().max_size(16).build(manager).await.expect("Failed to build pool");
    let pool = Arc::new(pool);
    POOL_CACHE.insert(db_name.to_string(), pool.clone());
    pool
}

#[tokio::main]
async fn main() {
    // Set up ODBC connection manager and pool
    let conn_str = "Driver={ODBC Driver 17 for SQL Server};Server=mjm-sql01;Trusted_Connection=Yes;";
    let manager = OdbcManager { conn_str: conn_str.to_string() };
    let pool = Pool::builder().max_size(16).build(manager).await.expect("Failed to build pool");
    let pool = Arc::new(pool);
    POOL_CACHE.insert("master".to_string(), pool.clone()); // Add master pool to cache

    let app = Router::new()
        .route("/", get(index))
        .route("/api/databases", get(get_databases))
        .route("/db/:db_name", get(db_control_space))
        .route("/api/db/:db_name/areaslevel3", get(get_areaslevel3))
        .route("/api/db/:db_name/areaslevel3/update", axum::routing::post(update_areaslevel3_cell))
        .route("/api/db/:db_name/areaslevel3/add", axum::routing::post(add_areaslevel3_row))
        .route("/api/db/:db_name/areaslevel3/delete", axum::routing::post(delete_areaslevel3_row))
        .route("/db/:db_name/areaslevel2/:parent_id", get(areaslevel2_page))
        .route("/api/db/:db_name/areaslevel2/:parent_id", get(get_areaslevel2))
        .route("/api/db/:db_name/areaslevel2/update", axum::routing::post(update_areaslevel2_cell))
        .route("/api/db/:db_name/areaslevel2/add", axum::routing::post(add_areaslevel2_row))
        .route("/api/db/:db_name/areaslevel2/delete", axum::routing::post(delete_areaslevel2_row))
        .route("/db/:db_name/areaslevel1/:parent_id", get(areaslevel1_page))
        .route("/api/db/:db_name/areaslevel1/:parent_id", get(get_areaslevel1))
        .route("/api/db/:db_name/areaslevel1/update", axum::routing::post(update_areaslevel1_cell))
        .route("/api/db/:db_name/areaslevel1/add", axum::routing::post(add_areaslevel1_row))
        .route("/api/db/:db_name/areaslevel1/delete", axum::routing::post(delete_areaslevel1_row))
        .route("/db/:db_name/room_schedule/:parent_id", get(room_schedule_page))
        .route("/api/db/:db_name/room_schedule/:parent_id", get(get_room_schedule))
        .route("/api/db/:db_name/room_schedule/update", axum::routing::post(update_room_schedule_cell))
        .route("/api/db/:db_name/room_schedule/add", axum::routing::post(add_room_schedule_row))
        .route("/api/db/:db_name/room_schedule/delete", axum::routing::post(delete_room_schedule_row))
        .route("/db/:db_name/room_schedule_all", get(room_schedule_all_page))
        .route("/api/db/:db_name/room_schedule", get(get_room_schedule_all))
        .route("/db/:db_name/item_schedule/:room_code", get(item_schedule_page))
        .route("/api/db/:db_name/item_schedule/:room_code", get(get_item_schedule))
        .route("/api/db/:db_name/item_schedule/update", axum::routing::post(update_item_schedule_cell))
        .route("/api/db/:db_name/item_schedule/add", axum::routing::post(add_item_schedule_row))
        .route("/api/db/:db_name/item_schedule/delete", axum::routing::post(delete_item_schedule_row))
        .route("/db/:db_name/item_descriptions_search", get(item_descriptions_search_page))
        .route("/api/db/:db_name/item_descriptions", get(get_item_descriptions))
        .route("/db/:db_name/room_types_search", axum::routing::get(room_types_search_page))
        .route("/api/db/:db_name/room_types", axum::routing::get(get_room_types))
        .route("/api/db/:db_name/room_types/update", axum::routing::post(update_room_type_cell))
        .route("/api/db/:db_name/item_descriptions/update", axum::routing::post(update_item_description_cell))
        .route("/api/db/:db_name/item_descriptions/update_erm", axum::routing::post(update_erm_cell))
        .layer(Extension(pool));

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    println!("Listening on {}", addr);
    let listener = TcpListener::bind(addr).await.unwrap();

    // Open browser BEFORE serving
    if webbrowser::open("http://127.0.0.1:3000").is_ok() {
        println!("Opened browser to http://127.0.0.1:3000");
    }

    axum::serve(listener, app).await.unwrap();
}
