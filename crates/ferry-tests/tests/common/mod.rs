//! Shared test helpers for integration tests.
//!
//! Provides utilities for creating temporary DuckDB databases, test configs,
//! and test data for the integration test suite.

use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use duckdb::Connection;
use tempfile::TempDir;

use ferry_core::FerryConfig;
use ferry_core::config::{
    BackoffStrategy, CdcConfig, CdcMethod, DeliveryConfig, DestinationConfig, ModelConfig,
    RetryConfig, SourceConfig, StateBackend, StateConfig, SyncConfig, SyncMode, SyncSettings,
};
use ferry_core::engine::{Engine, RunOptions};
use ferry_core::error::FerryError;
use ferry_core::traits::{Destination, Source, StateStore};
use ferry_destinations::{FileDestination, FileFormat, MockRestDestination};
use ferry_sources::duckdb::DuckDbSource;

// ---------------------------------------------------------------------------
// Database setup helpers
// ---------------------------------------------------------------------------

/// Create a temporary DuckDB source database with test data.
///
/// # Arguments
///
/// * `tables_sql` - SQL statements to create tables (e.g., `CREATE TABLE users (...)`).
/// * `insert_sql` - SQL statements to insert test data.
///
/// Returns a `(TempDir, PathBuf)` where the `TempDir` keeps the database alive
/// and the `PathBuf` is the path to the DuckDB file.
pub fn setup_test_db(tables_sql: &str, insert_sql: &str) -> (TempDir, PathBuf) {
    let dir = TempDir::with_prefix("ferry-int-test-").expect("Failed to create temp dir");
    let db_path = dir.path().join("source.duckdb");
    let conn = Connection::open(&db_path).expect("Failed to open DuckDB");
    conn.execute_batch(tables_sql)
        .expect("Failed to create tables");
    conn.execute_batch(insert_sql)
        .expect("Failed to insert data");
    (dir, db_path)
}

/// Create a users table with id (INTEGER), name (VARCHAR), email (VARCHAR).
pub fn create_users_table() -> &'static str {
    "CREATE TABLE users (
         id INTEGER PRIMARY KEY,
         name VARCHAR NOT NULL,
         email VARCHAR NOT NULL
     );"
}

/// Insert N test users with deterministic data.
pub fn insert_users(n: usize) -> String {
    let mut sql = String::new();
    for i in 0..n {
        sql.push_str(&format!(
            "INSERT INTO users VALUES ({}, 'User {}', 'user{}@example.com');\n",
            i, i, i
        ));
    }
    sql
}

/// Create a test table with id (VARCHAR), name (VARCHAR), value (INTEGER).
pub fn create_test_table() -> &'static str {
    "CREATE TABLE test_table (
         id VARCHAR PRIMARY KEY,
         name VARCHAR NOT NULL,
         value INTEGER
     );"
}

/// Insert N test rows into test_table.
pub fn insert_test_rows(n: usize) -> String {
    let mut sql = String::new();
    for i in 0..n {
        sql.push_str(&format!(
            "INSERT INTO test_table VALUES ('pk-{:04}', 'name-{}', {});\n",
            i,
            i,
            i * 10
        ));
    }
    sql
}

// ---------------------------------------------------------------------------
// Config helpers
// ---------------------------------------------------------------------------

/// Create a minimal `FerryConfig` for testing with a given state DB path.
pub fn create_ferry_config(source_db_path: &str, state_db_path: &str) -> FerryConfig {
    FerryConfig {
        name: "test_project".to_string(),
        version: Some("1.0".to_string()),
        source: SourceConfig::DuckDB {
            path: source_db_path.to_string(),
            query: Some("SELECT * FROM test_table".to_string()),
        },
        state: StateConfig {
            backend: StateBackend::DuckDB,
            path: Some(state_db_path.to_string()),
        },
        dbt: None,
        defaults: None,
    }
}

/// Create a `SyncConfig` for testing with hash-based CDC.
pub fn create_sync_config(name: &str, mode: SyncMode, sql: &str) -> SyncConfig {
    SyncConfig {
        name: name.to_string(),
        description: Some("Integration test sync".to_string()),
        tags: None,
        model: ModelConfig::Sql {
            sql: sql.to_string(),
        },
        destination: DestinationConfig::Rest {
            url: "https://api.example.com/test".to_string(),
            method: Some("POST".to_string()),
            headers: None,
        },
        sync: SyncSettings {
            mode,
            cursor_field: Some("id".to_string()),
            cdc: Some(CdcConfig {
                method: CdcMethod::Hash,
                hash_columns: None, // None = hash all columns
            }),
            delivery: Some(DeliveryConfig {
                batch_size: 100,
                retry: Some(RetryConfig {
                    max_attempts: 3,
                    backoff: BackoffStrategy::Exponential,
                    initial_delay_secs: 1,
                    max_delay_secs: 10,
                }),
                on_reject: None,
                dead_letter: None,
                allow_redelivery: false,
            }),
            full_refresh: None,
        },
        tests: None,
    }
}

/// Create a `SyncConfig` for mirror mode testing.
pub fn create_mirror_sync_config(name: &str, sql: &str) -> SyncConfig {
    SyncConfig {
        name: name.to_string(),
        description: Some("Mirror mode integration test".to_string()),
        tags: None,
        model: ModelConfig::Sql {
            sql: sql.to_string(),
        },
        destination: DestinationConfig::Rest {
            url: "https://api.example.com/test".to_string(),
            method: Some("POST".to_string()),
            headers: None,
        },
        sync: SyncSettings {
            mode: SyncMode::Mirror,
            cursor_field: Some("id".to_string()),
            cdc: Some(CdcConfig {
                method: CdcMethod::Hash,
                hash_columns: None,
            }),
            delivery: Some(DeliveryConfig {
                batch_size: 100,
                retry: Some(RetryConfig {
                    max_attempts: 3,
                    backoff: BackoffStrategy::Exponential,
                    initial_delay_secs: 1,
                    max_delay_secs: 10,
                }),
                on_reject: None,
                dead_letter: None,
                allow_redelivery: false,
            }),
            full_refresh: None,
        },
        tests: None,
    }
}

/// Create a `SyncConfig` with `allow_redelivery: true`.
pub fn create_redelivery_sync_config(name: &str, sql: &str) -> SyncConfig {
    SyncConfig {
        name: name.to_string(),
        description: Some("Redelivery integration test".to_string()),
        tags: None,
        model: ModelConfig::Sql {
            sql: sql.to_string(),
        },
        destination: DestinationConfig::Rest {
            url: "https://api.example.com/test".to_string(),
            method: Some("POST".to_string()),
            headers: None,
        },
        sync: SyncSettings {
            mode: SyncMode::Incremental,
            cursor_field: Some("id".to_string()),
            cdc: Some(CdcConfig {
                method: CdcMethod::Hash,
                hash_columns: None,
            }),
            delivery: Some(DeliveryConfig {
                batch_size: 100,
                retry: Some(RetryConfig {
                    max_attempts: 3,
                    backoff: BackoffStrategy::Exponential,
                    initial_delay_secs: 1,
                    max_delay_secs: 10,
                }),
                on_reject: None,
                dead_letter: None,
                allow_redelivery: true,
            }),
            full_refresh: None,
        },
        tests: None,
    }
}

// ---------------------------------------------------------------------------
// Engine helpers
// ---------------------------------------------------------------------------

/// Create an `Engine` with a temporary state database.
///
/// Returns the `Engine` and the `TempDir` that keeps the state DB alive.
pub fn create_test_engine(source_db_path: &str) -> (Engine, TempDir) {
    let state_dir = TempDir::with_prefix("ferry-state-").expect("Failed to create state dir");
    let state_path = state_dir.path().join("state.duckdb");
    let state_path_str = state_path.to_str().unwrap().to_string();
    let config = create_ferry_config(source_db_path, &state_path_str);
    let engine = Engine::new(config).expect("Failed to create engine");
    (engine, state_dir)
}

/// Run a sync and return the result.
pub async fn run_sync(
    engine: &Engine,
    sync_config: &SyncConfig,
    source: &dyn Source,
    destination: &dyn Destination,
    options: &RunOptions,
) -> Result<ferry_core::engine::SyncResult, FerryError> {
    engine
        .run_sync(sync_config, source, destination, options)
        .await
}

/// Create a `DuckDbSource` from a database path.
pub fn create_source(db_path: &str) -> DuckDbSource {
    DuckDbSource::new(db_path).expect("Failed to create DuckDbSource")
}

/// Create a `MockRestDestination` that always succeeds.
pub fn create_success_destination() -> MockRestDestination {
    MockRestDestination::success()
}

/// Create a `MockRestDestination` that succeeds for the first N rows.
pub fn create_partial_destination(fail_after: usize) -> MockRestDestination {
    MockRestDestination::partial_success(fail_after)
}

/// Create a `FileDestination` that writes CSV files.
pub fn create_file_destination(output_dir: &std::path::Path, sync_name: &str) -> FileDestination {
    FileDestination::new(output_dir, FileFormat::Csv, sync_name)
}

// ---------------------------------------------------------------------------
// RecordBatch helpers
// ---------------------------------------------------------------------------

/// Create a test RecordBatch with id (Utf8), name (Utf8), value (Int32).
pub fn create_test_batch(rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Int32, true),
    ]));

    let ids: Vec<String> = (0..rows).map(|i| format!("pk-{:04}", i)).collect();
    let names: Vec<Option<String>> = (0..rows).map(|i| Some(format!("name-{}", i))).collect();
    let values: Vec<Option<i32>> = (0..rows).map(|i| Some(i as i32)).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int32Array::from(values)),
        ],
    )
    .expect("Failed to create test batch")
}

/// Create a test RecordBatch with id (Int32), name (Utf8), email (Utf8).
pub fn create_users_batch(rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, false),
    ]));

    let ids: Vec<i32> = (0..rows as i32).collect();
    let names: Vec<String> = (0..rows).map(|i| format!("User {}", i)).collect();
    let emails: Vec<String> = (0..rows)
        .map(|i| format!("user{}@example.com", i))
        .collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(emails)),
        ],
    )
    .expect("Failed to create users batch")
}

// ---------------------------------------------------------------------------
// State store helpers
// ---------------------------------------------------------------------------

/// Get the number of unique synced rows for a sync from the journal.
/// This counts ALL synced rows regardless of run completion status.
pub async fn count_synced_rows(engine: &Engine, sync_name: &str) -> usize {
    // Use get_dead_rows + get_pending_rows to get non-synced, then subtract from total
    // Actually, we need to query the DB directly since get_synced_pks only returns
    // rows from incomplete runs (for crash recovery purposes).
    let conn = engine.state().get_conn().unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM row_journal WHERE sync_name = ? AND status = 'synced'",
            duckdb::params![sync_name],
            |row| row.get(0),
        )
        .unwrap_or(0);
    count as usize
}

/// Get the number of pending rows for a sync.
pub async fn count_pending_rows(engine: &Engine, sync_name: &str) -> usize {
    engine
        .state()
        .get_pending_rows(sync_name)
        .await
        .unwrap_or_default()
        .len()
}

/// Get the number of dead rows for a sync.
pub async fn count_dead_rows(engine: &Engine, sync_name: &str) -> usize {
    engine
        .state()
        .get_dead_rows(sync_name)
        .await
        .unwrap_or_default()
        .len()
}
