//! Integration tests for schema drift handling.
//!
//! These tests verify that the engine correctly handles schema changes
//! in the source database, such as added or removed columns.

mod common;

use ferry_core::config::SyncMode;
use ferry_core::engine::RunOptions;

use common::*;

// ---------------------------------------------------------------------------
// test_add_column_with_hash_all_triggers_resync
// ---------------------------------------------------------------------------

/// Test: Adding a column with hash_columns: all triggers resync.
///
/// First run: 3 columns (id, name, value), 10 rows synced.
/// Add a 4th column to the source table.
/// Second run with hash_columns: all: all 10 rows appear "changed"
/// (hash differs) and are re-delivered.
#[tokio::test]
async fn test_add_column_with_hash_all_triggers_resync() {
    let (_db_dir, db_path) = setup_test_db(create_test_table(), &insert_test_rows(10));

    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest = create_success_destination();

    let sync_config = create_sync_config(
        "test_sync",
        SyncMode::Incremental,
        "SELECT id, name, value FROM test_table ORDER BY id",
    );

    let options = RunOptions::default();

    // First run: 10 rows synced
    let result1 = run_sync(&engine, &sync_config, &source, &dest, &options)
        .await
        .expect("First sync should succeed");
    assert_eq!(result1.rows_synced, 10);

    // Add a 4th column to the source table
    {
        let conn = duckdb::Connection::open(db_path_str).unwrap();
        conn.execute_batch(
            "ALTER TABLE test_table ADD COLUMN description VARCHAR DEFAULT 'default_desc';",
        )
        .unwrap();
    }

    // Second run: the hash includes all columns (hash_columns: None = all),
    // so the new column changes the hash for all 10 rows.
    let source2 = create_source(db_path_str);
    let dest2 = create_success_destination();

    // Update the sync config to include the new column in the query
    let sync_config2 = create_sync_config(
        "test_sync",
        SyncMode::Incremental,
        "SELECT id, name, value, description FROM test_table ORDER BY id",
    );

    let result2 = run_sync(&engine, &sync_config2, &source2, &dest2, &options)
        .await
        .expect("Second sync should succeed");

    // All 10 rows should appear "changed" because the hash now includes
    // the new column, which has a different value than before (null → "default_desc").
    assert_eq!(result2.rows_extracted, 10);
    assert_eq!(result2.rows_synced, 10);
    assert_eq!(result2.rows_pending, 0);
}

// ---------------------------------------------------------------------------
// test_remove_mapped_column_errors
// ---------------------------------------------------------------------------

/// Test: Removing a mapped column produces an error.
///
/// First run: columns (id, name, email) mapped. Drop the email column
/// from source. Second run: verify error at extraction time about
/// missing column.
#[tokio::test]
async fn test_remove_mapped_column_errors() {
    let (_db_dir, db_path) = setup_test_db(
        "CREATE TABLE users (
             id VARCHAR PRIMARY KEY,
             name VARCHAR NOT NULL,
             email VARCHAR NOT NULL
         );",
        "INSERT INTO users VALUES
             ('1', 'Alice', 'alice@example.com'),
             ('2', 'Bob', 'bob@example.com');",
    );

    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest = create_success_destination();

    let sync_config = create_sync_config(
        "users_sync",
        SyncMode::Incremental,
        "SELECT id, name, email FROM users ORDER BY id",
    );

    let options = RunOptions::default();

    // First run: 2 rows synced
    let result1 = run_sync(&engine, &sync_config, &source, &dest, &options)
        .await
        .expect("First sync should succeed");
    assert_eq!(result1.rows_synced, 2);

    // Drop the email column from source
    {
        let conn = duckdb::Connection::open(db_path_str).unwrap();
        conn.execute_batch("ALTER TABLE users DROP COLUMN email;")
            .unwrap();
    }

    // Second run: should fail because the query references a missing column
    let source2 = create_source(db_path_str);
    let dest2 = create_success_destination();

    let result2 = run_sync(&engine, &sync_config, &source2, &dest2, &options).await;

    // The sync should fail because the query references a column that no longer exists
    assert!(
        result2.is_err(),
        "Sync should fail when a mapped column is missing from the source"
    );

    let err = result2.unwrap_err();
    let err_str = err.to_string();
    assert!(
        err_str.contains("email") || err_str.contains("column") || err_str.contains("not found"),
        "Error should mention the missing column 'email': {}",
        err_str
    );
}
