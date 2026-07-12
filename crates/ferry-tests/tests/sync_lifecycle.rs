//! Integration tests for the full sync lifecycle.
//!
//! These tests verify that the end-to-end pipeline works correctly:
//! extract → diff → deliver → state commit.

mod common;

use ferry_core::StateStore;
use ferry_core::config::SyncMode;
use ferry_core::engine::RunOptions;

use common::*;

// ---------------------------------------------------------------------------
// test_duckdb_to_file_destination
// ---------------------------------------------------------------------------

/// Test: DuckDB source → File destination (CSV).
///
/// Creates a DuckDB with a users table, runs a sync to a CSV file destination,
/// and verifies the output file contains all rows.
#[tokio::test]
async fn test_duckdb_to_file_destination() {
    let (_db_dir, db_path) = setup_test_db(
        "CREATE TABLE users (
             id VARCHAR PRIMARY KEY,
             name VARCHAR NOT NULL,
             email VARCHAR NOT NULL
         );",
        "INSERT INTO users VALUES
             ('1', 'Alice', 'alice@example.com'),
             ('2', 'Bob', 'bob@example.com'),
             ('3', 'Carol', 'carol@example.com');",
    );

    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);

    // Create a file destination
    let output_dir = tempfile::TempDir::with_prefix("ferry-output-").unwrap();
    let dest = create_file_destination(output_dir.path(), "users_sync");

    let sync_config = create_sync_config(
        "users_sync",
        SyncMode::Incremental,
        "SELECT id, name, email FROM users ORDER BY id",
    );

    let options = RunOptions::default();
    let result = run_sync(&engine, &sync_config, &source, &dest, &options)
        .await
        .expect("Sync should succeed");

    assert_eq!(result.sync_name, "users_sync");
    assert_eq!(result.rows_extracted, 3);
    assert_eq!(result.rows_synced, 3);
    assert_eq!(result.rows_pending, 0);
    assert_eq!(result.rows_failed, 0);
    assert!(!result.dry_run);

    // Verify the CSV file was written
    let entries: Vec<_> = std::fs::read_dir(output_dir.path()).unwrap().collect();
    assert_eq!(entries.len(), 1, "Should have one output file");
    let path = entries.into_iter().next().unwrap().unwrap().path();
    assert!(
        path.to_string_lossy().ends_with(".csv"),
        "Output should be a CSV file: {:?}",
        path
    );

    // Verify file contents
    let contents = std::fs::read_to_string(&path).unwrap();
    assert!(contents.contains("id,name,email"), "Should have CSV header");
    assert!(
        contents.contains("1,Alice,alice@example.com"),
        "Should contain Alice"
    );
    assert!(
        contents.contains("2,Bob,bob@example.com"),
        "Should contain Bob"
    );
    assert!(
        contents.contains("3,Carol,carol@example.com"),
        "Should contain Carol"
    );
}

// ---------------------------------------------------------------------------
// test_duckdb_to_mock_rest_success
// ---------------------------------------------------------------------------

/// Test: DuckDB source → Mock REST destination (success).
///
/// Creates a DuckDB with test data, runs a sync to a mock REST destination,
/// and verifies all rows were synced and the journal has synced entries.
#[tokio::test]
async fn test_duckdb_to_mock_rest_success() {
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
    let result = run_sync(&engine, &sync_config, &source, &dest, &options)
        .await
        .expect("Sync should succeed");

    assert_eq!(result.rows_extracted, 10);
    assert_eq!(result.rows_synced, 10);
    assert_eq!(result.rows_pending, 0);

    // Verify run was recorded and completed
    let runs = engine.state().get_runs("test_sync", 10).await.unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].status, "completed");
    assert_eq!(runs[0].rows_synced, 10);

    // Verify CDC hashes were committed
    let hashes = engine.state().get_hashes("test_sync").await.unwrap();
    assert_eq!(
        hashes.len(),
        10,
        "CDC hashes should be committed for all 10 rows"
    );
}

// ---------------------------------------------------------------------------
// test_incremental_second_run_only_delivers_changes
// ---------------------------------------------------------------------------

/// Test: Incremental mode — second run only delivers changed rows.
///
/// First run: 10 rows, all synced.
/// Modify 3 rows, add 2 rows.
/// Second run: verify only 5 rows (3 changed + 2 added) delivered.
#[tokio::test]
async fn test_incremental_second_run_only_delivers_changes() {
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

    // First run: 10 rows
    let options = RunOptions::default();
    let result1 = run_sync(&engine, &sync_config, &source, &dest, &options)
        .await
        .expect("First sync should succeed");
    assert_eq!(result1.rows_synced, 10);

    // Modify data: change 3 rows, add 2 rows
    {
        let conn = duckdb::Connection::open(db_path_str).unwrap();
        conn.execute_batch(
            "UPDATE test_table SET name = 'modified-1', value = 999 WHERE id = 'pk-0001';
             UPDATE test_table SET name = 'modified-2', value = 888 WHERE id = 'pk-0003';
             UPDATE test_table SET name = 'modified-3', value = 777 WHERE id = 'pk-0005';
             INSERT INTO test_table VALUES ('pk-0010', 'new-10', 100);
             INSERT INTO test_table VALUES ('pk-0011', 'new-11', 110);",
        )
        .unwrap();
    }

    // Second run: should deliver only changed + new rows
    let source2 = create_source(db_path_str);
    let dest2 = create_success_destination();

    let result2 = run_sync(&engine, &sync_config, &source2, &dest2, &options)
        .await
        .expect("Second sync should succeed");

    // 12 rows extracted (10 original + 2 new), but only 5 delivered (3 changed + 2 new)
    assert_eq!(result2.rows_extracted, 12);
    assert_eq!(result2.rows_synced, 5);
    assert_eq!(result2.rows_pending, 0);
}

// ---------------------------------------------------------------------------
// test_dry_run_no_side_effects
// ---------------------------------------------------------------------------

/// Test: Dry run produces no side effects.
///
/// Run with dry_run=true. Verify: no journal entries, no CDC hash committed,
/// no destination writes, no run recorded.
#[tokio::test]
async fn test_dry_run_no_side_effects() {
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

    let options = RunOptions {
        dry_run: true,
        ..RunOptions::default()
    };

    let result = run_sync(&engine, &sync_config, &source, &dest, &options)
        .await
        .expect("Dry run should succeed");

    assert_eq!(result.rows_extracted, 10);
    assert_eq!(result.rows_synced, 0);
    assert!(result.dry_run);

    // Verify no state was committed
    let runs = engine.state().get_runs("test_sync", 10).await.unwrap();
    assert!(runs.is_empty(), "No runs should be recorded for dry run");

    // Verify no CDC hashes committed
    let hashes = engine.state().get_hashes("test_sync").await.unwrap();
    assert!(
        hashes.is_empty(),
        "No CDC hashes should be committed for dry run"
    );

    // Verify no journal entries
    let synced = engine.state().get_synced_pks("test_sync").await.unwrap();
    assert!(synced.is_empty(), "No journal entries for dry run");
}

// ---------------------------------------------------------------------------
// test_full_refresh_delivers_all
// ---------------------------------------------------------------------------

/// Test: Full refresh delivers all rows regardless of CDC state.
///
/// First run: 10 rows synced.
/// Second run with full_refresh=true: all 10 rows re-delivered.
#[tokio::test]
async fn test_full_refresh_delivers_all() {
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

    // First run: 10 rows
    let options = RunOptions::default();
    let result1 = run_sync(&engine, &sync_config, &source, &dest, &options)
        .await
        .expect("First sync should succeed");
    assert_eq!(result1.rows_synced, 10);

    // Second run with full_refresh=true: all 10 rows re-delivered
    let source2 = create_source(db_path_str);
    let dest2 = create_success_destination();

    let options2 = RunOptions {
        full_refresh: true,
        ..RunOptions::default()
    };

    let result2 = run_sync(&engine, &sync_config, &source2, &dest2, &options2)
        .await
        .expect("Full refresh sync should succeed");

    // All 10 rows should be re-delivered
    assert_eq!(result2.rows_synced, 10);
    assert_eq!(result2.rows_extracted, 10);
}
