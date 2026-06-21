//! Integration tests for mirror mode behavior.
//!
//! These tests verify that mirror mode correctly handles row removal
//! based on the destination's remove capability.

mod common;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use ferry_core::engine::RunOptions;
use ferry_core::error::FerryError;
use ferry_core::traits::{
    Destination, IdempotencyCapability, RateLimit, RemoveCapability, RemoveResult, WriteConfig,
    WriteResult,
};
use serde_json::Value;

use common::*;

// ---------------------------------------------------------------------------
// Mock destinations for mirror mode testing
// ---------------------------------------------------------------------------

/// A mock destination that records remove() and replace_all() calls.
struct TrackingMockDestination {
    name: String,
    written_rows: std::sync::Mutex<Vec<RecordBatch>>,
    removed_keys: std::sync::Mutex<Vec<Value>>,
    replace_all_called: std::sync::Mutex<bool>,
    remove_cap: RemoveCapability,
}

impl TrackingMockDestination {
    fn new(name: &str, remove_cap: RemoveCapability) -> Self {
        Self {
            name: name.to_string(),
            written_rows: std::sync::Mutex::new(Vec::new()),
            removed_keys: std::sync::Mutex::new(Vec::new()),
            replace_all_called: std::sync::Mutex::new(false),
            remove_cap,
        }
    }

    fn written_count(&self) -> usize {
        self.written_rows
            .lock()
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum()
    }

    fn removed_count(&self) -> usize {
        self.removed_keys.lock().unwrap().len()
    }

    fn was_replace_all_called(&self) -> bool {
        *self.replace_all_called.lock().unwrap()
    }
}

#[async_trait]
impl Destination for TrackingMockDestination {
    fn name(&self) -> &str {
        &self.name
    }

    async fn check_connection(&self) -> Result<(), FerryError> {
        Ok(())
    }

    async fn write(
        &self,
        batch: &RecordBatch,
        _config: &WriteConfig,
    ) -> Result<WriteResult, FerryError> {
        self.written_rows.lock().unwrap().push(batch.clone());
        Ok(WriteResult {
            rows_written: batch.num_rows(),
            errors: vec![],
        })
    }

    fn max_batch_size(&self) -> usize {
        100
    }

    fn rate_limit(&self) -> Option<RateLimit> {
        None
    }

    fn idempotency(&self) -> IdempotencyCapability {
        IdempotencyCapability::Idempotent
    }

    fn remove_capability(&self) -> RemoveCapability {
        self.remove_cap.clone()
    }

    async fn remove(
        &self,
        keys: &[Value],
        _config: &WriteConfig,
    ) -> Result<RemoveResult, FerryError> {
        self.removed_keys
            .lock()
            .unwrap()
            .extend(keys.iter().cloned());
        Ok(RemoveResult {
            rows_removed: keys.len(),
            errors: vec![],
        })
    }

    async fn replace_all(
        &self,
        batch: &RecordBatch,
        config: &WriteConfig,
    ) -> Result<WriteResult, FerryError> {
        *self.replace_all_called.lock().unwrap() = true;
        self.write(batch, config).await
    }
}

// ---------------------------------------------------------------------------
// test_mirror_with_remove_by_key
// ---------------------------------------------------------------------------

/// Test: Mirror mode with RemoveByKey destination.
///
/// First run: 5 rows delivered. Remove 2 rows from source.
/// Second run in mirror mode with RemoveByKey destination.
/// Verify: destination.remove() called with 2 keys.
#[tokio::test]
async fn test_mirror_with_remove_by_key() {
    let (_db_dir, db_path) = setup_test_db(
        "CREATE TABLE items (
             id VARCHAR PRIMARY KEY,
             name VARCHAR NOT NULL
         );",
        "INSERT INTO items VALUES
             ('1', 'item-a'),
             ('2', 'item-b'),
             ('3', 'item-c'),
             ('4', 'item-d'),
             ('5', 'item-e');",
    );

    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);

    let sync_config =
        create_mirror_sync_config("items_sync", "SELECT id, name FROM items ORDER BY id");

    // First run: 5 rows delivered
    let dest1 = TrackingMockDestination::new("mirror_dest", RemoveCapability::RemoveByKey);
    let options = RunOptions::default();

    let result1 = run_sync(&engine, &sync_config, &source, &dest1, &options)
        .await
        .expect("First mirror sync should succeed");
    assert_eq!(result1.rows_synced, 5);

    // Remove 2 rows from source
    {
        let conn = duckdb::Connection::open(db_path_str).unwrap();
        conn.execute_batch(
            "DELETE FROM items WHERE id = 2;
             DELETE FROM items WHERE id = 4;",
        )
        .unwrap();
    }

    // Second run: should detect 2 removed rows and call remove()
    let source2 = create_source(db_path_str);
    let dest2 = TrackingMockDestination::new("mirror_dest", RemoveCapability::RemoveByKey);

    let result2 = run_sync(&engine, &sync_config, &source2, &dest2, &options)
        .await
        .expect("Second mirror sync should succeed");

    // 3 rows should be delivered (the remaining ones)
    assert_eq!(result2.rows_synced, 3);

    // Verify remove() was called with 2 keys
    assert_eq!(
        dest2.removed_count(),
        2,
        "remove() should be called with 2 keys"
    );
}

// ---------------------------------------------------------------------------
// test_mirror_with_remove_all
// ---------------------------------------------------------------------------

/// Test: Mirror mode with RemoveAll destination.
///
/// First run: 5 rows. Second run: 3 rows in source.
/// Mirror mode with RemoveAll: replace_all() called with 3 rows.
#[tokio::test]
async fn test_mirror_with_remove_all() {
    let (_db_dir, db_path) = setup_test_db(
        "CREATE TABLE items (
             id VARCHAR PRIMARY KEY,
             name VARCHAR NOT NULL
         );",
        "INSERT INTO items VALUES
             ('1', 'item-a'),
             ('2', 'item-b'),
             ('3', 'item-c'),
             ('4', 'item-d'),
             ('5', 'item-e');",
    );

    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);

    let sync_config =
        create_mirror_sync_config("items_sync", "SELECT id, name FROM items ORDER BY id");

    // First run: 5 rows delivered
    let dest1 = TrackingMockDestination::new("mirror_dest", RemoveCapability::RemoveAll);
    let options = RunOptions::default();

    let result1 = run_sync(&engine, &sync_config, &source, &dest1, &options)
        .await
        .expect("First mirror sync should succeed");
    assert_eq!(result1.rows_synced, 5);

    // Remove 2 rows from source
    {
        let conn = duckdb::Connection::open(db_path_str).unwrap();
        conn.execute_batch(
            "DELETE FROM items WHERE id = 2;
             DELETE FROM items WHERE id = 4;",
        )
        .unwrap();
    }

    // Second run: should use replace_all()
    let source2 = create_source(db_path_str);
    let dest2 = TrackingMockDestination::new("mirror_dest", RemoveCapability::RemoveAll);

    let result2 = run_sync(&engine, &sync_config, &source2, &dest2, &options)
        .await
        .expect("Second mirror sync should succeed");

    // 3 rows should be delivered via replace_all()
    assert_eq!(result2.rows_synced, 3);
    assert!(
        dest2.was_replace_all_called(),
        "replace_all() should be called for RemoveAll destination"
    );
}

// ---------------------------------------------------------------------------
// test_mirror_with_none_logs_warning
// ---------------------------------------------------------------------------

/// Test: Mirror mode with None remove capability.
///
/// Mirror mode with None remove capability. Verify: warning logged,
/// all rows delivered, no removal attempted.
#[tokio::test]
async fn test_mirror_with_none_logs_warning() {
    let (_db_dir, db_path) = setup_test_db(
        "CREATE TABLE items (
             id VARCHAR PRIMARY KEY,
             name VARCHAR NOT NULL
         );",
        "INSERT INTO items VALUES
             ('1', 'item-a'),
             ('2', 'item-b'),
             ('3', 'item-c');",
    );

    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);

    let sync_config =
        create_mirror_sync_config("items_sync", "SELECT id, name FROM items ORDER BY id");

    let dest = TrackingMockDestination::new("mirror_dest", RemoveCapability::None);
    let options = RunOptions::default();

    let result = run_sync(&engine, &sync_config, &source, &dest, &options)
        .await
        .expect("Mirror sync with None capability should succeed");

    // All 3 rows should be delivered
    assert_eq!(result.rows_synced, 3);

    // No remove or replace_all should be called
    assert_eq!(dest.removed_count(), 0, "remove() should not be called");
    assert!(
        !dest.was_replace_all_called(),
        "replace_all() should not be called"
    );
}
