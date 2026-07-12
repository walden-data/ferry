//! Integration tests for crash recovery protocol.
//!
//! These tests verify that the engine correctly handles crashes during
//! delivery and recovers without data loss or duplication.

mod common;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use duckdb::params;
use ferry_core::StateStore;
use ferry_core::config::SyncMode;
use ferry_core::engine::RunOptions;
use ferry_core::error::FerryError;
use ferry_core::traits::{
    Destination, IdempotencyCapability, RateLimit, RemoveCapability, RemoveResult, RowError,
    WriteConfig, WriteResult,
};

use common::*;

// ---------------------------------------------------------------------------
// Custom destination that fails after N rows
// ---------------------------------------------------------------------------

/// A destination that succeeds for the first `fail_after` rows, then fails
/// the rest with proper PK matching.
struct FailingDestination {
    name: String,
    fail_after: usize,
}

impl FailingDestination {
    fn new(name: &str, fail_after: usize) -> Self {
        Self {
            name: name.to_string(),
            fail_after,
        }
    }
}

#[async_trait]
impl Destination for FailingDestination {
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
        let num_rows = batch.num_rows();
        let fail_after = self.fail_after.min(num_rows);

        // Extract PKs from the batch
        let pks = ferry_core::delivery::extract_pks(batch, "id")?;

        let errors: Vec<RowError> = pks[fail_after..]
            .iter()
            .map(|pk| RowError {
                primary_key: pk.clone(),
                error: "HTTP 500 Internal Server Error".to_string(),
            })
            .collect();

        Ok(WriteResult {
            rows_written: fail_after,
            errors,
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
        RemoveCapability::None
    }

    async fn remove(
        &self,
        _keys: &[serde_json::Value],
        _config: &WriteConfig,
    ) -> Result<RemoveResult, FerryError> {
        Ok(RemoveResult {
            rows_removed: 0,
            errors: vec![],
        })
    }

    async fn replace_all(
        &self,
        batch: &RecordBatch,
        config: &WriteConfig,
    ) -> Result<WriteResult, FerryError> {
        self.write(batch, config).await
    }
}

// ---------------------------------------------------------------------------
// test_crash_mid_delivery_no_duplicates
// ---------------------------------------------------------------------------

/// Test: Crash mid-delivery — no duplicates after recovery.
///
/// 10 rows to sync. Use a destination that fails after 5 rows.
/// Run sync: 5 synced, 5 pending (CDC hash NOT committed because sync
/// didn't complete). Run sync again with success destination: remaining
/// 5 delivered, no duplicates. Verify total rows at destination = 10.
#[tokio::test]
async fn test_crash_mid_delivery_no_duplicates() {
    let (_db_dir, db_path) = setup_test_db(create_test_table(), &insert_test_rows(10));

    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);

    let sync_config = create_sync_config(
        "test_sync",
        SyncMode::Incremental,
        "SELECT id, name, value FROM test_table ORDER BY id",
    );

    // First run: destination that fails after 5 rows
    let dest_failing = FailingDestination::new("failing", 5);

    let options = RunOptions::default();
    let result1 = run_sync(&engine, &sync_config, &source, &dest_failing, &options)
        .await
        .expect("First sync should complete with partial success");

    // 5 rows should be synced, 5 pending
    assert_eq!(result1.rows_synced, 5);
    assert_eq!(result1.rows_pending, 5);

    // Verify CDC hashes were NOT committed (pending rows remain)
    let hashes = engine.state().get_hashes("test_sync").await.unwrap();
    assert!(
        hashes.is_empty(),
        "CDC hashes should not be committed when pending rows remain"
    );

    // Verify the run is NOT marked as completed
    let runs = engine.state().get_runs("test_sync", 10).await.unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(
        runs[0].status, "running",
        "Run should still be 'running' (not completed)"
    );

    // "Recovery" run: new source with same data, success destination
    let source2 = create_source(db_path_str);
    let dest_success = create_success_destination();

    let result2 = run_sync(&engine, &sync_config, &source2, &dest_success, &options)
        .await
        .expect("Recovery sync should succeed");

    // The 5 pending rows should be delivered, and the 5 already-synced rows skipped.
    // Since CDC hashes weren't committed, the diff will see all 10 rows as "new".
    // But the delivery pipeline's exactly-once check (via journal) will skip the
    // 5 already-synced rows. So we should deliver 5 rows (the ones that were pending).
    assert_eq!(result2.rows_synced, 5);
    assert_eq!(result2.rows_pending, 0);

    // Verify total synced across both runs = 10 (not 15)
    let total_synced = count_synced_rows(&engine, "test_sync").await;
    assert_eq!(total_synced, 10, "Total synced rows should be 10, not 15");
}

// ---------------------------------------------------------------------------
// test_crash_after_delivery_before_hash_commit
// ---------------------------------------------------------------------------

/// Test: Crash after all deliveries but before CDC hash commit.
///
/// Sync 10 rows, all delivered successfully. Simulate a crash that loses
/// CDC hashes but preserves journal. Run again: all 10 rows appear "changed"
/// to CDC, but journal says all synced. Verify: 0 rows re-delivered.
#[tokio::test]
async fn test_crash_after_delivery_before_hash_commit() {
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

    // First run: 10 rows delivered successfully
    let options = RunOptions::default();
    let result1 = run_sync(&engine, &sync_config, &source, &dest, &options)
        .await
        .expect("First sync should succeed");
    assert_eq!(result1.rows_synced, 10);

    // Simulate a crash: the run completed successfully (hash committed, run
    // marked complete), but then something external corrupts the CDC hashes.
    // To properly simulate "crash after delivery but before hash commit",
    // we need to: (1) delete the hashes, AND (2) mark the run as incomplete.
    engine
        .state()
        .set_hashes("test_sync", &std::collections::HashMap::new())
        .await
        .unwrap();

    // Mark the run as "running" (incomplete) to simulate crash before completion
    let runs = engine.state().get_runs("test_sync", 10).await.unwrap();
    assert_eq!(runs.len(), 1);
    let run_id = &runs[0].run_id;
    // Re-open the run as incomplete by updating its status
    {
        let conn = engine.state().get_conn().unwrap();
        conn.execute(
            "UPDATE sync_runs SET status = 'running', completed_at = NULL WHERE sync_name = ? AND run_id = ?",
            params!["test_sync", run_id],
        )
        .unwrap();
    }

    // Verify hashes are gone
    let hashes = engine.state().get_hashes("test_sync").await.unwrap();
    assert!(
        hashes.is_empty(),
        "CDC hashes should be empty after simulated crash"
    );

    // Second run: all 10 rows appear "changed" to CDC (no previous hashes).
    // But the journal check (filter_undelivered) prevents re-delivery of
    // already-synced rows from the incomplete run. So 0 rows should be delivered.
    let source2 = create_source(db_path_str);
    let dest2 = create_success_destination();

    let result2 = run_sync(&engine, &sync_config, &source2, &dest2, &options)
        .await
        .expect("Second sync should succeed");

    // 0 rows re-delivered because journal check prevents it
    assert_eq!(result2.rows_synced, 0);
    assert_eq!(result2.rows_pending, 0);

    // Verify both runs completed (runs are returned in DESC order by started_at)
    let runs = engine.state().get_runs("test_sync", 10).await.unwrap();
    assert_eq!(runs.len(), 2, "Should have 2 completed runs");
    assert_eq!(
        runs[0].rows_synced, 0,
        "Second run synced 0 (journal prevented re-delivery)"
    );
    assert_eq!(runs[1].rows_synced, 10, "First run synced 10");
}

// ---------------------------------------------------------------------------
// test_reconciliation_finds_incomplete_runs
// ---------------------------------------------------------------------------

/// Test: Reconciliation finds incomplete runs and their synced rows.
///
/// Record a run as "running" (never completed). Call reconcile().
/// Verify it finds the incomplete run and any synced rows from it.
#[tokio::test]
async fn test_reconciliation_finds_incomplete_runs() {
    let (_db_dir, db_path) = setup_test_db(create_test_table(), &insert_test_rows(10));

    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);

    // Manually create an incomplete run
    let run = ferry_core::traits::SyncRun {
        sync_name: "test_sync".to_string(),
        run_id: "crash-run-001".to_string(),
        started_at: chrono::Utc::now(),
        completed_at: None,
        rows_extracted: 10,
        rows_synced: 5,
        rows_failed: 0,
        rows_retried: 0,
        rows_dead: 0,
        mode: "incremental".to_string(),
        dry_run: false,
        status: "running".to_string(),
    };
    engine.state().record_run(&run).await.unwrap();

    // Mark some rows as synced in that incomplete run
    let synced_pks: Vec<String> = (0..5).map(|i| format!("pk-{:04}", i)).collect();
    engine
        .state()
        .mark_synced("test_sync", &synced_pks, "crash-run-001")
        .await
        .unwrap();

    // Call reconcile
    let result = ferry_core::engine::reconcile(engine.state(), "test_sync")
        .await
        .expect("Reconcile should succeed");

    // Should find the 5 already-synced rows
    assert_eq!(result.already_synced.len(), 5);
    assert!(result.already_synced.contains("pk-0000"));
    assert!(result.already_synced.contains("pk-0004"));

    // No pending rows
    assert!(result.pending_rows.is_empty());
}
