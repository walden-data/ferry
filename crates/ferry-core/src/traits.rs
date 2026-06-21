use std::collections::HashMap;
use std::pin::Pin;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Stream;
use serde_json::Value;

use crate::error::FerryError;

/// A primary key value, serialized as a string.
pub type PrimaryKey = String;

/// A stream of RecordBatches produced by a source connector.
pub type RecordBatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch, FerryError>> + Send>>;

/// Schema information for a discovered stream.
#[derive(Debug, Clone)]
pub struct StreamSchema {
    pub name: String,
    pub schema: Schema,
}

/// Whether a destination supports idempotent writes.
#[derive(Debug, Clone, PartialEq)]
pub enum IdempotencyCapability {
    /// Writes are idempotent — re-delivering the same row is safe.
    Idempotent,
    /// Writes are not idempotent — duplicates may occur on retry.
    NotIdempotent,
}

/// Whether a destination supports row removal (for mirror mode).
#[derive(Debug, Clone, PartialEq)]
pub enum RemoveCapability {
    /// Can remove specific rows by primary key.
    RemoveByKey,
    /// Can replace the entire dataset (truncate + reload).
    RemoveAll,
    /// No removal capability — mirror mode is degraded.
    None,
}

/// Result of a single write operation.
#[derive(Debug, Clone)]
pub struct WriteResult {
    pub rows_written: usize,
    pub errors: Vec<RowError>,
}

/// Error for a specific row during delivery.
#[derive(Debug, Clone)]
pub struct RowError {
    pub primary_key: PrimaryKey,
    pub error: String,
}

/// Result of a remove operation.
#[derive(Debug, Clone)]
pub struct RemoveResult {
    pub rows_removed: usize,
    pub errors: Vec<RowError>,
}

/// Rate limit configuration for a destination.
#[derive(Debug, Clone)]
pub struct RateLimit {
    pub requests_per_second: Option<f64>,
    pub concurrent_requests: Option<usize>,
}

/// Configuration for a write operation.
#[derive(Debug, Clone)]
pub struct WriteConfig {
    pub sync_name: String,
    pub batch_index: usize,
    pub total_batches: usize,
}

/// A row entry in the journal.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RowEntry {
    pub primary_key: PrimaryKey,
    pub status: String,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub last_attempt_at: Option<DateTime<Utc>>,
    pub next_retry_at: Option<DateTime<Utc>>,
    pub last_sync_run_id: Option<String>,
}

/// A sync run record.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SyncRun {
    pub sync_name: String,
    pub run_id: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub rows_extracted: usize,
    pub rows_synced: usize,
    pub rows_failed: usize,
    pub rows_retried: usize,
    pub rows_dead: usize,
    pub mode: String,
    pub dry_run: bool,
    pub status: String,
}

/// Result of a CDC diff operation.
#[derive(Debug, Clone)]
pub struct DiffResult {
    pub added: Vec<PrimaryKey>,
    pub changed: Vec<PrimaryKey>,
    pub removed: Vec<PrimaryKey>,
    pub current_hashes: HashMap<PrimaryKey, u64>,
}

/// A source connector that produces Arrow RecordBatches.
#[async_trait]
pub trait Source: Send + Sync {
    /// The name of this source connector.
    fn name(&self) -> &str;

    /// Check that the source connection is valid.
    async fn check_connection(&self) -> Result<(), FerryError>;

    /// Discover available streams (tables/views) from the source.
    async fn discover(&self) -> Result<Vec<StreamSchema>, FerryError>;

    /// Execute a query and return a stream of RecordBatches.
    fn read(&self, query: &str) -> RecordBatchStream;
}

/// A destination connector that receives Arrow RecordBatches.
#[async_trait]
pub trait Destination: Send + Sync {
    /// The name of this destination connector.
    fn name(&self) -> &str;

    /// Check that the destination connection is valid.
    async fn check_connection(&self) -> Result<(), FerryError>;

    /// Write a batch of rows to the destination.
    async fn write(
        &self,
        batch: &RecordBatch,
        config: &WriteConfig,
    ) -> Result<WriteResult, FerryError>;

    /// The maximum number of rows per batch for this destination.
    fn max_batch_size(&self) -> usize;

    /// Rate limit configuration, if any.
    fn rate_limit(&self) -> Option<RateLimit>;

    /// Whether this destination supports idempotent writes.
    fn idempotency(&self) -> IdempotencyCapability;

    /// Whether this destination supports row removal.
    fn remove_capability(&self) -> RemoveCapability;

    /// Remove specific rows by primary key.
    async fn remove(
        &self,
        keys: &[Value],
        config: &WriteConfig,
    ) -> Result<RemoveResult, FerryError>;

    /// Replace all data in the destination with the given batch.
    async fn replace_all(
        &self,
        batch: &RecordBatch,
        config: &WriteConfig,
    ) -> Result<WriteResult, FerryError>;
}

/// A state store for CDC hashes, row journal, and run history.
#[async_trait]
pub trait StateStore: Send + Sync {
    // CDC state

    /// Get all stored hashes for a sync.
    async fn get_hashes(&self, sync_name: &str) -> Result<HashMap<PrimaryKey, u64>, FerryError>;

    /// Set (replace) all hashes for a sync.
    async fn set_hashes(
        &self,
        sync_name: &str,
        hashes: &HashMap<PrimaryKey, u64>,
    ) -> Result<(), FerryError>;

    /// Get the stored cursor value for a sync.
    async fn get_cursor(&self, sync_name: &str) -> Result<Option<String>, FerryError>;

    /// Set the cursor value for a sync.
    async fn set_cursor(&self, sync_name: &str, value: &str) -> Result<(), FerryError>;

    // Row journal

    /// Get all pending (retry-eligible) rows for a sync.
    async fn get_pending_rows(&self, sync_name: &str) -> Result<Vec<RowEntry>, FerryError>;

    /// Get all dead rows for a sync.
    async fn get_dead_rows(&self, sync_name: &str) -> Result<Vec<RowEntry>, FerryError>;

    /// Mark rows as successfully synced for a run.
    async fn mark_synced(
        &self,
        sync_name: &str,
        primary_keys: &[PrimaryKey],
        run_id: &str,
    ) -> Result<(), FerryError>;

    /// Mark a row as pending retry.
    async fn mark_pending(
        &self,
        sync_name: &str,
        pk: &PrimaryKey,
        error: &str,
        next_retry_at: DateTime<Utc>,
    ) -> Result<(), FerryError>;

    /// Mark a row as dead (permanently failed).
    async fn mark_dead(
        &self,
        sync_name: &str,
        pk: &PrimaryKey,
        error: &str,
    ) -> Result<(), FerryError>;

    /// Retry dead rows, optionally filtering by primary keys.
    async fn retry_dead_rows(
        &self,
        sync_name: &str,
        pks: Option<&[PrimaryKey]>,
    ) -> Result<usize, FerryError>;

    /// Purge dead rows older than the given duration.
    async fn purge_dead_rows(
        &self,
        sync_name: &str,
        older_than: chrono::Duration,
    ) -> Result<usize, FerryError>;

    // Crash recovery

    /// Get all primary keys that are currently synced for a sync (across all runs).
    async fn get_synced_pks(&self, sync_name: &str) -> Result<Vec<PrimaryKey>, FerryError>;

    /// Get all primary keys that were synced in a specific run.
    async fn get_synced_for_run(
        &self,
        sync_name: &str,
        run_id: &str,
    ) -> Result<Vec<PrimaryKey>, FerryError>;

    /// Get the last completed run for a sync.
    async fn get_last_completed_run(&self, sync_name: &str) -> Result<Option<SyncRun>, FerryError>;

    /// Get all incomplete (crashed) runs for a sync.
    async fn get_incomplete_runs(&self, sync_name: &str) -> Result<Vec<SyncRun>, FerryError>;

    /// Mark a run as completed with final stats.
    async fn complete_run(
        &self,
        sync_name: &str,
        run_id: &str,
        rows_synced: usize,
        rows_failed: usize,
        rows_retried: usize,
        rows_dead: usize,
    ) -> Result<(), FerryError>;

    // Run history

    /// Record a new run.
    async fn record_run(&self, run: &SyncRun) -> Result<(), FerryError>;

    /// Get recent runs for a sync.
    async fn get_runs(&self, sync_name: &str, limit: usize) -> Result<Vec<SyncRun>, FerryError>;
}
