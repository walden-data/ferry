use std::collections::HashSet;
use std::sync::Arc;

use arrow::compute::filter_record_batch;
use arrow_array::{
    Array, BooleanArray, Int32Array, Int64Array, LargeStringArray, RecordBatch, StringArray,
};
use arrow_schema::DataType;
use chrono::{DateTime, Utc};
use governor::{Jitter, Quota, RateLimiter};
use rand::Rng;
use serde_json::Value;
use tokio::time::sleep;
use tracing::{info, warn};

use crate::config::{BackoffStrategy, RejectAction, RejectConfig, RejectRule};
use crate::error::FerryError;
use crate::traits::{
    Destination, PrimaryKey, RemoveCapability, RowError, StateStore, WriteConfig, WriteResult,
};

// ---------------------------------------------------------------------------
// RetryPolicy
// ---------------------------------------------------------------------------

/// Backoff strategy for retry delays.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BackoffStrategyExt {
    Exponential,
    Linear,
    Fixed,
}

impl From<BackoffStrategy> for BackoffStrategyExt {
    fn from(s: BackoffStrategy) -> Self {
        match s {
            BackoffStrategy::Exponential => BackoffStrategyExt::Exponential,
            BackoffStrategy::Linear => BackoffStrategyExt::Linear,
            BackoffStrategy::Fixed => BackoffStrategyExt::Fixed,
        }
    }
}

/// Retry policy configuration for computing next retry time.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub backoff: BackoffStrategyExt,
    pub initial_delay: chrono::Duration,
    pub max_delay: chrono::Duration,
}

impl RetryPolicy {
    /// Compute the next retry time based on the number of attempts made so far.
    ///
    /// Adds jitter of 0-10% of the computed delay to avoid thundering herd.
    pub fn next_retry_at(&self, attempts: u32) -> DateTime<Utc> {
        let delay = self.compute_delay(attempts);
        let jitter = self.jitter(delay);
        Utc::now() + delay + jitter
    }

    /// Compute the base delay (without jitter) for a given attempt count.
    fn compute_delay(&self, attempts: u32) -> chrono::Duration {
        let base_secs = self.initial_delay.num_seconds();
        let delay_secs = match self.backoff {
            BackoffStrategyExt::Exponential => {
                let factor = 2u64.pow(attempts.saturating_sub(1));
                (base_secs as u64).saturating_mul(factor) as i64
            }
            BackoffStrategyExt::Linear => base_secs * attempts as i64,
            BackoffStrategyExt::Fixed => base_secs,
        };
        let capped = delay_secs.min(self.max_delay.num_seconds());
        chrono::Duration::seconds(capped)
    }

    /// Generate jitter: 0-10% of the delay.
    fn jitter(&self, delay: chrono::Duration) -> chrono::Duration {
        let delay_ms = delay.num_milliseconds();
        if delay_ms <= 0 {
            return chrono::Duration::zero();
        }
        let max_jitter_ms = (delay_ms as f64 * 0.10) as i64;
        let jitter_ms = rand::thread_rng().gen_range(0..=max_jitter_ms);
        chrono::Duration::milliseconds(jitter_ms)
    }
}

// ---------------------------------------------------------------------------
// DeliveryResult
// ---------------------------------------------------------------------------

/// Aggregated result of a delivery operation.
#[derive(Debug, Clone)]
pub struct DeliveryResult {
    pub rows_synced: usize,
    pub rows_pending: usize,
    pub rows_dead: usize,
    pub rows_skipped: usize,
    pub duration: chrono::Duration,
}

impl DeliveryResult {
    pub fn total_processed(&self) -> usize {
        self.rows_synced + self.rows_pending + self.rows_dead + self.rows_skipped
    }
}

// ---------------------------------------------------------------------------
// Batch splitting
// ---------------------------------------------------------------------------

/// Split a `RecordBatch` into smaller chunks of at most `chunk_size` rows.
///
/// Uses `RecordBatch::slice()` for zero-copy splitting — the original batch's
/// buffers stay alive.
pub fn split_record_batch(batch: &RecordBatch, chunk_size: usize) -> Vec<RecordBatch> {
    if chunk_size == 0 || batch.num_rows() == 0 {
        return Vec::new();
    }
    let total = batch.num_rows();
    (0..total)
        .step_by(chunk_size)
        .map(|offset| {
            let length = std::cmp::min(chunk_size, total - offset);
            batch.slice(offset, length)
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Error classification
// ---------------------------------------------------------------------------

/// Classify a `RowError` against the reject rules in `RejectConfig`.
///
/// Returns the matching `RejectAction`, or a default based on whether the
/// error looks retryable (5xx, timeout, etc.) vs permanent (4xx, validation).
pub fn classify_error(error: &RowError, config: &RejectConfig) -> RejectAction {
    let status_code = extract_status_code(&error.error);
    let body = &error.error;

    for rule in &config.classify {
        if rule_matches(rule, status_code, body) {
            return rule.action.clone();
        }
    }

    // Default: retry on 5xx / connection errors, dead letter on 4xx
    match status_code {
        Some(code) if (500..=599).contains(&code) => RejectAction::Retry,
        Some(_) => RejectAction::DeadLetter,
        None => RejectAction::Retry,
    }
}

/// Check if a reject rule matches the given status code and error body.
fn rule_matches(rule: &RejectRule, status_code: Option<u16>, body: &str) -> bool {
    if let Some(ref sc) = rule.match_.status_code {
        if status_code != Some(*sc) {
            return false;
        }
    }
    if let Some(ref contains) = rule.match_.body_contains {
        if !body.contains(contains) {
            return false;
        }
    }
    true
}

/// Extract an HTTP status code from an error string.
///
/// Looks for patterns like "429", "status: 429", "HTTP 429", etc.
fn extract_status_code(error: &str) -> Option<u16> {
    // Try to find a 3-digit number in the 100-599 range
    for word in error.split_whitespace() {
        let cleaned = word.trim_end_matches(|c: char| !c.is_ascii_digit());
        if let Ok(code) = cleaned.parse::<u16>() {
            if (100..=599).contains(&code) {
                return Some(code);
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// PK extraction helpers
// ---------------------------------------------------------------------------

/// Extract primary key values from a RecordBatch as strings.
pub fn extract_pks(batch: &RecordBatch, pk_col: &str) -> Result<Vec<PrimaryKey>, FerryError> {
    let schema = batch.schema();
    let idx = schema
        .index_of(pk_col)
        .map_err(|_| FerryError::Delivery(format!("PK column '{pk_col}' not found in schema")))?;

    let col = batch.column(idx);

    match col.data_type() {
        DataType::Int32 => {
            let arr = col.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                FerryError::Delivery(format!(
                    "PK column '{pk_col}' failed to downcast as Int32Array"
                ))
            })?;
            let pks: Vec<PrimaryKey> = (0..batch.num_rows())
                .map(|i| {
                    if arr.is_null(i) {
                        format!("__null__{}", i)
                    } else {
                        arr.value(i).to_string()
                    }
                })
                .collect();
            Ok(pks)
        }
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                FerryError::Delivery(format!(
                    "PK column '{pk_col}' failed to downcast as Int64Array"
                ))
            })?;
            let pks: Vec<PrimaryKey> = (0..batch.num_rows())
                .map(|i| {
                    if arr.is_null(i) {
                        format!("__null__{}", i)
                    } else {
                        arr.value(i).to_string()
                    }
                })
                .collect();
            Ok(pks)
        }
        DataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                FerryError::Delivery(format!(
                    "PK column '{pk_col}' failed to downcast as StringArray"
                ))
            })?;
            let pks: Vec<PrimaryKey> = (0..batch.num_rows())
                .map(|i| {
                    if arr.is_null(i) {
                        format!("__null__{}", i)
                    } else {
                        arr.value(i).to_string()
                    }
                })
                .collect();
            Ok(pks)
        }
        DataType::LargeUtf8 => {
            let arr = col
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    FerryError::Delivery(format!(
                        "PK column '{pk_col}' failed to downcast as LargeStringArray"
                    ))
                })?;
            let pks: Vec<PrimaryKey> = (0..batch.num_rows())
                .map(|i| {
                    if arr.is_null(i) {
                        format!("__null__{}", i)
                    } else {
                        arr.value(i).to_string()
                    }
                })
                .collect();
            Ok(pks)
        }
        other => Err(FerryError::Delivery(format!(
            "PK column '{pk_col}' has unsupported type {:?} — expected Int32, Int64, Utf8, or LargeUtf8",
            other
        ))),
    }
}

/// Extract primary key values from a RecordBatch as `serde_json::Value` (for remove operations).
pub fn extract_pks_as_values(batch: &RecordBatch, pk_col: &str) -> Result<Vec<Value>, FerryError> {
    let pks = extract_pks(batch, pk_col)?;
    Ok(pks.into_iter().map(Value::String).collect())
}

// ---------------------------------------------------------------------------
// Filter undelivered rows
// ---------------------------------------------------------------------------

/// Filter out rows from a batch whose primary keys are already in the synced set.
///
/// Builds a boolean mask where `true` means the row should be included (not yet synced),
/// then uses `filter_record_batch` to produce a new batch.
pub fn filter_undelivered(
    batch: &RecordBatch,
    pk_col: &str,
    synced_pks: &HashSet<PrimaryKey>,
) -> Result<RecordBatch, FerryError> {
    if synced_pks.is_empty() {
        return Ok(batch.clone());
    }

    let pks = extract_pks(batch, pk_col)?;
    let mask: Vec<bool> = pks.iter().map(|pk| !synced_pks.contains(pk)).collect();
    let predicate = BooleanArray::from(mask);

    filter_record_batch(batch, &predicate)
        .map_err(|e| FerryError::Delivery(format!("Failed to filter batch: {e}")))
}

// ---------------------------------------------------------------------------
// DeliveryPipeline
// ---------------------------------------------------------------------------

/// The main delivery pipeline that orchestrates batch delivery, rate limiting,
/// retry classification, and per-batch journal commits.
pub struct DeliveryPipeline<'a> {
    destination: &'a dyn Destination,
    state: &'a dyn StateStore,
    retry_policy: RetryPolicy,
    allow_redelivery: bool,
    pk_col: String,
    sync_name: String,
}

impl<'a> DeliveryPipeline<'a> {
    /// Create a new delivery pipeline.
    pub fn new(
        destination: &'a dyn Destination,
        state: &'a dyn StateStore,
        retry_policy: RetryPolicy,
        allow_redelivery: bool,
        pk_col: String,
        sync_name: String,
    ) -> Self {
        Self {
            destination,
            state,
            retry_policy,
            allow_redelivery,
            pk_col,
            sync_name,
        }
    }

    /// Deliver rows to the destination with exactly-once enforcement,
    /// rate limiting, per-batch journal commits, and error classification.
    pub async fn deliver(
        &self,
        batch: &RecordBatch,
        reject_config: Option<&RejectConfig>,
        run_id: &str,
    ) -> Result<DeliveryResult, FerryError> {
        let start = Utc::now();

        // Step 1: Exactly-once check — filter out already-synced rows
        let batch = if self.allow_redelivery {
            batch.clone()
        } else {
            let synced_pks = self.get_synced_pks().await?;
            let filtered = filter_undelivered(batch, &self.pk_col, &synced_pks)?;
            let skipped = batch.num_rows() - filtered.num_rows();
            if skipped > 0 {
                info!(
                    sync = %self.sync_name,
                    skipped = skipped,
                    "Skipping already-synced rows"
                );
            }
            filtered
        };

        if batch.num_rows() == 0 {
            return Ok(DeliveryResult {
                rows_synced: 0,
                rows_pending: 0,
                rows_dead: 0,
                rows_skipped: 0,
                duration: Utc::now() - start,
            });
        }

        // Step 2: Determine batch size from destination
        let max_batch_size = self.destination.max_batch_size();
        let batches = split_record_batch(&batch, max_batch_size);

        // Step 3: Set up rate limiter if destination has rate limits
        let rate_limiter = self.destination.rate_limit().and_then(|rl| {
            rl.requests_per_second.map(|rps| {
                let quota =
                    Quota::per_second(std::num::NonZeroU32::new(rps.max(1.0) as u32).unwrap());
                Arc::new(RateLimiter::keyed(quota))
            })
        });

        // Step 4: Deliver each batch
        let mut rows_synced = 0usize;
        let mut rows_pending = 0usize;
        let mut rows_dead = 0usize;
        let total_batches = batches.len();

        for (batch_idx, chunk) in batches.iter().enumerate() {
            // 4a: Wait on rate limiter
            if let Some(ref limiter) = rate_limiter {
                let dest_name = self.destination.name().to_string();
                let jitter = Jitter::up_to(std::time::Duration::from_millis(100));
                limiter
                    .until_key_ready_with_jitter(&dest_name, jitter)
                    .await;
            }

            // 4b: Build write config and deliver
            let write_config = WriteConfig {
                sync_name: self.sync_name.clone(),
                batch_index: batch_idx,
                total_batches,
            };

            let write_result = match self.destination.write(chunk, &write_config).await {
                Ok(result) => result,
                Err(e) => {
                    // Destination-level error (not per-row) — mark all rows as pending
                    let pks = extract_pks(chunk, &self.pk_col)?;
                    for pk in &pks {
                        let next_retry = self.retry_policy.next_retry_at(1);
                        self.state
                            .mark_pending(&self.sync_name, pk, &e.to_string(), next_retry)
                            .await?;
                    }
                    rows_pending += pks.len();
                    continue;
                }
            };

            // 4c: Mark successful rows as Synced
            // Successful rows = all rows minus errored rows
            let all_pks = extract_pks(chunk, &self.pk_col)?;
            let errored_pks: HashSet<PrimaryKey> = write_result
                .errors
                .iter()
                .map(|e| e.primary_key.clone())
                .collect();

            let success_pks: Vec<PrimaryKey> = all_pks
                .iter()
                .filter(|pk| !errored_pks.contains(*pk))
                .cloned()
                .collect();

            if !success_pks.is_empty() {
                self.state
                    .mark_synced(&self.sync_name, &success_pks, run_id)
                    .await?;
                rows_synced += success_pks.len();
            }

            // 4d/4e: Classify errors and mark rows
            for error in &write_result.errors {
                let action = match reject_config {
                    Some(config) => classify_error(error, config),
                    None => RejectAction::Retry,
                };

                match action {
                    RejectAction::Retry => {
                        let next_retry = self.retry_policy.next_retry_at(1);
                        self.state
                            .mark_pending(
                                &self.sync_name,
                                &error.primary_key,
                                &error.error,
                                next_retry,
                            )
                            .await?;
                        rows_pending += 1;
                    }
                    RejectAction::DeadLetter => {
                        self.state
                            .mark_dead(&self.sync_name, &error.primary_key, &error.error)
                            .await?;
                        rows_dead += 1;
                    }
                    RejectAction::Skip => {
                        // Skip: mark as synced to avoid re-processing
                        self.state
                            .mark_synced(&self.sync_name, &[error.primary_key.clone()], run_id)
                            .await?;
                        rows_synced += 1;
                    }
                    RejectAction::FailSync => {
                        // FailSync: return error immediately
                        return Err(FerryError::Delivery(format!(
                            "Sync failed due to error on row {}: {}",
                            error.primary_key, error.error
                        )));
                    }
                }
            }

            // 4f: Handle Retry-After (if error has retry_after info)
            if let Some(retry_after) = extract_retry_after(&write_result) {
                info!(
                    sync = %self.sync_name,
                    batch = batch_idx,
                    retry_after_ms = retry_after.as_millis(),
                    "Rate limited by destination, sleeping for Retry-After"
                );
                sleep(retry_after).await;
            }
        }

        let duration = Utc::now() - start;
        Ok(DeliveryResult {
            rows_synced,
            rows_pending,
            rows_dead,
            rows_skipped: 0,
            duration,
        })
    }

    /// Deliver rows in mirror mode, handling row removal based on the
    /// destination's `remove_capability()`.
    pub async fn deliver_mirror(
        &self,
        current: &RecordBatch,
        removed_keys: &[Value],
        reject_config: Option<&RejectConfig>,
        run_id: &str,
    ) -> Result<DeliveryResult, FerryError> {
        match self.destination.remove_capability() {
            RemoveCapability::RemoveByKey => {
                // Deliver current rows, then remove deleted keys
                let result = self.deliver(current, reject_config, run_id).await?;

                if !removed_keys.is_empty() {
                    let write_config = WriteConfig {
                        sync_name: self.sync_name.clone(),
                        batch_index: 0,
                        total_batches: 1,
                    };
                    let remove_result = self
                        .destination
                        .remove(removed_keys, &write_config)
                        .await
                        .map_err(|e| FerryError::Delivery(format!("Remove failed: {e}")))?;

                    info!(
                        sync = %self.sync_name,
                        removed = remove_result.rows_removed,
                        "Removed rows from destination"
                    );
                }

                Ok(result)
            }
            RemoveCapability::RemoveAll => {
                // Replace all data in the destination
                let write_config = WriteConfig {
                    sync_name: self.sync_name.clone(),
                    batch_index: 0,
                    total_batches: 1,
                };
                let _write_result = self
                    .destination
                    .replace_all(current, &write_config)
                    .await
                    .map_err(|e| FerryError::Delivery(format!("Replace all failed: {e}")))?;

                // Mark all rows as synced
                let pks = extract_pks(current, &self.pk_col)?;
                self.state
                    .mark_synced(&self.sync_name, &pks, run_id)
                    .await?;

                Ok(DeliveryResult {
                    rows_synced: pks.len(),
                    rows_pending: 0,
                    rows_dead: 0,
                    rows_skipped: 0,
                    duration: chrono::Duration::zero(),
                })
            }
            RemoveCapability::None => {
                warn!(
                    sync = %self.sync_name,
                    destination = %self.destination.name(),
                    "Destination has no remove capability — mirror mode is degraded. \
                     Delivering current rows only."
                );
                self.deliver(current, reject_config, run_id).await
            }
        }
    }

    /// Get all currently synced primary keys for this sync.
    async fn get_synced_pks(&self) -> Result<HashSet<PrimaryKey>, FerryError> {
        let pks = self.state.get_synced_pks(&self.sync_name).await?;
        Ok(pks.into_iter().collect())
    }
}

/// Extract a Retry-After duration from a `WriteResult`'s errors.
///
/// Looks for "retry_after" or "Retry-After" followed by a duration in the
/// error messages. Returns `None` if no Retry-After is found.
fn extract_retry_after(result: &WriteResult) -> Option<std::time::Duration> {
    for error in &result.errors {
        let lower = error.error.to_lowercase();
        // Look for patterns like "retry_after: 30", "retry-after: 30s", etc.
        if let Some(pos) = lower.find("retry_after") {
            let rest = &lower[pos + 11..];
            if let Some(secs) = extract_number(rest) {
                return Some(std::time::Duration::from_secs(secs));
            }
        }
        if let Some(pos) = lower.find("retry-after") {
            let rest = &lower[pos + 11..];
            if let Some(secs) = extract_number(rest) {
                return Some(std::time::Duration::from_secs(secs));
            }
        }
    }
    None
}

/// Extract the first number found in a string.
///
/// Skips leading non-digit characters (colons, spaces, etc.) before extracting.
fn extract_number(s: &str) -> Option<u64> {
    let s = s.trim();
    // Skip leading non-digit characters
    let s = s.trim_start_matches(|c: char| !c.is_ascii_digit());
    let num_str: String = s.chars().take_while(|c| c.is_ascii_digit()).collect();
    if num_str.is_empty() {
        None
    } else {
        num_str.parse().ok()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;

    use crate::config::{RejectConfig, RejectMatch, RejectRule};
    use crate::traits::{
        IdempotencyCapability, RateLimit, RemoveCapability, RemoveResult, RowError,
    };

    // ── Mock Destination ──────────────────────────────────────────────

    struct MockDestination {
        name: String,
        max_batch: usize,
        rate_limit: Option<RateLimit>,
        idempotency: IdempotencyCapability,
        remove_cap: RemoveCapability,
        write_result: WriteResult,
    }

    impl MockDestination {
        fn new(name: &str, max_batch: usize) -> Self {
            Self {
                name: name.to_string(),
                max_batch,
                rate_limit: None,
                idempotency: IdempotencyCapability::Idempotent,
                remove_cap: RemoveCapability::None,
                write_result: WriteResult {
                    rows_written: 0,
                    errors: Vec::new(),
                },
            }
        }

        fn with_errors(mut self, errors: Vec<RowError>) -> Self {
            self.write_result = WriteResult {
                rows_written: 0,
                errors,
            };
            self
        }

        fn with_remove_capability(mut self, cap: RemoveCapability) -> Self {
            self.remove_cap = cap;
            self
        }
    }

    #[async_trait]
    impl Destination for MockDestination {
        fn name(&self) -> &str {
            &self.name
        }

        async fn check_connection(&self) -> Result<(), FerryError> {
            Ok(())
        }

        async fn write(
            &self,
            _batch: &RecordBatch,
            _config: &WriteConfig,
        ) -> Result<WriteResult, FerryError> {
            Ok(self.write_result.clone())
        }

        fn max_batch_size(&self) -> usize {
            self.max_batch
        }

        fn rate_limit(&self) -> Option<RateLimit> {
            self.rate_limit.clone()
        }

        fn idempotency(&self) -> IdempotencyCapability {
            self.idempotency.clone()
        }

        fn remove_capability(&self) -> RemoveCapability {
            self.remove_cap.clone()
        }

        async fn remove(
            &self,
            _keys: &[Value],
            _config: &WriteConfig,
        ) -> Result<RemoveResult, FerryError> {
            Ok(RemoveResult {
                rows_removed: _keys.len(),
                errors: Vec::new(),
            })
        }

        async fn replace_all(
            &self,
            _batch: &RecordBatch,
            _config: &WriteConfig,
        ) -> Result<WriteResult, FerryError> {
            Ok(WriteResult {
                rows_written: _batch.num_rows(),
                errors: Vec::new(),
            })
        }
    }

    // ── Mock StateStore ───────────────────────────────────────────────

    struct MockStateStore {
        synced_pks: HashSet<PrimaryKey>,
    }

    impl MockStateStore {
        fn new() -> Self {
            Self {
                synced_pks: HashSet::new(),
            }
        }

        fn with_synced(mut self, pks: &[&str]) -> Self {
            for pk in pks {
                self.synced_pks.insert(pk.to_string());
            }
            self
        }
    }

    #[async_trait]
    impl StateStore for MockStateStore {
        async fn get_hashes(
            &self,
            _sync_name: &str,
        ) -> Result<HashMap<PrimaryKey, u64>, FerryError> {
            Ok(HashMap::new())
        }

        async fn set_hashes(
            &self,
            _sync_name: &str,
            _hashes: &HashMap<PrimaryKey, u64>,
        ) -> Result<(), FerryError> {
            Ok(())
        }

        async fn get_cursor(&self, _sync_name: &str) -> Result<Option<String>, FerryError> {
            Ok(None)
        }

        async fn set_cursor(&self, _sync_name: &str, _value: &str) -> Result<(), FerryError> {
            Ok(())
        }

        async fn get_pending_rows(
            &self,
            _sync_name: &str,
        ) -> Result<Vec<crate::traits::RowEntry>, FerryError> {
            Ok(Vec::new())
        }

        async fn get_dead_rows(
            &self,
            _sync_name: &str,
        ) -> Result<Vec<crate::traits::RowEntry>, FerryError> {
            Ok(Vec::new())
        }

        async fn mark_synced(
            &self,
            _sync_name: &str,
            _primary_keys: &[PrimaryKey],
            _run_id: &str,
        ) -> Result<(), FerryError> {
            Ok(())
        }

        async fn mark_pending(
            &self,
            _sync_name: &str,
            _pk: &PrimaryKey,
            _error: &str,
            _next_retry_at: DateTime<Utc>,
        ) -> Result<(), FerryError> {
            Ok(())
        }

        async fn mark_dead(
            &self,
            _sync_name: &str,
            _pk: &PrimaryKey,
            _error: &str,
        ) -> Result<(), FerryError> {
            Ok(())
        }

        async fn retry_dead_rows(
            &self,
            _sync_name: &str,
            _pks: Option<&[PrimaryKey]>,
        ) -> Result<usize, FerryError> {
            Ok(0)
        }

        async fn purge_dead_rows(
            &self,
            _sync_name: &str,
            _older_than: chrono::Duration,
        ) -> Result<usize, FerryError> {
            Ok(0)
        }

        async fn get_synced_pks(&self, _sync_name: &str) -> Result<Vec<PrimaryKey>, FerryError> {
            Ok(self.synced_pks.iter().cloned().collect())
        }

        async fn get_synced_for_run(
            &self,
            _sync_name: &str,
            _run_id: &str,
        ) -> Result<Vec<PrimaryKey>, FerryError> {
            Ok(Vec::new())
        }

        async fn get_last_completed_run(
            &self,
            _sync_name: &str,
        ) -> Result<Option<crate::traits::SyncRun>, FerryError> {
            Ok(None)
        }

        async fn get_incomplete_runs(
            &self,
            _sync_name: &str,
        ) -> Result<Vec<crate::traits::SyncRun>, FerryError> {
            Ok(Vec::new())
        }

        async fn complete_run(
            &self,
            _sync_name: &str,
            _run_id: &str,
            _rows_synced: usize,
            _rows_failed: usize,
            _rows_retried: usize,
            _rows_dead: usize,
        ) -> Result<(), FerryError> {
            Ok(())
        }

        async fn record_run(&self, _run: &crate::traits::SyncRun) -> Result<(), FerryError> {
            Ok(())
        }

        async fn get_runs(
            &self,
            _sync_name: &str,
            _limit: usize,
        ) -> Result<Vec<crate::traits::SyncRun>, FerryError> {
            Ok(Vec::new())
        }
    }

    // ── Helper to create test batches ─────────────────────────────────

    fn create_test_batch(rows: usize) -> RecordBatch {
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

    // ── split_record_batch tests ──────────────────────────────────────

    #[test]
    fn test_split_record_batch() {
        let batch = create_test_batch(100);
        let chunks = split_record_batch(&batch, 25);
        assert_eq!(chunks.len(), 4);
        for chunk in &chunks {
            assert_eq!(chunk.num_rows(), 25);
        }
        // Verify data integrity: first and last rows
        let first_chunk = &chunks[0];
        let last_chunk = &chunks[3];
        let first_id = extract_pks(first_chunk, "id").unwrap();
        let last_id = extract_pks(last_chunk, "id").unwrap();
        assert_eq!(first_id[0], "pk-0000");
        assert_eq!(last_id[24], "pk-0099");
    }

    #[test]
    fn test_split_uneven() {
        let batch = create_test_batch(103);
        let chunks = split_record_batch(&batch, 25);
        assert_eq!(chunks.len(), 5);
        for (i, chunk) in chunks.iter().enumerate() {
            if i < 4 {
                assert_eq!(chunk.num_rows(), 25);
            } else {
                assert_eq!(chunk.num_rows(), 3);
            }
        }
    }

    #[test]
    fn test_split_empty_batch() {
        let batch = create_test_batch(0);
        let chunks = split_record_batch(&batch, 25);
        assert!(chunks.is_empty());
    }

    // ── RetryPolicy tests ──────────────────────────────────────────────

    #[test]
    fn test_retry_policy_exponential() {
        let policy = RetryPolicy {
            max_attempts: 5,
            backoff: BackoffStrategyExt::Exponential,
            initial_delay: chrono::Duration::seconds(10),
            max_delay: chrono::Duration::seconds(300),
        };

        // Attempt 1: 10s
        let delay1 = policy.compute_delay(1);
        assert_eq!(delay1.num_seconds(), 10);

        // Attempt 2: 20s
        let delay2 = policy.compute_delay(2);
        assert_eq!(delay2.num_seconds(), 20);

        // Attempt 3: 40s
        let delay3 = policy.compute_delay(3);
        assert_eq!(delay3.num_seconds(), 40);

        // Attempt 4: 80s
        let delay4 = policy.compute_delay(4);
        assert_eq!(delay4.num_seconds(), 80);

        // Attempt 5: 160s
        let delay5 = policy.compute_delay(5);
        assert_eq!(delay5.num_seconds(), 160);
    }

    #[test]
    fn test_retry_policy_linear() {
        let policy = RetryPolicy {
            max_attempts: 5,
            backoff: BackoffStrategyExt::Linear,
            initial_delay: chrono::Duration::seconds(10),
            max_delay: chrono::Duration::seconds(300),
        };

        // Attempt 1: 10s
        assert_eq!(policy.compute_delay(1).num_seconds(), 10);
        // Attempt 2: 20s
        assert_eq!(policy.compute_delay(2).num_seconds(), 20);
        // Attempt 3: 30s
        assert_eq!(policy.compute_delay(3).num_seconds(), 30);
    }

    #[test]
    fn test_retry_policy_fixed() {
        let policy = RetryPolicy {
            max_attempts: 5,
            backoff: BackoffStrategyExt::Fixed,
            initial_delay: chrono::Duration::seconds(30),
            max_delay: chrono::Duration::seconds(300),
        };

        // All attempts: 30s
        for i in 1..=5 {
            assert_eq!(policy.compute_delay(i).num_seconds(), 30);
        }
    }

    #[test]
    fn test_retry_policy_max_delay_cap() {
        let policy = RetryPolicy {
            max_attempts: 10,
            backoff: BackoffStrategyExt::Exponential,
            initial_delay: chrono::Duration::seconds(10),
            max_delay: chrono::Duration::seconds(60),
        };

        // Attempt 1: 10s
        assert_eq!(policy.compute_delay(1).num_seconds(), 10);
        // Attempt 2: 20s
        assert_eq!(policy.compute_delay(2).num_seconds(), 20);
        // Attempt 3: 40s
        assert_eq!(policy.compute_delay(3).num_seconds(), 40);
        // Attempt 4: capped at 60s
        assert_eq!(policy.compute_delay(4).num_seconds(), 60);
        // Attempt 5: still capped at 60s
        assert_eq!(policy.compute_delay(5).num_seconds(), 60);
    }

    #[test]
    fn test_retry_policy_jitter_range() {
        let policy = RetryPolicy {
            max_attempts: 3,
            backoff: BackoffStrategyExt::Fixed,
            initial_delay: chrono::Duration::seconds(100),
            max_delay: chrono::Duration::seconds(300),
        };

        // Jitter should be 0-10% of 100s = 0-10000ms
        for _ in 0..100 {
            let jitter = policy.jitter(chrono::Duration::seconds(100));
            let ms = jitter.num_milliseconds();
            assert!(ms >= 0, "Jitter should be non-negative");
            assert!(ms <= 10000, "Jitter should be at most 10% of delay");
        }
    }

    // ── classify_error tests ───────────────────────────────────────────

    #[test]
    fn test_classify_error_default_retry() {
        let config = RejectConfig { classify: vec![] };
        let error = RowError {
            primary_key: "pk1".to_string(),
            error: "HTTP 500 Internal Server Error".to_string(),
        };
        let action = classify_error(&error, &config);
        assert_eq!(action, RejectAction::Retry);
    }

    #[test]
    fn test_classify_error_default_dead_letter() {
        let config = RejectConfig { classify: vec![] };
        let error = RowError {
            primary_key: "pk1".to_string(),
            error: "HTTP 400 Bad Request: invalid field".to_string(),
        };
        let action = classify_error(&error, &config);
        assert_eq!(action, RejectAction::DeadLetter);
    }

    #[test]
    fn test_classify_error_rule_match_429() {
        let config = RejectConfig {
            classify: vec![RejectRule {
                match_: RejectMatch {
                    status_code: Some(429),
                    body_contains: None,
                },
                action: RejectAction::Retry,
            }],
        };
        let error = RowError {
            primary_key: "pk1".to_string(),
            error: "HTTP 429 Too Many Requests".to_string(),
        };
        let action = classify_error(&error, &config);
        assert_eq!(action, RejectAction::Retry);
    }

    #[test]
    fn test_classify_error_rule_match_400() {
        let config = RejectConfig {
            classify: vec![RejectRule {
                match_: RejectMatch {
                    status_code: Some(400),
                    body_contains: Some("invalid_email".to_string()),
                },
                action: RejectAction::DeadLetter,
            }],
        };
        let error = RowError {
            primary_key: "pk1".to_string(),
            error: "HTTP 400 Bad Request: invalid_email".to_string(),
        };
        let action = classify_error(&error, &config);
        assert_eq!(action, RejectAction::DeadLetter);
    }

    #[test]
    fn test_classify_error_rule_no_match_different_status() {
        let config = RejectConfig {
            classify: vec![RejectRule {
                match_: RejectMatch {
                    status_code: Some(429),
                    body_contains: None,
                },
                action: RejectAction::Retry,
            }],
        };
        let error = RowError {
            primary_key: "pk1".to_string(),
            error: "HTTP 500 Server Error".to_string(),
        };
        let action = classify_error(&error, &config);
        // No rule matches 500, so default: 5xx → Retry
        assert_eq!(action, RejectAction::Retry);
    }

    #[test]
    fn test_classify_error_rule_match_body_contains() {
        let config = RejectConfig {
            classify: vec![RejectRule {
                match_: RejectMatch {
                    status_code: None,
                    body_contains: Some("rate_limit_exceeded".to_string()),
                },
                action: RejectAction::Retry,
            }],
        };
        let error = RowError {
            primary_key: "pk1".to_string(),
            error: "rate_limit_exceeded: too many requests".to_string(),
        };
        let action = classify_error(&error, &config);
        assert_eq!(action, RejectAction::Retry);
    }

    // ── filter_undelivered tests ──────────────────────────────────────

    #[test]
    fn test_filter_undelivered() {
        let batch = create_test_batch(10);
        let synced: HashSet<PrimaryKey> = ["pk-0000", "pk-0002", "pk-0005"]
            .iter()
            .map(|s| s.to_string())
            .collect();

        let filtered = filter_undelivered(&batch, "id", &synced).unwrap();
        assert_eq!(filtered.num_rows(), 7);

        // Verify the filtered rows don't include synced PKs
        let pks = extract_pks(&filtered, "id").unwrap();
        for pk in &pks {
            assert!(
                !synced.contains(pk),
                "Filtered batch should not contain synced PK {pk}"
            );
        }
        // Verify the correct rows remain
        assert!(pks.contains(&"pk-0001".to_string()));
        assert!(pks.contains(&"pk-0009".to_string()));
    }

    #[test]
    fn test_filter_undelivered_all_synced() {
        let batch = create_test_batch(5);
        let synced: HashSet<PrimaryKey> = (0..5).map(|i| format!("pk-{:04}", i)).collect();

        let filtered = filter_undelivered(&batch, "id", &synced).unwrap();
        assert_eq!(filtered.num_rows(), 0);
    }

    #[test]
    fn test_filter_undelivered_none_synced() {
        let batch = create_test_batch(5);
        let synced = HashSet::new();

        let filtered = filter_undelivered(&batch, "id", &synced).unwrap();
        assert_eq!(filtered.num_rows(), 5);
    }

    // ── extract_pks tests ──────────────────────────────────────────────

    #[test]
    fn test_extract_pks() {
        let batch = create_test_batch(5);
        let pks = extract_pks(&batch, "id").unwrap();
        assert_eq!(pks.len(), 5);
        assert_eq!(pks[0], "pk-0000");
        assert_eq!(pks[4], "pk-0004");
    }

    #[test]
    fn test_extract_pks_missing_column() {
        let batch = create_test_batch(5);
        let result = extract_pks(&batch, "nonexistent");
        assert!(result.is_err());
    }

    // ── extract_status_code tests ─────────────────────────────────────

    #[test]
    fn test_extract_status_code_429() {
        assert_eq!(extract_status_code("HTTP 429 Too Many Requests"), Some(429));
    }

    #[test]
    fn test_extract_status_code_500() {
        assert_eq!(extract_status_code("status: 500"), Some(500));
    }

    #[test]
    fn test_extract_status_code_none() {
        assert_eq!(extract_status_code("connection timeout"), None);
    }

    // ── extract_retry_after tests ──────────────────────────────────────

    #[test]
    fn test_extract_retry_after_found() {
        let result = WriteResult {
            rows_written: 0,
            errors: vec![RowError {
                primary_key: "pk1".to_string(),
                error: "HTTP 429 retry_after: 30".to_string(),
            }],
        };
        let duration = extract_retry_after(&result);
        assert!(duration.is_some());
        assert_eq!(duration.unwrap().as_secs(), 30);
    }

    #[test]
    fn test_extract_retry_after_not_found() {
        let result = WriteResult {
            rows_written: 0,
            errors: vec![RowError {
                primary_key: "pk1".to_string(),
                error: "HTTP 500 Server Error".to_string(),
            }],
        };
        assert!(extract_retry_after(&result).is_none());
    }

    // ── DeliveryPipeline integration tests ─────────────────────────────

    #[tokio::test]
    async fn test_deliver_empty_batch() {
        let dest = MockDestination::new("test", 100);
        let state = MockStateStore::new();
        let policy = RetryPolicy {
            max_attempts: 3,
            backoff: BackoffStrategyExt::Exponential,
            initial_delay: chrono::Duration::seconds(5),
            max_delay: chrono::Duration::seconds(300),
        };

        let pipeline = DeliveryPipeline::new(
            &dest,
            &state,
            policy,
            false,
            "id".to_string(),
            "test_sync".to_string(),
        );

        let empty_batch = create_test_batch(0);
        let result = pipeline
            .deliver(&empty_batch, None, "run-001")
            .await
            .unwrap();
        assert_eq!(result.rows_synced, 0);
        assert_eq!(result.rows_pending, 0);
        assert_eq!(result.rows_dead, 0);
    }

    #[tokio::test]
    async fn test_deliver_with_errors_classified() {
        let errors = vec![
            RowError {
                primary_key: "pk-0000".to_string(),
                error: "HTTP 429 Too Many Requests".to_string(),
            },
            RowError {
                primary_key: "pk-0001".to_string(),
                error: "HTTP 400 Bad Request: invalid_email".to_string(),
            },
        ];

        let dest = MockDestination::new("test", 100).with_errors(errors);
        let state = MockStateStore::new();
        let policy = RetryPolicy {
            max_attempts: 3,
            backoff: BackoffStrategyExt::Exponential,
            initial_delay: chrono::Duration::seconds(5),
            max_delay: chrono::Duration::seconds(300),
        };

        let reject_config = RejectConfig {
            classify: vec![
                RejectRule {
                    match_: RejectMatch {
                        status_code: Some(429),
                        body_contains: None,
                    },
                    action: RejectAction::Retry,
                },
                RejectRule {
                    match_: RejectMatch {
                        status_code: Some(400),
                        body_contains: Some("invalid_email".to_string()),
                    },
                    action: RejectAction::DeadLetter,
                },
            ],
        };

        let pipeline = DeliveryPipeline::new(
            &dest,
            &state,
            policy,
            false,
            "id".to_string(),
            "test_sync".to_string(),
        );

        let batch = create_test_batch(10);
        let result = pipeline
            .deliver(&batch, Some(&reject_config), "run-001")
            .await
            .unwrap();

        // 8 rows should be synced (10 total - 2 errors)
        assert_eq!(result.rows_synced, 8);
        // pk-0000 (429) → Retry → pending
        assert_eq!(result.rows_pending, 1);
        // pk-0001 (400 + invalid_email) → DeadLetter
        assert_eq!(result.rows_dead, 1);
    }

    #[tokio::test]
    async fn test_deliver_skips_already_synced() {
        let dest = MockDestination::new("test", 100);
        let state = MockStateStore::new().with_synced(&["pk-0000", "pk-0001", "pk-0002"]);
        let policy = RetryPolicy {
            max_attempts: 3,
            backoff: BackoffStrategyExt::Exponential,
            initial_delay: chrono::Duration::seconds(5),
            max_delay: chrono::Duration::seconds(300),
        };

        let pipeline = DeliveryPipeline::new(
            &dest,
            &state,
            policy,
            false,
            "id".to_string(),
            "test_sync".to_string(),
        );

        let batch = create_test_batch(10);
        let result = pipeline.deliver(&batch, None, "run-001").await.unwrap();
        // 7 rows should be synced (10 - 3 already synced)
        assert_eq!(result.rows_synced, 7);
    }

    #[tokio::test]
    async fn test_deliver_with_allow_redelivery() {
        let dest = MockDestination::new("test", 100);
        let state = MockStateStore::new().with_synced(&["pk-0000", "pk-0001"]);
        let policy = RetryPolicy {
            max_attempts: 3,
            backoff: BackoffStrategyExt::Exponential,
            initial_delay: chrono::Duration::seconds(5),
            max_delay: chrono::Duration::seconds(300),
        };

        let pipeline = DeliveryPipeline::new(
            &dest,
            &state,
            policy,
            true,
            "id".to_string(),
            "test_sync".to_string(),
        );

        let batch = create_test_batch(10);
        let result = pipeline.deliver(&batch, None, "run-001").await.unwrap();
        // All 10 rows should be synced (allow_redelivery = true)
        assert_eq!(result.rows_synced, 10);
    }

    #[tokio::test]
    async fn test_deliver_mirror_remove_by_key() {
        let dest =
            MockDestination::new("test", 100).with_remove_capability(RemoveCapability::RemoveByKey);
        let state = MockStateStore::new();
        let policy = RetryPolicy {
            max_attempts: 3,
            backoff: BackoffStrategyExt::Exponential,
            initial_delay: chrono::Duration::seconds(5),
            max_delay: chrono::Duration::seconds(300),
        };

        let pipeline = DeliveryPipeline::new(
            &dest,
            &state,
            policy,
            false,
            "id".to_string(),
            "test_sync".to_string(),
        );

        let batch = create_test_batch(5);
        let removed = vec![Value::String("pk-0000".to_string())];
        let result = pipeline
            .deliver_mirror(&batch, &removed, None, "run-001")
            .await
            .unwrap();

        assert_eq!(result.rows_synced, 5);
    }

    #[tokio::test]
    async fn test_deliver_mirror_remove_all() {
        let dest =
            MockDestination::new("test", 100).with_remove_capability(RemoveCapability::RemoveAll);
        let state = MockStateStore::new();
        let policy = RetryPolicy {
            max_attempts: 3,
            backoff: BackoffStrategyExt::Exponential,
            initial_delay: chrono::Duration::seconds(5),
            max_delay: chrono::Duration::seconds(300),
        };

        let pipeline = DeliveryPipeline::new(
            &dest,
            &state,
            policy,
            false,
            "id".to_string(),
            "test_sync".to_string(),
        );

        let batch = create_test_batch(5);
        let result = pipeline
            .deliver_mirror(&batch, &[], None, "run-001")
            .await
            .unwrap();

        assert_eq!(result.rows_synced, 5);
    }

    #[tokio::test]
    async fn test_deliver_mirror_no_capability() {
        let dest = MockDestination::new("test", 100).with_remove_capability(RemoveCapability::None);
        let state = MockStateStore::new();
        let policy = RetryPolicy {
            max_attempts: 3,
            backoff: BackoffStrategyExt::Exponential,
            initial_delay: chrono::Duration::seconds(5),
            max_delay: chrono::Duration::seconds(300),
        };

        let pipeline = DeliveryPipeline::new(
            &dest,
            &state,
            policy,
            false,
            "id".to_string(),
            "test_sync".to_string(),
        );

        let batch = create_test_batch(5);
        let result = pipeline
            .deliver_mirror(&batch, &[], None, "run-001")
            .await
            .unwrap();

        assert_eq!(result.rows_synced, 5);
    }
}
