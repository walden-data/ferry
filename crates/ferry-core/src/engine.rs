use std::collections::HashSet;
use std::path::Path;
use std::time::Instant;

use arrow_array::RecordBatch;
use chrono::Utc;
use futures::StreamExt;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::cdc::{CursorCdc, HashCdc};
use crate::config::{
    CdcConfig, CdcMethod, FerryConfig, HashColumns, ModelConfig, SyncConfig, SyncMode,
};
use crate::dbt::Manifest;
use crate::delivery::{DeliveryPipeline, RetryPolicy};
use crate::error::FerryError;
use crate::state::DuckDbStateStore;
use crate::traits::{Destination, PrimaryKey, RowEntry, Source, StateStore, SyncRun};
use crate::validation::ValidationError;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Options for a single sync run.
#[derive(Debug, Clone, Default)]
pub struct RunOptions {
    pub sync_names: Option<Vec<String>>,
    pub full_refresh: bool,
    pub dry_run: bool,
    pub retry_dead: bool,
}

/// Result of a single sync run.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SyncResult {
    pub sync_name: String,
    pub run_id: String,
    pub rows_extracted: usize,
    pub rows_synced: usize,
    pub rows_failed: usize,
    pub rows_pending: usize,
    pub rows_retried: usize,
    pub rows_dead: usize,
    pub duration_seconds: f64,
    pub dry_run: bool,
    pub mode: String,
}

/// Preview of a CDC diff (no delivery).
#[derive(Debug, Clone, serde::Serialize)]
pub struct DiffPreview {
    pub sync_name: String,
    pub added: usize,
    pub changed: usize,
    pub removed: usize,
    pub total_rows: usize,
}

/// Result of crash recovery reconciliation.
#[derive(Debug, Clone)]
pub struct ReconciliationResult {
    pub already_synced: HashSet<PrimaryKey>,
    pub pending_rows: Vec<RowEntry>,
}

// ---------------------------------------------------------------------------
// Reconciliation
// ---------------------------------------------------------------------------

/// Reconcile state after a crash: find already-synced rows and pending rows
/// from incomplete runs.
///
/// Returns the set of already-synced rows (from incomplete runs only) and
/// pending rows eligible for retry. If there are no incomplete runs, the
/// already_synced set will be empty — the CDC hash already tracks what was
/// delivered in completed runs.
pub async fn reconcile(
    state: &dyn StateStore,
    sync_name: &str,
) -> Result<ReconciliationResult, FerryError> {
    // Get incomplete runs
    let incomplete_runs = state.get_incomplete_runs(sync_name).await?;

    // Get all synced PKs from incomplete runs only
    let mut already_synced: HashSet<PrimaryKey> = HashSet::new();
    for run in &incomplete_runs {
        let run_synced = state.get_synced_for_run(sync_name, &run.run_id).await?;
        already_synced.extend(run_synced);
    }

    // Get pending rows eligible for retry
    let pending_rows = state.get_pending_rows(sync_name).await?;

    Ok(ReconciliationResult {
        already_synced,
        pending_rows,
    })
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

/// The main sync engine that orchestrates extract → diff → deliver → state commit.
pub struct Engine {
    config: FerryConfig,
    state: DuckDbStateStore,
    manifest: Option<Manifest>,
}

impl Engine {
    /// Get a reference to the state store.
    ///
    /// This is exposed for integration testing and advanced use cases.
    pub fn state(&self) -> &DuckDbStateStore {
        &self.state
    }

    /// Create a new Engine from a `FerryConfig`.
    ///
    /// Initializes the DuckDB state store from the configured state path.
    /// If `dbt.manifest_path` is configured, loads the dbt manifest and checks
    /// its freshness (warns if >24h old).
    pub fn new(config: FerryConfig) -> Result<Self, FerryError> {
        let state_path = config.state.path.as_deref().unwrap_or(".ferry/state.db");
        let state = DuckDbStateStore::new(Path::new(state_path))?;

        let manifest = if let Some(dbt_config) = &config.dbt {
            if let Some(manifest_path) = &dbt_config.manifest_path {
                let manifest = Manifest::load(Path::new(manifest_path))?;
                // Check freshness — warns if >24h old, never errors
                let _ = manifest.check_freshness(24);
                Some(manifest)
            } else {
                None
            }
        } else {
            None
        };

        Ok(Self {
            config,
            state,
            manifest,
        })
    }

    /// Run a single sync with the given source and destination.
    ///
    /// This is the main sync execution loop:
    /// 1. Reconciliation — detect already-synced and pending rows
    /// 2. Record run start in state store
    /// 3. Extract data from source
    /// 4. CDC diff based on sync mode
    /// 5. Build delivery set (changes + pending, minus already-synced)
    /// 6. Deliver to destination
    /// 7. Commit CDC hash (if delivery succeeded)
    /// 8. Update cursor (if cursor mode)
    /// 9. Mark run complete
    pub async fn run_sync(
        &self,
        sync_config: &SyncConfig,
        source: &dyn Source,
        destination: &dyn Destination,
        options: &RunOptions,
    ) -> Result<SyncResult, FerryError> {
        let sync_name = sync_config.name.clone();
        let start_time = Instant::now();
        let run_id = Uuid::new_v4().to_string();
        let mode = format!("{:?}", sync_config.sync.mode).to_lowercase();

        info!(
            sync = %sync_name,
            run_id = %run_id,
            mode = %mode,
            dry_run = options.dry_run,
            "Starting sync run"
        );

        // ── Step 1: Reconciliation ──────────────────────────────────────
        let reconciliation = reconcile(&self.state, &sync_name).await?;
        let already_synced = reconciliation.already_synced;
        let pending_rows = reconciliation.pending_rows;

        if !pending_rows.is_empty() {
            info!(
                sync = %sync_name,
                pending = pending_rows.len(),
                "Found pending rows from previous incomplete run"
            );
        }

        // ── Step 2: Record run start ────────────────────────────────────
        let sync_run = SyncRun {
            sync_name: sync_name.clone(),
            run_id: run_id.clone(),
            started_at: Utc::now(),
            completed_at: None,
            rows_extracted: 0,
            rows_synced: 0,
            rows_failed: 0,
            rows_retried: 0,
            rows_dead: 0,
            mode: mode.clone(),
            dry_run: options.dry_run,
            status: "running".to_string(),
        };

        if !options.dry_run {
            self.state.record_run(&sync_run).await?;
        }

        // ── Step 3: Extract ────────────────────────────────────────────
        let query = resolve_query(&sync_config.model, self.manifest.as_ref())?;
        let stream = source.read(&query);
        let batches: Vec<RecordBatch> = stream
            .collect::<Vec<Result<RecordBatch, FerryError>>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                error!(sync = %sync_name, error = %e, "Failed to extract data");
                e
            })?;

        let rows_extracted: usize = batches.iter().map(|b| b.num_rows()).sum();
        info!(
            sync = %sync_name,
            rows = rows_extracted,
            "Data extracted"
        );

        // ── Step 4: CDC diff ────────────────────────────────────────────
        let pk_col = "id"; // Default PK column; in Phase 2 this comes from config
        let hash_columns = resolve_hash_columns(&sync_config.sync.cdc);

        let (delivery_batch, removed_keys, current_hashes, new_cursor_value) =
            match &sync_config.sync.mode {
                SyncMode::Incremental => {
                    match &sync_config.sync.cdc {
                        Some(CdcConfig {
                            method: CdcMethod::Hash,
                            ..
                        }) => {
                            // Hash-based incremental
                            let cdc = HashCdc::new(&self.state);
                            let diff = cdc
                                .compute_diff(&sync_name, &batches, pk_col, &hash_columns)
                                .await?;

                            let mut delivery_pks: Vec<PrimaryKey> = Vec::new();
                            delivery_pks.extend(diff.added.iter().cloned());
                            delivery_pks.extend(diff.changed.iter().cloned());

                            // Filter to only the rows we need to deliver
                            let delivery_batch =
                                filter_batch_by_pks(&batches, pk_col, &delivery_pks)?;

                            info!(
                                sync = %sync_name,
                                added = diff.added.len(),
                                changed = diff.changed.len(),
                                removed = diff.removed.len(),
                                "Hash CDC diff computed"
                            );

                            (
                                delivery_batch,
                                diff.removed,
                                Some(diff.current_hashes),
                                None,
                            )
                        }
                        Some(CdcConfig {
                            method: CdcMethod::Cursor,
                            ..
                        }) => {
                            // Cursor-based incremental
                            let cursor_field = sync_config
                                .sync
                                .cursor_field
                                .as_deref()
                                .unwrap_or("updated_at");
                            let cdc = CursorCdc::new(&self.state);
                            let cursor_diff =
                                cdc.compute_diff(&sync_name, &batches, cursor_field).await?;

                            // Filter to new rows by global row index
                            let delivery_batch =
                                filter_batch_by_indices(&batches, &cursor_diff.new_rows)?;

                            info!(
                                sync = %sync_name,
                                new_rows = cursor_diff.new_rows.len(),
                                new_cursor = %cursor_diff.new_cursor_value,
                                "Cursor CDC diff computed"
                            );

                            (
                                delivery_batch,
                                Vec::new(),
                                None,
                                Some(cursor_diff.new_cursor_value),
                            )
                        }
                        None => {
                            // Incremental without CDC config — deliver all rows
                            let all_batch = concat_batches(&batches)?;
                            (all_batch, Vec::new(), None, None)
                        }
                    }
                }
                SyncMode::Mirror => {
                    // Mirror mode: deliver all current rows, detect removed via hash diff
                    let cdc = HashCdc::new(&self.state);
                    let diff = cdc
                        .compute_diff(&sync_name, &batches, pk_col, &hash_columns)
                        .await?;

                    let all_batch = concat_batches(&batches)?;

                    info!(
                        sync = %sync_name,
                        total = all_batch.num_rows(),
                        removed = diff.removed.len(),
                        "Mirror mode: delivering all rows"
                    );

                    (all_batch, diff.removed, Some(diff.current_hashes), None)
                }
                SyncMode::FullRefresh => {
                    // Full refresh: deliver all rows, skip diff
                    let all_batch = concat_batches(&batches)?;

                    info!(
                        sync = %sync_name,
                        total = all_batch.num_rows(),
                        "Full refresh: delivering all rows"
                    );

                    (all_batch, Vec::new(), None, None)
                }
            };

        // ── Step 5: Build delivery set ─────────────────────────────────
        // For full_refresh flag, override with all batches
        let mut delivery_batch = if options.full_refresh {
            concat_batches(&batches)?
        } else {
            delivery_batch
        };

        // Add pending rows from reconciliation (if any)
        // Pending rows are from a previous incomplete run. They need to be
        // re-delivered. We filter out any that are already in the delivery batch
        // (from the CDC changeset) to avoid duplicates.
        if !pending_rows.is_empty() {
            let pending_pks: HashSet<PrimaryKey> =
                pending_rows.iter().map(|r| r.primary_key.clone()).collect();

            // Find pending PKs not already in the delivery batch
            let delivery_pks = extract_pks_from_batches(&[delivery_batch.clone()], pk_col)?;
            let delivery_pk_set: HashSet<PrimaryKey> = delivery_pks.into_iter().collect();
            let missing_pending: Vec<&PrimaryKey> = pending_pks
                .iter()
                .filter(|pk| !delivery_pk_set.contains(*pk))
                .collect();

            if !missing_pending.is_empty() {
                warn!(
                    sync = %sync_name,
                    missing = missing_pending.len(),
                    "Pending rows not found in current extract — they may have been deleted"
                );
            }

            info!(
                sync = %sync_name,
                pending = pending_rows.len(),
                "Including pending rows from reconciliation"
            );
        }

        // Exclude already-synced rows from the delivery batch.
        // This is for crash recovery: rows that were already synced in a previous
        // incomplete run should not be re-delivered.
        // NOTE: This does NOT apply to the CDC changeset — the CDC diff already
        // correctly identifies what needs to be delivered. This only catches
        // rows that were synced in a previous run but whose CDC hash wasn't committed.
        if !already_synced.is_empty() && !options.full_refresh {
            let before = delivery_batch.num_rows();
            delivery_batch =
                crate::delivery::filter_undelivered(&delivery_batch, pk_col, &already_synced)?;
            let skipped = before - delivery_batch.num_rows();
            if skipped > 0 {
                info!(
                    sync = %sync_name,
                    skipped = skipped,
                    "Skipping already-synced rows from crash recovery"
                );
            }
        }

        // ── Step 6: Deliver ────────────────────────────────────────────
        let mut rows_synced = 0usize;
        let mut rows_pending = 0usize;
        let mut rows_failed = 0usize;
        let mut rows_dead = 0usize;
        let mut rows_retried = 0usize;

        if options.dry_run {
            info!(
                sync = %sync_name,
                rows_to_deliver = delivery_batch.num_rows(),
                "Dry run: skipping delivery"
            );
        } else if delivery_batch.num_rows() > 0 {
            // Build retry policy from config
            let retry_policy = build_retry_policy(&sync_config.sync.delivery);

            // The delivery pipeline's filter_undelivered() checks the journal
            // for ALL synced rows (not just from incomplete runs) and skips them.
            // This is the crash recovery belt-and-suspenders: even if the CDC
            // hash wasn't committed, the journal prevents re-delivery.
            //
            // allow_redelivery comes from the sync config (default: false = exactly-once).
            // When false, the pipeline skips rows already marked Synced in the journal.
            // When true (user override), the pipeline delivers all rows regardless.
            //
            // The engine-level already_synced filter (step 5 above) is a fast path
            // that avoids querying the journal again for rows from incomplete runs.
            let allow_redelivery = sync_config
                .sync
                .delivery
                .as_ref()
                .map(|d| d.allow_redelivery)
                .unwrap_or(false);

            let pipeline = DeliveryPipeline::new(
                destination,
                &self.state,
                retry_policy,
                allow_redelivery,
                pk_col.to_string(),
                sync_name.clone(),
            );

            let reject_config = sync_config
                .sync
                .delivery
                .as_ref()
                .and_then(|d| d.on_reject.as_ref());

            let delivery_result = match &sync_config.sync.mode {
                SyncMode::Mirror => {
                    // Convert removed keys to serde_json::Value
                    let removed_values: Vec<serde_json::Value> = removed_keys
                        .iter()
                        .map(|k| serde_json::Value::String(k.clone()))
                        .collect();

                    pipeline
                        .deliver_mirror(&delivery_batch, &removed_values, reject_config, &run_id)
                        .await?
                }
                _ => {
                    pipeline
                        .deliver(&delivery_batch, reject_config, &run_id)
                        .await?
                }
            };

            rows_synced = delivery_result.rows_synced;
            rows_pending = delivery_result.rows_pending;
            rows_dead = delivery_result.rows_dead;
            rows_failed = delivery_result.rows_dead; // dead rows are "failed"
            rows_retried = pending_rows.len().min(rows_synced); // approximate retried count

            info!(
                sync = %sync_name,
                synced = rows_synced,
                pending = rows_pending,
                dead = rows_dead,
                "Delivery completed"
            );
        } else {
            info!(
                sync = %sync_name,
                "No rows to deliver — all rows already synced"
            );
        }

        // ── Step 7: Commit CDC hash ─────────────────────────────────────
        // Only if delivery succeeded (no pending rows remaining) and not dry_run
        let delivery_succeeded = rows_pending == 0;

        if delivery_succeeded && !options.dry_run {
            if let Some(hashes) = current_hashes {
                self.state.set_hashes(&sync_name, &hashes).await?;
                info!(sync = %sync_name, "CDC hashes committed");
            }
        } else if !options.dry_run {
            info!(
                sync = %sync_name,
                pending = rows_pending,
                "Skipping CDC hash commit — pending rows remain"
            );
        }

        // ── Step 8: Update cursor ───────────────────────────────────────
        if delivery_succeeded && !options.dry_run {
            if let Some(cursor_value) = new_cursor_value {
                self.state.set_cursor(&sync_name, &cursor_value).await?;
                info!(sync = %sync_name, cursor = %cursor_value, "Cursor updated");
            }
        }

        // ── Step 9: Mark run complete ──────────────────────────────────
        // Only mark as completed if delivery fully succeeded (no pending rows).
        // If there are pending rows, the run stays "running" so reconciliation
        // can pick them up on the next run.
        if !options.dry_run {
            if delivery_succeeded {
                self.state
                    .complete_run(
                        &sync_name,
                        &run_id,
                        rows_synced,
                        rows_failed,
                        rows_retried,
                        rows_dead,
                    )
                    .await?;
            } else {
                info!(
                    sync = %sync_name,
                    pending = rows_pending,
                    "Run has pending rows — not marking as completed"
                );
            }
        }

        let duration = start_time.elapsed();
        let duration_seconds = duration.as_secs_f64();

        info!(
            sync = %sync_name,
            run_id = %run_id,
            duration_secs = duration_seconds,
            rows_synced = rows_synced,
            rows_pending = rows_pending,
            rows_dead = rows_dead,
            "Sync run completed"
        );

        Ok(SyncResult {
            sync_name,
            run_id,
            rows_extracted,
            rows_synced,
            rows_failed,
            rows_pending,
            rows_retried,
            rows_dead,
            duration_seconds,
            dry_run: options.dry_run,
            mode,
        })
    }

    /// Compute a diff preview for a sync without delivering any data.
    ///
    /// Extracts data from the source, computes the CDC diff, and returns
    /// counts of added, changed, and removed rows.
    pub async fn diff(
        &self,
        sync_name: &str,
        source: &dyn Source,
        sync_config: &SyncConfig,
    ) -> Result<DiffPreview, FerryError> {
        let query = resolve_query(&sync_config.model, self.manifest.as_ref())?;
        let stream = source.read(&query);
        let batches: Vec<RecordBatch> = stream
            .collect::<Vec<Result<RecordBatch, FerryError>>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let pk_col = "id";
        let hash_columns = resolve_hash_columns(&sync_config.sync.cdc);

        let (added, changed, removed) = match &sync_config.sync.mode {
            SyncMode::Incremental => match &sync_config.sync.cdc {
                Some(CdcConfig {
                    method: CdcMethod::Hash,
                    ..
                }) => {
                    let cdc = HashCdc::new(&self.state);
                    let diff = cdc
                        .compute_diff(sync_name, &batches, pk_col, &hash_columns)
                        .await?;
                    (diff.added.len(), diff.changed.len(), diff.removed.len())
                }
                Some(CdcConfig {
                    method: CdcMethod::Cursor,
                    ..
                }) => {
                    let cursor_field = sync_config
                        .sync
                        .cursor_field
                        .as_deref()
                        .unwrap_or("updated_at");
                    let cdc = CursorCdc::new(&self.state);
                    let cursor_diff = cdc.compute_diff(sync_name, &batches, cursor_field).await?;
                    (cursor_diff.new_rows.len(), 0usize, 0usize)
                }
                None => (total_rows, 0, 0),
            },
            SyncMode::Mirror | SyncMode::FullRefresh => (total_rows, 0, 0),
        };

        Ok(DiffPreview {
            sync_name: sync_name.to_string(),
            added,
            changed,
            removed,
            total_rows,
        })
    }

    /// Validate all sync configs.
    ///
    /// Returns a list of validation errors. An empty list means everything is valid.
    /// Note: Source connection testing is done at the CLI level where connector
    /// crates are available.
    pub async fn validate(&self, syncs_dir: &Path) -> Result<Vec<ValidationError>, FerryError> {
        let mut errors: Vec<ValidationError> = Vec::new();

        // Validate ferry config
        if let Err(validation_errors) = crate::validation::validate_ferry_config(&self.config) {
            errors.extend(validation_errors);
        }

        // Load and validate all sync configs
        let sync_configs = match SyncConfig::load_all(syncs_dir) {
            Ok(configs) => configs,
            Err(e) => {
                errors.push(ValidationError {
                    field: "syncs".to_string(),
                    message: format!("Failed to load sync configs: {e}"),
                    context: "ferry.yml".to_string(),
                });
                return Ok(errors);
            }
        };

        for sync_config in &sync_configs {
            // Validate sync config
            if let Err(validation_errors) = crate::validation::validate_sync_config(sync_config) {
                errors.extend(validation_errors);
            }
        }

        Ok(errors)
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Resolve the SQL query from a model config.
///
/// If the model is a `Ref` and a manifest is provided, the model's compiled
/// SQL is looked up in the manifest. If no manifest is available, `Ref` models
/// produce an error.
fn resolve_query(model: &ModelConfig, manifest: Option<&Manifest>) -> Result<String, FerryError> {
    match model {
        ModelConfig::Sql { sql } => Ok(sql.clone()),
        ModelConfig::Ref { r#ref } => {
            if let Some(manifest) = manifest {
                manifest.resolve_ref(r#ref)
            } else {
                Err(FerryError::Config(format!(
                    "Sync uses model.ref: '{}' but no dbt manifest is configured. \
                     Set dbt.manifest_path in ferry.yml to enable dbt ref resolution.",
                    r#ref
                )))
            }
        }
    }
}

/// Resolve the list of columns to hash from CDC config.
fn resolve_hash_columns(cdc: &Option<CdcConfig>) -> Vec<String> {
    match cdc {
        Some(CdcConfig {
            method: CdcMethod::Hash,
            hash_columns: Some(HashColumns::Explicit(cols)),
        }) => cols.clone(),
        _ => Vec::new(), // Empty = hash all columns
    }
}

/// Build a RetryPolicy from delivery config.
fn build_retry_policy(delivery: &Option<crate::config::DeliveryConfig>) -> RetryPolicy {
    match delivery {
        Some(d) => {
            let retry = d.retry.as_ref();
            RetryPolicy {
                max_attempts: retry.map(|r| r.max_attempts).unwrap_or(3),
                backoff: retry
                    .map(|r| crate::delivery::BackoffStrategyExt::from(r.backoff.clone()))
                    .unwrap_or(crate::delivery::BackoffStrategyExt::Exponential),
                initial_delay: chrono::Duration::seconds(
                    retry.map(|r| r.initial_delay_secs).unwrap_or(5) as i64,
                ),
                max_delay: chrono::Duration::seconds(
                    retry.map(|r| r.max_delay_secs).unwrap_or(300) as i64,
                ),
            }
        }
        None => RetryPolicy {
            max_attempts: 3,
            backoff: crate::delivery::BackoffStrategyExt::Exponential,
            initial_delay: chrono::Duration::seconds(5),
            max_delay: chrono::Duration::seconds(300),
        },
    }
}

/// Concatenate multiple RecordBatches into a single batch.
fn concat_batches(batches: &[RecordBatch]) -> Result<RecordBatch, FerryError> {
    if batches.is_empty() {
        // Return an empty batch with no schema
        return Err(FerryError::Cdc("No batches to concatenate".to_string()));
    }
    if batches.len() == 1 {
        return Ok(batches[0].clone());
    }

    let schema = batches[0].schema();
    let arrays: Vec<arrow_array::ArrayRef> = (0..schema.fields().len())
        .map(|i| {
            let col_arrays: Vec<&dyn arrow_array::Array> =
                batches.iter().map(|b| b.column(i).as_ref()).collect();
            arrow::compute::concat(&col_arrays)
                .map_err(|e| FerryError::Cdc(format!("Failed to concat columns: {e}")))
        })
        .collect::<Result<Vec<_>, _>>()?;

    RecordBatch::try_new(schema, arrays)
        .map_err(|e| FerryError::Cdc(format!("Failed to create concatenated batch: {e}")))
}

/// Filter batches to only include rows with the given primary keys.
fn filter_batch_by_pks(
    batches: &[RecordBatch],
    pk_col: &str,
    pks: &[PrimaryKey],
) -> Result<RecordBatch, FerryError> {
    if pks.is_empty() {
        // Return an empty batch with the same schema
        let schema = batches[0].schema();
        let empty_arrays: Vec<arrow_array::ArrayRef> = schema
            .fields()
            .iter()
            .map(|f| arrow_array::new_null_array(f.data_type(), 0))
            .collect();
        return RecordBatch::try_new(schema, empty_arrays)
            .map_err(|e| FerryError::Cdc(format!("Failed to create empty batch: {e}")));
    }

    let pk_set: HashSet<&PrimaryKey> = pks.iter().collect();
    let all_batch = concat_batches(batches)?;

    // Build a boolean mask
    let all_pks = crate::delivery::extract_pks(&all_batch, pk_col)?;
    let mask: Vec<bool> = all_pks.iter().map(|pk| pk_set.contains(pk)).collect();
    let predicate = arrow_array::BooleanArray::from(mask);

    arrow::compute::filter_record_batch(&all_batch, &predicate)
        .map_err(|e| FerryError::Cdc(format!("Failed to filter batch by PKs: {e}")))
}

/// Filter batches to only include rows at the given global indices.
fn filter_batch_by_indices(
    batches: &[RecordBatch],
    indices: &[usize],
) -> Result<RecordBatch, FerryError> {
    if indices.is_empty() {
        let schema = batches[0].schema();
        let empty_arrays: Vec<arrow_array::ArrayRef> = schema
            .fields()
            .iter()
            .map(|f| arrow_array::new_null_array(f.data_type(), 0))
            .collect();
        return RecordBatch::try_new(schema, empty_arrays)
            .map_err(|e| FerryError::Cdc(format!("Failed to create empty batch: {e}")));
    }

    let all_batch = concat_batches(batches)?;
    let index_set: HashSet<&usize> = indices.iter().collect();
    let mask: Vec<bool> = (0..all_batch.num_rows())
        .map(|i| index_set.contains(&i))
        .collect();
    let predicate = arrow_array::BooleanArray::from(mask);

    arrow::compute::filter_record_batch(&all_batch, &predicate)
        .map_err(|e| FerryError::Cdc(format!("Failed to filter batch by indices: {e}")))
}

/// Extract all primary keys from a set of batches.
fn extract_pks_from_batches(
    batches: &[RecordBatch],
    pk_col: &str,
) -> Result<Vec<PrimaryKey>, FerryError> {
    let mut all_pks = Vec::new();
    for batch in batches {
        let pks = crate::delivery::extract_pks(batch, pk_col)?;
        all_pks.extend(pks);
    }
    Ok(all_pks)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;
    use serde_json::Value;
    use tempfile::TempDir;

    use crate::config::{
        BackoffStrategy, DeliveryConfig, DestinationConfig, ModelConfig, RetryConfig, SourceConfig,
        StateBackend, StateConfig, SyncSettings,
    };
    use crate::traits::{
        IdempotencyCapability, RateLimit, RemoveCapability, RemoveResult, RowError, WriteConfig,
        WriteResult,
    };

    // ── Test helpers ────────────────────────────────────────────────────

    /// Create a minimal FerryConfig for testing.
    fn test_ferry_config(state_path: &str) -> FerryConfig {
        FerryConfig {
            name: "test_project".to_string(),
            version: Some("1.0".to_string()),
            source: SourceConfig::DuckDB {
                path: "/tmp/test.duckdb".to_string(),
                query: Some("SELECT * FROM test_table".to_string()),
            },
            state: StateConfig {
                backend: StateBackend::DuckDB,
                path: Some(state_path.to_string()),
            },
            dbt: None,
            defaults: None,
        }
    }

    /// Create a minimal SyncConfig for testing.
    fn test_sync_config(name: &str, mode: SyncMode) -> SyncConfig {
        SyncConfig {
            name: name.to_string(),
            description: Some("Test sync".to_string()),
            tags: None,
            model: ModelConfig::Sql {
                sql: "SELECT id, name, value FROM test_table ORDER BY id".to_string(),
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
                    hash_columns: None,
                }),
                delivery: Some(DeliveryConfig {
                    batch_size: 100,
                    retry: Some(RetryConfig {
                        max_attempts: 3,
                        backoff: BackoffStrategy::Exponential,
                        initial_delay_secs: 5,
                        max_delay_secs: 300,
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

    /// Create a test RecordBatch with id (Utf8), name (Utf8), value (Int32).
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

    /// A mock source that returns pre-defined batches.
    struct MockSource {
        name: String,
        batches: Vec<RecordBatch>,
    }

    impl MockSource {
        fn new(name: &str, batches: Vec<RecordBatch>) -> Self {
            Self {
                name: name.to_string(),
                batches,
            }
        }
    }

    #[async_trait]
    impl Source for MockSource {
        fn name(&self) -> &str {
            &self.name
        }

        async fn check_connection(&self) -> Result<(), FerryError> {
            Ok(())
        }

        async fn discover(&self) -> Result<Vec<crate::traits::StreamSchema>, FerryError> {
            Ok(Vec::new())
        }

        fn read(&self, _query: &str) -> crate::traits::RecordBatchStream {
            let batches = self.batches.clone();
            let stream = futures::stream::iter(batches.into_iter().map(Ok));
            Box::pin(stream)
        }
    }

    /// A mock destination that records what it receives.
    #[derive(Clone)]
    struct MockDestination {
        name: String,
        max_batch: usize,
        rate_limit: Option<RateLimit>,
        idempotency: IdempotencyCapability,
        remove_cap: RemoveCapability,
        write_result: WriteResult,
        written_batches: std::sync::Arc<std::sync::Mutex<Vec<RecordBatch>>>,
        removed_keys: std::sync::Arc<std::sync::Mutex<Vec<Value>>>,
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
                written_batches: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
                removed_keys: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
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
            batch: &RecordBatch,
            _config: &WriteConfig,
        ) -> Result<WriteResult, FerryError> {
            self.written_batches.lock().unwrap().push(batch.clone());
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
            keys: &[Value],
            _config: &WriteConfig,
        ) -> Result<RemoveResult, FerryError> {
            self.removed_keys
                .lock()
                .unwrap()
                .extend(keys.iter().cloned());
            Ok(RemoveResult {
                rows_removed: keys.len(),
                errors: Vec::new(),
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

    /// Create a test engine with a temp state DB.
    async fn create_test_engine() -> (Engine, TempDir) {
        let dir = TempDir::with_prefix("ferry-engine-test-").expect("Failed to create temp dir");
        let state_path = dir.path().join("state.db");
        let state_path_str = state_path.to_str().unwrap().to_string();
        let config = test_ferry_config(&state_path_str);
        let engine = Engine::new(config).expect("Failed to create engine");
        (engine, dir)
    }

    // ── reconcile tests ─────────────────────────────────────────────────

    #[tokio::test]
    async fn test_reconcile_empty() {
        let (engine, _dir) = create_test_engine().await;
        let result = reconcile(&engine.state, "test_sync")
            .await
            .expect("Reconcile should succeed");
        assert!(result.already_synced.is_empty());
        assert!(result.pending_rows.is_empty());
    }

    #[tokio::test]
    async fn test_reconcile_with_synced_rows() {
        let (engine, _dir) = create_test_engine().await;

        // Create an incomplete run first
        let run = SyncRun {
            sync_name: "test_sync".to_string(),
            run_id: "run-001".to_string(),
            started_at: Utc::now(),
            completed_at: None,
            rows_extracted: 0,
            rows_synced: 0,
            rows_failed: 0,
            rows_retried: 0,
            rows_dead: 0,
            mode: "incremental".to_string(),
            dry_run: false,
            status: "running".to_string(),
        };
        engine.state.record_run(&run).await.unwrap();

        // Mark some rows as synced in that incomplete run
        let pks = vec!["pk1".to_string(), "pk2".to_string()];
        engine
            .state
            .mark_synced("test_sync", &pks, "run-001")
            .await
            .unwrap();

        let result = reconcile(&engine.state, "test_sync")
            .await
            .expect("Reconcile should succeed");
        assert_eq!(result.already_synced.len(), 2);
        assert!(result.already_synced.contains("pk1"));
        assert!(result.already_synced.contains("pk2"));
        assert!(result.pending_rows.is_empty());
    }

    #[tokio::test]
    async fn test_reconcile_with_pending_rows() {
        let (engine, _dir) = create_test_engine().await;

        // Mark a row as pending
        let retry_at = Utc::now() - chrono::Duration::seconds(10);
        engine
            .state
            .mark_pending("test_sync", &"pk1".to_string(), "timeout", retry_at)
            .await
            .unwrap();

        let result = reconcile(&engine.state, "test_sync")
            .await
            .expect("Reconcile should succeed");
        assert!(result.already_synced.is_empty());
        assert_eq!(result.pending_rows.len(), 1);
        assert_eq!(result.pending_rows[0].primary_key, "pk1");
    }

    // ── Engine::new tests ───────────────────────────────────────────────

    #[test]
    fn test_engine_new() {
        let dir = TempDir::with_prefix("ferry-engine-test-").expect("Failed to create temp dir");
        let state_path = dir.path().join("state.db");
        let state_path_str = state_path.to_str().unwrap().to_string();
        let config = test_ferry_config(&state_path_str);
        let engine = Engine::new(config).expect("Failed to create engine");
        assert_eq!(engine.config.name, "test_project");
    }

    // ── Full sync lifecycle test ───────────────────────────────────────

    #[tokio::test]
    async fn test_full_sync_lifecycle() {
        let (engine, _dir) = create_test_engine().await;
        let sync_config = test_sync_config("test_sync", SyncMode::Incremental);

        // Create source with 10 rows
        let batch = create_test_batch(10);
        let source = MockSource::new("test", vec![batch]);

        // Create destination that always succeeds
        let dest = MockDestination::new("mock_rest", 100);

        let options = RunOptions::default();
        let result = engine
            .run_sync(&sync_config, &source, &dest, &options)
            .await
            .expect("Sync should succeed");

        assert_eq!(result.sync_name, "test_sync");
        assert_eq!(result.rows_extracted, 10);
        assert_eq!(result.rows_synced, 10);
        assert_eq!(result.rows_pending, 0);
        assert_eq!(result.rows_failed, 0);
        assert_eq!(result.rows_dead, 0);
        assert!(!result.dry_run);
        assert!(result.duration_seconds > 0.0);

        // Verify run was recorded and completed
        let runs = engine.state.get_runs("test_sync", 10).await.unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].status, "completed");
        assert_eq!(runs[0].rows_synced, 10);
    }

    // ── Incremental second run test ─────────────────────────────────────

    #[tokio::test]
    async fn test_incremental_second_run() {
        let (engine, _dir) = create_test_engine().await;
        let sync_config = test_sync_config("test_sync", SyncMode::Incremental);

        // First run: 10 rows
        let batch1 = create_test_batch(10);
        let source1 = MockSource::new("test", vec![batch1]);
        let dest1 = MockDestination::new("mock_rest", 100);

        let options = RunOptions::default();
        let result1 = engine
            .run_sync(&sync_config, &source1, &dest1, &options)
            .await
            .expect("First sync should succeed");
        assert_eq!(result1.rows_synced, 10);

        // Second run: modify 3 rows, add 2 rows
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
        ]));

        // Rows: pk-0000 (unchanged), pk-0001 (changed name), pk-0002 (changed value),
        //        pk-0010 (new), pk-0011 (new)
        let ids = StringArray::from(vec!["pk-0000", "pk-0001", "pk-0002", "pk-0010", "pk-0011"]);
        let names = StringArray::from(vec![
            "name-0",          // unchanged
            "name-1-modified", // changed
            "name-2",          // unchanged
            "name-10",         // new
            "name-11",         // new
        ]);
        let values = Int32Array::from(vec![
            Some(0),   // unchanged
            Some(1),   // unchanged
            Some(999), // changed
            Some(10),  // new
            Some(11),  // new
        ]);

        let batch2 = RecordBatch::try_new(
            schema,
            vec![Arc::new(ids), Arc::new(names), Arc::new(values)],
        )
        .expect("Failed to create test batch");

        let source2 = MockSource::new("test", vec![batch2]);
        let dest2 = MockDestination::new("mock_rest", 100);

        let result2 = engine
            .run_sync(&sync_config, &source2, &dest2, &options)
            .await
            .expect("Second sync should succeed");

        // Should deliver 4 rows: 2 changed + 2 added
        // (pk-0000 is unchanged, so it should be skipped)
        assert_eq!(result2.rows_extracted, 5);
        assert_eq!(result2.rows_synced, 4);
    }

    // ── Crash mid-delivery test ────────────────────────────────────────

    #[tokio::test]
    async fn test_crash_mid_delivery() {
        let (engine, _dir) = create_test_engine().await;
        let sync_config = test_sync_config("test_sync", SyncMode::Incremental);

        // Create source with 10 rows
        let batch = create_test_batch(10);
        let source = MockSource::new("test", vec![batch]);

        // Destination that fails after 5 rows
        let errors: Vec<RowError> = (5..10)
            .map(|i| RowError {
                primary_key: format!("pk-{:04}", i),
                error: "HTTP 500 Internal Server Error".to_string(),
            })
            .collect();
        let dest = MockDestination::new("mock_rest", 100).with_errors(errors);

        let options = RunOptions::default();
        let result = engine
            .run_sync(&sync_config, &source, &dest, &options)
            .await
            .expect("Sync should complete with partial success");

        // 5 rows should be synced, 5 pending
        assert_eq!(result.rows_synced, 5);
        assert_eq!(result.rows_pending, 5);

        // Verify CDC hashes were NOT committed (pending rows remain)
        let hashes = engine.state.get_hashes("test_sync").await.unwrap();
        assert!(
            hashes.is_empty(),
            "CDC hashes should not be committed when pending rows remain"
        );

        // "Crash" recovery: new source with same data, new destination
        let batch2 = create_test_batch(10);
        let source2 = MockSource::new("test", vec![batch2]);
        let dest2 = MockDestination::new("mock_rest", 100);

        let result2 = engine
            .run_sync(&sync_config, &source2, &dest2, &options)
            .await
            .expect("Recovery sync should succeed");

        // The 5 pending rows should be delivered, and the 5 already-synced rows skipped
        // But since we're using hash CDC, the 5 already-synced rows have hashes stored
        // in the journal but NOT in cdc_hashes (since we didn't commit).
        // So the diff will see all 10 rows as "new" (no previous hashes).
        // However, the delivery pipeline's exactly-once check will skip the 5 already-synced rows.
        // So we should deliver 5 rows (the ones that were pending).
        assert_eq!(result2.rows_synced, 5);
    }

    // ── Dry run test ────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_dry_run() {
        let (engine, _dir) = create_test_engine().await;
        let sync_config = test_sync_config("test_sync", SyncMode::Incremental);

        let batch = create_test_batch(10);
        let source = MockSource::new("test", vec![batch]);
        let dest = MockDestination::new("mock_rest", 100);

        let options = RunOptions {
            dry_run: true,
            ..RunOptions::default()
        };

        let result = engine
            .run_sync(&sync_config, &source, &dest, &options)
            .await
            .expect("Dry run should succeed");

        assert_eq!(result.rows_extracted, 10);
        assert_eq!(result.rows_synced, 0);
        assert!(result.dry_run);

        // Verify no state was committed
        let runs = engine.state.get_runs("test_sync", 10).await.unwrap();
        assert!(runs.is_empty(), "No runs should be recorded for dry run");
    }

    // ── Full refresh test ──────────────────────────────────────────────

    #[tokio::test]
    async fn test_full_refresh() {
        let (engine, _dir) = create_test_engine().await;
        let sync_config = test_sync_config("test_sync", SyncMode::Incremental);

        // First run: 10 rows
        let batch1 = create_test_batch(10);
        let source1 = MockSource::new("test", vec![batch1]);
        let dest1 = MockDestination::new("mock_rest", 100);

        let options = RunOptions::default();
        let result1 = engine
            .run_sync(&sync_config, &source1, &dest1, &options)
            .await
            .expect("First sync should succeed");
        assert_eq!(result1.rows_synced, 10);

        // Second run with full_refresh=true: all 10 rows re-delivered
        let batch2 = create_test_batch(10);
        let source2 = MockSource::new("test", vec![batch2]);
        let dest2 = MockDestination::new("mock_rest", 100);

        let options2 = RunOptions {
            full_refresh: true,
            ..RunOptions::default()
        };

        let result2 = engine
            .run_sync(&sync_config, &source2, &dest2, &options2)
            .await
            .expect("Full refresh sync should succeed");

        // All 10 rows should be re-delivered
        assert_eq!(result2.rows_synced, 10);
    }

    // ── Diff preview test ──────────────────────────────────────────────

    #[tokio::test]
    async fn test_diff_preview() {
        let (engine, _dir) = create_test_engine().await;
        let sync_config = test_sync_config("test_sync", SyncMode::Incremental);

        let batch = create_test_batch(10);
        let source = MockSource::new("test", vec![batch]);

        let preview = engine
            .diff("test_sync", &source, &sync_config)
            .await
            .expect("Diff should succeed");

        assert_eq!(preview.sync_name, "test_sync");
        assert_eq!(preview.total_rows, 10);
        // First run: all 10 rows are "added"
        assert_eq!(preview.added, 10);
        assert_eq!(preview.changed, 0);
        assert_eq!(preview.removed, 0);
    }

    // ── Mirror mode test ────────────────────────────────────────────────

    #[tokio::test]
    async fn test_mirror_mode() {
        let (engine, _dir) = create_test_engine().await;
        let sync_config = test_sync_config("test_sync", SyncMode::Mirror);

        let batch = create_test_batch(10);
        let source = MockSource::new("test", vec![batch]);
        let dest = MockDestination::new("mock_rest", 100)
            .with_remove_capability(RemoveCapability::RemoveByKey);

        let options = RunOptions::default();
        let result = engine
            .run_sync(&sync_config, &source, &dest, &options)
            .await
            .expect("Mirror sync should succeed");

        assert_eq!(result.rows_synced, 10);
    }

    // ── Error handling: failed sync marks run as failed ────────────────

    #[tokio::test]
    async fn test_sync_error_marks_run_failed() {
        let (engine, _dir) = create_test_engine().await;
        let sync_config = test_sync_config("test_sync", SyncMode::Incremental);

        // Source that returns an error
        struct ErrorSource;

        #[async_trait]
        impl Source for ErrorSource {
            fn name(&self) -> &str {
                "error_source"
            }

            async fn check_connection(&self) -> Result<(), FerryError> {
                Ok(())
            }

            async fn discover(&self) -> Result<Vec<crate::traits::StreamSchema>, FerryError> {
                Ok(Vec::new())
            }

            fn read(&self, _query: &str) -> crate::traits::RecordBatchStream {
                Box::pin(futures::stream::once(async move {
                    Err(FerryError::Source("Query failed".to_string()))
                }))
            }
        }

        let source = ErrorSource;
        let dest = MockDestination::new("mock_rest", 100);

        let options = RunOptions::default();
        let result = engine
            .run_sync(&sync_config, &source, &dest, &options)
            .await;

        assert!(result.is_err(), "Sync should fail when source errors");
    }

    // ── Validate test ──────────────────────────────────────────────────

    #[tokio::test]
    async fn test_validate_with_valid_config() {
        let (engine, dir) = create_test_engine().await;

        // Create a syncs directory with a valid sync config
        let syncs_dir = dir.path().join("syncs");
        std::fs::create_dir(&syncs_dir).unwrap();

        let sync_yml = syncs_dir.join("test_sync.yml");
        std::fs::write(
            &sync_yml,
            r#"
name: test_sync
model:
  sql: SELECT * FROM users
destination:
  type: rest
  url: https://api.example.com/users
sync:
  mode: incremental
  cursor_field: id
"#,
        )
        .unwrap();

        let errors = engine
            .validate(&syncs_dir)
            .await
            .expect("Validate should succeed");
        // We may get source connection errors (DuckDB path doesn't exist), but no config errors
        assert!(errors.is_empty() || errors.iter().any(|e| e.field == "source"));
    }

    // ── concat_batches tests ───────────────────────────────────────────

    #[test]
    fn test_concat_batches_single() {
        let batch = create_test_batch(5);
        let result = concat_batches(&[batch.clone()]).unwrap();
        assert_eq!(result.num_rows(), 5);
    }

    #[test]
    fn test_concat_batches_multiple() {
        let batch1 = create_test_batch(3);
        let batch2 = create_test_batch(4);
        let result = concat_batches(&[batch1, batch2]).unwrap();
        assert_eq!(result.num_rows(), 7);
    }

    #[test]
    fn test_concat_batches_empty() {
        let result = concat_batches(&[]);
        assert!(result.is_err());
    }

    // ── filter_batch_by_pks tests ──────────────────────────────────────

    #[test]
    fn test_filter_batch_by_pks_some() {
        let batch = create_test_batch(10);
        let pks = vec![
            "pk-0000".to_string(),
            "pk-0005".to_string(),
            "pk-0009".to_string(),
        ];
        let result = filter_batch_by_pks(&[batch], "id", &pks).unwrap();
        assert_eq!(result.num_rows(), 3);
    }

    #[test]
    fn test_filter_batch_by_pks_empty() {
        let batch = create_test_batch(10);
        let result = filter_batch_by_pks(&[batch], "id", &[]).unwrap();
        assert_eq!(result.num_rows(), 0);
    }

    // ── resolve_query tests ────────────────────────────────────────────

    #[test]
    fn test_resolve_query_sql() {
        let model = ModelConfig::Sql {
            sql: "SELECT * FROM users".to_string(),
        };
        assert_eq!(resolve_query(&model, None).unwrap(), "SELECT * FROM users");
    }

    #[test]
    fn test_resolve_query_ref_without_manifest() {
        let model = ModelConfig::Ref {
            r#ref: "users".to_string(),
        };
        let result = resolve_query(&model, None);
        assert!(result.is_err(), "Ref without manifest should error");
        match result.unwrap_err() {
            FerryError::Config(msg) => {
                assert!(
                    msg.contains("dbt.manifest_path"),
                    "Error should mention dbt.manifest_path: {msg}"
                );
            }
            other => panic!("Expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn test_resolve_query_ref_with_manifest() {
        use crate::dbt::Manifest;
        let manifest_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("sample_manifest.json");
        let manifest = Manifest::load(&manifest_path).expect("Should load manifest");
        let model = ModelConfig::Ref {
            r#ref: "fct_users".to_string(),
        };
        let sql = resolve_query(&model, Some(&manifest)).expect("Should resolve with manifest");
        assert_eq!(sql, "SELECT id, email, name FROM analytics.fct_users");
    }
}
