# Technical Specification: Ferry

**Document Status:** Draft
**Version:** 0.1
**Author:** walden-data
**Date:** 2026-05-02
**Last Updated:** 2026-05-02

## Executive Summary

**Problem:** Reverse ETL tools are either expensive SaaS (Hightouch, Census) or immature Python scripts (drt) that lack durability and performance at scale.

**Solution:** A Rust-native reverse ETL engine with durable row-level delivery tracking, Arrow-native data flow, and Python bindings for orchestrator integration.

**Impact:** Data teams can activate warehouse data in any tool with zero vendor cost, production-grade reliability, and 10-50x better performance than Python alternatives.

---

## 1. Background

### Context

The modern data stack has mature solutions for extraction (dlt, Airbyte), transformation (dbt), and orchestration (Dagster, Airflow). The "last mile" — pushing modeled data from warehouses into operational tools (CRMs, marketing platforms, ad networks) — remains underserved in the code-first ecosystem.

Existing solutions fall into two camps:
1. **SaaS platforms** (Hightouch, Census): Powerful but expensive ($12K-50K+/year), GUI-first, and opaque.
2. **OSS Python tools** (drt): Young, slow at scale, and lacking durability guarantees.

### Goals

- Deliver a production-grade reverse ETL engine competitive with Hightouch's core sync functionality
- Provide durability guarantees inspired by Temporal (no silent data loss, retryable failures)
- Achieve 10-50x performance improvement over Python implementations for the extract→serialize→POST loop
- Integrate cleanly with Dagster as first-class materialized assets
- Ship as a single binary with zero runtime dependencies

### Non-Goals

- Real-time / streaming CDC (batch-only)
- Visual audience builder or marketing UI
- Warehouse-to-warehouse replication
- Built-in scheduling (defer to orchestrators)
- Event stream processing

---

## 2. System Architecture

### Component Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                          ferry-cli (Rust)                            │
│                         clap-based binary                           │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
┌────────────────────────────────▼────────────────────────────────────┐
│                         ferry-core (Rust)                            │
│                                                                      │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────────┐   │
│  │  Config  │  │  Engine  │  │  Diff    │  │  State Manager   │   │
│  │  Parser  │  │  Loop    │  │  (CDC)   │  │  (Row Journal)   │   │
│  └──────────┘  └──────────┘  └──────────┘  └──────────────────┘   │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │                    Delivery Pipeline                           │   │
│  │  Batcher → Rate Limiter → Writer → Result Classifier          │   │
│  └──────────────────────────────────────────────────────────────┘   │
└────────────┬─────────────────────────────────────┬──────────────────┘
             │                                     │
┌────────────▼────────────┐          ┌─────────────▼──────────────────┐
│    ferry-sources (Rust)  │          │    ferry-destinations (Rust)    │
│                          │          │                                 │
│  ┌────────┐ ┌─────────┐ │          │  ┌────────┐ ┌───────────────┐  │
│  │DuckDB  │ │Postgres │ │          │  │REST API│ │    Braze      │  │
│  └────────┘ └─────────┘ │          │  └────────┘ └───────────────┘  │
│  ┌────────┐ ┌─────────┐ │          │  ┌────────┐ ┌───────────────┐  │
│  │BigQuery│ │Snowflake│ │          │  │  Slack │ │   HubSpot     │  │
│  └────────┘ └─────────┘ │          │  └────────┘ └───────────────┘  │
└──────────────────────────┘          │  ┌────────┐ ┌───────────────┐  │
                                      │  │  S3    │ │  Salesforce   │  │
                                      │  └────────┘ └───────────────┘  │
                                      └────────────────────────────────┘
             │
┌────────────▼────────────┐
│   ferry-python (PyO3)    │
│   + dagster-ferry        │
└──────────────────────────┘
```

### Data Flow

```
Source Query → Arrow RecordBatch Stream
    │
    ▼
CDC Diff Engine (hash/cursor/mirror)
    │
    ├── Added rows ──────────────────┐
    ├── Changed rows ────────────────┤
    ├── Removed rows (mirror mode) ──┤
    │                                ▼
    │                        Delivery Pipeline
    │                                │
    │                    ┌───────────┼───────────┐
    │                    ▼           ▼           ▼
    │               ┌────────┐ ┌────────┐ ┌──────────┐
    │               │ Synced │ │Pending │ │  Dead    │
    │               └────────┘ └───┬────┘ └──────────┘
    │                              │
    │                    Next run includes
    │                    pending rows in
    ▼                    delivery set
State Commit (CDC hash + journal update)
```

---

## 3. Functional Requirements

### FR-1: Sync Execution

**Priority:** P0 (Must Have)
**Description:** Execute a sync definition — extract from source, diff, deliver to destination.

**Acceptance Criteria:**
- [ ] Parse sync YAML config and validate all fields
- [ ] Connect to configured source and execute model query
- [ ] Return data as Arrow RecordBatch stream
- [ ] Apply configured CDC method to determine changeset
- [ ] Batch rows according to destination limits
- [ ] Deliver batches to destination with configured retry policy
- [ ] Report structured results (rows_synced, rows_failed, rows_pending, duration)

**Dependencies:** FR-2, FR-3, FR-4

### FR-2: Source Extraction

**Priority:** P0 (Must Have)
**Description:** Query data from warehouse sources and return as Arrow RecordBatches.

**Acceptance Criteria:**
- [ ] Support raw SQL queries
- [ ] Support dbt model references (`ref: model_name`) via manifest.json — resolve to model's **compiled SQL** (not just relation name), preserving model logic
- [ ] **Reject ephemeral dbt models** with clear error: "Ephemeral models are not supported. Materialize as view/table or use `model.sql`."
- [ ] **Stale manifest detection**: warn if manifest is older than 24 hours
- [ ] Return typed Arrow RecordBatches (infer schema from query results)
- [ ] Stream large result sets without loading entirely into memory
- [ ] **Schema drift handling**: error if a mapped column is missing from source results; warn on type narrowing
- [ ] Test connection before executing (`ferry validate`)

**Dependencies:** FR-9

### FR-3: CDC (Change Data Capture)

**Priority:** P0 (Must Have)
**Description:** Detect which rows have changed since the last sync run.

**Acceptance Criteria:**
- [ ] **Hash mode**: Compute xxhash of all mapped columns per row; compare to stored hashes; emit added/changed/removed sets
- [ ] **Cursor mode**: Filter rows where cursor_field > stored cursor value; emit only new/updated rows
- [ ] **Mirror mode**: Full replace — behavior depends on destination `RemoveCapability`:
  - `RemoveByKey`: deliver added/changed rows via `write()`, remove stale rows via `remove()`
  - `RemoveAll`: call `replace_all()` for atomic full-replace
  - `None`: re-deliver all rows via `write()`, log warning that stale rows may persist (degraded mirror)
- [ ] Full refresh override (`--full-refresh`) bypasses CDC but does NOT reset stored state
- [ ] CDC state persists across runs in configured state backend
- [ ] Changing mapped columns triggers a re-hash (not a full refresh)
- [ ] **Schema drift — added columns**: if `hash_columns: all`, new column is included in hash automatically (all rows appear changed — correct behavior). If explicit list, new column is ignored.
- [ ] **Schema drift — removed mapped columns**: error at extraction time before any delivery
- [ ] **Schema drift — removed cursor field**: error at extraction time

**Dependencies:** FR-5

### FR-4: Durable Delivery

**Priority:** P0 (Must Have)
**Description:** Track per-row delivery outcomes independently of CDC state, with exactly-once delivery by default.

**Acceptance Criteria:**
- [ ] Record delivery outcome per row (synced/pending/dead) in row journal
- [ ] **Exactly-once delivery by default**: before delivering, check journal and skip rows already marked Synced (unless `--full-refresh` or `delivery.allow_redelivery: true`)
- [ ] **Idempotency enforcement**: for `AppendOnly`/`None` destinations, re-delivery is blocked unless `allow_redelivery: true`
- [ ] **Journal commits per-batch** (serves as write-ahead log for crash recovery)
- [ ] **CDC hash commits only on successful sync completion** (atomic)
- [ ] **Reconciliation on startup**: skip rows already synced in incomplete prior runs, re-include pending rows
- [ ] Pending rows are automatically included in the next run's delivery set
- [ ] Dead rows (exceeded max_attempts) are excluded from automatic retry
- [ ] Dead rows are queryable via `ferry dlq list`
- [ ] Dead rows are manually retryable via `ferry dlq retry`
- [ ] Delivery failures NEVER corrupt CDC hash state
- [ ] Classify errors by HTTP status / response body using configurable rules
- [ ] Support exponential, linear, and fixed backoff strategies
- [ ] **Respect `Retry-After` header** on 429/503 responses — pause rate limiter for requested duration
- [ ] Respect `next_retry_at` — don't retry too early
- [ ] If `Retry-After` exceeds `max_delay`, clamp to `max_delay`

**Dependencies:** FR-5

### FR-5: State Management

**Priority:** P0 (Must Have)
**Description:** Persist sync state (CDC snapshots, row journal, run history) with crash recovery guarantees.

**Acceptance Criteria:**
- [ ] DuckDB backend: store state in `.ferry/state.duckdb`
- [ ] Warehouse backend: store state in configurable schema within source warehouse
- [ ] **Row journal committed per-batch** (write-ahead log)
- [ ] **CDC hash committed only on successful sync completion** (atomic transaction)
- [ ] **Run status tracked**: "running" → "completed" / "failed" / "crashed"
- [ ] **Reconciliation on startup**: detect incomplete runs, skip already-synced rows, re-include pending rows
- [ ] State is atomic — partial sync failures don't corrupt state (DuckDB transactions)
- [ ] State is queryable (SQL for warehouse backend, CLI for DuckDB)
- [ ] Run history retained with configurable TTL
- [ ] **Single-writer model**: one sync run at a time per state file (serialized writes)

**Dependencies:** None

### FR-6: CLI Interface

**Priority:** P0 (Must Have)
**Description:** Command-line interface for sync execution, monitoring, and DLQ management.

**Acceptance Criteria:**
- [ ] `ferry init` — scaffold project with example config
- [ ] `ferry run` — execute syncs with filtering (--select, --tags)
- [ ] `ferry run --dry-run` — preview without writing
- [ ] `ferry run --full-refresh` — bypass CDC
- [ ] `ferry run --retry-dead` — include DLQ rows
- [ ] `ferry run --output json` — structured output for CI
- [ ] `ferry diff` — preview what CDC would detect
- [ ] `ferry validate` — check configs, test connections, detect schema drift (removed columns)
- [ ] `ferry status` / `ferry history` — run results
- [ ] `ferry dlq list/retry/purge` — dead letter queue management
- [ ] `ferry sources` / `ferry destinations` — list available connectors
- [ ] Shell completion (bash/zsh/fish)

**Note:** `ferry test` (post-sync assertions) is **not implemented** in v0.1. Post-sync assertions are deferred to dbt tests and Dagster asset checks. `ferry serve` and `ferry mcp` are Phase 2+.

**Dependencies:** FR-1 through FR-5

### FR-7: Python Bindings

**Priority:** P1 (Should Have)
**Description:** PyO3-based Python library exposing core engine functionality.

**Acceptance Criteria:**
- [ ] `Project` class: open project, list syncs, run syncs, get status
- [ ] `SyncResult` class: structured result with all metadata fields
- [ ] Arrow-native interop via pyo3-arrow (PyCapsule interface for zero-copy to Python consumers that want Arrow)
- [ ] Tokio runtime managed internally (no async leaking to Python)
- [ ] Published as `ferry-core` on PyPI via maturin wheels
- [ ] Type stubs (.pyi) for IDE support

**Dependencies:** FR-1 through FR-5

### FR-8: Dagster Integration

**Priority:** P1 (Should Have)
**Description:** Pure Python package exposing ferry syncs as Dagster assets, with first-class dependency tracking and metadata.

**Acceptance Criteria:**
- [x] `@ferry_assets` decorator creates multi_asset with can_subset=True
- [x] `DagsterFerryResource` yields MaterializeResult per sync
- [x] `DagsterFerryTranslator` for customizing:
  - Asset keys (default: sync name)
  - Group names (default: first tag or "default")
  - ~~Dependencies (default: auto-detected dbt deps if dagster-dbt present)~~ deferred to FERRY-9
  - Kinds (default: `{"ferry", "<destination_type>"}`)
- [ ] **dbt dependency auto-detection**: if a sync uses `model.ref: <model>` and dagster-dbt is present, ferry asset depends on the corresponding dbt asset (resolved via dbt manifest node name → Dagster asset key)
- [x] Sync `tags:` map to Dagster asset groups
- [x] Asset kinds: `{"ferry", "<destination_type>"}`
- [ ] Dry-run controllable from Dagster UI RunConfig (not hardcoded)
- [ ] DLQ row count surfaced as asset metadata (non-zero does not fail materialization)
- [ ] `build_ferry_asset_specs()` for Dagster Pipes / remote execution (deferred to v0.2)

**Dependencies:** FR-7

### FR-9: dbt Integration

**Priority:** P1 (Should Have)
**Description:** Resolve dbt model references from manifest.json, using compiled SQL for correctness.

**Acceptance Criteria:**
- [ ] Parse `target/manifest.json` (path configurable in `ferry.yml` via `dbt.manifest_path`)
- [ ] Resolve `ref: model_name` to the model's **compiled SQL** (not just relation name) — preserves model logic (filters, joins, CASE statements) for views
- [ ] **Reject ephemeral models** with clear error: "Ephemeral models are not supported. Materialize as view/table or use `model.sql` to inline the query."
- [ ] **Stale manifest detection**: warn if manifest is older than 24 hours
- [ ] Error clearly if ref cannot be resolved: "Model '<name>' not found in dbt manifest"
- [ ] Error if `dbt.manifest_path` is not configured when `ref:` is used
- [ ] Error if manifest file does not exist (with file path for debugging)

**Dependencies:** None

### FR-10: HTTP Trigger (Phase 2+)

**Priority:** P2 (Nice to Have)
**Description:** Lightweight HTTP server for webhook-triggered syncs.

**Acceptance Criteria:**
- [ ] `ferry serve --port 8080` starts HTTP listener
- [ ] POST /api/v1/run triggers sync execution
- [ ] Request body specifies sync_names, dry_run, full_refresh
- [ ] Returns structured JSON result
- [ ] **HMAC signature verification** (not bearer tokens) — verify `X-Webhook-Signature` header against request body using `FERRY_WEBHOOK_SECRET` env var. Standard for dbt Cloud webhooks and automated triggers.

**Dependencies:** FR-1

### FR-11: MCP Server (Phase 3+)

**Priority:** P2 (Nice to Have)
**Description:** MCP protocol server for AI tool integration. Deferred — useful for demos and AI-assisted ops, not core to reverse ETL workflow.

**Acceptance Criteria:**
- [ ] `ferry mcp run` starts MCP server
- [ ] Tools: list_syncs, run_sync, validate, status, dlq_list
- [ ] Compatible with Claude Desktop, Cursor, and other MCP clients

**Dependencies:** FR-6

---

## 4. Non-Functional Requirements

### Performance

| Metric | Target | Notes |
|--------|--------|-------|
| 1M row sync (REST API dest) | < 5 minutes | Braze-like destination with 75-row batches |
| CDC hash comparison (1M rows) | < 10 seconds | xxhash on Arrow columns (columnar — this is where the performance win is real) |
| Cold start (CLI) | < 50ms | Static binary, no interpreter |
| Memory usage (1M rows) | < 500MB | Arrow columnar, streaming where possible |
| Python library import | < 200ms | PyO3 module load |

### Reliability

- **Zero silent data loss**: Every row is either synced, pending retry, or in DLQ. Never dropped.
- **Exactly-once delivery by default**: Row journal prevents re-delivery unless `allow_redelivery: true`. Idempotency capability per destination determines re-delivery safety.
- **Atomic state updates**: Partial failures don't corrupt CDC or journal state. DuckDB transactions are atomic.
- **Crash recovery**: Journal commits per-batch (WAL), CDC hash commits on success. Reconciliation on startup closes the gap. See §5.4.1.
- **Idempotent delivery**: Re-running a sync doesn't duplicate rows — enforced by journal check before delivery, not assumed by destination behavior.

### Scalability

- **Row volume**: Handle 10M+ row tables for CDC hash comparison
- **Concurrent syncs**: Support parallel execution of independent syncs (different destinations) via `--threads`. Same-destination syncs are serialized (rate limits).
- **Batch parallelism**: Multiple HTTP requests in flight per sync (within rate limits and `concurrent` cap)
- **State growth**: Journal auto-prunes synced rows; DLQ has configurable TTL
- **Single-writer state**: One sync run at a time per state file (DuckDB). Parallel syncs with separate destinations can share state via serialized writes.

### Security

- **No secrets in YAML**: Use `${ENV_VAR}` substitution for credentials
- **Optional secrets.toml**: Local file (gitignored, permissions 600 required) for development convenience. Precedence: env vars > secrets.toml > YAML inline (non-secret only).
- **No network calls without explicit config**: Ferry never phones home
- **Destination credentials scoped**: Each sync only accesses its configured destination
- **Secret masking**: Ferry never logs secret values (masked as `***`)

### Portability

- **Binary targets**: Linux x86_64, Linux aarch64, macOS x86_64, macOS aarch64
- **Python wheels**: manylinux, macOS universal2
- **Container**: Docker image (< 100MB, includes DuckDB C++ runtime which dominates binary size)
- **Static linking**: No OpenSSL/libpq required at runtime (Rust TLS, pure-Rust Postgres driver)

---

## 5. Technical Design

### 5.1 Connector Trait System

Connectors are built-in to the ferry binary and selected via YAML config. No compile-time feature flags, no runtime plugin loading. The `Destination` trait is object-safe and includes capability declarations that drive delivery semantics.

```rust
// crates/ferry-sources/src/lib.rs
use arrow::record_batch::RecordBatch;
use futures::Stream;
use std::pin::Pin;

pub type RecordBatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch, SourceError>> + Send>>;

#[async_trait]
pub trait Source: Send + Sync {
    /// Validate connection credentials
    async fn check_connection(&self) -> Result<(), SourceError>;

    /// Discover available tables/views
    async fn discover(&self) -> Result<Vec<StreamSchema>, SourceError>;

    /// Execute query, return streaming Arrow batches
    fn read(&self, query: &str) -> RecordBatchStream;
}

// crates/ferry-destinations/src/lib.rs
#[async_trait]
pub trait Destination: Send + Sync {
    /// Validate connection credentials
    async fn check_connection(&self) -> Result<(), DestinationError>;

    /// Write a batch of rows, return per-row outcomes
    async fn write(&self, batch: &RecordBatch, config: &WriteConfig) -> WriteResult;

    /// Maximum rows per API call
    fn max_batch_size(&self) -> usize;

    /// Rate limit constraint (if any)
    fn rate_limit(&self) -> Option<RateLimit>;

    /// Idempotency capability — determines re-delivery safety.
    /// The delivery pipeline uses this to decide whether to skip
    /// already-synced rows (exactly-once) or allow re-delivery.
    fn idempotency(&self) -> IdempotencyCapability;

    /// Remove capability — determines mirror-mode delete behavior.
    /// None = mirror mode operates as degraded full-replace (logs warning).
    fn remove_capability(&self) -> RemoveCapability;

    /// Remove rows by primary key. Only called if remove_capability is RemoveByKey.
    /// Used by mirror mode to delete rows that no longer exist in the source.
    async fn remove(&self, keys: &[Value], config: &WriteConfig) -> Result<RemoveResult, DestinationError>;

    /// Replace entire dataset atomically. Only called if remove_capability is RemoveAll.
    /// Used by mirror mode for destinations that support atomic full-replace (e.g. file overwrite).
    async fn replace_all(&self, batch: &RecordBatch, config: &WriteConfig) -> WriteResult;
}

pub enum IdempotencyCapability {
    UpsertByKey,  // Destination upserts by PK — safe for re-delivery
    Overwrite,    // Destination overwrites by key (S3 PUT) — safe for re-delivery
    AppendOnly,   // Destination appends (Slack webhook) — UNSAFE for re-delivery
    None,          // No guarantee — UNSAFE for re-delivery
}

pub enum RemoveCapability {
    RemoveByKey,  // Can delete specific rows (Braze remove from segment)
    RemoveAll,    // Can replace entire dataset (file overwrite, S3 prefix clear)
    None,          // Cannot remove rows (Slack webhook, append-only logs)
}

pub struct WriteResult {
    pub rows_synced: usize,
    pub rows_failed: usize,
    pub rows_skipped: usize,
    pub errors: Vec<RowError>,
    pub duration: Duration,
}

pub struct RowError {
    pub primary_key: Value,
    pub error_type: ErrorClassification,
    pub message: String,
    pub http_status: Option<u16>,
    pub retry_after: Option<Duration>,  // From Retry-After header on 429/503
    pub retryable: bool,
}

pub struct RemoveResult {
    pub rows_removed: usize,
    pub rows_failed: usize,
    pub errors: Vec<RowError>,
}
```

### 5.2 CDC Hash Engine

```rust
// crates/ferry-core/src/diff.rs
use arrow::record_batch::RecordBatch;
use xxhash_rust::xxh3::xxh3_64;

pub struct HashCdc {
    state_store: Box<dyn StateStore>,
}

impl HashCdc {
    /// Compare current data against stored hashes
    pub async fn compute_diff(
        &self,
        sync_name: &str,
        current: &[RecordBatch],
        primary_key_col: &str,
        hash_columns: &[String],  // columns to include in hash
    ) -> Result<DiffResult, CdcError> {
        // 1. Load previous hashes from state store
        let previous = self.state_store.get_hashes(sync_name).await?;

        // 2. Compute current hashes (Arrow columnar operation)
        let current_hashes = hash_record_batches(current, primary_key_col, hash_columns)?;

        // 3. Diff
        let added: Vec<PrimaryKey> = current_hashes.keys()
            .filter(|k| !previous.contains_key(*k))
            .cloned().collect();

        let changed: Vec<PrimaryKey> = current_hashes.iter()
            .filter(|(k, hash)| previous.get(*k).map_or(false, |prev| prev != *hash))
            .map(|(k, _)| k.clone()).collect();

        let removed: Vec<PrimaryKey> = previous.keys()
            .filter(|k| !current_hashes.contains_key(*k))
            .cloned().collect();

        Ok(DiffResult { added, changed, removed, current_hashes })
    }
}

/// Hash a row by concatenating column bytes and computing xxh3_64
fn hash_row(columns: &[&dyn Array], row_idx: usize) -> u64 {
    let mut hasher_input = Vec::new();
    for col in columns {
        // Append raw bytes of each column value
        append_array_value_bytes(&mut hasher_input, *col, row_idx);
    }
    xxh3_64(&hasher_input)
}
```

### 5.3 Delivery Pipeline

```rust
// crates/ferry-core/src/delivery.rs
pub struct DeliveryPipeline {
    destination: Box<dyn Destination>,
    rate_limiter: Governor,
    retry_policy: RetryPolicy,
    journal: RowJournal,
}

impl DeliveryPipeline {
    pub async fn deliver(
        &mut self,
        rows: &RecordBatch,
        config: &DeliveryConfig,
        sync_run_id: &str,
    ) -> DeliveryResult {
        // --- Exactly-once check: skip rows already synced in this or prior incomplete runs ---
        let idempotency = self.destination.idempotency();
        let allow_redelivery = config.allow_redelivery;
        let rows_to_deliver = if allow_redelivery {
            rows.clone()  // deliver everything regardless of journal state
        } else {
            self.journal.filter_undelivered(rows, sync_run_id).await?
            // Returns only rows not already marked Synced in the journal
        };

        if rows_to_deliver.num_rows() == 0 {
            return Ok(DeliveryResult::default()); // nothing to do
        }

        let batch_size = self.destination.max_batch_size();
        let batches = split_record_batch(&rows_to_deliver, batch_size);

        let mut results = DeliveryResult::default();

        for batch in batches {
            // Wait for rate limiter (token bucket)
            self.rate_limiter.until_ready().await;

            // Attempt delivery
            let write_result = self.destination.write(&batch, &config.write_config).await;

            // --- Commit journal per-batch (crash recovery: journal is the WAL) ---
            // Mark successful rows as Synced
            if write_result.rows_synced > 0 {
                let synced_pks = extract_successful_pks(&batch, &write_result);
                self.journal.mark_synced_batch(&synced_pks, sync_run_id).await?;
            }
            results.rows_synced += write_result.rows_synced;

            // --- Classify and handle errors ---
            for error in &write_result.errors {
                let classification = self.classify_error(error, config);
                match classification {
                    Action::Retry => {
                        // Respect Retry-After header if present, else use backoff policy
                        let next_retry_at = error.retry_after
                            .map(|d| Instant::now() + d)
                            .unwrap_or_else(|| {
                                self.retry_policy.next_retry(error.attempts)
                            });
                        self.journal.mark_pending(
                            &error.primary_key,
                            &error.message,
                            next_retry_at,
                        ).await?;
                        results.rows_pending += 1;
                    }
                    Action::DeadLetter => {
                        self.journal.mark_dead(&error.primary_key, &error.message).await?;
                        results.rows_dead += 1;
                    }
                    Action::Skip => {
                        results.rows_skipped += 1;
                    }
                    Action::FailSync => {
                        return Err(DeliveryError::SyncAborted(error.message.clone()));
                    }
                }
            }

            // --- Handle 429 with Retry-After: pause rate limiter ---
            if let Some(retry_after) = write_result.rate_limit_retry_after {
                // Pause token bucket for the requested duration
                self.rate_limiter.pause_for(retry_after).await;
            }
        }

        Ok(results)
    }

    /// Mirror mode delivery: write added/changed rows, then remove stale rows
    pub async fn deliver_mirror(
        &mut self,
        current: &RecordBatch,
        removed_keys: &[Value],
        config: &DeliveryConfig,
        sync_run_id: &str,
    ) -> DeliveryResult {
        match self.destination.remove_capability() {
            RemoveCapability::RemoveByKey => {
                // 1. Deliver current rows (added + changed)
                let write_result = self.deliver(current, config, sync_run_id).await?;

                // 2. Remove stale rows
                if !removed_keys.is_empty() {
                    let remove_result = self.destination.remove(removed_keys, &config.write_config).await?;
                    // Log any removal failures — they don't fail the sync
                    // (stale rows at destination are a warning, not a data loss event)
                }

                Ok(write_result)
            }
            RemoveCapability::RemoveAll => {
                // Atomic full-replace: destination handles the swap
                self.rate_limiter.until_ready().await;
                let write_result = self.destination.replace_all(current, &config.write_config).await?;
                Ok(write_result.into())
            }
            RemoveCapability::None => {
                // Degraded mirror: re-deliver all, don't remove. Log warning.
                tracing::warn!(
                    "Destination does not support removal; mirror mode is degraded. \
                     Stale rows may persist at destination."
                );
                self.deliver(current, config, sync_run_id).await
            }
        }
    }

    fn classify_error(&self, error: &RowError, config: &DeliveryConfig) -> Action {
        for rule in &config.on_reject.classify {
            if rule.matches(error) {
                return rule.action.clone();
            }
        }
        // Default: retry if retryable, dead_letter otherwise
        if error.retryable { Action::Retry } else { Action::DeadLetter }
    }
}
```

### 5.4 State Store Interface

The state store manages three independent data structures with different commit semantics:
- **Row journal**: committed per-batch (serves as the write-ahead log for crash recovery)
- **CDC hash snapshot**: committed only on successful sync completion
- **Run history**: committed at sync start and end

```rust
// crates/ferry-core/src/state.rs
#[async_trait]
pub trait StateStore: Send + Sync {
    // CDC state — committed only on successful sync completion
    async fn get_hashes(&self, sync_name: &str) -> Result<HashMap<PrimaryKey, u64>>;
    async fn set_hashes(&self, sync_name: &str, hashes: &HashMap<PrimaryKey, u64>) -> Result<()>;
    async fn get_cursor(&self, sync_name: &str) -> Result<Option<Value>>;
    async fn set_cursor(&self, sync_name: &str, value: Value) -> Result<()>;

    // Row journal — committed per-batch (acts as WAL)
    async fn get_pending_rows(&self, sync_name: &str) -> Result<Vec<RowEntry>>;
    async fn get_dead_rows(&self, sync_name: &str) -> Result<Vec<RowEntry>>;
    async fn mark_synced(&self, sync_name: &str, primary_keys: &[PrimaryKey], run_id: &str) -> Result<()>;
    async fn mark_pending(&self, sync_name: &str, pk: &PrimaryKey, error: &str, next_retry_at: Instant) -> Result<()>;
    async fn mark_dead(&self, sync_name: &str, pk: &PrimaryKey, error: &str) -> Result<()>;
    async fn retry_dead_rows(&self, sync_name: &str, pks: Option<&[PrimaryKey]>) -> Result<usize>;
    async fn purge_dead_rows(&self, sync_name: &str, older_than: Duration) -> Result<usize>;

    // --- Crash recovery helpers ---
    /// Get rows marked Synced in a specific (possibly incomplete) run.
    /// Used at sync start to skip already-delivered rows from crashed prior runs.
    async fn get_synced_for_run(&self, sync_name: &str, run_id: &str) -> Result<Vec<PrimaryKey>>;

    /// Get the last completed sync run (status = "completed").
    /// Used to find the boundary between complete and incomplete runs.
    async fn get_last_completed_run(&self, sync_name: &str) -> Result<Option<SyncRun>>;

    /// Get all incomplete runs (status != "completed") for reconciliation.
    async fn get_incomplete_runs(&self, sync_name: &str) -> Result<Vec<SyncRun>>;

    /// Mark a run as complete (called after CDC hash commit succeeds).
    async fn complete_run(&self, sync_name: &str, run_id: &str) -> Result<()>;

    // Run history
    async fn record_run(&self, run: &SyncRun) -> Result<()>;
    async fn get_runs(&self, sync_name: &str, limit: usize) -> Result<Vec<SyncRun>>;
}
```

### 5.4.1 Crash Recovery Protocol

The row journal and CDC hash commit at different times by design. The row journal serves as the write-ahead log (WAL) — no separate WAL is needed beyond DuckDB's native transaction durability.

**Commit ordering:**

```
Sync run start
    │
    ▼
Record run (status = "running") in sync_runs  ← committed
    │
    ▼
Extract source data → Arrow RecordBatch
    │
    ▼
CDC diff (hash/cursor/mirror) → changeset
    │
    ▼
For each delivery batch:
    ├── Deliver to destination
    ├── Mark synced rows in journal       ← committed per-batch (WAL)
    └── Mark pending/dead rows in journal ← committed per-batch (WAL)
    │
    ▼
All batches delivered
    │
    ▼
Commit CDC hash snapshot                 ← committed once (atomic)
    │
    ▼
Mark run as completed in sync_runs       ← committed once
```

**Reconciliation on startup (before each sync run):**

```rust
async fn reconcile(state: &dyn StateStore, sync_name: &str) -> Result<ReconciliationResult> {
    // 1. Find incomplete runs (crashed before completion)
    let incomplete = state.get_incomplete_runs(sync_name).await?;

    // 2. For each incomplete run, get rows that were already delivered
    let mut already_synced = HashSet::new();
    for run in &incomplete {
        let synced = state.get_synced_for_run(sync_name, &run.run_id).await?;
        already_synced.extend(synced);
    }

    // 3. Get pending rows from incomplete runs (eligible for retry)
    let pending = state.get_pending_rows(sync_name).await?;

    // 4. The last committed CDC hash is from the last *completed* run.
    //    Diff against it. Rows that appear "changed" but are in
    //    already_synced will be skipped by the delivery pipeline.

    Ok(ReconciliationResult {
        already_synced,  // skip these in delivery
        pending,          // include these in delivery set
        incomplete_runs: incomplete,
    })
}
```

**Crash scenarios and recovery:**

| Crash point | Journal state | Hash state | On restart |
|-------------|---------------|------------|-----------|
| Before delivery | Empty | Old hash | Fresh start. No data loss. |
| Mid-delivery (batch N/M) | Batches 1..N committed | Old hash | Re-diff against old hash. Journal skips already-synced rows. Pending rows re-delivered. Exactly-once preserved. |
| After all deliveries, before hash commit | All rows Synced | Old hash | Re-diff against old hash. All rows appear "changed". Journal skips all (already synced). No-op run. Exactly-once preserved. |
| During hash commit | All rows Synced | Old or new (atomic) | If old: same as above. If new: clean state, normal next run. |

**Why this works without a separate WAL:**
The row journal IS the WAL. It records delivery intent (which rows we're about to deliver) and outcome (synced/pending/dead). DuckDB's native transaction durability ensures journal commits survive crashes. Reconciliation at startup reads the journal to determine what was already done, and the CDC hash (only committed on full success) determines what still needs doing. The gap between "journal says done" and "hash says not done" is closed by the delivery pipeline checking the journal before re-delivering.

### 5.5 Configuration Schema

```rust
// crates/ferry-core/src/config.rs
#[derive(Deserialize, Validate)]
pub struct SyncConfig {
    pub name: String,
    pub description: Option<String>,
    pub tags: Vec<String>,
    pub model: ModelConfig,
    pub destination: DestinationConfig,
    pub sync: SyncSettings,
    // Note: `tests` block is reserved but not implemented in v0.1.
    // Post-sync assertions are deferred to dbt tests and Dagster asset checks.
}

#[derive(Deserialize)]
#[serde(untagged)]
pub enum ModelConfig {
    Sql { sql: String },
    Ref { r#ref: String },  // dbt model reference — resolved to compiled SQL via manifest.json
}

#[derive(Deserialize)]
pub struct SyncSettings {
    pub mode: SyncMode,                 // incremental | full_refresh | mirror
    pub cursor_field: Option<String>,
    pub cdc: CdcConfig,
    pub delivery: DeliveryConfig,
    pub full_refresh: Option<FullRefreshConfig>,
}

#[derive(Deserialize)]
pub struct CdcConfig {
    pub method: CdcMethod,              // hash | cursor
    pub hash_columns: HashColumns,      // all | list of column names
}

#[derive(Deserialize)]
pub struct DeliveryConfig {
    pub batch_size: usize,
    pub retry: RetryConfig,
    pub on_reject: RejectConfig,
    pub dead_letter: DeadLetterConfig,
    pub allow_redelivery: bool,         // default: false (exactly-once). true = at-least-once.
}
```

### 5.6 PyO3 Boundary

```rust
// crates/ferry-python/src/lib.rs
#[pyclass]
pub struct Project {
    inner: ferry_core::Project,
    runtime: tokio::runtime::Runtime,
}

#[pymethods]
impl Project {
    #[new]
    fn new(project_dir: &str) -> PyResult<Self> { ... }

    fn list_syncs(&self) -> PyResult<Vec<String>> { ... }

    #[pyo3(signature = (sync_names=None, dry_run=false, full_refresh=false, retry_dead=false))]
    fn run(
        &self,
        sync_names: Option<Vec<String>>,
        dry_run: bool,
        full_refresh: bool,
        retry_dead: bool,
    ) -> PyResult<Vec<SyncResult>> { ... }

    fn validate(&self) -> PyResult<Vec<ValidationError>> { ... }
    fn diff(&self, sync_name: &str) -> PyResult<DiffPreview> { ... }
    fn dlq_list(&self, sync_name: Option<&str>) -> PyResult<Vec<DeadRow>> { ... }
    fn dlq_retry(&self, sync_name: Option<&str>) -> PyResult<usize> { ... }
}

#[pyclass]
pub struct SyncResult {
    #[pyo3(get)] pub sync_name: String,
    #[pyo3(get)] pub rows_extracted: usize,
    #[pyo3(get)] pub rows_synced: usize,
    #[pyo3(get)] pub rows_failed: usize,
    #[pyo3(get)] pub rows_pending: usize,
    #[pyo3(get)] pub rows_retried: usize,
    #[pyo3(get)] pub rows_dead: usize,
    #[pyo3(get)] pub duration_seconds: f64,
    #[pyo3(get)] pub dry_run: bool,
}

#[pymodule]
fn _ferry_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Project>()?;
    m.add_class::<SyncResult>()?;
    Ok(())
}
```

---

## 6. Testing Strategy

### Unit Tests

- Config parsing (valid/invalid YAML, env var substitution)
- CDC hash computation (deterministic, collision-resistant)
- Diff engine (added/changed/removed detection)
- Error classification (rule matching)
- Retry backoff calculation
- State store operations (CRUD)

### Integration Tests

- DuckDB source → REST API destination (using httpbin or wiremock)
- Full sync lifecycle: extract → diff → deliver → state commit
- Retry behavior: simulate 429/500 responses, verify retry scheduling
- **Retry-After header**: simulate 429 with Retry-After, verify rate limiter pauses for requested duration
- DLQ workflow: fail rows → query DLQ → retry → verify delivery
- Full refresh: verify CDC state preserved after full refresh
- Mirror mode: verify removals detected and handled (RemoveByKey, RemoveAll, None)
- **Crash recovery**: kill process mid-delivery, restart, verify no duplicates and no drops
- **Exactly-once**: re-run sync, verify already-synced rows are skipped
- **Idempotency enforcement**: AppendOnly destination, verify re-delivery is blocked
- **Schema drift**: add/remove columns, verify correct behavior (re-hash on add, error on removed mapped column)

### End-to-End Tests

- Docker Compose with Postgres source + mock HTTP destination
- Multi-sync parallel execution (different destinations, shared state file)
- State persistence across process restarts (crash recovery)
- Python bindings: import, run, verify results match CLI output
- Dagster integration: materialize assets, verify metadata, verify dbt dependency auto-detection

### Performance Benchmarks

- 1M row hash CDC comparison (target: < 10s)
- 100K row sync to mock HTTP (target: < 60s)
- Memory profiling under 10M row workload (target: < 1GB peak)
- Startup time measurement (target: < 50ms)

---

## 7. Risks and Mitigations

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| Arrow schema inference failures on complex types | Sync failures on certain data types | Medium | Explicit type mapping in config; fallback to string |
| State corruption on crash during write | Silent data loss or duplicates | Low | Row journal as WAL (per-batch commit); atomic CDC hash commit; reconciliation on startup (see §5.4.1) |
| Rate limit exhaustion at destination | Sync stalls or bans | Medium | Governor-based rate limiting; respect Retry-After headers; configurable per destination |
| CDC hash table too large for memory (100M+ rows) | OOM | Low | Streaming hash comparison; chunked state queries |
| PyO3 version conflicts with user's Python env | Import failures | Medium | Stable ABI (abi3) builds; wide Python version support |
| Destination API changes breaking connectors | Silent sync failures | Medium | Versioned connector configs; schema validation on responses |
| Schema drift (source columns added/removed) | Sync failures or stale data | Medium | Explicit handling: added columns re-hash automatically; removed mapped columns error at extraction; `ferry validate` catches in CI |
| AppendOnly destination re-delivery | Duplicate data at destination | Medium | Idempotency capability enforcement; journal check before delivery; `allow_redelivery` must be explicitly set |
| Stale dbt manifest | Ferry syncs against old model logic | Medium | Stale manifest warning (24h); `ferry validate` checks manifest age |

---

## 8. Implementation Timeline

> **Note:** Timelines are approximate. Actual velocity depends on Rust/Arrow familiarity and AI-assisted development. Coding agents can materially compress these phases, but the sequencing matters more than the week count — each phase has hard dependencies on the previous.

### Phase 1: Core Engine

| Step | Deliverable | Dependencies |
|------|-------------|--------------|
| 1 | Cargo workspace scaffold, config parsing (serde + validation), project structure | None |
| 2 | Source trait + DuckDB implementation, Arrow extraction | Step 1 |
| 3 | State store (DuckDB backend), row journal (per-batch commit), run tracking | Step 1 |
| 4 | CDC engine (hash + cursor + mirror), crash recovery reconciliation | Steps 2, 3 |
| 5 | Delivery pipeline (batching, retries, classification, journal, exactly-once enforcement, Retry-After) | Steps 3, 4 |
| 6 | Connector capability system (IdempotencyCapability, RemoveCapability, remove/replace_all) | Step 5 |

### Phase 2: CLI + First Destinations

| Step | Deliverable | Dependencies |
|------|-------------|--------------|
| 7 | CLI (clap): init, run, validate, status, diff, dlq, sources, destinations | Phase 1 |
| 8 | REST API destination (generic, templated body via minijinja, pluggable auth, response mapping) | Phase 1 |
| 9 | Braze destination, Slack webhook destination, CSV/JSON/Parquet file destinations | Step 8 |
| 10 | Dry-run mode, `--full-refresh`, `--retry-dead`, `--output json` | Steps 7, 9 |

### Phase 3: Python + Dagster

| Step | Deliverable | Dependencies |
|------|-------------|--------------|
| 11 | PyO3 bindings (Project, SyncResult), maturin build, type stubs | Phase 2 |
| 12 | dagster-ferry: @ferry_assets, DagsterFerryResource, DagsterFerryTranslator, dbt dep auto-detection | Step 11 |
| 13 | CI/CD, PyPI publishing, Docker image | Steps 11, 12 |

### Phase 4: Source Expansion

| Step | Deliverable | Dependencies |
|------|-------------|--------------|
| 14 | PostgreSQL source | Phase 1 |
| 15 | dbt manifest reader, `ref()` resolution (compiled SQL, ephemeral rejection, stale detection) | Steps 14 |
| 16 | BigQuery source | Step 14 |
| 17 | Snowflake source | Step 14 |
| 18 | Additional destinations: HubSpot, Salesforce, S3/GCS, PostgreSQL upsert | Phase 2 |

### Phase 5+ (Future)

- HTTP trigger (`ferry serve` with HMAC auth)
- MCP server
- Warehouse state backend
- Shell completions
- Additional connectors (Databricks, Redshift, ClickHouse, SFTP, Google Sheets)

---

## 9. Success Criteria

| Metric | Target | Measurement |
|--------|--------|-------------|
| Cold start to first sync | < 5 minutes | Time from `pip install` to successful DuckDB → REST API sync |
| 1M row Braze sync | < 5 minutes | Benchmark with mock Braze endpoint |
| Binary size | < 100MB | Static release build with DuckDB bundled (DuckDB C++ runtime dominates size) |
| Zero data loss | 100% | No row is ever silently dropped (synced, pending, or dead) |
| Exactly-once delivery | 100% | Re-run sync, verify no duplicates (journal check before delivery) |
| Crash recovery | Deterministic | Kill process mid-sync, restart, verify no duplicates and no drops |
| Python import time | < 200ms | `time python -c "from ferry import Project"` |
| Dagster asset metadata | Complete | All fields populated in Dagster UI after materialization |

---

## 10. Open Questions

| # | Question | Status | Decision |
|---|----------|--------|----------|
| ~~1~~ | ~~Should mirror mode support configurable delete behavior at destination?~~ | **Resolved** | Destination declares `RemoveCapability` (RemoveByKey / RemoveAll / None). Mirror mode behavior adapts. |
| 2 | Should state store support Redis for distributed runners? | Deferred | Start with DuckDB + warehouse; Redis if needed later |
| 3 | Should CDC support column-level change tracking (only sync changed columns)? | Deferred | Full row sync initially; column-level is an optimization |
| 4 | Should there be a `ferry watch` mode for polling-based continuous sync? | Deferred | Out of scope for v1; orchestrator handles scheduling |
| ~~5~~ | ~~Should destinations support schema discovery for field mapping validation?~~ | **Resolved** | `ferry validate` checks for removed mapped columns at validate-time. Full schema discovery deferred to v0.2+ (would catch type mismatches and unmapped source columns). |
| 6 | Should the generic REST destination support pagination for reading destination state (e.g. verifying delivery)? | Open | Not needed for v0.1; destinations are write-only in the core loop |

---

## Appendix A: Cargo Workspace Structure

```toml
# Cargo.toml (workspace root)
[workspace]
members = [
    "crates/ferry-core",
    "crates/ferry-sources",
    "crates/ferry-destinations",
    "crates/ferry-cli",
    "crates/ferry-python",
]
resolver = "2"

[workspace.dependencies]
arrow = "54"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_yaml = "0.9"
reqwest = { version = "0.12", features = ["json"] }
pyo3 = { version = "0.22", features = ["extension-module"] }
pyo3-arrow = "0.17"
duckdb = { version = "1.1" }
clap = { version = "4", features = ["derive"] }
tracing = "0.1"
governor = "0.7"
backon = "1"
xxhash-rust = { version = "0.8", features = ["xxh3"] }
minijinja = "2"               # for generic REST destination body templating
async-trait = "0.1"
thiserror = "2"
toml = "0.8"                  # for secrets.toml parsing
```

> **Note:** `sqlx` is used for the PostgreSQL source (Phase 4). `inventory` was removed — connectors are config-driven, not compile-time registered. Each source/destination is a built-in implementation selected via YAML `type:` field.

## Appendix B: Example ferry.yml

```yaml
# ferry.yml (project root config)
name: my-data-activation
version: 1

source:
  type: snowflake
  connection:
    account: ${SNOWFLAKE_ACCOUNT}
    warehouse: COMPUTE_WH
    database: ANALYTICS
    schema: PUBLIC
    user: ${SNOWFLAKE_USER}
    password: ${SNOWFLAKE_PASSWORD}

state:
  backend: duckdb
  path: .ferry/state.duckdb
  # OR:
  # backend: warehouse
  # schema: _ferry_state

dbt:
  manifest_path: ../dbt-project/target/manifest.json

# Secrets are resolved in order: environment variables > secrets.toml > YAML inline (non-secret only)
# secrets.toml is gitignored, must have permissions 600, and is auto-created by `ferry init` with a template

defaults:
  sync:
    delivery:
      retry:
        max_attempts: 3
        backoff: exponential
        initial_delay: 30s
      allow_redelivery: false    # exactly-once by default
      on_reject:
        action: retry
      dead_letter:
        max_age: 7d
```
