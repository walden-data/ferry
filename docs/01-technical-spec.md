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
- [ ] Support dbt model references (`ref: model_name`) via manifest.json
- [ ] Return typed Arrow RecordBatches (infer schema from query results)
- [ ] Stream large result sets without loading entirely into memory
- [ ] Test connection before executing (`ferry validate`)

**Dependencies:** None

### FR-3: CDC (Change Data Capture)

**Priority:** P0 (Must Have)
**Description:** Detect which rows have changed since the last sync run.

**Acceptance Criteria:**
- [ ] **Hash mode**: Compute xxhash of all mapped columns per row; compare to stored hashes; emit added/changed/removed sets
- [ ] **Cursor mode**: Filter rows where cursor_field > stored cursor value; emit only new/updated rows
- [ ] **Mirror mode**: Skip diffing entirely; deliver all rows every run
- [ ] Full refresh override (`--full-refresh`) bypasses CDC but does NOT reset stored state
- [ ] CDC state persists across runs in configured state backend
- [ ] Changing mapped columns triggers a re-hash (not a full refresh)

**Dependencies:** FR-5

### FR-4: Durable Delivery

**Priority:** P0 (Must Have)
**Description:** Track per-row delivery outcomes independently of CDC state.

**Acceptance Criteria:**
- [ ] Record delivery outcome per row (synced/pending/dead) in row journal
- [ ] Pending rows are automatically included in the next run's delivery set
- [ ] Dead rows (exceeded max_attempts) are excluded from automatic retry
- [ ] Dead rows are queryable via `ferry dlq list`
- [ ] Dead rows are manually retryable via `ferry dlq retry`
- [ ] Delivery failures NEVER corrupt CDC hash state
- [ ] Classify errors by HTTP status / response body using configurable rules
- [ ] Support exponential, linear, and fixed backoff strategies
- [ ] Respect `next_retry_at` — don't retry too early

**Dependencies:** FR-5

### FR-5: State Management

**Priority:** P0 (Must Have)
**Description:** Persist sync state (CDC snapshots, row journal, run history).

**Acceptance Criteria:**
- [ ] DuckDB backend: store state in `.ferry/state.duckdb`
- [ ] Warehouse backend: store state in configurable schema within source warehouse
- [ ] State is atomic — partial sync failures don't corrupt state
- [ ] State is queryable (SQL for warehouse backend, CLI for DuckDB)
- [ ] Run history retained with configurable TTL

**Dependencies:** None

### FR-6: CLI Interface

**Priority:** P0 (Must Have)
**Description:** Full-featured command-line interface for all operations.

**Acceptance Criteria:**
- [ ] `ferry init` — scaffold project with example config
- [ ] `ferry run` — execute syncs with filtering (--select, --tags)
- [ ] `ferry run --dry-run` — preview without writing
- [ ] `ferry run --full-refresh` — bypass CDC
- [ ] `ferry run --retry-dead` — include DLQ rows
- [ ] `ferry run --output json` — structured output for CI
- [ ] `ferry diff` — preview what CDC would detect
- [ ] `ferry validate` — check configs and test connections
- [ ] `ferry status` / `ferry history` — run results
- [ ] `ferry dlq list/retry/purge` — dead letter queue management
- [ ] Shell completion (bash/zsh/fish)

**Dependencies:** FR-1 through FR-5

### FR-7: Python Bindings

**Priority:** P1 (Should Have)
**Description:** PyO3-based Python library exposing core engine functionality.

**Acceptance Criteria:**
- [ ] `Project` class: open project, list syncs, run syncs, get status
- [ ] `SyncResult` class: structured result with all metadata fields
- [ ] Zero-copy Arrow interop via pyo3-arrow (PyCapsule interface)
- [ ] Tokio runtime managed internally (no async leaking to Python)
- [ ] Published as `ferry-core` on PyPI via maturin wheels
- [ ] Type stubs (.pyi) for IDE support

**Dependencies:** FR-1 through FR-5

### FR-8: Dagster Integration

**Priority:** P1 (Should Have)
**Description:** Pure Python package exposing ferry syncs as Dagster assets.

**Acceptance Criteria:**
- [ ] `@ferry_assets` decorator creates multi_asset with can_subset=True
- [ ] `DagsterFerryResource` yields MaterializeResult per sync
- [ ] `DagsterFerryTranslator` for customizing asset keys, groups, deps
- [ ] `build_ferry_asset_specs()` for Dagster Pipes / remote execution
- [ ] Asset kinds: `{"ferry", "<destination_type>"}`
- [ ] Dry-run controllable from Dagster UI RunConfig
- [ ] DLQ row count surfaced as asset metadata

**Dependencies:** FR-7

### FR-9: dbt Integration

**Priority:** P1 (Should Have)
**Description:** Resolve dbt model references from manifest.json.

**Acceptance Criteria:**
- [ ] Parse `target/manifest.json` to resolve `ref: model_name` → fully qualified table
- [ ] Support configurable manifest path in `ferry.yml`
- [ ] Error clearly if ref cannot be resolved

**Dependencies:** None

### FR-10: HTTP Trigger

**Priority:** P2 (Nice to Have)
**Description:** Lightweight HTTP server for webhook-triggered syncs.

**Acceptance Criteria:**
- [ ] `ferry serve --port 8080` starts HTTP listener
- [ ] POST /api/v1/run triggers sync execution
- [ ] Request body specifies sync_names, dry_run, full_refresh
- [ ] Returns structured JSON result
- [ ] Bearer token authentication

**Dependencies:** FR-1

### FR-11: MCP Server

**Priority:** P2 (Nice to Have)
**Description:** MCP protocol server for AI tool integration.

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
| CDC hash comparison (1M rows) | < 10 seconds | xxhash on Arrow columns |
| Cold start (CLI) | < 50ms | Static binary, no interpreter |
| Memory usage (1M rows) | < 500MB | Arrow columnar, streaming where possible |
| Python library import | < 200ms | PyO3 module load |

### Reliability

- **Zero silent data loss**: Every row is either synced, pending retry, or in DLQ. Never dropped.
- **Atomic state updates**: Partial failures don't corrupt CDC or journal state.
- **Idempotent delivery**: Re-running a sync doesn't duplicate rows at the destination (primary key-based).
- **Crash recovery**: If the process dies mid-sync, the next run picks up from last committed state.

### Scalability

- **Row volume**: Handle 10M+ row tables for CDC hash comparison
- **Concurrent syncs**: Support parallel execution of independent syncs
- **Batch parallelism**: Multiple HTTP requests in flight per sync (within rate limits)
- **State growth**: Journal auto-prunes synced rows; DLQ has configurable TTL

### Security

- **No secrets in YAML**: Use `${ENV_VAR}` substitution for credentials
- **Optional secrets.toml**: Local file (gitignored) for development convenience
- **No network calls without explicit config**: Ferry never phones home
- **Destination credentials scoped**: Each sync only accesses its configured destination

### Portability

- **Binary targets**: Linux x86_64, Linux aarch64, macOS x86_64, macOS aarch64
- **Python wheels**: manylinux, macOS universal2
- **Container**: Scratch-based Docker image (< 30MB)
- **No system dependencies**: Static linking, no OpenSSL/libpq required at runtime

---

## 5. Technical Design

### 5.1 Connector Trait System

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
    pub retryable: bool,
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
    ) -> DeliveryResult {
        let batch_size = self.destination.max_batch_size();
        let batches = split_record_batch(rows, batch_size);

        let mut results = DeliveryResult::default();

        for batch in batches {
            // Wait for rate limiter
            self.rate_limiter.until_ready().await;

            // Attempt delivery
            let write_result = self.destination.write(&batch, &config.write_config).await;

            // Classify each row outcome
            for error in &write_result.errors {
                let classification = self.classify_error(error, config);
                match classification {
                    Action::Retry => {
                        self.journal.mark_pending(&error.primary_key, &error.message).await?;
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

            results.rows_synced += write_result.rows_synced;
        }

        Ok(results)
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

```rust
// crates/ferry-core/src/state.rs
#[async_trait]
pub trait StateStore: Send + Sync {
    // CDC state
    async fn get_hashes(&self, sync_name: &str) -> Result<HashMap<PrimaryKey, u64>>;
    async fn set_hashes(&self, sync_name: &str, hashes: &HashMap<PrimaryKey, u64>) -> Result<()>;
    async fn get_cursor(&self, sync_name: &str) -> Result<Option<Value>>;
    async fn set_cursor(&self, sync_name: &str, value: Value) -> Result<()>;

    // Row journal
    async fn get_pending_rows(&self, sync_name: &str) -> Result<Vec<RowEntry>>;
    async fn get_dead_rows(&self, sync_name: &str) -> Result<Vec<RowEntry>>;
    async fn mark_synced(&self, sync_name: &str, primary_keys: &[PrimaryKey]) -> Result<()>;
    async fn mark_pending(&self, sync_name: &str, pk: &PrimaryKey, error: &str) -> Result<()>;
    async fn mark_dead(&self, sync_name: &str, pk: &PrimaryKey, error: &str) -> Result<()>;
    async fn retry_dead_rows(&self, sync_name: &str, pks: Option<&[PrimaryKey]>) -> Result<usize>;
    async fn purge_dead_rows(&self, sync_name: &str, older_than: Duration) -> Result<usize>;

    // Run history
    async fn record_run(&self, run: &SyncRun) -> Result<()>;
    async fn get_runs(&self, sync_name: &str, limit: usize) -> Result<Vec<SyncRun>>;
}
```

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
    pub tests: Vec<TestConfig>,
}

#[derive(Deserialize)]
#[serde(untagged)]
pub enum ModelConfig {
    Sql { sql: String },
    Ref { r#ref: String },  // dbt model reference
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
- DLQ workflow: fail rows → query DLQ → retry → verify delivery
- Full refresh: verify CDC state preserved after full refresh
- Mirror mode: verify removals detected and handled

### End-to-End Tests

- Docker Compose with Postgres source + mock HTTP destination
- Multi-sync parallel execution
- State persistence across process restarts (crash recovery)
- Python bindings: import, run, verify results match CLI output
- Dagster integration: materialize assets, verify metadata

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
| State corruption on crash during write | Silent data loss or duplicates | Low | Write-ahead journal; atomic state commits |
| Rate limit exhaustion at destination | Sync stalls or bans | Medium | Governor-based rate limiting; configurable per destination |
| CDC hash table too large for memory (100M+ rows) | OOM | Low | Streaming hash comparison; chunked state queries |
| PyO3 version conflicts with user's Python env | Import failures | Medium | Stable ABI (abi3) builds; wide Python version support |
| Destination API changes breaking connectors | Silent sync failures | Medium | Versioned connector configs; schema validation on responses |

---

## 8. Implementation Timeline

### Phase 1: Core Engine (Weeks 1-8)

| Week | Deliverable |
|------|-------------|
| 1-2 | Cargo workspace scaffold, config parsing (serde), project structure |
| 3-4 | Source trait + DuckDB implementation, Arrow extraction |
| 5-6 | CDC engine (hash + cursor), state store (DuckDB backend) |
| 7-8 | Delivery pipeline (batching, retries, classification, journal) |

### Phase 2: CLI + First Destinations (Weeks 9-12)

| Week | Deliverable |
|------|-------------|
| 9 | CLI (clap): init, run, validate, status, dlq |
| 10 | REST API destination (generic, configurable) |
| 11 | Braze destination, Slack webhook destination |
| 12 | CSV/JSON/Parquet file destinations, dry-run mode |

### Phase 3: Python + Dagster (Weeks 13-16)

| Week | Deliverable |
|------|-------------|
| 13-14 | PyO3 bindings, maturin build, type stubs |
| 15 | dagster-ferry: @ferry_assets, DagsterFerryResource |
| 16 | CI/CD, PyPI publishing, Docker image |

### Phase 4: Source Expansion (Weeks 17-20)

| Week | Deliverable |
|------|-------------|
| 17 | PostgreSQL source (sqlx) |
| 18 | BigQuery source |
| 19 | Snowflake source |
| 20 | dbt manifest reader, `ref()` resolution |

---

## 9. Success Criteria

| Metric | Target | Measurement |
|--------|--------|-------------|
| Cold start to first sync | < 5 minutes | Time from `pip install` to successful DuckDB → REST API sync |
| 1M row Braze sync | < 5 minutes | Benchmark with mock Braze endpoint |
| Binary size | < 30MB | Static release build |
| Zero data loss | 100% | No row is ever silently dropped (synced, pending, or dead) |
| Crash recovery | Deterministic | Kill process mid-sync, restart, verify correct resumption |
| Python import time | < 200ms | `time python -c "from ferry import Project"` |
| Dagster asset metadata | Complete | All fields populated in Dagster UI after materialization |

---

## 10. Open Questions

| # | Question | Status | Decision |
|---|----------|--------|----------|
| 1 | Should mirror mode support configurable delete behavior at destination? | Open | Options: soft delete, hard delete, unset attributes, no-op |
| 2 | Should state store support Redis for distributed runners? | Deferred | Start with DuckDB + warehouse; Redis if needed later |
| 3 | Should CDC support column-level change tracking (only sync changed columns)? | Deferred | Full row sync initially; column-level is an optimization |
| 4 | Should there be a `ferry watch` mode for polling-based continuous sync? | Deferred | Out of scope for v1; orchestrator handles scheduling |
| 5 | Should destinations support schema discovery for field mapping validation? | Open | Would catch config errors at validate-time rather than runtime |

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
sqlx = { version = "0.8", features = ["runtime-tokio", "postgres"] }
duckdb = { version = "1.1" }
clap = { version = "4", features = ["derive"] }
tracing = "0.1"
governor = "0.7"
backon = "1"
xxhash-rust = { version = "0.8", features = ["xxh3"] }
minijinja = "2"
inventory = "0.3"
async-trait = "0.1"
thiserror = "2"
```

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

defaults:
  sync:
    delivery:
      retry:
        max_attempts: 3
        backoff: exponential
        initial_delay: 30s
      on_reject:
        action: retry
      dead_letter:
        max_age: 7d
```
