# Ferry — Product Requirements Document

> Rust-native reverse ETL. Moves your warehouse data to every tool, durably.

## Overview

Ferry is a code-first reverse ETL engine built in Rust with Python bindings. It queries data from warehouses, detects changes via configurable CDC, and durably delivers rows to external services (SaaS APIs, cloud storage, SFTP, databases).

Ferry is designed to be embedded in existing data stacks — invoked from Dagster, Airflow, or CI/CD — not to replace your orchestrator.

### Positioning

```
dlt (extract/load) → dbt (transform) → ferry (activate / reverse ETL)
```

Ferry is to reverse ETL what dlt is to data ingestion: a lightweight, code-first Python library backed by a fast engine, deployable anywhere Python (or a single binary) runs.

### Design Principles

1. **Durability first** — Inspired by Temporal. Every row delivery is tracked. Failed rows retry with backoff. Dead rows land in a queryable DLQ. CDC state is never corrupted by delivery failures.
2. **Arrow-native** — Data flows as Apache Arrow RecordBatches from source to destination. Zero-copy where possible. Columnar diffing for CDC.
3. **Embeddable** — Single binary CLI, Python library (PyO3), or HTTP trigger. No server to deploy for basic usage.
4. **Code-first, Git-native** — Sync definitions are YAML files in version control. No UI, no database-backed config.
5. **Compile-time connectors** — Connectors are Rust crates enabled via feature flags. No runtime plugin discovery (initially).
6. **Orchestrator-agnostic** — Dagster, Airflow, Prefect integrations are separate pure-Python packages. Ferry core never depends on them.

---

## Target Users

| Persona | Interaction | Cares about |
|---------|-------------|-------------|
| Data engineer | Writes sync YAML, runs CLI, configures Dagster assets | Reliability, debuggability, Git-native config |
| Platform engineer | Deploys, monitors, scales | Binary size, resource usage, observability |
| Analytics engineer | Defines what data to push (dbt models) | dbt integration, `ref()` support |

---

## Architecture

### High-Level

```
┌─────────────────────────────────────────────────────────────────┐
│                      User-Facing Surfaces                        │
│  ┌───────────┐  ┌────────────┐  ┌──────────────┐  ┌──────────┐│
│  │ CLI       │  │ Python lib │  │ dagster-ferry│  │ HTTP API ││
│  │ (binary)  │  │ (PyO3)     │  │ (pure Python)│  │ (trigger)││
│  └─────┬─────┘  └─────┬──────┘  └──────┬───────┘  └────┬─────┘│
├────────┼───────────────┼────────────────┼────────────────┼──────┤
│        └───────────────┼────────────────┘                │      │
│                  ┌─────▼─────┐                           │      │
│                  │ferry-core │◄──────────────────────────┘      │
│                  │ (Rust)    │                                   │
│                  └─────┬─────┘                                   │
│            ┌───────────┼───────────┐                             │
│       ┌────▼────┐ ┌────▼────┐ ┌────▼────┐                      │
│       │ Extract │ │  Diff   │ │  Load   │                      │
│       │ (source)│ │  (CDC)  │ │  (dest) │                      │
│       └─────────┘ └─────────┘ └─────────┘                      │
│                                                                  │
│         Arrow RecordBatch throughout (zero-copy)                 │
└──────────────────────────────────────────────────────────────────┘
```

### Workspace Layout

```
ferry/
├── Cargo.toml                    # workspace root
├── pyproject.toml                # maturin build config
├── crates/
│   ├── ferry-core/               # engine: config, orchestration, CDC, state
│   ├── ferry-sources/            # source connectors (feature-flagged)
│   ├── ferry-destinations/       # destination connectors (feature-flagged)
│   ├── ferry-cli/                # CLI binary (clap)
│   └── ferry-python/             # PyO3 bindings
├── python/
│   └── ferry/                    # pure Python wrapper + type stubs
├── integrations/
│   └── dagster-ferry/            # Dagster asset integration
└── tests/
```

---

## Core Concepts

### Sync

A sync is a YAML file defining: what data to read (model), where to send it (destination), and how to handle delivery (sync config).

```yaml
# syncs/push_users_to_braze.yml
name: push_users_to_braze
description: "Sync active users to Braze for lifecycle campaigns"
tags: [critical, braze, users]

model:
  ref: fct_active_users              # dbt model reference
  # OR
  # sql: SELECT * FROM analytics.fct_active_users WHERE is_active = true

destination:
  type: braze
  instance: us-01
  object: users
  mapping:
    external_id: user_id
    email: email_address
    first_name: first_name
    custom.plan_tier: subscription_tier

sync:
  mode: incremental                  # incremental | full_refresh | mirror
  cursor_field: updated_at           # for incremental mode

  cdc:
    method: hash                     # hash | cursor
    hash_columns: all                # or: [email, first_name, plan_tier]

  delivery:
    batch_size: 75
    retry:
      max_attempts: 5
      backoff: exponential
      initial_delay: 60s
      max_delay: 1h
    on_reject:
      classify:
        - match: { status: 429 }
          action: retry
        - match: { status: 400, body_contains: "invalid email" }
          action: dead_letter
        - match: { status: 5xx }
          action: retry
    dead_letter:
      max_age: 7d
      alert: true

  full_refresh:
    schedule: weekly                 # periodic full refresh regardless of CDC

tests:
  - type: row_count_positive
  - type: freshness
    max_age: 2h
```

### CDC Modes

| Mode | Behavior | Use when |
|------|----------|----------|
| **hash** | Hash all mapped columns per row, compare to stored snapshot. Detects adds, changes, AND removals. | Default. Most accurate. |
| **cursor** | Only sync rows where `cursor_field > last_cursor_value`. Cannot detect removals or changes to non-cursor fields. | Large tables where full hash is too expensive. |
| **mirror** | Full replace on every run. No diffing. Destination always matches source exactly. | Small lookup tables, audiences for ad platforms. |

### Full Refresh

Available as:
- `ferry run --full-refresh` (manual override)
- `sync.full_refresh.schedule: weekly` (periodic automatic)
- Programmatic trigger via Python/API

Full refresh re-delivers all rows regardless of CDC state but does NOT reset the CDC hash. After a full refresh, the next incremental run uses the refresh as the new baseline.

### Durable Delivery (Row Journal)

Every row delivery attempt is tracked independently of CDC:

| Row Status | Meaning | Next action |
|------------|---------|-------------|
| **Synced** | Delivered successfully | None — row is done until source changes |
| **Pending** | Failed, will retry | Included in next run's delivery set |
| **Dead** | Failed after max retries | Lands in DLQ, requires manual intervention |

**Key invariant**: CDC state is NEVER affected by delivery failures. A row that fails to deliver does not corrupt the diff hash. It stays in the journal as Pending/Dead while CDC continues to track source changes normally.

### Dead Letter Queue (DLQ)

```bash
ferry dlq list                               # show all dead rows
ferry dlq list --sync push_users_to_braze    # filter by sync
ferry dlq retry                              # retry all dead rows
ferry dlq retry --sync push_users_to_braze   # retry for one sync
ferry dlq purge --older-than 30d             # clean up
```

Dead rows are queryable, exportable, and retryable without affecting ongoing sync operations.

---

## State Storage

Ferry persists CDC snapshots, row journals, and run history. Two backends:

### Option 1: Local DuckDB (default)

```yaml
# ferry.yml
state:
  backend: duckdb
  path: .ferry/state.duckdb
```

Zero-config. State lives alongside the project. Good for single-machine or CI/CD runs.

### Option 2: Warehouse write-back

```yaml
# ferry.yml
state:
  backend: warehouse
  schema: _ferry_state
```

State tables are written to the same warehouse being read from. Enables shared state across multiple runners, queryable with SQL, and visible in dbt lineage.

### State Tables

```sql
-- CDC snapshots (row hashes for diff comparison)
_ferry_state.cdc_snapshots (sync_name, primary_key, row_hash, cursor_value, snapshot_at)

-- Row journal (delivery outcomes)
_ferry_state.row_journal (sync_name, primary_key, status, attempts, last_error, last_attempt_at, next_retry_at)

-- Sync run history
_ferry_state.sync_runs (sync_name, run_id, started_at, completed_at, rows_extracted, rows_synced, rows_failed, rows_retried, rows_dead, mode, dry_run)
```

---

## Connectors

### Sources (compile-time feature flags)

| Source | Crate feature | Status |
|--------|---------------|--------|
| DuckDB | `duckdb` (default) | MVP |
| PostgreSQL | `postgres` | MVP |
| BigQuery | `bigquery` | v0.2 |
| Snowflake | `snowflake` | v0.2 |
| Databricks | `databricks` | v0.3 |
| Redshift | `redshift` | v0.3 |
| ClickHouse | `clickhouse` | v0.3 |
| MySQL | `mysql` | v0.3 |

### Destinations (compile-time feature flags)

| Destination | Crate feature | Status |
|-------------|---------------|--------|
| REST API (generic) | default | MVP |
| Braze | default | MVP |
| HubSpot | default | v0.2 |
| Salesforce (Bulk API 2.0) | `salesforce` | v0.2 |
| Slack (webhook) | default | MVP |
| Google Sheets | `sheets` | v0.3 |
| PostgreSQL (upsert) | `postgres` | v0.2 |
| S3 / GCS / Azure Blob | `cloud-storage` | v0.2 |
| SFTP | `sftp` | v0.3 |
| CSV / Parquet / JSON file | default | MVP |

### Connector Traits

```rust
#[async_trait]
pub trait Source: Send + Sync {
    async fn check_connection(&self) -> Result<()>;
    async fn discover(&self) -> Result<Vec<StreamSchema>>;
    fn read(&self, query: &str) -> RecordBatchStream;
}

#[async_trait]
pub trait Destination: Send + Sync {
    async fn check_connection(&self) -> Result<()>;
    async fn write(&self, batch: &RecordBatch, config: &SyncConfig) -> WriteResult;
    fn max_batch_size(&self) -> usize;
    fn rate_limit(&self) -> Option<RateLimit>;
}
```

---

## Product Surfaces

### 1. CLI (primary interface)

```bash
ferry init                                # scaffold project
ferry run                                 # run all syncs (incremental)
ferry run --select <name>                 # run specific sync
ferry run --select tag:<tag>              # run by tag
ferry run --full-refresh                  # ignore CDC, re-deliver everything
ferry run --dry-run                       # preview without writing
ferry run --threads 4                     # parallel execution
ferry run --retry-dead                    # include DLQ rows in this run
ferry run --output json                   # structured output for CI

ferry diff --select <name>                # preview what CDC would detect
ferry test                                # run post-sync assertions
ferry validate                            # check all YAML configs
ferry status                              # last run results
ferry history                             # run history

ferry dlq list                            # dead letter queue
ferry dlq retry                           # retry dead rows
ferry dlq purge --older-than 30d          # cleanup

ferry sources                             # list available source connectors
ferry destinations                        # list available destination connectors

ferry serve                               # HTTP webhook trigger
ferry mcp run                             # MCP server for AI tools
```

### 2. Python Library (embeddable)

```python
from ferry import Project, SyncResult

project = Project("./my-ferry-project")
results: list[SyncResult] = project.run(sync_names=["push_users_to_braze"])

for r in results:
    print(f"{r.sync_name}: {r.rows_synced} synced, {r.rows_failed} failed")
```

Published as: `pip install ferry-core` (includes Rust engine via maturin wheel)

### 3. Dagster Integration

```python
from dagster import AssetExecutionContext, Definitions
from dagster_ferry import ferry_assets, DagsterFerryResource

@ferry_assets(project_dir="path/to/ferry-project")
def reverse_etl(context: AssetExecutionContext, ferry: DagsterFerryResource):
    yield from ferry.run(context=context)

defs = Definitions(
    assets=[reverse_etl],
    resources={"ferry": DagsterFerryResource(project_dir="path/to/ferry-project")},
)
```

Each sync becomes a materialized Dagster asset with:
- `MaterializeResult` metadata (rows_synced, rows_failed, rows_retried, rows_dead, duration_seconds)
- Asset kinds: `{"ferry", "<destination_type>"}`
- Subset execution (can_subset=True)
- DLQ count surfaced as asset metadata

Published as: `pip install dagster-ferry`

### 4. HTTP Trigger

```bash
ferry serve --port 8080
# POST /api/v1/run {"sync_names": ["push_users_to_braze"], "dry_run": false}
```

For triggering from dbt Cloud webhooks, CI pipelines, or custom orchestration.

### 5. MCP Server

```bash
ferry mcp run
# Tools: ferry_list_syncs, ferry_run_sync, ferry_validate, ferry_status, ferry_dlq_list
```

---

## Rust Crate Dependencies (key choices)

| Concern | Crate | Rationale |
|---------|-------|-----------|
| Arrow | `arrow-rs` | Native RecordBatch, zero-copy |
| Python bindings | `pyo3` + `maturin` | Industry standard |
| Arrow ↔ Python | `pyo3-arrow` | Zero-copy FFI via PyCapsule |
| Source queries | `sqlx` (Postgres/MySQL), `duckdb-rs` | Async, Arrow-native |
| HTTP destinations | `reqwest` | Async, connection pooling |
| Rate limiting | `governor` | Token bucket, production-grade |
| Retries | `backon` | Exponential backoff with jitter |
| Async runtime | `tokio` | Standard |
| CLI | `clap` | Derive macros, completions |
| Config | `serde` + `serde_yaml` | Validated configs |
| Templates | `minijinja` | Jinja2-compatible (for REST body templates) |
| Connector registry | `inventory` | Auto-registration |
| State (local) | `duckdb-rs` | Embedded analytics DB |
| Hashing (CDC) | `xxhash-rust` | Fast non-crypto hash |
| Logging | `tracing` | Structured, async-aware |
| Parallelism | `tokio::JoinSet` | Concurrent sync execution |

---

## Delivery Phases

### Phase 1: MVP (8-10 weeks)

- ferry-core: config parsing, engine loop, CDC (hash + cursor), state (DuckDB), row journal, DLQ
- ferry-sources: DuckDB, PostgreSQL
- ferry-destinations: REST API (generic), Braze, Slack, CSV/JSON file
- ferry-cli: init, run, status, diff, dlq, validate
- ferry-python: PyO3 bindings (Project, SyncResult)
- Tests: unit + integration with DuckDB + httpbin

### Phase 2: Ecosystem (4-6 weeks)

- dagster-ferry: asset decorator, resource, translator
- Additional sources: BigQuery, Snowflake
- Additional destinations: HubSpot, Salesforce, S3/GCS, PostgreSQL upsert
- dbt manifest reader (resolve `ref()`)
- `ferry serve` (HTTP trigger)
- `ferry test` (post-sync assertions)

### Phase 3: Production Hardening (4-6 weeks)

- Warehouse state backend (write-back)
- Mirror mode (full replace with delete detection)
- Parallel sync execution (`--threads`)
- MCP server
- Structured JSON output for CI
- Shell completions
- Docker image (static binary, scratch base)

### Phase 4: Scale (ongoing)

- Additional connectors (Databricks, Redshift, ClickHouse, SFTP, Google Sheets)
- Performance: streaming Arrow extraction, connection pooling
- Observability: OpenTelemetry traces, Prometheus metrics
- Advanced CDC: column-level change tracking, SCD2 support

---

## Non-Goals (explicitly out of scope)

- **Streaming / real-time CDC** — Ferry is batch-oriented. Use Debezium/Estuary for streaming.
- **Warehouse-to-warehouse replication** — Use dlt, Sling, or Airbyte for that.
- **Visual UI / audience builder** — Ferry is code-first. Use Hightouch/Census if you need a marketing UI.
- **Built-in scheduling** — Use your orchestrator (Dagster, Airflow, cron).
- **Event processing** — Ferry moves rows, not events. Different paradigm.

---

## Success Metrics

- Cold start to first sync < 5 minutes (DuckDB source + REST API destination)
- 1M row sync to Braze completes in < 5 minutes
- Binary size < 30MB (static, no runtime deps)
- Python wheel installs in < 10 seconds
- Zero data loss: no row is ever silently dropped

---

## License

MIT
