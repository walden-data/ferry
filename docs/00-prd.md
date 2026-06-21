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
2. **Arrow-native** — Data flows as Apache Arrow RecordBatches from source to destination. Arrow-native internal data flow with zero-copy Python interop via PyCapsule. Columnar CDC diffing. Note: serialization to JSON/CSV at HTTP destinations is unavoidable — the "zero-copy" claim applies to internal Arrow operations and Python interop, not end-to-end delivery to HTTP-based destinations.
3. **Embeddable** — Single binary CLI, Python library (PyO3), or HTTP trigger. No server to deploy for basic usage.
4. **Code-first, Git-native** — Sync definitions are YAML files in version control. No UI, no database-backed config.
5. **Config-driven connectors** — Built-in connectors are selected and configured via YAML. No runtime plugin discovery or compilation required. The generic REST destination (templated body via Jinja, pluggable auth, response-to-row-status mapping) covers most custom API destinations without writing Rust. Runtime plugin loading (WASM, Lua) is a future goal.
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
│   ├── ferry-core/               # engine: config, orchestration, CDC, state, delivery
│   ├── ferry-sources/            # built-in source connectors (selected via YAML)
│   ├── ferry-destinations/       # built-in destination connectors (selected via YAML)
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
  ref: fct_active_users              # dbt model reference (resolved via manifest.json)
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
    allow_redelivery: false           # default: false (exactly-once). true = at-least-once.
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
  # Note: ferry defers post-sync assertions to dbt tests and Dagster asset checks.
  # The `tests` block is reserved for future use but not implemented in v0.1.
  # Use dbt tests on the source model and Dagster asset checks on the ferry asset.
```

### CDC Modes

| Mode | Behavior | Use when |
|------|----------|----------|
| **hash** | Hash all mapped columns per row, compare to stored snapshot. Detects adds, changes, AND removals. | Default. Most accurate. |
| **cursor** | Only sync rows where `cursor_field > last_cursor_value`. Cannot detect removals or changes to non-cursor fields. | Large tables where full hash is too expensive. |
| **mirror** | Full replace on every run. No diffing. Destination always matches source exactly. Requires destination with `RemoveByKey` or `RemoveAll` capability for delete support. If destination has `None` remove capability, mirror operates as full-replace (re-delivers all, does not remove stale rows) and logs a warning. | Small lookup tables, audiences for ad platforms. |

### Mirror Mode Delete Behavior

Mirror mode makes the destination match the source exactly. What "exactly" means depends on the destination's `RemoveCapability`:

| RemoveCapability | Mirror behavior |
|------------------|----------------|
| `RemoveByKey` | ferry sends only added/changed rows via `write()`, then sends removed rows via `remove()`. Efficient — only deltas are sent. |
| `RemoveAll` | ferry calls `replace_all()` with the full current dataset. Destination handles the swap atomically. |
| `None` | ferry re-delivers all rows via `write()`. Rows that exist at the destination but not in the source are **not removed**. Ferry logs a warning: "Destination does not support removal; stale rows may persist." This is a degraded mirror — use only when stale rows are acceptable (e.g. overwriting a file that gets fully replaced). |

### Full Refresh

Available as:
- `ferry run --full-refresh` (manual override)
- `sync.full_refresh.schedule: weekly` (periodic automatic)
- Programmatic trigger via Python/API

Full refresh re-delivers all rows regardless of CDC state but does NOT reset the CDC hash. After a full refresh, the next incremental run uses the refresh as the new baseline.

### dbt Integration

Sync definitions can reference dbt models via `model.ref: model_name`. Ferry resolves these via the dbt `manifest.json`:

**Resolution behavior:**
1. Load `manifest.json` from the path configured in `ferry.yml` (`dbt.manifest_path`)
2. Find the model node by name
3. Use the model's **compiled SQL** (not just the relation name) to extract data. This preserves any model logic (filters, joins, CASE statements) that exist in views. For materialized tables, this is equivalent to `SELECT * FROM <relation>` but using compiled SQL ensures consistency.
4. Emit a warning if the manifest is older than 24 hours (stale manifest detection)

**Ephemeral models:**
- Ephemeral dbt models are CTEs, not queryable tables. Ferry **rejects ephemeral refs in v0.1** with a clear error: "Ephemeral models are not supported. Materialize as a view or table, or use `model.sql` to inline the query."
- Future versions may support executing the compiled SQL (which includes the CTE) directly against the source.

**Error handling:**
- If `ref` cannot be found in the manifest: error with "Model '<name>' not found in dbt manifest. Check the manifest path and model name."
- If `dbt.manifest_path` is not configured: error with "dbt ref() requires `dbt.manifest_path` in ferry.yml"
- If manifest file does not exist: error with file path for debugging

### Durable Delivery (Row Journal)

Every row delivery attempt is tracked independently of CDC:

| Row Status | Meaning | Next action |
|------------|---------|-------------|
| **Synced** | Delivered successfully | None — row is done until source changes |
| **Pending** | Failed, will retry | Included in next run's delivery set |
| **Dead** | Failed after max retries | Lands in DLQ, requires manual intervention |

**Key invariant**: CDC state is NEVER affected by delivery failures. A row that fails to deliver does not corrupt the diff hash. It stays in the journal as Pending/Dead while CDC continues to track source changes normally.

#### Exactly-Once Delivery

By default, ferry enforces **exactly-once delivery** — a row is delivered to the destination exactly one time, no duplicates, no drops. This is enforced via the row journal:

1. Before delivering a batch, check the journal for each row's status
2. Skip rows already marked `Synced` (unless `--full-refresh` or `delivery.allow_redelivery: true`)
3. After successful delivery, mark rows `Synced` in the journal
4. After failed delivery, mark rows `Pending` (retryable) or `Dead` (DLQ)

The destination's idempotency capability (see Connectors) determines what happens if a re-delivery *does* occur (e.g. after a crash mid-batch):

- **UpsertByKey / Overwrite**: Safe — re-delivery overwrites cleanly, no duplicates
- **AppendOnly / None**: Unsafe — re-delivery creates duplicates. The journal-based skip prevents this, but a crash between "destination received the request" and "journal committed" is the vulnerability window. For these destinations, the journal commit must happen *before* sending the request (write-intent), then be confirmed after.

**Override**: Set `delivery.allow_redelivery: true` to enable at-least-once semantics for a sync. Useful for destinations where re-delivery is harmless (Overwrite capability) and you want to force re-delivery on every run regardless of journal state.

#### Crash Recovery Protocol

The row journal and CDC hash store commit at different times, by design:

| State | When committed | Granularity |
|-------|----------------|-------------|
| **Row journal** | After every batch delivery attempt | Per-batch (cheap DuckDB upsert) |
| **CDC hash snapshot** | Only after a full successful sync completes | Per-sync (expensive full rewrite) |

This split is the core of the durability guarantee. A crash at any point has a well-defined recovery path:

**Crash during extraction (before any delivery):**
- No journal commits, no hash commit. Next run starts fresh. No data loss, no duplicates.

**Crash mid-delivery (batch N of M delivered, then crash):**
- Journal has committed outcomes for batches 1..N (some Synced, some Pending)
- CDC hash is NOT committed (sync did not complete)
- On restart: ferry reads the journal, sees rows already Synced for this sync run, skips them. Re-extracts source data, re-diffs against the *last committed hash* (from the previous successful sync), delivers only the remaining changed rows + any Pending rows from the crashed run.
- Result: exactly-once delivery preserved. No duplicates, no drops.

**Crash after all deliveries but before hash commit:**
- Journal has all rows as Synced
- CDC hash is NOT committed
- On restart: ferry re-extracts, re-diffs against old hash. All rows appear "changed" (because new hash was never saved). Delivery pipeline checks journal, sees all rows already Synced, skips them all.
- Result: no-op run. Exactly-once preserved. The next *real* change will be detected against the old hash and delivered correctly.

**Crash during hash commit:**
- DuckDB transaction is atomic. Either the hash commit completes or it doesn't. No partial state.
- If it didn't commit: same as above (next run re-delivers, journal skips already-synced rows).
- If it did commit: clean state, next run is normal incremental.

**Reconciliation on startup:**
Before each sync run, ferry reconciles journal state:
1. Load last committed CDC hash for this sync
2. Load journal entries with status `Synced` and `last_sync_run_id == current_run_id - 1` (or any incomplete prior run)
3. If found: those rows were delivered in a prior run but the hash wasn't committed. Skip them in this run's delivery set. They will be re-detected as "changed" by the diff (since hash is stale) but skipped by the journal.
4. Mark any `Pending` rows from prior incomplete runs as eligible for this run's delivery set (respecting `next_retry_at`)

This protocol means ferry never needs a write-ahead log (WAL) beyond what DuckDB provides natively. The row journal *is* the WAL — it records delivery intent and outcome, and reconciliation on startup closes the gap.

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

-- Row journal (delivery outcomes) — committed per-batch, serves as the write-ahead log
_ferry_state.row_journal (sync_name, primary_key, status, attempts, last_error, last_attempt_at, next_retry_at, last_sync_run_id)

-- Sync run history
_ferry_state.sync_runs (sync_name, run_id, started_at, completed_at, rows_extracted, rows_synced, rows_failed, rows_retried, rows_dead, mode, dry_run, status)
```

### Concurrency Model

Ferry uses a single-writer model for state: one sync run at a time per state file. This is intentional — SaaS destinations have rate limits that would be violated if multiple syncs delivered in parallel to the same destination, and the complexity of multi-writer state (distributed locking, conflict resolution) is not justified for the target use case.

For parallel execution of *independent* syncs (different destinations), each sync can use its own state file (`state.path` per sync in `ferry.yml`), or the `--threads` flag can execute multiple syncs with separate destinations concurrently while sharing a single state file via serialized writes. The state backend serializes writes internally (DuckDB single-writer).

See the [Crash Recovery Protocol](#crash-recovery-protocol) section above for how journal and hash commits are coordinated for durability.

### Secrets Management

Ferry never stores credentials in YAML. Three sources of secrets, with strict precedence:

| Priority | Source | Use case |
|----------|--------|----------|
| 1 (highest) | Environment variables (`${VAR_NAME}` in YAML) | Production, CI/CD, containerized |
| 2 | `secrets.toml` (local, gitignored) | Development convenience |
| 3 (lowest) | YAML inline (non-secret config only) | Non-sensitive config like instance URLs |

**`secrets.toml` format:**
```toml
# secrets.toml (gitignored, permissions 600)
[source.snowflake]
password = "my-password"
user = "my-user"

[destination.braze]
api_key = "braze-api-key"
```

**Resolution:** When ferry encounters `${VAR_NAME}` in YAML, it resolves in order:
1. Check environment variables
2. Check `secrets.toml` for a matching key (section + key name)
3. If not found in either: error with "Secret '${VAR_NAME}' not found in environment or secrets.toml"

**Security:**
- `secrets.toml` must have file permissions 600 (ferry refuses to read it otherwise)
- `secrets.toml` is added to `.gitignore` by `ferry init`
- Ferry never logs secret values (masked as `***` in all output)
- No network calls without explicit config — ferry never phones home

---

## Connectors

Connectors are built-in to the ferry binary and selected/configured via YAML. No compilation or feature flags required. The generic REST destination (templated body via minijinja, pluggable auth, response-to-row-status mapping) is designed to cover most custom API destinations without writing Rust.

Future versions may support runtime plugin loading (WASM, Lua) for fully custom connectors.

### Sources

| Source | Status |
|--------|--------|
| DuckDB | MVP (default, included) |
| PostgreSQL | MVP |
| BigQuery | v0.2 |
| Snowflake | v0.2 |
| Databricks | v0.3 |
| Redshift | v0.3 |
| ClickHouse | v0.3 |
| MySQL | v0.3 |

### Destinations

| Destination | Idempotency | Remove Support | Status |
|-------------|-------------|-----------------|--------|
| REST API (generic) | Configurable (upsert/append) | Configurable (DELETE endpoint) | MVP |
| Braze | Upsert by external_id | Remove from segment | MVP |
| Slack (webhook) | Append-only | None (no-op) | MVP |
| CSV / Parquet / JSON file | Overwrite | Full replace | MVP |
| HubSpot | Upsert by email/object_id | Delete endpoint | v0.2 |
| Salesforce (Bulk API 2.0) | Upsert by External ID | Delete | v0.2 |
| S3 / GCS / Azure Blob | Overwrite by key | Delete by key | v0.2 |
| PostgreSQL (upsert) | Upsert by primary key | Delete | v0.2 |
| SFTP | Overwrite | Full replace | v0.3 |
| Google Sheets | Overwrite by row key | Delete rows | v0.3 |

### Connector Capabilities

Each destination declares its capabilities via the `Destination` trait. Two capabilities are critical for delivery semantics:

**Idempotency capability** — declares whether re-delivery is safe:

| Capability | Behavior | Re-delivery handling |
|------------|----------|---------------------|
| `UpsertByKey` | Destination upserts by primary key | Safe — re-delivery overwrites, no duplicates |
| `Overwrite` | Destination overwrites by key (e.g. S3 PUT) | Safe — re-delivery overwrites, no duplicates |
| `AppendOnly` | Destination appends (e.g. Slack webhook, POST without key) | Unsafe — re-delivery duplicates data |
| `None` | No idempotency guarantee | Unsafe — re-delivery may duplicate or corrupt |

The delivery pipeline enforces exactly-once delivery by default. For `AppendOnly` / `None` destinations, re-delivery is blocked unless the row journal confirms the row has not been successfully delivered. This can be overridden per-sync with `delivery.allow_redelivery: true` (use with caution — enables at-least-once semantics for that destination).

**Remove capability** — declares whether the destination supports row removal, used by mirror mode:

| Capability | Behavior |
|------------|----------|
| `RemoveByKey` | Can delete specific rows by key (e.g. Braze remove from segment) |
| `RemoveAll` | Can replace entire dataset (e.g. file overwrite, S3 prefix clear) |
| `None` | Cannot remove rows (e.g. Slack webhook, append-only logs) |

Mirror mode with remove requires a destination that declares `RemoveByKey` or `RemoveAll`. If the destination declares `None`, mirror mode operates as full-replace-at-destination (re-delivers all rows, does not attempt removals) and logs a warning that the destination may contain stale rows not present in the source.

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

    /// Idempotency capability — determines re-delivery safety
    fn idempotency(&self) -> IdempotencyCapability;

    /// Remove capability — determines mirror-mode delete behavior
    fn remove_capability(&self) -> RemoveCapability;

    /// Remove rows by primary key (only called if remove_capability is RemoveByKey)
    async fn remove(&self, keys: &[Value], config: &SyncConfig) -> Result<RemoveResult>;

    /// Replace entire dataset (only called if remove_capability is RemoveAll)
    async fn replace_all(&self, batch: &RecordBatch, config: &SyncConfig) -> WriteResult;
}

pub enum IdempotencyCapability {
    UpsertByKey,
    Overwrite,
    AppendOnly,
    None,
}

pub enum RemoveCapability {
    RemoveByKey,
    RemoveAll,
    None,
}
```

---

## Rate Limiting & Retry Behavior

### Rate Limit Enforcement

Ferry uses a token-bucket rate limiter (`governor` crate) per destination. Each destination declares its rate limit via `rate_limit()` on the `Destination` trait:

```rust
pub struct RateLimit {
    pub requests_per_second: Option<f64>,
    pub requests_per_minute: Option<u32>,
    pub concurrent: Option<usize>,
}
```

**Retry-After header interaction:**

When a destination returns HTTP 429 (Too Many Requests) with a `Retry-After` header, ferry:
1. Pauses the rate limiter (does not consume tokens while waiting)
2. Waits for the duration specified in `Retry-After` (seconds or HTTP date format)
3. Resumes delivery — the rate limiter's bucket is refilled but not overridden

If `Retry-After` is absent on a 429, ferry falls back to exponential backoff per the retry config. If `Retry-After` requests a wait longer than `max_delay`, ferry waits `max_delay` and retries (clamping).

This two-layer approach (token bucket for steady-state pacing + Retry-After for explicit backoff signals) respects both the destination's stated limits and its real-time feedback.

### Retry Behavior

Retries are per-row, per-batch. When a batch fails:
1. Rows that succeeded within the batch are marked `Synced` in the journal
2. Rows that failed are classified via `on_reject` rules (retry / dead_letter / skip / fail_sync)
3. Retried rows get `next_retry_at = now + backoff(attempts)` (exponential/linear/fixed, with jitter)
4. On next run (or same run if within retry window), pending rows are re-delivered

**Backoff strategies:**
| Strategy | Formula | Use case |
|----------|---------|----------|
| `exponential` | `initial_delay * 2^(attempts-1)`, capped at `max_delay`, with jitter | Default. Most APIs. |
| `linear` | `initial_delay * attempts`, with jitter | APIs with predictable recovery. |
| `fixed` | `initial_delay` (constant) | APIs with known cooldown. |

---

## Schema Drift Handling

Source schemas change in production. Ferry handles this explicitly:

### Added columns

| CDC mode | Behavior |
|----------|----------|
| `hash` with `hash_columns: all` | New column is included in the hash automatically. All existing rows appear "changed" (hash differs) and are re-delivered. This is correct — the data changed. |
| `hash` with explicit `hash_columns: [col1, col2]` | New column is NOT included in the hash (not in the list). No rows appear changed unless a listed column changed. This is intentional — you control what matters. |
| `cursor` | No effect (cursor only tracks `cursor_field`). |
| `mirror` | New column is delivered to destination if it's in the `mapping:` block. If not mapped, it's ignored. |

### Removed columns

| Scenario | Behavior |
|----------|----------|
| Mapped column removed from source | **Error** at extraction time: "Column '<name>' referenced in mapping not found in source query results. Update the sync config or restore the column." Sync aborts before any delivery. |
| Non-mapped column removed from source | No effect. Hash recomputes without it (if `hash_columns: all`). Rows may appear "changed" if the column was previously non-null and is now absent. |
| `cursor_field` removed from source | **Error**: "Cursor column '<name>' not found in source. Update `cursor_field` or restore the column." |

### Type changes

| Scenario | Behavior |
|----------|----------|
| Column type changed (e.g. INT → BIGINT) | Arrow handles widening casts automatically. Hash recomputes with new byte representation — rows appear "changed" and are re-delivered. Correct behavior. |
| Column type narrowed (e.g. BIGINT → INT) | Potential data loss. Ferry logs a warning: "Column '<name>' type changed from X to Y, may lose precision." Continues delivery. |
| Column type incompatible with destination mapping | **Error** at delivery time per-row. Row is classified via `on_reject` rules (typically dead_letter). |

### Best practices for schema drift

- Pin `hash_columns` to explicit list if you want to control what triggers re-sync
- Use `ferry validate` before `ferry run` in CI to catch removed columns before they hit production
- Monitor `ferry status` for unexpected "changed" row counts (indicates schema change)

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
ferry validate                            # check all YAML configs
ferry status                              # last run results
ferry history                             # run history

ferry dlq list                            # dead letter queue
ferry dlq retry                           # retry dead rows
ferry dlq purge --older-than 30d          # cleanup

ferry sources                             # list available source connectors
ferry destinations                        # list available destination connectors

# Phase 2+ (not in MVP):
ferry serve                               # HTTP webhook trigger (HMAC auth, for dbt Cloud webhooks)
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

#### Dagster Integration Design

**Asset mapping:**
- Each sync in `syncs/*.yml` becomes one Dagster asset with key derived from the sync name (e.g. `push_users_to_braze`)
- Sync `tags:` map to Dagster asset groups (e.g. `tags: [critical, braze]` → asset group `critical`, metadata tag `braze`)
- Asset description comes from sync `description:` field

**Dependencies:**
- If a sync uses `model.ref: <dbt_model>` and a Dagster dbt integration is present (e.g. `dagster-dbt`), ferry assets automatically depend on the corresponding dbt asset. This is resolved via the dbt manifest — ferry reads the manifest to find the dbt node name, and the Dagster translator maps it to the dbt asset key.
- Dependencies between ferry syncs (if any) are not auto-inferred — use Dagster's explicit `deps=` in the asset definition if needed.

**Subset execution:**
- `can_subset=True` on the multi-asset. Dagster's `AssetSelection` works natively — selecting individual syncs for materialization.
- `--select tag:<tag>` from the CLI maps to selecting assets by group/tag in the Dagster UI.

**Translator customization:**
`DagsterFerryTranslator` allows customizing:
- `get_asset_key(sync_config) -> AssetKey` — override the default sync-name-based key
- `get_group_name(sync_config) -> str` — override group assignment (default: first tag or "default")
- `get_deps(sync_config) -> list[AssetKey]` — add explicit dependencies beyond dbt auto-detection
- `get_kinds(sync_config) -> set[str]` — override the default `{"ferry", "<destination_type>"}`

**Dry-run from UI:**
- Dry-run is controllable from the Dagster UI RunConfig, not hardcoded in the asset definition
- The resource accepts a `dry_run` config field, overridable per-materialization

**DLQ metadata:**
- After each materialization, the DLQ row count for each sync is included in `MaterializeResult.metadata`
- A non-zero DLQ count does not fail the materialization (the sync succeeded), but is visible as a metadata field for monitoring/alerting via Dagster sensors

**Remote execution (future):**
- `build_ferry_asset_specs()` generates asset specs for Dagster Pipes / remote execution scenarios where ferry runs as a subprocess and communicates via Pipes messages
- Not in v0.1 — deferred to v0.2

### 4. HTTP Trigger (Phase 2+)

```bash
ferry serve --port 8080
# POST /api/v1/run {"sync_names": ["push_users_to_braze"], "dry_run": false}
```

For triggering from dbt Cloud webhooks, CI pipelines, or custom orchestration.

**Authentication:** HMAC signature verification (not bearer tokens). dbt Cloud and most webhook senders sign requests with a shared secret. Ferry verifies the `X-Webhook-Signature` header against the request body using the configured `FERRY_WEBHOOK_SECRET` env var. This is more secure than bearer tokens for automated triggers and is the standard for webhook authentication.

### 5. MCP Server (Phase 3+)

```bash
ferry mcp run
# Tools: ferry_list_syncs, ferry_run_sync, ferry_validate, ferry_status, ferry_dlq_list
```

Deferred — useful for demos and AI-assisted operations, but not core to the reverse ETL workflow. Ship after the engine, CLI, and Python bindings are stable.

---

## Rust Crate Dependencies (key choices)

| Concern | Crate | Rationale |
|---------|-------|-----------|
| Arrow | `arrow-rs` | Native RecordBatch, columnar operations |
| Python bindings | `pyo3` + `maturin` | Industry standard |
| Arrow ↔ Python | `pyo3-arrow` | Zero-copy FFI via PyCapsule (for Python consumers that want Arrow) |
| Source queries | `duckdb-rs` (MVP), `sqlx` (Postgres, Phase 4) | Async, Arrow-native |
| HTTP destinations | `reqwest` | Async, connection pooling |
| Rate limiting | `governor` | Token bucket, production-grade, with Retry-After pause support |
| Retries | `backon` | Exponential backoff with jitter |
| Async runtime | `tokio` | Standard |
| CLI | `clap` | Derive macros, completions |
| Config | `serde` + `serde_yaml` | Validated configs |
| Templates | `minijinja` | Jinja2-compatible (for generic REST body templates) |
| State (local) | `duckdb-rs` | Embedded analytics DB, atomic transactions |
| Secrets | `toml` | Parse secrets.toml (gitignored, permissions 600) |
| Hashing (CDC) | `xxhash-rust` | Fast non-crypto hash, columnar-friendly |
| Logging | `tracing` | Structured, async-aware |
| Parallelism | `tokio::JoinSet` | Concurrent sync execution (--threads) |

---

## Delivery Phases

> **Note:** Timelines are approximate and will be compressed by AI-assisted development. The sequencing matters more than the week count — each phase has hard dependencies on the previous.

### Phase 1: Core Engine

- ferry-core: config parsing, engine loop, CDC (hash + cursor + mirror), state (DuckDB), row journal (per-batch commit), DLQ, crash recovery protocol
- ferry-sources: DuckDB
- ferry-destinations: REST API (generic, templated body), Braze, Slack, CSV/JSON file
- Connector capability system (IdempotencyCapability, RemoveCapability)
- Exactly-once delivery enforcement (journal check before delivery)
- Retry-After header support
- Schema drift handling (added/removed columns)
- Tests: unit + integration with DuckDB + httpbin/wiremock, crash recovery tests

### Phase 2: CLI + Python Bindings

- ferry-cli: init, run, validate, status, diff, dlq, sources, destinations
- Dry-run mode, `--full-refresh`, `--retry-dead`, `--output json`
- ferry-python: PyO3 bindings (Project, SyncResult), maturin build, type stubs
- PostgreSQL source
- dbt manifest reader (compiled SQL resolution, ephemeral rejection, stale detection)
- CI/CD, PyPI publishing

### Phase 3: Dagster Integration

- dagster-ferry: @ferry_assets, DagsterFerryResource, DagsterFerryTranslator
- dbt dependency auto-detection (via manifest)
- DLQ metadata in MaterializeResult
- Dry-run from UI RunConfig
- Docker image

### Phase 4: Ecosystem Expansion

- Additional sources: BigQuery, Snowflake
- Additional destinations: HubSpot, Salesforce, S3/GCS, PostgreSQL upsert
- `ferry serve` (HTTP trigger with HMAC auth)
- Shell completions

### Phase 5+ (Future)

- MCP server
- Warehouse state backend
- Additional connectors (Databricks, Redshift, ClickHouse, SFTP, Google Sheets)
- Performance: streaming Arrow extraction, connection pooling
- Observability: OpenTelemetry traces, Prometheus metrics
- Advanced CDC: column-level change tracking, SCD2 support
- Runtime plugin loading (WASM, Lua) for custom connectors

---

## Non-Goals (explicitly out of scope)

- **Streaming / real-time CDC** — Ferry is batch-oriented. Use Debezium/Estuary for streaming.
- **Warehouse-to-warehouse replication** — Use dlt, Sling, or Airbyte for that.
- **Visual UI / audience builder** — Ferry is code-first. Use Hightouch/Census if you need a marketing UI.
- **Built-in scheduling** — Use your orchestrator (Dagster, Airflow, cron).
- **Built-in post-sync assertions** — `ferry test` is not implemented. Use dbt tests on the source model and Dagster asset checks on the ferry asset.
- **Event processing** — Ferry moves rows, not events. Different paradigm.
- **Runtime plugin loading** — v0.1 connectors are built-in. WASM/Lua plugins are a future goal.

---

## Success Metrics

- Cold start to first sync < 5 minutes (DuckDB source + REST API destination)
- 1M row sync to Braze completes in < 5 minutes
- Binary size < 100MB (static build with DuckDB bundled; DuckDB's C++ runtime dominates size)
- Python wheel installs in < 10 seconds
- Zero data loss: no row is ever silently dropped (synced, pending, or dead — always accounted for)
- Exactly-once delivery by default (via row journal + idempotency capability enforcement)

---

## License

MIT
