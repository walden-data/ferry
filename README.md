# ferry

> Rust-native reverse ETL. Moves your warehouse data to every tool, durably.

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

---

**ferry** syncs data from your warehouse to external services — durably, via YAML and CLI.

Think `dbt build` then `ferry run`. Same developer experience, opposite data direction.

```bash
pip install ferry-core          # core engine (DuckDB included)
ferry init && ferry run
```

## Why ferry?

| Problem | ferry's answer |
|---------|---------------|
| Hightouch/Census are expensive SaaS | Free, open-source, self-hosted |
| GUI-first tools don't fit CI/CD | CLI + YAML, Git-native |
| Failed rows corrupt sync state | Durable delivery — row journal + DLQ, independent of CDC |
| Python reverse ETL is slow at scale | Rust engine, Arrow-native, true async parallelism |
| Need a server to run reverse ETL | Single binary, zero dependencies, embed anywhere |

## Quickstart

### 1. Install

```bash
pip install ferry-core
```

> For cloud sources: `pip install ferry-core[bigquery]`, `ferry-core[snowflake]`, etc.

### 2. Initialize a project

```bash
mkdir my-ferry-project && cd my-ferry-project
ferry init
```

### 3. Define a sync

```yaml
# syncs/push_users_to_braze.yml
name: push_users_to_braze
description: "Sync active users to Braze"
model:
  sql: SELECT user_id, email, first_name, plan_tier FROM active_users

destination:
  type: braze
  instance: us-01
  object: users
  mapping:
    external_id: user_id
    email: email
    first_name: first_name
    custom.plan_tier: plan_tier

sync:
  mode: incremental
  cursor_field: updated_at
  cdc:
    method: hash
  delivery:
    batch_size: 75
    retry:
      max_attempts: 5
      backoff: exponential
```

### 4. Run

```bash
ferry run --dry-run   # preview, no data sent
ferry run             # execute
ferry status          # check results
```

## Features

### Durable Delivery

Every row delivery is tracked independently of CDC state. Failed rows retry with exponential backoff. After max attempts, rows land in a queryable Dead Letter Queue — never silently dropped, never corrupting your diff state.

```bash
ferry dlq list                             # see failed rows
ferry dlq retry --sync push_users_to_braze # retry them
ferry run --retry-dead                     # include DLQ rows in next run
```

### Configurable CDC

| Mode | Behavior |
|------|----------|
| `hash` | Hash mapped columns, compare to snapshot. Detects adds, changes, AND removals. |
| `cursor` | Sync rows where cursor_field > last value. Fast, but can't detect removals. |
| `mirror` | Full replace every run. Destination always matches source. |

Plus `--full-refresh` flag to override any mode and re-deliver everything.

### Arrow-Native Performance

Data flows as Apache Arrow RecordBatches from source to destination. Columnar CDC diffing. Zero-copy Python interop via PyCapsule. True async parallelism via tokio — no GIL.

## CLI Reference

```bash
ferry init                          # scaffold project
ferry run                           # run all syncs (incremental)
ferry run --select <name>           # run specific sync
ferry run --select tag:<tag>        # run by tag
ferry run --full-refresh            # ignore CDC, re-deliver all
ferry run --dry-run                 # preview without writing
ferry run --threads 4               # parallel execution
ferry run --retry-dead              # include DLQ rows
ferry run --output json             # structured output for CI

ferry diff --select <name>          # preview CDC changes
ferry test                          # post-sync assertions
ferry validate                      # check all YAML configs
ferry status                        # last run results
ferry history                       # run history

ferry dlq list                      # dead letter queue
ferry dlq retry                     # retry dead rows
ferry dlq purge --older-than 30d    # cleanup

ferry sources                       # list available sources
ferry destinations                  # list available destinations
ferry serve                         # HTTP webhook trigger
ferry mcp run                       # MCP server for AI tools
```

## Connectors

### Sources

| Connector | Install | Status |
|-----------|---------|--------|
| DuckDB | `ferry-core` (included) | Planned |
| PostgreSQL | `ferry-core[postgres]` | Planned |
| BigQuery | `ferry-core[bigquery]` | Planned |
| Snowflake | `ferry-core[snowflake]` | Planned |
| Databricks | `ferry-core[databricks]` | Planned |
| Redshift | `ferry-core[redshift]` | Planned |

### Destinations

| Connector | Install | Status |
|-----------|---------|--------|
| REST API (generic) | `ferry-core` (included) | Planned |
| Braze | `ferry-core` (included) | Planned |
| HubSpot | `ferry-core` (included) | Planned |
| Salesforce (Bulk API 2.0) | `ferry-core[salesforce]` | Planned |
| Slack (webhook) | `ferry-core` (included) | Planned |
| S3 / GCS / Azure Blob | `ferry-core[cloud-storage]` | Planned |
| SFTP | `ferry-core[sftp]` | Planned |
| PostgreSQL (upsert) | `ferry-core[postgres]` | Planned |
| CSV / Parquet / JSON file | `ferry-core` (included) | Planned |

## Orchestration: dagster-ferry

```bash
pip install dagster-ferry
```

```python
from dagster import AssetExecutionContext, Definitions
from dagster_ferry import ferry_assets, DagsterFerryResource

@ferry_assets(project_dir="path/to/ferry-project")
def my_syncs(context: AssetExecutionContext, ferry: DagsterFerryResource):
    yield from ferry.run(context=context)

defs = Definitions(
    assets=[my_syncs],
    resources={"ferry": DagsterFerryResource(project_dir="path/to/ferry-project")},
)
```

Each sync becomes a materialized Dagster asset with structured metadata (rows_synced, rows_failed, rows_retried, rows_dead, duration_seconds), subset execution, and dry-run support from the UI.

## Ecosystem

ferry is designed to complement the modern data stack:

```
dlt (extract/load) → dbt (transform) → ferry (activate)
       ↓                    ↓                   ↓
  dagster-dlt          dagster-dbt         dagster-ferry
```

## Architecture

- **Language:** Rust core, Python bindings via PyO3/maturin
- **Data format:** Apache Arrow RecordBatches throughout
- **State:** Local DuckDB or warehouse write-back
- **Config:** YAML (serde), Git-native
- **Async:** tokio runtime, reqwest for HTTP
- **CDC:** xxhash-based row diffing on Arrow columns

## Contributing

We welcome contributions. See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

MIT — see [LICENSE](LICENSE).
