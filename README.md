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

> **Naming:** the PyPI distribution is `ferry-core`, but the Python import is `ferry` and the compiled extension is `ferry._native`. `pip install ferry-core` provides `import ferry`.

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
| REST API (generic) | `ferry-core` (included) | Implemented |
| Braze | `ferry-core` (included) | Planned |
| HubSpot | `ferry-core` (included) | Planned |
| Salesforce (Bulk API 2.0) | `ferry-core[salesforce]` | Planned |
| Slack (webhook) | `ferry-core` (included) | Planned |
| S3 / GCS / Azure Blob | `ferry-core[cloud-storage]` | Planned |
| SFTP | `ferry-core[sftp]` | Planned |
| PostgreSQL (upsert) | `ferry-core[postgres]` | Planned |
| CSV / Parquet / JSON file | `ferry-core` (included) | Implemented |

#### REST destination

The generic REST destination sends one HTTP request per batch (default JSON array of row objects; an optional minijinja `body_template` overrides the payload).

```yaml
# syncs/push_users_to_api.yml
name: push_users_to_api
description: "Sync users to a REST endpoint"
model:
  sql: SELECT id, email, plan_tier FROM users
destination:
  type: rest
  url: https://api.example.com/users/ingest
  method: POST
  headers:
    - name: X-Source
      value: ferry
  auth:
    type: bearer       # bearer | basic | api_key | none
    token: ""           # resolved from secrets.toml [destination.rest] bearer_token
  body_template: '{"events": {{ rows | tojson }}}'   # optional
  timeout_secs: 30
  connect_timeout_secs: 10
  max_response_bytes: 1048576   # 1 MiB
  allow_http: false              # https-only by default; set true for localhost testing
  max_batch_size: 100
sync:
  mode: incremental
  cursor_field: updated_at
  cdc:
    method: hash
  delivery:
    batch_size: 100
    retry:
      max_attempts: 5
      backoff: exponential
```

Secrets (bearer tokens, basic auth, API keys) live in `secrets.toml` under `[destination.rest]`, not in YAML:

```toml
# secrets.toml (chmod 600)
[destination.rest]
bearer_token = "your-secret-token"
# basic_username = "alice"
# basic_password = "p@ss"
# api_key = "your-api-key"
# api_key_header_name = "X-Api-Key"
# header.<name> = "value"   # resolve raw header values by key
```

**Defaults**: `method=POST`, `timeout_secs=30`, `connect_timeout_secs=10`, `max_response_bytes=1 MiB`, `max_batch_size=100`, `allow_http=false`.

**Status classification** (drives the pipeline's retry / dead-letter behavior):
- `2xx` → all rows succeed.
- `408, 425, 429, 5xx` → retryable; `Retry-After` (delta-seconds or HTTP-date) is parsed, capped at 300s, and surfaced via the existing delivery string contract.
- other `4xx` → permanent (dead-letter) — configure `sync.delivery.on_reject` rules to override.
- network/transport errors → retryable (default backoff).

**Security**:
- HTTPS by default; `http://` requires explicit `allow_http: true` (intended for localhost testing).
- Redirects are disabled (`Policy::none()`); 3xx responses surface as errors. This prevents credential leakage to attacker-controlled redirect hosts and blocks HTTPS→HTTP downgrade attacks.
- Auth headers are applied per-request with `set_sensitive(true)` and never enter the shared client's `default_headers`. Configured static header values are also marked sensitive.
- Response bodies in errors are truncated to 512 bytes; `retry_after` markers are stripped from bodies (preventing injection); exact known auth values (bearer tokens, Basic base64 credentials, API keys, configured header values) are replaced with `***` before persisting.
- Secrets never appear in `RowError` strings or the state DB journal (unit + integration tested, including server-echo scenarios).
- URL userinfo (`user:pass@host`) is rejected at validation and construction; query strings are redacted in Debug output.

**Limitations (deferred from the initial release)**: per-row request mode; per-row response→row-status mapping; SSRF private-IP/loopback blocking; configurable idempotency-key templates; per-destination rate limiting (governor stays pipeline-level); custom CA certificates; streaming response bodies; retry of non-idempotent POST on network errors.

## Orchestration: dagster-ferry

```bash
pip install dagster-ferry
```

```python
from dagster import AssetExecutionContext, Definitions, EnvVar
from dagster_ferry import DagsterFerryResource, ferry_assets

@ferry_assets(project_dir="path/to/ferry/project")
def customer_syncs(context: AssetExecutionContext, ferry: DagsterFerryResource):
    yield from ferry.run(context)

defs = Definitions(
    assets=[customer_syncs],
    resources={
        "ferry": DagsterFerryResource(project_dir=EnvVar("FERRY_PROJECT_DIR")),
    },
)
```

`@ferry_assets` discovers every configured sync once at decoration time and returns one subsettable multi-asset, one `AssetSpec` per sync. `DagsterFerryResource.run` executes exactly the syncs Dagster selected for the current materialization and yields one `MaterializeResult` per selected sync with typed run metadata from the native `SyncResult`. `DagsterFerryTranslator` customizes asset key, description, group, tags, kinds, and the upstream dbt asset-key mapping for `model.ref` syncs. Ferry never owns or emits the dbt-owned `AssetSpec`; it only adds the dbt key to `deps` for lineage. No `dagster-dbt` runtime dependency is required.

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
