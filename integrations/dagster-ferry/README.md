# dagster-ferry

A Dagster integration for [Ferry](https://github.com/walden-data/ferry), a Rust-native reverse ETL engine.

This package is independently versioned from the Ferry core. It exposes a resource, an asset decorator, and a translator that represent configured Ferry syncs as subsettable Dagster assets.

## Public API

- `dagster_ferry.DagsterFerryResource`
- `dagster_ferry.ferry_assets`
- `dagster_ferry.DagsterFerryTranslator`
- `dagster_ferry.__version__`

## Installation

```bash
pip install dagster-ferry
```

The package depends on `dagster>=1.8.10` and `ferry-core>=0.2.0`. Both are declared as normal package requirements in the published metadata. The `dagster` floor is `1.8.10` because `AssetSpec(kinds=...)` first shipped in that release. Ferry does not depend on `dagster-dbt` at runtime.

## Resource configuration

`DagsterFerryResource` is a `dagster.ConfigurableResource` with one field:

- `project_dir: str` path to a Ferry project directory that contains a `ferry.yml` file. The value is a plain string so Dagster can serialize it and resolve `EnvVar` values at launch time.

```python
from dagster import EnvVar, Definitions
from dagster_ferry import DagsterFerryResource

defs = Definitions(
    resources={
        "ferry": DagsterFerryResource(project_dir=EnvVar("FERRY_PROJECT_DIR")),
    }
)
```

The path is expanded for `~` and environment variables, resolved to an absolute path, and checked to be an existing directory containing `ferry.yml` before the native project is constructed.

## Asset decorator

`@ferry_assets` discovers every sync configured under `syncs/` once at decoration time and returns one `multi_asset` with `can_subset=True`, one `AssetSpec` per sync. The decorated function name becomes the Dagster definition and op name. The decorated body is not executed during discovery. It runs only at materialization time, where it delegates to `DagsterFerryResource.run(context)`.

```python
from dagster import AssetExecutionContext, Definitions
from dagster_ferry import DagsterFerryResource, ferry_assets


@ferry_assets(project_dir="path/to/ferry/project")
def customer_syncs(context: AssetExecutionContext, ferry: DagsterFerryResource):
    yield from ferry.run(context)


defs = Definitions(
    assets=[customer_syncs],
    resources={
        "ferry": DagsterFerryResource(project_dir="/path/to/ferry/project"),
    },
)
```

### Default asset properties

For each discovered sync, the default translator produces:

- Asset key: `AssetKey(sync.name)` with no sanitization or prefix.
- Description: the configured description, or `Ferry sync: <name>` when absent.
- Group: the first configured tag, or `default` when the sync has no tags.
- Tags: the ordered Ferry tag list under `ferry/tags`, joined with `.` into a Dagster-safe value.
- Kinds: `ferry` plus the destination type (`file`, `rest`, `braze`, `slack`, `google_sheets`).
- dbt dependency: when a sync uses `model.ref`, the upstream dbt model is resolved from the configured manifest and added to `deps` as an `AssetKey`. Ferry never owns or emits an `AssetSpec` for the dbt-owned key.

An internal `ferry/sync_name` metadata entry on each `AssetSpec` maps the asset key back to the native sync name so execution routes correctly even when a translator customizes keys.

### dbt dependencies

When a Ferry sync uses `model.ref: <model_name>`, the integration resolves the referenced dbt model from the manifest configured at `dbt.manifest_path` in `ferry.yml` and adds the model's `AssetKey` to the Ferry sync's `deps`. Ferry never creates or emits an `AssetSpec` for the dbt-owned key. That key is owned by the user's dbt asset definition elsewhere (for example `@dbt_assets`). The `deps` edge is non-I/O: Ferry declares the lineage dependency, it never loads or executes the dbt model.

The default dbt asset-key mapping mirrors dagster-dbt's `default_asset_key_fn` exactly:

- `config.meta.dagster.asset_key` is checked first.
- Top-level `meta.dagster.asset_key` is the fallback override.
- Versioned models (dbt >= 1.5, `version` present) use `AssetKey([alias])`.
- Otherwise the key is `AssetKey([config_schema, name])` when the configured schema (`config.schema`) is available.
- Otherwise the key is `AssetKey([name])`.

This means Ferry uses the configured schema (`config.schema`), not the resolved top-level schema, matching dagster-dbt. Projects with a custom `generate_schema_name` macro get correct lineage edges.

Override `DagsterFerryTranslator.get_dbt_asset_key` to match a custom `dagster-dbt` translator. The method receives a `ferry.SyncMetadata` whose `dbt_model` field carries the resolved model identity (`unique_id`, `name`, `alias`, `schema`, `config_schema`, `package_name`, `database`, `fqn`, `config_dagster_asset_key`, `dagster_asset_key`, `version`). Ferry does not import `dagster-dbt` at runtime.

#### Behavior

- SQL-only projects with no `dbt:` block work unchanged. Every sync has `dbt_model = None` and no `deps`.
- A `model.ref` sync without `dbt.manifest_path` configured fails discovery with a `ferry.ConfigError` naming the sync and the missing configuration.
- A configured but missing or malformed manifest file fails discovery with the same `ferry.ConfigError` native execution raises.
- A dbt ref to a model not in the manifest fails with a contextual error listing available model names.
- A dbt ref to an ephemeral model fails because ephemeral models cannot be Ferry sources.
- A dbt ref to a non-model node (seed, snapshot, test) fails with a contextual error.
- An ambiguous model name (two models with the same name across packages) fails and lists candidate `unique_id`s so operators can disambiguate.
- A stale manifest loads and resolves normally. Staleness is advisory (warn only), matching Ferry's existing freshness policy.

### Custom translator

Subclass `DagsterFerryTranslator` to override `key`, `description`, `group_name`, `tags`, `kinds`, or `get_dbt_asset_key`. Each method is optional; the base class provides documented defaults.

```python
from dagster import AssetKey
from dagster_ferry import DagsterFerryTranslator


class PrefixedTranslator(DagsterFerryTranslator):
    def key(self, sync):
        return AssetKey(["ferry", sync.name])


class CustomDbtTranslator(DagsterFerryTranslator):
    def get_dbt_asset_key(self, sync):
        dbt = sync.dbt_model
        if dbt is None:
            return None
        return AssetKey(["custom_prefix", dbt.name])
```

## Selected execution

`DagsterFerryResource.run(context)` reads the asset keys Dagster selected for the current materialization and maps them back to native sync names. It calls native `Project.run(sync_names=[...])` exactly once with the sorted selected names. It validates that the returned result names exactly match the selection before yielding any materialization. Then it yields one `MaterializeResult(asset_key=..., metadata={...})` per selected successful sync with typed run metadata from the native `SyncResult`.

- Empty selection yields nothing and does not call Ferry.
- A result-name mismatch raises `RuntimeError` with the diff. No materialization is yielded before the full selected set validates.
- Native Ferry execution errors propagate through Dagster's normal boundary. The resource does not broad-wrap them.
- The resource never infers a sync name from `AssetKey.path[-1]`. It reads the stable metadata stored on the bound `AssetsDefinition`.

### Materialization metadata

Each `MaterializeResult` carries typed Dagster metadata built from the native `SyncResult` fields:

| Key | Dagster type | Source field |
|-----|-------------|-------------|
| `dagster/row_count` | `MetadataValue.int` | `rows_synced` (delivered rows) |
| `ferry/run_id` | `MetadataValue.text` | `run_id` (Ferry UUID, not a Dagster run id) |
| `ferry/rows_extracted` | `MetadataValue.int` | `rows_extracted` |
| `ferry/rows_delivered` | `MetadataValue.int` | `rows_synced` |
| `ferry/rows_failed` | `MetadataValue.int` | `rows_failed` |
| `ferry/rows_pending` | `MetadataValue.int` | `rows_pending` |
| `ferry/rows_retried` | `MetadataValue.int` | `rows_retried` |
| `ferry/rows_dead` | `MetadataValue.int` | `rows_dead` |
| `ferry/duration_seconds` | `MetadataValue.float` | `duration_seconds` |
| `ferry/mode` | `MetadataValue.text` | `mode` |
| `ferry/dry_run` | `MetadataValue.bool` | `dry_run` |

Genuine zero counts are emitted as `MetadataValue.int(0)`. Metrics Ferry does not expose on `SyncResult` (changed, skipped) and any invented status are omitted entirely rather than fabricated. Ferry run UUIDs are rendered as text because they are foreign ids, not Dagster run ids.

## Lifecycle

The native `ferry.Project` is constructed once per resource lifecycle. During a Dagster run, `setup_for_execution` builds it before the resource is used. For direct access and tests, the `project` property constructs it lazily on first access. Importing the package or evaluating the config class never constructs a native project.

Path and config errors surface early with actionable messages (`FileNotFoundError`, `NotADirectoryError`, `ValueError`). Native `ferry` errors (`ferry.FerryError` and subclasses) and `ValueError` raised by the native constructor propagate unchanged with their original causes preserved.

## Local development

This integration lives under `integrations/dagster-ferry/` and uses [uv](https://docs.astral.sh/uv/) for Python tooling. The native `ferry-core` extension must be available on the interpreter.

To build and install `ferry-core` from the Rust workspace into a uv-managed environment:

```bash
# from the repository root
maturin build --release -m crates/ferry-python/Cargo.toml --interpreter <path-to-python>
uv pip install --python <path-to-python> target/wheels/ferry_core-*.whl
```

Then run the integration checks from `integrations/dagster-ferry/`:

```bash
uv run --project integrations/dagster-ferry ruff check
uv run --project integrations/dagster-ferry ruff format --check
uv run --project integrations/dagster-ferry pyright
uv run --project integrations/dagster-ferry pytest
uv build --project integrations/dagster-ferry
```

## License

MIT, same as the Ferry workspace.
