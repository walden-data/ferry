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

The package depends on `dagster>=1.8.10` and `ferry-core>=0.1.0`. Both are declared as normal package requirements in the published metadata. The `dagster` floor is `1.8.10` because `AssetSpec(kinds=...)` first shipped in that release.

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

An internal `ferry/sync_name` metadata entry on each `AssetSpec` maps the asset key back to the native sync name so execution routes correctly even when a translator customizes keys.

### Custom translator

Subclass `DagsterFerryTranslator` to override `key`, `description`, `group_name`, `tags`, or `kinds`. Only these properties are customizable. dbt dependencies and rich run metadata are deferred.

```python
from dagster import AssetKey
from dagster_ferry import DagsterFerryTranslator

class PrefixedTranslator(DagsterFerryTranslator):
    def key(self, sync):
        return AssetKey(["ferry", sync.name])
```

## Selected execution

`DagsterFerryResource.run(context)` reads the asset keys Dagster selected for the current materialization and maps them back to native sync names. It calls native `Project.run(sync_names=[...])` exactly once with the sorted selected names. It validates that the returned result names exactly match the selection, then yields one minimal `MaterializeResult(asset_key=...)` per selected successful sync.

- Empty selection yields nothing and does not call Ferry.
- A result-name mismatch raises `RuntimeError` with the diff.
- Native Ferry execution errors propagate through Dagster's normal boundary. The resource does not broad-wrap them.
- The resource never infers a sync name from `AssetKey.path[-1]`. It reads the stable metadata stored on the bound `AssetsDefinition`.

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
