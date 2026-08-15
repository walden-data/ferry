# dagster-ferry

A Dagster integration for [Ferry](https://github.com/walden-data/ferry), a Rust-native reverse ETL engine.

This package is independently versioned from the Ferry core. It exposes a single resource that loads a native `ferry.Project` for use inside Dagster definitions.

## Public API

- `dagster_ferry.DagsterFerryResource`
- `dagster_ferry.__version__`

Asset factories, translators, and prebuilt definitions are intentionally out of scope for this release. They will land in later tickets.

## Installation

```bash
pip install dagster-ferry
```

The package depends on `dagster>=1.8` and `ferry-core>=0.1.0`. Both are declared as normal package requirements in the published metadata.

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
