"""DagsterFerryResource path handling, lifecycle, and error propagation tests."""

from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import ferry
import pytest
from dagster import (
    ConfigurableResource,
    EnvVar,
    asset,
    build_init_resource_context,
    materialize,
)

from dagster_ferry import DagsterFerryResource

# ---------------------------------------------------------------------------
# Path normalization
# ---------------------------------------------------------------------------


def test_resource_is_configurable_resource_subclass() -> None:
    assert issubclass(DagsterFerryResource, ConfigurableResource)


def test_project_dir_is_plain_str_field() -> None:
    # EnvVar is accepted as a config value because Dagster resolves it later.
    res = DagsterFerryResource(project_dir=EnvVar("FERRY_PROJECT_DIR"))
    # Before resolution the raw config holds the EnvVar marker, not a string.
    # The point of this test is that the field accepts EnvVar without raising.
    assert res is not None


def test_resolve_project_dir_normalizes_relative_path(
    ferry_project_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(ferry_project_dir)
    res = DagsterFerryResource(project_dir=".")
    resolved = res._resolve_project_dir()
    assert resolved == ferry_project_dir.resolve()
    assert resolved.is_absolute()


def test_resolve_project_dir_expands_user(
    ferry_project_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Point HOME at the temp project's parent and reference the dir by ~.
    parent = ferry_project_dir.parent
    monkeypatch.setenv("HOME", str(parent))
    rel = ferry_project_dir.name
    res = DagsterFerryResource(project_dir=f"~/{rel}")
    resolved = res._resolve_project_dir()
    assert resolved == ferry_project_dir.resolve()


def test_resolve_project_dir_expands_env_var(
    ferry_project_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FERRY_TEST_DIR", str(ferry_project_dir))
    res = DagsterFerryResource(project_dir="$FERRY_TEST_DIR")
    resolved = res._resolve_project_dir()
    assert resolved == ferry_project_dir.resolve()


# ---------------------------------------------------------------------------
# Actionable path/config errors
# ---------------------------------------------------------------------------


def test_empty_project_dir_raises_value_error() -> None:
    res = DagsterFerryResource(project_dir="   ")
    with pytest.raises(ValueError, match="project_dir must not be empty"):
        res._resolve_project_dir()


def test_missing_project_dir_raises_file_not_found_error(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist"
    res = DagsterFerryResource(project_dir=str(missing))
    with pytest.raises(FileNotFoundError, match="project_dir does not exist"):
        res._resolve_project_dir()


def test_project_dir_pointing_at_file_raises_not_a_directory_error(
    tmp_path: Path,
) -> None:
    file_path = tmp_path / "not-a-dir.txt"
    file_path.write_text("nope", encoding="utf-8")
    res = DagsterFerryResource(project_dir=str(file_path))
    with pytest.raises(NotADirectoryError, match="project_dir is not a directory"):
        res._resolve_project_dir()


def test_missing_ferry_yml_raises_file_not_found_error(tmp_path: Path) -> None:
    res = DagsterFerryResource(project_dir=str(tmp_path))
    with pytest.raises(FileNotFoundError, match="ferry.yml not found"):
        res._resolve_project_dir()


# ---------------------------------------------------------------------------
# Lifecycle and lazy property
# ---------------------------------------------------------------------------


def test_lazy_project_property_constructs_native_project(
    ferry_project_dir: Path,
) -> None:
    res = DagsterFerryResource(project_dir=str(ferry_project_dir))
    assert res._project is None
    project = res.project
    assert isinstance(project, ferry.Project)
    # Second access returns the cached instance.
    assert res.project is project
    assert res._project is project


def test_setup_for_execution_constructs_and_caches_project(
    ferry_project_dir: Path,
) -> None:
    res = DagsterFerryResource(project_dir=str(ferry_project_dir))
    context = build_init_resource_context()
    assert res._project is None
    res.setup_for_execution(context)
    assert isinstance(res._project, ferry.Project)
    assert res.project is res._project


def test_importing_module_does_not_construct_native_project() -> None:
    # The resource class itself must not construct a project at definition time.
    res = DagsterFerryResource(project_dir=str(Path(__file__).parent))
    assert res._project is None


# ---------------------------------------------------------------------------
# Native error propagation
# ---------------------------------------------------------------------------


def test_native_config_error_propagates(tmp_path: Path) -> None:
    # ferry.yml exists but is invalid YAML.
    (tmp_path / "ferry.yml").write_text(": not valid yaml : [", encoding="utf-8")
    res = DagsterFerryResource(project_dir=str(tmp_path))
    with pytest.raises(ferry.FerryError):
        _ = res.project


def test_native_validation_error_propagates(tmp_path: Path) -> None:
    # ferry.yml parses but fails Ferry validation (empty name).
    (tmp_path / "ferry.yml").write_text(
        dedent(
            """\
            name: ""
            source:
              type: duckdb
              path: /data/db.duckdb
            state:
              backend: duckdb
              path: .ferry/state.db
            """,
        ),
        encoding="utf-8",
    )
    res = DagsterFerryResource(project_dir=str(tmp_path))
    with pytest.raises(ferry.FerryError):
        _ = res.project


def test_native_value_error_for_missing_directory_is_preserved() -> None:
    # Bypass Python pre-flight by pointing at a directory that exists but has
    # no ferry.yml is caught in Python. To exercise the native constructor
    # directly, call ferry.Project with a path that passes Python checks but
    # would still be re-validated by the native side.
    # This test ensures ferry.Project itself raises ValueError for a path that
    # does not exist, documenting the native contract we preserve.
    with pytest.raises(ValueError):
        ferry.Project("/this/path/does/not/exist/anywhere")


# ---------------------------------------------------------------------------
# Minimal Dagster execution probe
# ---------------------------------------------------------------------------


def test_materialize_asset_using_resource(ferry_project_dir: Path) -> None:
    # A trivial asset that reads the resource's lazy project property inside a
    # real Dagster execution. This exercises setup_for_execution and the
    # resource boundary without implementing Ferry assets.
    captured: dict[str, object] = {}

    @asset
    def probe_asset(ferry_res: DagsterFerryResource) -> str:
        # setup_for_execution has already constructed the project.
        assert ferry_res._project is not None
        captured["project_type"] = type(ferry_res._project).__name__
        return "ok"

    defs_resources = {"ferry_res": DagsterFerryResource(project_dir=str(ferry_project_dir))}

    result = materialize([probe_asset], resources=defs_resources)
    assert result.success
    assert captured["project_type"] == "Project"
