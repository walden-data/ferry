"""Tests for the ferry_assets decorator, translator, and resource.run.

These tests cover discovery, deterministic ordering, default and custom
translator behavior, duplicate and invalid-config errors, exact subset
selection, full selection, empty selection, result-name mismatch, native
runtime failure propagation, and one credentials-free real execution.

Spies/fakes are used for exact selection and mismatch assertions. A real
local Ferry fixture (DuckDB source and file destination) backs the
credentials-free execution path.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import ferry
import pytest
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    DagsterInvalidDefinitionError,
    MaterializeResult,
    materialize,
)

from dagster_ferry import (
    DagsterFerryResource,
    DagsterFerryTranslator,
    ferry_assets,
)
from dagster_ferry._assets import _FERRY_SYNC_NAME_META, key_to_sync_map, sync_name_for_key


@dataclass
class _FakeResult:
    """A duck-typed stand-in for ferry.SyncResult for spy-based tests.

    The native SyncResult pyclass does not expose a Python constructor, so
    tests that need to fake native return values use this dataclass instead.
    resource.run only reads `sync_name` from each result.
    """

    sync_name: str


# ---------------------------------------------------------------------------
# Discovery and deterministic ordering
# ---------------------------------------------------------------------------


def test_discovery_returns_real_metadata_from_yaml(
    ferry_multi_sync_project: Path,
) -> None:
    project = ferry.Project(str(ferry_multi_sync_project))
    metas = list(project.list_syncs_metadata())
    assert [m.name for m in metas] == ["alpha_sync", "beta_sync"]
    alpha = metas[0]
    assert alpha.description == "Alpha sync"
    assert list(alpha.tags) == ["team_a", "p1"]
    assert alpha.destination_type == "file"
    beta = metas[1]
    assert beta.description == "Beta sync"
    assert list(beta.tags) == ["team_b"]
    assert beta.destination_type == "file"


def test_sync_metadata_is_immutable(ferry_multi_sync_project: Path) -> None:
    project = ferry.Project(str(ferry_multi_sync_project))
    metas = project.list_syncs_metadata()
    with pytest.raises(AttributeError):
        metas[0].name = "mutated"  # type: ignore[misc]


def test_list_syncs_metadata_missing_syncs_dir_raises_config_error(
    ferry_project_dir: Path,
) -> None:
    """list_syncs_metadata raises ferry.ConfigError when syncs/ is missing.

    The native Project constructor only loads ferry.yml, so construction
    succeeds without a syncs/ directory. The error surfaces from
    SyncConfig::load_all when list_syncs_metadata calls it. This tests the
    method itself, not an earlier construction failure.
    """
    project = ferry.Project(str(ferry_project_dir))
    with pytest.raises(ferry.ConfigError, match="Syncs directory not found") as exc_info:
        project.list_syncs_metadata()
    # The error path includes the resolved syncs/ path for actionable debugging.
    assert "syncs" in str(exc_info.value)
    # ConfigError is a subclass of FerryError, confirming the hierarchy.
    assert isinstance(exc_info.value, ferry.FerryError)


def test_decorator_builds_one_spec_per_sync_in_deterministic_order(
    ferry_multi_sync_project: Path,
) -> None:
    @ferry_assets(project_dir=str(ferry_multi_sync_project))
    def my_syncs(context, ferry: DagsterFerryResource) -> Iterator[MaterializeResult[Any]]:
        yield from ferry.run(context)

    assert isinstance(my_syncs, AssetsDefinition)
    # specs preserves discovery order; keys may be set-ordered.
    keys = [s.key for s in my_syncs.specs]
    assert keys == [AssetKey("alpha_sync"), AssetKey("beta_sync")]
    # Definition/op name is the decorated function name.
    assert my_syncs.node_def.name == "my_syncs"


def test_asset_keys_and_properties_are_reload_stable(
    ferry_multi_sync_project: Path,
) -> None:
    def build() -> AssetsDefinition:
        @ferry_assets(project_dir=str(ferry_multi_sync_project))
        def syncs(context, ferry: DagsterFerryResource) -> Iterator[MaterializeResult[Any]]:
            yield from ferry.run(context)

        return syncs

    first = build()
    second = build()
    first_keys = [s.key for s in first.specs]
    second_keys = [s.key for s in second.specs]
    assert first_keys == second_keys
    first_props = [(s.key, s.group_name, s.description, s.tags, s.kinds) for s in first.specs]
    second_props = [(s.key, s.group_name, s.description, s.tags, s.kinds) for s in second.specs]
    assert first_props == second_props


def test_default_translator_properties(ferry_multi_sync_project: Path) -> None:
    @ferry_assets(project_dir=str(ferry_multi_sync_project))
    def syncs(context, ferry: DagsterFerryResource) -> Iterator[MaterializeResult[Any]]:
        yield from ferry.run(context)

    specs_by_key = {s.key: s for s in syncs.specs}
    alpha = specs_by_key[AssetKey("alpha_sync")]
    assert alpha.group_name == "team_a"
    assert alpha.description == "Alpha sync"
    assert alpha.tags["ferry/tags"] == "team_a.p1"
    assert alpha.kinds == {"ferry", "file"}
    # Internal sync-name mapping is present.
    assert alpha.metadata[_FERRY_SYNC_NAME_META] == "alpha_sync"

    beta = specs_by_key[AssetKey("beta_sync")]
    assert beta.group_name == "team_b"
    assert beta.kinds == {"ferry", "file"}


def test_default_translator_fallbacks_for_missing_description_and_tags(
    ferry_multi_sync_project: Path,
) -> None:
    # Rewrite alpha to drop description and tags.
    syncs_dir = ferry_multi_sync_project / "syncs"
    (syncs_dir / "alpha_sync.yml").write_text(
        "name: alpha_sync\n"
        "model:\n"
        "  sql: SELECT id, name FROM users ORDER BY id\n"
        "destination:\n"
        "  type: file\n"
        f"  output_dir: {ferry_multi_sync_project.parent / 'out'}\n"
        "  format: csv\n"
        "sync:\n"
        "  mode: incremental\n"
        "  cursor_field: id\n"
        "  cdc:\n"
        "    method: hash\n",
        encoding="utf-8",
    )

    @ferry_assets(project_dir=str(ferry_multi_sync_project))
    def syncs(context, ferry: DagsterFerryResource) -> Iterator[MaterializeResult[Any]]:
        yield from ferry.run(context)

    alpha = {s.key: s for s in syncs.specs}[AssetKey("alpha_sync")]
    assert alpha.description == "Ferry sync: alpha_sync"
    assert alpha.group_name == "default"
    assert "ferry/tags" not in alpha.tags


# ---------------------------------------------------------------------------
# Custom translator
# ---------------------------------------------------------------------------


class _PrefixedTranslator(DagsterFerryTranslator):
    """A translator that prefixes keys and overrides group/kinds."""

    def key(self, sync: ferry.SyncMetadata) -> AssetKey:
        return AssetKey(["ferry", sync.name])

    def group_name(self, sync: ferry.SyncMetadata) -> str:
        return "custom_group"

    def kinds(self, sync: ferry.SyncMetadata) -> set[str]:
        return {"ferry", "file", "custom"}


def test_custom_translator_changes_key_group_and_kinds(
    ferry_multi_sync_project: Path,
) -> None:
    @ferry_assets(project_dir=str(ferry_multi_sync_project), translator=_PrefixedTranslator())
    def syncs(context, ferry: DagsterFerryResource) -> Iterator[MaterializeResult[Any]]:
        yield from ferry.run(context)

    # Use specs order, which preserves the deterministic discovery order.
    keys = [s.key for s in syncs.specs]
    assert keys == [AssetKey(["ferry", "alpha_sync"]), AssetKey(["ferry", "beta_sync"])]
    for spec in syncs.specs:
        assert spec.group_name == "custom_group"
        assert spec.kinds == {"ferry", "file", "custom"}
        # Internal mapping still points at the native sync name.
        assert _FERRY_SYNC_NAME_META in spec.metadata


def test_key_to_sync_map_and_sync_name_for_key(ferry_multi_sync_project: Path) -> None:
    @ferry_assets(project_dir=str(ferry_multi_sync_project), translator=_PrefixedTranslator())
    def syncs(context, ferry: DagsterFerryResource) -> Iterator[MaterializeResult[Any]]:
        yield from ferry.run(context)

    mapping = key_to_sync_map(syncs)
    assert mapping[AssetKey(["ferry", "alpha_sync"])] == "alpha_sync"
    assert mapping[AssetKey(["ferry", "beta_sync"])] == "beta_sync"
    assert sync_name_for_key(syncs, AssetKey(["ferry", "alpha_sync"])) == "alpha_sync"
    assert sync_name_for_key(syncs, AssetKey("nonexistent")) is None


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


def test_no_syncs_raises_value_error(ferry_empty_syncs_project: Path) -> None:
    with pytest.raises(ValueError, match="No Ferry syncs discovered"):

        @ferry_assets(project_dir=str(ferry_empty_syncs_project))
        def syncs(context, ferry: DagsterFerryResource) -> Iterator[MaterializeResult[Any]]:
            yield from ferry.run(context)


def test_malformed_ferry_yml_propagates_native_error(tmp_path: Path) -> None:
    (tmp_path / "ferry.yml").write_text(": not valid yaml : [", encoding="utf-8")
    (tmp_path / "syncs").mkdir()
    with pytest.raises(ferry.FerryError):

        @ferry_assets(project_dir=str(tmp_path))
        def syncs(context, ferry: DagsterFerryResource) -> Iterator[MaterializeResult[Any]]:
            yield from ferry.run(context)


def test_duplicate_sync_names_raises_value_error(
    ferry_multi_sync_project: Path,
) -> None:
    # Copy alpha_sync.yml to a second file with the same name field.
    syncs_dir = ferry_multi_sync_project / "syncs"
    alpha_content = (syncs_dir / "alpha_sync.yml").read_text(encoding="utf-8")
    (syncs_dir / "alpha_dup.yml").write_text(alpha_content, encoding="utf-8")
    with pytest.raises(ValueError, match="Two Ferry syncs translated to the same asset key"):

        @ferry_assets(project_dir=str(ferry_multi_sync_project))
        def syncs(context, ferry: DagsterFerryResource) -> Iterator[MaterializeResult[Any]]:
            yield from ferry.run(context)


def test_duplicate_translated_keys_raises_value_error(
    ferry_multi_sync_project: Path,
) -> None:
    class _CollidingTranslator(DagsterFerryTranslator):
        """Maps every sync to the same asset key."""

        def key(self, sync: ferry.SyncMetadata) -> AssetKey:
            return AssetKey("colliding")

    with pytest.raises(ValueError, match="Two Ferry syncs translated to the same asset key"):

        @ferry_assets(
            project_dir=str(ferry_multi_sync_project),
            translator=_CollidingTranslator(),
        )
        def syncs(context, ferry: DagsterFerryResource) -> Iterator[MaterializeResult[Any]]:
            yield from ferry.run(context)


def test_invalid_group_name_raises_dagster_error(
    ferry_multi_sync_project: Path,
) -> None:
    class _BadGroupTranslator(DagsterFerryTranslator):
        def group_name(self, sync: ferry.SyncMetadata) -> str:
            return "has space"

    with pytest.raises(DagsterInvalidDefinitionError):

        @ferry_assets(
            project_dir=str(ferry_multi_sync_project),
            translator=_BadGroupTranslator(),
        )
        def syncs(context, ferry: DagsterFerryResource) -> Iterator[MaterializeResult[Any]]:
            yield from ferry.run(context)


def test_translator_returning_wrong_key_type_raises_type_error(
    ferry_multi_sync_project: Path,
) -> None:
    class _BadKeyTranslator(DagsterFerryTranslator):
        def key(self, sync: ferry.SyncMetadata) -> AssetKey:  # type: ignore[override]
            return "not an asset key"  # type: ignore[return-value]

    with pytest.raises(TypeError, match="must return an AssetKey"):

        @ferry_assets(
            project_dir=str(ferry_multi_sync_project),
            translator=_BadKeyTranslator(),
        )
        def syncs(context, ferry: DagsterFerryResource) -> Iterator[MaterializeResult[Any]]:
            yield from ferry.run(context)


# ---------------------------------------------------------------------------
# Selected execution: spies for exact subset and mismatch assertions
# ---------------------------------------------------------------------------


def _resource_with_fake_project(project_dir: Path, run_impl: Any) -> DagsterFerryResource:
    """Build a resource whose native project.run is replaced by a fake.

    The native ``ferry.Project`` pyclass does not allow monkeypatching its
    ``run`` method, so this helper constructs a resource subclass that skips
    ``setup_for_execution`` rebuilding the real project. The fake project is
    installed up front and preserved across the Dagster run lifecycle.
    """
    # Build the real resource first to validate the project directory.
    res = DagsterFerryResource(project_dir=str(project_dir))
    _ = res.project  # triggers path/config validation

    class _FakeProject:
        def run(
            self,
            sync_names: list[str] | None = None,
            dry_run: bool = False,
            full_refresh: bool = False,
            retry_dead: bool = False,
        ) -> Any:
            return run_impl(sync_names=sync_names or [])

    class _FakeResource(DagsterFerryResource):
        def setup_for_execution(self, context: Any) -> None:
            # Preserve the fake project installed below instead of rebuilding.
            if self._project is None:
                self._project = _FakeProject()  # type: ignore[assignment]

    fake_res = _FakeResource(project_dir=str(project_dir))
    # Install the fake project up front so both lazy access and
    # setup_for_execution see it.
    fake_res._project = _FakeProject()  # type: ignore[assignment]
    return fake_res


def _fake_result(name: str) -> _FakeResult:
    return _FakeResult(sync_name=name)


def test_subset_selection_runs_only_selected_sync(
    ferry_multi_sync_project: Path,
) -> None:
    captured: dict[str, Any] = {}

    def run_impl(sync_names: list[str]) -> list[_FakeResult]:
        captured["sync_names"] = sync_names
        return [_fake_result("beta_sync")]

    res = _resource_with_fake_project(ferry_multi_sync_project, run_impl)

    @ferry_assets(project_dir=str(ferry_multi_sync_project))
    def syncs(context, ferry: DagsterFerryResource) -> Iterator[MaterializeResult[Any]]:
        yield from ferry.run(context)

    result = materialize(
        [syncs],
        resources={"ferry": res},
        selection=[AssetKey("beta_sync")],
    )
    assert result.success
    # Only the selected sync name was passed to the native run.
    assert captured == {"sync_names": ["beta_sync"]}


def test_full_selection_runs_all_syncs_with_one_call(
    ferry_multi_sync_project: Path,
) -> None:
    captured: dict[str, Any] = {}

    def run_impl(sync_names: list[str]) -> list[_FakeResult]:
        captured["sync_names"] = sync_names
        return [_fake_result("alpha_sync"), _fake_result("beta_sync")]

    res = _resource_with_fake_project(ferry_multi_sync_project, run_impl)

    @ferry_assets(project_dir=str(ferry_multi_sync_project))
    def syncs(context, ferry: DagsterFerryResource) -> Iterator[MaterializeResult[Any]]:
        yield from ferry.run(context)

    result = materialize([syncs], resources={"ferry": res})
    assert result.success
    # One native call with all syncs, deterministically sorted.
    assert captured == {"sync_names": ["alpha_sync", "beta_sync"]}


def test_empty_selection_yields_nothing_and_does_not_call_ferry(
    ferry_multi_sync_project: Path,
) -> None:
    run_called: list[bool] = []

    def run_impl(sync_names: list[str]) -> list[_FakeResult]:
        run_called.append(True)
        return []

    res = _resource_with_fake_project(ferry_multi_sync_project, run_impl)

    @ferry_assets(project_dir=str(ferry_multi_sync_project))
    def syncs(context, ferry: DagsterFerryResource) -> Iterator[MaterializeResult[Any]]:
        yield from ferry.run(context)

    # Build a fake context with an empty selected_asset_keys and a bound
    # AssetsDefinition carrying the sync-name metadata. Dagster's materialize
    # cannot produce an empty selection at plan time, so exercise the generator
    # directly to assert empty-selection behavior.
    fake_context = MagicMock(spec=AssetExecutionContext)
    fake_context.selected_asset_keys = set()
    fake_context.assets_def = syncs

    iterator = res.run(fake_context)
    assert list(iterator) == []
    assert run_called == []


def test_result_name_mismatch_raises_runtime_error(
    ferry_multi_sync_project: Path,
) -> None:
    # Native returns a result for a sync that was not selected.
    def run_impl(sync_names: list[str]) -> list[_FakeResult]:
        return [_fake_result("alpha_sync"), _fake_result("unknown")]

    res = _resource_with_fake_project(ferry_multi_sync_project, run_impl)

    @ferry_assets(project_dir=str(ferry_multi_sync_project))
    def syncs(context, ferry: DagsterFerryResource) -> Iterator[MaterializeResult[Any]]:
        yield from ferry.run(context)

    with pytest.raises(RuntimeError, match="do not exactly match"):
        materialize([syncs], resources={"ferry": res})


def test_native_runtime_failure_propagates(
    ferry_multi_sync_project: Path,
) -> None:
    def run_impl(sync_names: list[str]) -> list[_FakeResult]:
        raise ferry.FerryError("boom")

    res = _resource_with_fake_project(ferry_multi_sync_project, run_impl)

    @ferry_assets(project_dir=str(ferry_multi_sync_project))
    def syncs(context, ferry: DagsterFerryResource) -> Iterator[MaterializeResult[Any]]:
        yield from ferry.run(context)

    with pytest.raises(ferry.FerryError, match="boom"):
        materialize([syncs], resources={"ferry": res})


# ---------------------------------------------------------------------------
# Real credentials-free execution
# ---------------------------------------------------------------------------


def test_real_execution_duckdb_to_file(ferry_multi_sync_project: Path) -> None:
    res = DagsterFerryResource(project_dir=str(ferry_multi_sync_project))

    @ferry_assets(project_dir=str(ferry_multi_sync_project))
    def syncs(context, ferry: DagsterFerryResource) -> Iterator[MaterializeResult[Any]]:
        yield from ferry.run(context)

    # Select only alpha_sync for a real run.
    result = materialize(
        [syncs],
        resources={"ferry": res},
        selection=[AssetKey("alpha_sync")],
    )
    assert result.success
    out_dir = ferry_multi_sync_project.parent / "out"
    files = list(out_dir.glob("alpha_sync_*.csv"))
    assert files, f"expected alpha_sync output file in {out_dir}"
    contents = files[0].read_text(encoding="utf-8")
    assert "id,name" in contents
    assert "1,Alice" in contents
    assert "2,Bob" in contents
    assert "3,Carol" in contents


# ---------------------------------------------------------------------------
# Decorator behavior: discovery only, body not executed
# ---------------------------------------------------------------------------


def test_decorator_body_not_executed_at_decoration_time(
    ferry_multi_sync_project: Path,
) -> None:
    body_called: list[bool] = []

    @ferry_assets(project_dir=str(ferry_multi_sync_project))
    def syncs(context, ferry: DagsterFerryResource) -> Iterator[MaterializeResult[Any]]:
        body_called.append(True)
        yield from ferry.run(context)

    assert body_called == []
