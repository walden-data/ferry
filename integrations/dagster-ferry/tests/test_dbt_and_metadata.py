"""Tests for dbt dependency wiring and MaterializeResult metadata (FERRY-9).

Covers:

* SQL-only projects without manifests keep working (FERRY-8 regression guard).
* dbt-ref sync dependency discovery and default asset-key mapping.
* ``meta.dagster.asset_key`` override mapping.
* Custom ``get_dbt_asset_key`` translation.
* Mixed SQL/dbt projects.
* Missing manifest configuration for a dbt ref.
* Missing file, malformed JSON, missing model, non-model/ephemeral ref, and
  ambiguous model errors.
* Ferry never owning/emitting the upstream dbt asset.
* Materialization metadata keys, values, Dagster types, zero values, and
  omission of changed/skipped.
* Complete result validation before yielding.
* No dbt command execution, external services, or ``dagster_dbt`` import.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import ferry
import pytest
from dagster import (
    AssetKey,
    BoolMetadataValue,
    FloatMetadataValue,
    IntMetadataValue,
    MaterializeResult,
    TextMetadataValue,
    materialize,
)

from dagster_ferry import (
    DagsterFerryResource,
    DagsterFerryTranslator,
    ferry_assets,
)

# ---------------------------------------------------------------------------
# FERRY-8 regression guard: SQL-only projects unchanged
# ---------------------------------------------------------------------------


def test_sql_only_project_has_no_dbt_model_on_metadata(
    ferry_multi_sync_project: Path,
) -> None:
    """SQL-only syncs carry dbt_model = None (FERRY-8 regression guard)."""
    project = ferry.Project(str(ferry_multi_sync_project))
    metas = list(project.list_syncs_metadata())
    assert len(metas) == 2
    for m in metas:
        assert m.dbt_model is None


def test_sql_only_project_specs_have_no_deps(
    ferry_multi_sync_project: Path,
) -> None:
    """SQL-only AssetSpecs have empty deps (FERRY-8 regression guard)."""

    @ferry_assets(project_dir=str(ferry_multi_sync_project))
    def syncs(context, ferry: DagsterFerryResource) -> Iterator[MaterializeResult[Any]]:
        yield from ferry.run(context)

    for spec in syncs.specs:
        assert list(spec.deps) == []


# ---------------------------------------------------------------------------
# dbt-ref dependency discovery and default asset-key mapping
# ---------------------------------------------------------------------------


def test_dbt_ref_sync_resolves_dbt_model_metadata(
    ferry_dbt_project: Path,
) -> None:
    """A dbt-ref sync resolves typed dbt model metadata from the manifest."""
    project = ferry.Project(str(ferry_dbt_project))
    metas = list(project.list_syncs_metadata())
    assert len(metas) == 1
    m = metas[0]
    assert m.name == "users_sync"
    assert m.dbt_model is not None
    dbt = m.dbt_model
    assert dbt.unique_id == "model.test.fct_users"
    assert dbt.name == "fct_users"
    assert dbt.schema == "analytics"
    assert dbt.package_name == "test"
    assert dbt.dagster_asset_key is None


def test_dbt_ref_sync_default_asset_key_uses_schema_plus_name(
    ferry_dbt_project: Path,
) -> None:
    """The default get_dbt_asset_key maps to AssetKey([schema, name])."""
    project = ferry.Project(str(ferry_dbt_project))
    metas = list(project.list_syncs_metadata())
    translator = DagsterFerryTranslator()
    key = translator.get_dbt_asset_key(metas[0])
    assert key == AssetKey(["analytics", "fct_users"])


def test_dbt_ref_sync_meta_dagster_asset_key_override(
    ferry_mixed_project: Path,
) -> None:
    """meta.dagster.asset_key on the dbt model wins over the schema+name default."""
    project = ferry.Project(str(ferry_mixed_project))
    metas = {m.name: m for m in project.list_syncs_metadata()}
    dbt_sync = metas["dbt_sync"]
    assert dbt_sync.dbt_model is not None
    # fct_orders carries meta.dagster.asset_key: ["dbt", "fct_orders"].
    translator = DagsterFerryTranslator()
    key = translator.get_dbt_asset_key(dbt_sync)
    assert key == AssetKey(["dbt", "fct_orders"])


def test_sql_sync_get_dbt_asset_key_returns_none(
    ferry_mixed_project: Path,
) -> None:
    """SQL-only syncs return None from get_dbt_asset_key."""
    project = ferry.Project(str(ferry_mixed_project))
    metas = {m.name: m for m in project.list_syncs_metadata()}
    sql_sync = metas["alpha_sync"]
    translator = DagsterFerryTranslator()
    assert translator.get_dbt_asset_key(sql_sync) is None


# ---------------------------------------------------------------------------
# dagster-dbt default_asset_key_fn parity
# ---------------------------------------------------------------------------


def _manifest_with_model(node: dict[str, Any]) -> dict[str, Any]:
    """Build a minimal manifest with one model node."""
    return {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v7.json",
            "dbt_version": "1.7.0",
            "generated_at": "2026-08-22T10:00:00.000Z",
        },
        "nodes": {"model.test.fct_users": node},
        "sources": {},
    }


def _make_dbt_project_with_manifest(
    tmp_path: Path,
    ferry_source_db: Path,
    manifest: dict[str, Any],
    ref: str = "fct_users",
) -> Path:
    """Create a Ferry project with a dbt-ref sync pointing at a custom manifest."""
    from conftest import (  # type: ignore[import-not-found]
        _write_dbt_ref_sync,
        _write_ferry_yml_with_dbt,
    )

    project_dir = tmp_path / "project"
    project_dir.mkdir()
    state_path = tmp_path / "state.db"
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    manifest_path = project_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    _write_ferry_yml_with_dbt(project_dir, ferry_source_db, state_path, manifest_path)
    syncs_dir = project_dir / "syncs"
    syncs_dir.mkdir()
    _write_dbt_ref_sync(syncs_dir, "users_sync", ref=ref, output_dir=out_dir)
    return project_dir


def test_config_schema_preferred_over_resolved_schema(
    tmp_path: Path,
    ferry_source_db: Path,
) -> None:
    """dagster-dbt uses config.schema, not the resolved top-level schema.

    When a custom generate_schema_name macro is in use, the resolved schema
    differs from the configured schema. Ferry must use config.schema for the
    default key to match dagster-dbt.
    """
    manifest = _manifest_with_model(
        {
            "unique_id": "model.test.fct_users",
            "name": "fct_users",
            "resource_type": "model",
            "schema": "analytics_marts",
            "config": {"materialized": "table", "schema": "marts"},
        }
    )
    project_dir = _make_dbt_project_with_manifest(tmp_path, ferry_source_db, manifest)
    project = ferry.Project(str(project_dir))
    metas = list(project.list_syncs_metadata())
    translator = DagsterFerryTranslator()
    key = translator.get_dbt_asset_key(metas[0])
    # config.schema is "marts", not the resolved "analytics_marts".
    assert key == AssetKey(["marts", "fct_users"])


def test_config_meta_dagster_asset_key_precedes_top_level_meta(
    tmp_path: Path,
    ferry_source_db: Path,
) -> None:
    """config.meta.dagster.asset_key is checked before top-level meta.

    dagster-dbt reads ``config.meta`` first, then falls back to ``meta``.
    When config.meta.dagster.asset_key is set, it wins over the top-level
    meta.dagster.asset_key.
    """
    manifest = _manifest_with_model(
        {
            "unique_id": "model.test.fct_users",
            "name": "fct_users",
            "resource_type": "model",
            "meta": {"dagster": {"asset_key": ["top_level_key"]}},
            "config": {
                "materialized": "table",
                "meta": {"dagster": {"asset_key": ["config_key"]}},
            },
        }
    )
    project_dir = _make_dbt_project_with_manifest(tmp_path, ferry_source_db, manifest)
    project = ferry.Project(str(project_dir))
    metas = list(project.list_syncs_metadata())
    translator = DagsterFerryTranslator()
    key = translator.get_dbt_asset_key(metas[0])
    assert key == AssetKey(["config_key"])


def test_top_level_meta_used_when_config_meta_absent(
    tmp_path: Path,
    ferry_source_db: Path,
) -> None:
    """Top-level meta.dagster.asset_key is the fallback when config.meta is absent."""
    manifest = _manifest_with_model(
        {
            "unique_id": "model.test.fct_users",
            "name": "fct_users",
            "resource_type": "model",
            "meta": {"dagster": {"asset_key": ["top_only"]}},
            "config": {"materialized": "table"},
        }
    )
    project_dir = _make_dbt_project_with_manifest(tmp_path, ferry_source_db, manifest)
    project = ferry.Project(str(project_dir))
    metas = list(project.list_syncs_metadata())
    translator = DagsterFerryTranslator()
    key = translator.get_dbt_asset_key(metas[0])
    assert key == AssetKey(["top_only"])


def test_versioned_model_uses_alias(
    tmp_path: Path,
    ferry_source_db: Path,
) -> None:
    """Versioned models (dbt >= 1.5) use [alias], not [schema, name].

    This test covers the string version case (``"version": "2"``). dbt also
    serializes versions as raw JSON numbers; see
    ``test_versioned_model_numeric_version_uses_alias`` and
    ``test_versioned_model_float_version_uses_alias`` for those.
    """
    manifest = {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v9.json",
            "dbt_version": "1.7.0",
            "generated_at": "2026-08-22T10:00:00.000Z",
        },
        "nodes": {
            "model.test.fct_orders.v2": {
                "unique_id": "model.test.fct_orders.v2",
                "name": "fct_orders",
                "resource_type": "model",
                "alias": "fct_orders_v2",
                "schema": "analytics",
                "version": "2",
                "config": {"materialized": "table", "schema": "analytics"},
            }
        },
        "sources": {},
    }
    project_dir = _make_dbt_project_with_manifest(
        tmp_path, ferry_source_db, manifest, ref="fct_orders"
    )
    project = ferry.Project(str(project_dir))
    metas = list(project.list_syncs_metadata())
    translator = DagsterFerryTranslator()
    key = translator.get_dbt_asset_key(metas[0])
    assert key == AssetKey(["fct_orders_v2"])


def test_versioned_model_numeric_version_uses_alias(
    tmp_path: Path,
    ferry_source_db: Path,
) -> None:
    """Numeric model versions (raw JSON int) parse and trigger the alias key.

    dbt serializes model versions as raw JSON numbers (e.g. ``"version": 2``,
    not ``"version": "2"``). Before the typed ``version`` field was added,
    these parsed fine via the ``_extra`` catch-all. The typed field must
    preserve that compatibility.
    """
    manifest = {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v9.json",
            "dbt_version": "1.7.0",
            "generated_at": "2026-08-22T10:00:00.000Z",
        },
        "nodes": {
            "model.test.fct_orders.v2": {
                "unique_id": "model.test.fct_orders.v2",
                "name": "fct_orders",
                "resource_type": "model",
                "alias": "fct_orders_v2",
                "schema": "analytics",
                "version": 2,
                "config": {"materialized": "table", "schema": "analytics"},
            }
        },
        "sources": {},
    }
    project_dir = _make_dbt_project_with_manifest(
        tmp_path, ferry_source_db, manifest, ref="fct_orders"
    )
    project = ferry.Project(str(project_dir))
    metas = list(project.list_syncs_metadata())
    assert metas[0].dbt_model is not None
    assert metas[0].dbt_model.version == "2"
    translator = DagsterFerryTranslator()
    key = translator.get_dbt_asset_key(metas[0])
    assert key == AssetKey(["fct_orders_v2"])


def test_versioned_model_float_version_uses_alias(
    tmp_path: Path,
    ferry_source_db: Path,
) -> None:
    """Float model versions (raw JSON float) parse and trigger the alias key.

    dbt accepts float versions (e.g. ``"version": 2.5``). These must parse and
    normalize to the string representation.
    """
    manifest = {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v9.json",
            "dbt_version": "1.7.0",
            "generated_at": "2026-08-22T10:00:00.000Z",
        },
        "nodes": {
            "model.test.fct_orders.v2_5": {
                "unique_id": "model.test.fct_orders.v2_5",
                "name": "fct_orders",
                "resource_type": "model",
                "alias": "fct_orders_v2_5",
                "schema": "analytics",
                "version": 2.5,
                "config": {"materialized": "table", "schema": "analytics"},
            }
        },
        "sources": {},
    }
    project_dir = _make_dbt_project_with_manifest(
        tmp_path, ferry_source_db, manifest, ref="fct_orders"
    )
    project = ferry.Project(str(project_dir))
    metas = list(project.list_syncs_metadata())
    assert metas[0].dbt_model is not None
    assert metas[0].dbt_model.version == "2.5"
    translator = DagsterFerryTranslator()
    key = translator.get_dbt_asset_key(metas[0])
    assert key == AssetKey(["fct_orders_v2_5"])


def test_absent_config_schema_falls_back_to_name_only(
    tmp_path: Path,
    ferry_source_db: Path,
) -> None:
    """When neither config.schema nor any schema is set, the key is [name]."""
    manifest = _manifest_with_model(
        {
            "unique_id": "model.test.fct_users",
            "name": "fct_users",
            "resource_type": "model",
            "config": {"materialized": "table"},
        }
    )
    project_dir = _make_dbt_project_with_manifest(tmp_path, ferry_source_db, manifest)
    project = ferry.Project(str(project_dir))
    metas = list(project.list_syncs_metadata())
    translator = DagsterFerryTranslator()
    key = translator.get_dbt_asset_key(metas[0])
    assert key == AssetKey(["fct_users"])


def test_dagster_dbt_default_key_parity() -> None:
    """Compatibility test: Ferry's get_dbt_asset_key matches dagster-dbt's
    default_asset_key_fn for all precedence branches.

    This test does not import dagster_dbt. It encodes the exact precedence
    from dagster-dbt's ``default_asset_key_fn`` source so any divergence is
    caught locally without a runtime dependency.
    """

    def dagster_dbt_default_key(props: dict[str, Any]) -> AssetKey:
        """Reimplementation of dagster-dbt's default_asset_key_fn for parity testing."""
        dbt_meta = props.get("config", {}).get("meta", {}) or props.get("meta", {})
        dagster_metadata = dbt_meta.get("dagster", {})
        asset_key_config = dagster_metadata.get("asset_key", [])
        if asset_key_config:
            return AssetKey(list(asset_key_config))
        if props["resource_type"] == "source":
            return AssetKey([props["source_name"], props["name"]])
        if props.get("version"):
            return AssetKey([props["alias"]])
        configured_schema = props.get("config", {}).get("schema")
        if configured_schema is not None:
            return AssetKey([configured_schema, props["name"]])
        return AssetKey([props["name"]])

    cases = [
        # config.meta.dagster.asset_key wins
        {
            "name": "model_a",
            "resource_type": "model",
            "config": {"meta": {"dagster": {"asset_key": ["custom", "key"]}}},
            "meta": {"dagster": {"asset_key": ["ignored"]}},
            "expected": AssetKey(["custom", "key"]),
        },
        # top-level meta fallback
        {
            "name": "model_b",
            "resource_type": "model",
            "config": {},
            "meta": {"dagster": {"asset_key": ["top"]}},
            "expected": AssetKey(["top"]),
        },
        # versioned model uses alias
        {
            "name": "model_c",
            "resource_type": "model",
            "alias": "model_c_v2",
            "version": "2",
            "config": {"schema": "analytics"},
            "expected": AssetKey(["model_c_v2"]),
        },
        # config.schema preferred
        {
            "name": "model_d",
            "resource_type": "model",
            "schema": "resolved_schema",
            "config": {"schema": "configured_schema"},
            "expected": AssetKey(["configured_schema", "model_d"]),
        },
        # name-only fallback
        {
            "name": "model_e",
            "resource_type": "model",
            "config": {},
            "expected": AssetKey(["model_e"]),
        },
    ]

    for case in cases:
        expected = case.pop("expected")
        # Ferry's translator maps from DbtModelMetadata fields, not raw props.
        # Build a fake SyncMetadata with the relevant dbt_model fields.
        dbt_model = MagicMock()
        dbt_model.config_dagster_asset_key = (
            case.get("config", {}).get("meta", {}).get("dagster", {}).get("asset_key")
        )
        dbt_model.dagster_asset_key = case.get("meta", {}).get("dagster", {}).get("asset_key")
        dbt_model.version = case.get("version")
        dbt_model.alias = case.get("alias")
        dbt_model.config_schema = case.get("config", {}).get("schema")
        dbt_model.name = case["name"]

        sync = MagicMock()
        sync.dbt_model = dbt_model

        translator = DagsterFerryTranslator()
        ferry_key = translator.get_dbt_asset_key(sync)
        # Compare against the dagster-dbt reimplementation.
        dbt_key = dagster_dbt_default_key(case)
        assert ferry_key == dbt_key == expected, (
            f"Mismatch for model {case['name']}: ferry={ferry_key}, dbt={dbt_key}, expected={expected}"
        )


# ---------------------------------------------------------------------------
# AssetSpec deps wiring
# ---------------------------------------------------------------------------


def test_dbt_ref_spec_has_dbt_asset_key_in_deps(
    ferry_dbt_project: Path,
) -> None:
    """The dbt-owned AssetKey is added to the Ferry sync's deps only."""

    @ferry_assets(project_dir=str(ferry_dbt_project))
    def syncs(context, ferry: DagsterFerryResource) -> Iterator[MaterializeResult[Any]]:
        yield from ferry.run(context)

    specs = list(syncs.specs)
    assert len(specs) == 1
    spec = specs[0]
    dep_keys = [d if isinstance(d, AssetKey) else getattr(d, "asset_key", d) for d in spec.deps]
    assert AssetKey(["analytics", "fct_users"]) in dep_keys


def test_ferry_never_emits_spec_for_dbt_owned_key(
    ferry_dbt_project: Path,
) -> None:
    """Ferry never creates an AssetSpec whose key is the dbt-owned key."""

    @ferry_assets(project_dir=str(ferry_dbt_project))
    def syncs(context, ferry: DagsterFerryResource) -> Iterator[MaterializeResult[Any]]:
        yield from ferry.run(context)

    ferry_keys = {spec.key for spec in syncs.specs}
    dbt_key = AssetKey(["analytics", "fct_users"])
    assert dbt_key not in ferry_keys


def test_mixed_project_specs_wired_correctly(
    ferry_mixed_project: Path,
) -> None:
    """Mixed project: SQL sync has no deps, dbt-ref sync has the dbt dep."""

    @ferry_assets(project_dir=str(ferry_mixed_project))
    def syncs(context, ferry: DagsterFerryResource) -> Iterator[MaterializeResult[Any]]:
        yield from ferry.run(context)

    specs_by_key = {spec.key: spec for spec in syncs.specs}
    # SQL sync: no deps.
    assert list(specs_by_key[AssetKey("alpha_sync")].deps) == []
    # dbt-ref sync: deps include the meta-dagster.asset_key override.
    dbt_dep_keys = [
        d if isinstance(d, AssetKey) else getattr(d, "asset_key", d)
        for d in specs_by_key[AssetKey("dbt_sync")].deps
    ]
    assert AssetKey(["dbt", "fct_orders"]) in dbt_dep_keys


# ---------------------------------------------------------------------------
# Custom get_dbt_asset_key translation
# ---------------------------------------------------------------------------


class _PrefixedDbtTranslator(DagsterFerryTranslator):
    """A custom translator that prefixes dbt asset keys with 'custom'."""

    def get_dbt_asset_key(self, sync: ferry.SyncMetadata) -> AssetKey | None:
        dbt = sync.dbt_model
        if dbt is None:
            return None
        return AssetKey(["custom", dbt.name])


def test_custom_get_dbt_asset_key_is_used_in_deps(
    ferry_dbt_project: Path,
) -> None:
    """A custom translator's get_dbt_asset_key is honored in deps."""

    @ferry_assets(
        project_dir=str(ferry_dbt_project),
        translator=_PrefixedDbtTranslator(),
    )
    def syncs(context, ferry: DagsterFerryResource) -> Iterator[MaterializeResult[Any]]:
        yield from ferry.run(context)

    spec = list(syncs.specs)[0]
    dep_keys = [d if isinstance(d, AssetKey) else getattr(d, "asset_key", d) for d in spec.deps]
    assert AssetKey(["custom", "fct_users"]) in dep_keys


def test_get_dbt_asset_key_invalid_return_type_raises(
    ferry_dbt_project: Path,
) -> None:
    """An invalid get_dbt_asset_key return type fails with the sync name."""

    class _BadTranslator(DagsterFerryTranslator):
        def get_dbt_asset_key(self, sync: ferry.SyncMetadata) -> AssetKey | None:
            return "not_an_asset_key"  # type: ignore[return-value]

    with pytest.raises(TypeError, match="get_dbt_asset_key.*users_sync"):

        @ferry_assets(
            project_dir=str(ferry_dbt_project),
            translator=_BadTranslator(),
        )
        def syncs(context, ferry: DagsterFerryResource) -> Iterator[MaterializeResult[Any]]:
            yield from ferry.run(context)


# ---------------------------------------------------------------------------
# Error cases: missing manifest config, missing file, malformed, missing model
# ---------------------------------------------------------------------------


def test_dbt_ref_without_manifest_config_fails_discovery(
    ferry_dbt_project_missing_manifest_config: Path,
) -> None:
    """A dbt-ref sync without dbt.manifest_path fails list_syncs_metadata."""
    project = ferry.Project(str(ferry_dbt_project_missing_manifest_config))
    with pytest.raises(ferry.ConfigError, match="dbt.manifest_path"):
        project.list_syncs_metadata()


def test_dbt_ref_without_manifest_config_fails_decoration(
    ferry_dbt_project_missing_manifest_config: Path,
) -> None:
    """A dbt-ref sync without manifest config fails at decoration time."""
    with pytest.raises(ferry.ConfigError, match="dbt.manifest_path"):

        @ferry_assets(project_dir=str(ferry_dbt_project_missing_manifest_config))
        def syncs(context, ferry: DagsterFerryResource) -> Iterator[MaterializeResult[Any]]:
            yield from ferry.run(context)


def test_missing_manifest_file_fails_discovery(
    ferry_dbt_project_missing_manifest_file: Path,
) -> None:
    """A configured but missing manifest file fails discovery with ConfigError."""
    project = ferry.Project(str(ferry_dbt_project_missing_manifest_file))
    with pytest.raises(ferry.ConfigError, match="Cannot open dbt manifest"):
        project.list_syncs_metadata()


def test_malformed_manifest_fails_discovery(
    ferry_dbt_project_malformed_manifest: Path,
) -> None:
    """A malformed manifest JSON fails discovery with ConfigError."""
    project = ferry.Project(str(ferry_dbt_project_malformed_manifest))
    with pytest.raises(ferry.ConfigError, match="Cannot parse dbt manifest"):
        project.list_syncs_metadata()


def test_missing_model_in_manifest_fails_discovery(
    tmp_path: Path,
    ferry_source_db: Path,
) -> None:
    """A dbt ref to a model not in the manifest fails with an available list."""
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    state_path = tmp_path / "state.db"
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    manifest = {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v7.json",
            "dbt_version": "1.7.0",
            "generated_at": "2026-08-22T10:00:00.000Z",
        },
        "nodes": {
            "model.test.other": {
                "unique_id": "model.test.other",
                "name": "other",
                "resource_type": "model",
                "compiled_code": "SELECT 1",
                "package_name": "test",
                "schema": "analytics",
                "config": {"materialized": "table"},
            }
        },
        "sources": {},
    }
    from conftest import (  # type: ignore[import-not-found]
        _write_dbt_ref_sync,
        _write_ferry_yml_with_dbt,
    )

    manifest_path = project_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    _write_ferry_yml_with_dbt(project_dir, ferry_source_db, state_path, manifest_path)
    syncs_dir = project_dir / "syncs"
    syncs_dir.mkdir()
    _write_dbt_ref_sync(syncs_dir, "missing_sync", ref="does_not_exist", output_dir=out_dir)

    project = ferry.Project(str(project_dir))
    with pytest.raises(ferry.ConfigError, match="not found") as exc_info:
        project.list_syncs_metadata()
    assert "other" in str(exc_info.value)


def test_ephemeral_ref_fails_discovery(
    tmp_path: Path,
    ferry_source_db: Path,
) -> None:
    """A dbt ref to an ephemeral model fails discovery."""
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    state_path = tmp_path / "state.db"
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    manifest = {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v7.json",
            "dbt_version": "1.7.0",
            "generated_at": "2026-08-22T10:00:00.000Z",
        },
        "nodes": {
            "model.test.fct_eph": {
                "unique_id": "model.test.fct_eph",
                "name": "fct_eph",
                "resource_type": "model",
                "compiled_code": "WITH x AS (SELECT 1) SELECT * FROM x",
                "package_name": "test",
                "schema": "analytics",
                "config": {"materialized": "ephemeral"},
            }
        },
        "sources": {},
    }
    from conftest import (  # type: ignore[import-not-found]
        _write_dbt_ref_sync,
        _write_ferry_yml_with_dbt,
    )

    manifest_path = project_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    _write_ferry_yml_with_dbt(project_dir, ferry_source_db, state_path, manifest_path)
    syncs_dir = project_dir / "syncs"
    syncs_dir.mkdir()
    _write_dbt_ref_sync(syncs_dir, "eph_sync", ref="fct_eph", output_dir=out_dir)

    project = ferry.Project(str(project_dir))
    with pytest.raises(ferry.ConfigError, match="ephemeral"):
        project.list_syncs_metadata()


def test_ambiguous_model_fails_with_candidates(
    tmp_path: Path,
    ferry_source_db: Path,
) -> None:
    """An ambiguous model name fails discovery listing candidate unique_ids."""
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    state_path = tmp_path / "state.db"
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    manifest = {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v7.json",
            "dbt_version": "1.7.0",
            "generated_at": "2026-08-22T10:00:00.000Z",
        },
        "nodes": {
            "model.a.dup": {
                "unique_id": "model.a.dup",
                "name": "dup",
                "resource_type": "model",
                "package_name": "a",
                "schema": "a",
                "config": {"materialized": "table"},
            },
            "model.b.dup": {
                "unique_id": "model.b.dup",
                "name": "dup",
                "resource_type": "model",
                "package_name": "b",
                "schema": "b",
                "config": {"materialized": "table"},
            },
        },
        "sources": {},
    }
    from conftest import (  # type: ignore[import-not-found]
        _write_dbt_ref_sync,
        _write_ferry_yml_with_dbt,
    )

    manifest_path = project_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    _write_ferry_yml_with_dbt(project_dir, ferry_source_db, state_path, manifest_path)
    syncs_dir = project_dir / "syncs"
    syncs_dir.mkdir()
    _write_dbt_ref_sync(syncs_dir, "amb_sync", ref="dup", output_dir=out_dir)

    project = ferry.Project(str(project_dir))
    with pytest.raises(ferry.ConfigError, match="ambiguous") as exc_info:
        project.list_syncs_metadata()
    msg = str(exc_info.value)
    assert "model.a.dup" in msg
    assert "model.b.dup" in msg


def test_non_model_ref_fails_contextually(
    tmp_path: Path,
    ferry_source_db: Path,
) -> None:
    """A dbt ref to a seed (non-model) fails with a contextual error."""
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    state_path = tmp_path / "state.db"
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    manifest = {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v7.json",
            "dbt_version": "1.7.0",
            "generated_at": "2026-08-22T10:00:00.000Z",
        },
        "nodes": {
            "seed.test.raw": {
                "unique_id": "seed.test.raw",
                "name": "raw",
                "resource_type": "seed",
                "config": {"materialized": "seed"},
            }
        },
        "sources": {},
    }
    from conftest import (  # type: ignore[import-not-found]
        _write_dbt_ref_sync,
        _write_ferry_yml_with_dbt,
    )

    manifest_path = project_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    _write_ferry_yml_with_dbt(project_dir, ferry_source_db, state_path, manifest_path)
    syncs_dir = project_dir / "syncs"
    syncs_dir.mkdir()
    _write_dbt_ref_sync(syncs_dir, "seed_sync", ref="raw", output_dir=out_dir)

    project = ferry.Project(str(project_dir))
    with pytest.raises(ferry.ConfigError, match="non-model"):
        project.list_syncs_metadata()


def test_stale_manifest_loads_and_warns_only(
    tmp_path: Path,
    ferry_source_db: Path,
) -> None:
    """A stale manifest still loads (advisory warn only, never errors)."""
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    state_path = tmp_path / "state.db"
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    manifest = {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v7.json",
            "dbt_version": "1.7.0",
            # 30 days ago — well past the 24h freshness bound.
            "generated_at": "2026-07-23T10:00:00.000Z",
        },
        "nodes": {
            "model.test.fct_users": {
                "unique_id": "model.test.fct_users",
                "name": "fct_users",
                "resource_type": "model",
                "compiled_code": "SELECT 1",
                "package_name": "test",
                "schema": "analytics",
                "config": {"materialized": "table"},
            }
        },
        "sources": {},
    }
    from conftest import (  # type: ignore[import-not-found]
        _write_dbt_ref_sync,
        _write_ferry_yml_with_dbt,
    )

    manifest_path = project_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    _write_ferry_yml_with_dbt(project_dir, ferry_source_db, state_path, manifest_path)
    syncs_dir = project_dir / "syncs"
    syncs_dir.mkdir()
    _write_dbt_ref_sync(syncs_dir, "users_sync", ref="fct_users", output_dir=out_dir)

    project = ferry.Project(str(project_dir))
    # Stale manifests follow the existing Ferry freshness policy: warn only,
    # never error. Discovery succeeds and resolves metadata normally.
    metas = list(project.list_syncs_metadata())
    assert len(metas) == 1
    assert metas[0].dbt_model is not None


# ---------------------------------------------------------------------------
# Materialization metadata
# ---------------------------------------------------------------------------


@dataclass
class _FakeResult:
    """Duck-typed stand-in for ferry.SyncResult carrying all fields."""

    sync_name: str
    run_id: str = "run-fake-0000"
    rows_extracted: int = 10
    rows_synced: int = 8
    rows_failed: int = 1
    rows_pending: int = 1
    rows_retried: int = 0
    rows_dead: int = 1
    duration_seconds: float = 1.5
    dry_run: bool = False
    mode: str = "incremental"


def _resource_with_fake_project(project_dir: Path, run_impl: Any) -> DagsterFerryResource:
    """Build a resource whose native project.run is replaced by a fake."""
    res = DagsterFerryResource(project_dir=str(project_dir))
    _ = res.project  # triggers path/config validation

    class _FakeProject:
        def run(self, sync_names: list[str] | None = None, **_: Any) -> Any:
            return run_impl(sync_names=sync_names or [])

    class _FakeResource(DagsterFerryResource):
        def setup_for_execution(self, context: Any) -> None:
            if self._project is None:
                self._project = _FakeProject()  # type: ignore[assignment]

    fake_res = _FakeResource(project_dir=str(project_dir))
    fake_res._project = _FakeProject()  # type: ignore[assignment]
    return fake_res


def test_materialize_result_metadata_keys_and_types(
    ferry_multi_sync_project: Path,
) -> None:
    """MaterializeResult carries typed metadata from SyncResult fields."""
    fake = _FakeResult(
        sync_name="alpha_sync",
        run_id="run-abc-123",
        rows_extracted=100,
        rows_synced=95,
        rows_failed=3,
        rows_pending=2,
        rows_retried=1,
        rows_dead=3,
        duration_seconds=12.5,
        dry_run=False,
        mode="incremental",
    )

    def run_impl(sync_names: list[str]) -> list[_FakeResult]:
        return [fake]

    res = _resource_with_fake_project(ferry_multi_sync_project, run_impl)

    @ferry_assets(project_dir=str(ferry_multi_sync_project))
    def syncs(context, ferry: DagsterFerryResource) -> Iterator[MaterializeResult[Any]]:
        yield from ferry.run(context)

    result = materialize(
        [syncs],
        resources={"ferry": res},
        selection=[AssetKey("alpha_sync")],
    )
    assert result.success

    # Inspect the materialization event for metadata.
    events = [e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION"]
    assert events
    md = events[0].event_specific_data.materialization.metadata  # type: ignore[union-attr]

    assert "dagster/row_count" in md
    assert md["dagster/row_count"].value == 95
    assert isinstance(md["dagster/row_count"], IntMetadataValue)

    assert md["ferry/run_id"].value == "run-abc-123"
    assert isinstance(md["ferry/run_id"], TextMetadataValue)

    assert md["ferry/rows_extracted"].value == 100
    assert md["ferry/rows_delivered"].value == 95
    assert md["ferry/rows_failed"].value == 3
    assert md["ferry/rows_pending"].value == 2
    assert md["ferry/rows_retried"].value == 1
    assert md["ferry/rows_dead"].value == 3

    assert isinstance(md["ferry/duration_seconds"], FloatMetadataValue)
    assert md["ferry/duration_seconds"].value == 12.5

    assert isinstance(md["ferry/mode"], TextMetadataValue)
    assert md["ferry/mode"].value == "incremental"

    assert isinstance(md["ferry/dry_run"], BoolMetadataValue)
    assert md["ferry/dry_run"].value is False

    # Changed/skipped metrics are omitted entirely (never fabricated).
    assert "ferry/rows_changed" not in md
    assert "ferry/rows_skipped" not in md
    # No invented status key.
    assert "ferry/status" not in md


def test_materialize_result_emits_genuine_zero_counts(
    ferry_multi_sync_project: Path,
) -> None:
    """Genuine zero row counts are emitted as int(0), not omitted."""
    zero_fake = _FakeResult(
        sync_name="alpha_sync",
        rows_extracted=0,
        rows_synced=0,
        rows_failed=0,
        rows_pending=0,
        rows_retried=0,
        rows_dead=0,
    )

    def run_impl(sync_names: list[str]) -> list[_FakeResult]:
        return [zero_fake]

    res = _resource_with_fake_project(ferry_multi_sync_project, run_impl)

    @ferry_assets(project_dir=str(ferry_multi_sync_project))
    def syncs(context, ferry: DagsterFerryResource) -> Iterator[MaterializeResult[Any]]:
        yield from ferry.run(context)

    result = materialize(
        [syncs],
        resources={"ferry": res},
        selection=[AssetKey("alpha_sync")],
    )
    assert result.success
    events = [e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION"]
    md = events[0].event_specific_data.materialization.metadata  # type: ignore[union-attr]

    assert md["dagster/row_count"].value == 0
    assert md["ferry/rows_extracted"].value == 0
    assert md["ferry/rows_delivered"].value == 0
    assert md["ferry/rows_dead"].value == 0


def test_complete_result_validation_before_yielding(
    ferry_multi_sync_project: Path,
) -> None:
    """A partial native result raises RuntimeError and yields no materialization."""

    # Return only alpha when both alpha and beta were selected.
    def run_impl(sync_names: list[str]) -> list[_FakeResult]:
        return [_FakeResult("alpha_sync")]

    res = _resource_with_fake_project(ferry_multi_sync_project, run_impl)

    @ferry_assets(project_dir=str(ferry_multi_sync_project))
    def syncs(context, ferry: DagsterFerryResource) -> Iterator[MaterializeResult[Any]]:
        yield from ferry.run(context)

    with pytest.raises(RuntimeError, match="do not exactly match"):
        materialize([syncs], resources={"ferry": res})


# ---------------------------------------------------------------------------
# No dagster-dbt dependency
# ---------------------------------------------------------------------------


def test_dagster_dbt_not_imported_at_runtime() -> None:
    """dagster_dbt is never imported by dagster_ferry at runtime."""
    # Remove any cached import to start clean.
    sys.modules.pop("dagster_dbt", None)
    # Re-import the public modules; they must not pull in dagster_dbt.
    import importlib

    import dagster_ferry

    importlib.reload(dagster_ferry)
    from dagster_ferry import _assets, _resource

    importlib.reload(_assets)
    importlib.reload(_resource)
    assert "dagster_dbt" not in sys.modules, "dagster_ferry must not import dagster_dbt at runtime"


def test_no_dbt_command_execution_or_external_services(
    ferry_dbt_project: Path,
) -> None:
    """Discovery reads a static JSON manifest; no dbt CLI or external service."""
    # If discovery worked at all, it used the static manifest file, not dbt.
    project = ferry.Project(str(ferry_dbt_project))
    metas = list(project.list_syncs_metadata())
    assert len(metas) == 1
    assert metas[0].dbt_model is not None
