"""Public API surface tests."""

from __future__ import annotations

import importlib

import dagster_ferry


def test_public_exports_are_exactly_the_documented_set() -> None:
    assert dagster_ferry.__all__ == ["DagsterFerryResource", "__version__"]


def test_dagster_ferry_resource_is_exported() -> None:
    from dagster_ferry import DagsterFerryResource

    assert DagsterFerryResource is dagster_ferry.DagsterFerryResource


def test_version_is_independent_and_static() -> None:
    import dagster_ferry._version as version_module

    assert dagster_ferry.__version__ == "0.1.0"
    assert version_module.__version__ == "0.1.0"


def test_no_asset_or_translator_placeholders_shipped() -> None:
    for forbidden in (
        "DagsterFerryTranslator",
        "ferry_assets",
        "FerryAssetSpec",
        "build_ferry_assets",
    ):
        assert not hasattr(dagster_ferry, forbidden), forbidden


def test_module_has_no_side_effects_on_reimport() -> None:
    # Re-importing must not construct a native project or mutate state.
    importlib.reload(dagster_ferry)
    assert dagster_ferry.__all__ == ["DagsterFerryResource", "__version__"]
