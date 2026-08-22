"""Shared test fixtures for dagster-ferry."""

from __future__ import annotations

import json
from pathlib import Path
from textwrap import dedent
from typing import Any

import pytest


def _write_ferry_yml(project_dir: Path, source_path: Path, state_path: Path) -> None:
    """Write a credentials-free valid ferry.yml pointing at real DuckDB files.

    FerryConfig::load requires a non-empty name, a source block, and a state
    block with a non-empty path. Paths are absolute so the project can be
    loaded from any working directory.
    """
    (project_dir / "ferry.yml").write_text(
        dedent(
            f"""\
            name: test_project
            version: "1.0"
            source:
              type: duckdb
              path: {source_path}
              query: SELECT * FROM users
            state:
              backend: duckdb
              path: {state_path}
            """,
        ),
        encoding="utf-8",
    )


def _write_ferry_yml_minimal(project_dir: Path) -> None:
    """Write the smallest credentials-free valid ferry.yml.

    Uses placeholder paths that are never opened for discovery-only tests.
    """
    (project_dir / "ferry.yml").write_text(
        dedent(
            """\
            name: test_project
            version: "1.0"
            source:
              type: duckdb
              path: /data/db.duckdb
              query: SELECT 1
            state:
              backend: duckdb
              path: .ferry/state.db
            """,
        ),
        encoding="utf-8",
    )


def _write_ferry_yml_with_dbt(
    project_dir: Path,
    source_path: Path,
    state_path: Path,
    manifest_path: Path,
) -> None:
    """Write a credentials-free ferry.yml with a configured dbt manifest path."""
    (project_dir / "ferry.yml").write_text(
        dedent(
            f"""\
            name: test_project
            version: "1.0"
            source:
              type: duckdb
              path: {source_path}
              query: SELECT * FROM users
            state:
              backend: duckdb
              path: {state_path}
            dbt:
              manifest_path: {manifest_path}
            """,
        ),
        encoding="utf-8",
    )


def _write_sync(
    syncs_dir: Path,
    name: str,
    *,
    description: str | None = None,
    tags: list[str] | None = None,
    sql: str = "SELECT id, name FROM users ORDER BY id",
    output_dir: Path | None = None,
) -> None:
    """Write a single credentials-free file-destination sync YAML."""
    desc_line = f'description: "{description}"' if description else ""
    tags_line = f"tags: {tags}" if tags is not None else ""
    dest_dir = output_dir if output_dir is not None else syncs_dir.parent / "out"
    dest_dir.mkdir(parents=True, exist_ok=True)
    content = dedent(
        f"""\
        name: {name}
        {desc_line}
        {tags_line}
        model:
          sql: {sql}
        destination:
          type: file
          output_dir: {dest_dir}
          format: csv
        sync:
          mode: incremental
          cursor_field: id
          cdc:
            method: hash
        """,
    )
    # Remove blank lines from optional fields for cleaner YAML.
    content = "\n".join(line for line in content.splitlines() if line.strip())
    (syncs_dir / f"{name}.yml").write_text(content + "\n", encoding="utf-8")


def _write_dbt_ref_sync(
    syncs_dir: Path,
    name: str,
    ref: str,
    *,
    description: str | None = None,
    tags: list[str] | None = None,
    output_dir: Path | None = None,
) -> None:
    """Write a single credentials-free file-destination sync YAML using a dbt ref."""
    desc_line = f'description: "{description}"' if description else ""
    tags_line = f"tags: {tags}" if tags is not None else ""
    dest_dir = output_dir if output_dir is not None else syncs_dir.parent / "out"
    dest_dir.mkdir(parents=True, exist_ok=True)
    content = dedent(
        f"""\
        name: {name}
        {desc_line}
        {tags_line}
        model:
          ref: {ref}
        destination:
          type: file
          output_dir: {dest_dir}
          format: csv
        sync:
          mode: incremental
          cursor_field: id
          cdc:
            method: hash
        """,
    )
    content = "\n".join(line for line in content.splitlines() if line.strip())
    (syncs_dir / f"{name}.yml").write_text(content + "\n", encoding="utf-8")


def _write_manifest(project_dir: Path, manifest: dict[str, Any]) -> Path:
    """Write a manifest dict as manifest.json in the project directory."""
    manifest_path = project_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return manifest_path


def _sample_manifest_dict() -> dict[str, Any]:
    """Return a minimal dbt manifest with fct_users, fct_orders, fct_ephemeral."""
    return {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v7.json",
            "dbt_version": "1.7.0",
            "generated_at": "2026-08-22T10:00:00.000Z",
        },
        "nodes": {
            "model.test.fct_users": {
                "unique_id": "model.test.fct_users",
                "name": "fct_users",
                "resource_type": "model",
                "compiled_code": "SELECT id, name FROM analytics.fct_users",
                "relation_name": '"analytics"."fct_users"',
                "alias": "fct_users",
                "package_name": "test",
                "schema": "analytics",
                "database": "warehouse",
                "fqn": ["test", "models", "analytics", "fct_users.sql"],
                "config": {"materialized": "table", "schema": "analytics"},
                "meta": {},
            },
            "model.test.fct_orders": {
                "unique_id": "model.test.fct_orders",
                "name": "fct_orders",
                "resource_type": "model",
                "compiled_code": "SELECT order_id, user_id, total FROM analytics.fct_orders",
                "relation_name": '"analytics"."fct_orders"',
                "alias": "fct_orders",
                "package_name": "test",
                "schema": "analytics",
                "fqn": ["test", "models", "analytics", "fct_orders.sql"],
                "config": {"materialized": "view", "schema": "analytics"},
                "meta": {"dagster": {"asset_key": ["dbt", "fct_orders"]}},
            },
            "model.test.fct_ephemeral": {
                "unique_id": "model.test.fct_ephemeral",
                "name": "fct_ephemeral",
                "resource_type": "model",
                "compiled_code": "WITH x AS (SELECT 1) SELECT * FROM x",
                "package_name": "test",
                "schema": "analytics",
                "config": {"materialized": "ephemeral"},
            },
        },
        "sources": {},
    }


@pytest.fixture
def ferry_project_dir(tmp_path: Path) -> Path:
    """A project directory containing a minimal valid ferry.yml (no syncs)."""
    _write_ferry_yml_minimal(tmp_path)
    return tmp_path


@pytest.fixture
def ferry_source_db(tmp_path: Path) -> Path:
    """A real credentials-free DuckDB source database with a users table."""
    import duckdb

    db_path = tmp_path / "source.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE users (id VARCHAR PRIMARY KEY, name VARCHAR NOT NULL)")
    conn.execute("INSERT INTO users VALUES ('1', 'Alice'), ('2', 'Bob'), ('3', 'Carol')")
    conn.close()
    return db_path


@pytest.fixture
def ferry_multi_sync_project(tmp_path: Path, ferry_source_db: Path) -> Path:
    """A real Ferry project with two syncs backed by a DuckDB source and file
    destinations. Suitable for discovery, subset execution, and real runs.
    """
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    state_path = tmp_path / "state.db"
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    _write_ferry_yml(project_dir, ferry_source_db, state_path)
    syncs_dir = project_dir / "syncs"
    syncs_dir.mkdir()
    _write_sync(
        syncs_dir,
        "alpha_sync",
        description="Alpha sync",
        tags=["team_a", "p1"],
        output_dir=out_dir,
    )
    _write_sync(
        syncs_dir,
        "beta_sync",
        description="Beta sync",
        tags=["team_b"],
        sql="SELECT id, name FROM users WHERE id = '1' ORDER BY id",
        output_dir=out_dir,
    )
    return project_dir


@pytest.fixture
def ferry_empty_syncs_project(tmp_path: Path, ferry_source_db: Path) -> Path:
    """A real Ferry project with no syncs."""
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    state_path = tmp_path / "state.db"
    _write_ferry_yml(project_dir, ferry_source_db, state_path)
    (project_dir / "syncs").mkdir()
    return project_dir


@pytest.fixture
def ferry_dbt_project(tmp_path: Path, ferry_source_db: Path) -> Path:
    """A Ferry project with one dbt-ref sync backed by a real manifest.

    The manifest contains fct_users (table, schema analytics) and fct_orders
    (view with meta.dagster.asset_key). The dbt-ref sync references fct_users.
    """
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    state_path = tmp_path / "state.db"
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    manifest_path = project_dir / "manifest.json"
    manifest_path.write_text(json.dumps(_sample_manifest_dict()), encoding="utf-8")
    _write_ferry_yml_with_dbt(project_dir, ferry_source_db, state_path, manifest_path)
    syncs_dir = project_dir / "syncs"
    syncs_dir.mkdir()
    _write_dbt_ref_sync(
        syncs_dir,
        "users_sync",
        ref="fct_users",
        description="Users from dbt",
        output_dir=out_dir,
    )
    return project_dir


@pytest.fixture
def ferry_mixed_project(tmp_path: Path, ferry_source_db: Path) -> Path:
    """A Ferry project with one SQL-only sync and one dbt-ref sync.

    The SQL sync and the dbt-ref sync coexist, with a manifest configured for
    the dbt-ref. This exercises mixed-project discovery and dependency wiring.
    """
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    state_path = tmp_path / "state.db"
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    manifest_path = project_dir / "manifest.json"
    manifest_path.write_text(json.dumps(_sample_manifest_dict()), encoding="utf-8")
    _write_ferry_yml_with_dbt(project_dir, ferry_source_db, state_path, manifest_path)
    syncs_dir = project_dir / "syncs"
    syncs_dir.mkdir()
    _write_sync(
        syncs_dir,
        "alpha_sync",
        description="SQL only",
        tags=["team_a"],
        output_dir=out_dir,
    )
    _write_dbt_ref_sync(
        syncs_dir,
        "dbt_sync",
        ref="fct_orders",
        description="dbt ref",
        tags=["team_b"],
        output_dir=out_dir,
    )
    return project_dir


@pytest.fixture
def ferry_dbt_project_missing_manifest_config(
    tmp_path: Path,
    ferry_source_db: Path,
) -> Path:
    """A Ferry project with a dbt-ref sync but no dbt.manifest_path configured."""
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    state_path = tmp_path / "state.db"
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    # No dbt block: ferry.yml has no manifest_path.
    _write_ferry_yml(project_dir, ferry_source_db, state_path)
    syncs_dir = project_dir / "syncs"
    syncs_dir.mkdir()
    _write_dbt_ref_sync(
        syncs_dir,
        "users_sync",
        ref="fct_users",
        output_dir=out_dir,
    )
    return project_dir


@pytest.fixture
def ferry_dbt_project_malformed_manifest(
    tmp_path: Path,
    ferry_source_db: Path,
) -> Path:
    """A Ferry project with a dbt-ref sync and a malformed manifest file."""
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    state_path = tmp_path / "state.db"
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    manifest_path = project_dir / "manifest.json"
    manifest_path.write_text("{ not valid json", encoding="utf-8")
    _write_ferry_yml_with_dbt(project_dir, ferry_source_db, state_path, manifest_path)
    syncs_dir = project_dir / "syncs"
    syncs_dir.mkdir()
    _write_dbt_ref_sync(
        syncs_dir,
        "users_sync",
        ref="fct_users",
        output_dir=out_dir,
    )
    return project_dir


@pytest.fixture
def ferry_dbt_project_missing_manifest_file(
    tmp_path: Path,
    ferry_source_db: Path,
) -> Path:
    """A Ferry project pointing at a manifest path that does not exist."""
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    state_path = tmp_path / "state.db"
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    manifest_path = project_dir / "does_not_exist.json"
    _write_ferry_yml_with_dbt(project_dir, ferry_source_db, state_path, manifest_path)
    syncs_dir = project_dir / "syncs"
    syncs_dir.mkdir()
    _write_dbt_ref_sync(
        syncs_dir,
        "users_sync",
        ref="fct_users",
        output_dir=out_dir,
    )
    return project_dir
