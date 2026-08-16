"""Shared test fixtures for dagster-ferry."""

from __future__ import annotations

from pathlib import Path
from textwrap import dedent

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
