"""Shared test fixtures for dagster-ferry."""

from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import pytest


def _write_ferry_yml(project_dir: Path) -> None:
    """Write the smallest credentials-free valid ferry.yml.

    FerryConfig::load requires a non-empty name, a source block, and a state
    block with a non-empty path. The DuckDB source path is a string check only,
    so no real DuckDB file is needed for native Project construction.
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


@pytest.fixture
def ferry_project_dir(tmp_path: Path) -> Path:
    """A project directory containing a minimal valid ferry.yml."""
    _write_ferry_yml(tmp_path)
    return tmp_path
