"""Dagster integration for Ferry, a Rust-native reverse ETL engine."""

from dagster_ferry._resource import DagsterFerryResource
from dagster_ferry._version import __version__

__all__ = ["DagsterFerryResource", "__version__"]
