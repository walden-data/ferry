"""Dagster resource that wraps a native Ferry project."""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import ferry
from dagster import (
    AssetExecutionContext,
    AssetKey,
    ConfigurableResource,
    InitResourceContext,
    MaterializeResult,
    MetadataValue,
)
from pydantic import PrivateAttr

__all__ = ["DagsterFerryResource"]


class DagsterFerryResource(ConfigurableResource["DagsterFerryResource"]):
    """Dagster resource that loads a native ``ferry.Project``.

    The resource accepts a ``project_dir`` string so Dagster can serialize the
    configuration and resolve ``EnvVar`` values at launch time. The native
    ``ferry.Project`` is constructed once per resource lifecycle, either in
    :meth:`setup_for_execution` (during a Dagster run) or lazily through the
    :attr:`project` property (for direct/test usage). Importing this module or
    evaluating the config class never constructs a native project.

    The ``project_dir`` is expanded (``~`` and environment variables) and
    resolved to an absolute path. It is checked to be an existing directory
    containing a ``ferry.yml`` file before the native project is constructed. Actionable
    path and config errors raise ``FileNotFoundError``, ``NotADirectoryError``,
    or ``ValueError`` with a clear message. Native ``ferry`` errors
    (``ferry.FerryError`` and its subclasses, plus ``ValueError`` raised by
    the native constructor) propagate with their original causes preserved.

    Use :meth:`run` inside a ``@ferry_assets`` decorated function. It executes
    exactly the syncs Dagster selected for the current materialization and
    yields one ``MaterializeResult`` per selected successful sync.
    """

    project_dir: str

    _project: ferry.Project | None = PrivateAttr(default=None)

    def _resolve_project_dir(self) -> Path:
        """Expand, resolve, and validate the configured project directory.

        Raises FileNotFoundError when the path does not exist,
        NotADirectoryError when it is not a directory, and FileNotFoundError
        when ferry.yml is missing inside it. Returns the resolved absolute path.
        """
        if not self.project_dir or not self.project_dir.strip():
            msg = "project_dir must not be empty"
            raise ValueError(msg)

        expanded = os.path.expanduser(os.path.expandvars(self.project_dir))
        resolved = Path(expanded).resolve()

        if not resolved.exists():
            msg = f"project_dir does not exist: {resolved}"
            raise FileNotFoundError(msg)

        if not resolved.is_dir():
            msg = f"project_dir is not a directory: {resolved}"
            raise NotADirectoryError(msg)

        ferry_yml = resolved / "ferry.yml"
        if not ferry_yml.exists():
            msg = f"ferry.yml not found in project_dir: {resolved}"
            raise FileNotFoundError(msg)

        return resolved

    def _build_project(self) -> ferry.Project:
        """Construct the native ferry.Project from the resolved project_dir.

        Path validation runs first so actionable path errors surface with clear
        messages. The native constructor then re-validates the config and may
        raise ferry.FerryError (or a subclass) or ValueError, which propagate
        unchanged with their original causes.
        """
        resolved = self._resolve_project_dir()
        return ferry.Project(str(resolved))

    def setup_for_execution(self, context: InitResourceContext) -> None:
        """Construct the native project once for the resource lifecycle.

        Dagster calls this before the resource is used in a run. The native
        project is cached so subsequent access within the same run reuses it.
        """
        self._project = self._build_project()

    @property
    def project(self) -> ferry.Project:
        """Lazily construct and return the native ferry.Project.

        Used for direct access and tests. Within a Dagster run, prefer
        ``setup_for_execution`` so the project is constructed once up front.
        """
        if self._project is None:
            self._project = self._build_project()
        return self._project

    def run(self, context: AssetExecutionContext) -> Iterator[MaterializeResult[Any]]:
        """Execute exactly the syncs Dagster selected for the current run.

        Reads the selected asset keys from ``context.selected_asset_keys``.
        It maps them back to native Ferry sync names via stable metadata on
        the bound ``AssetsDefinition``. It then calls native
        ``Project.run(sync_names=[...])`` exactly once with the sorted selected
        sync names. It validates that the returned result names exactly match
        the selected set, then yields one ``MaterializeResult`` per
        successful result with typed run metadata attached.

        Behavior:

        * Empty selection: yields nothing and does not call Ferry.
        * Result-name mismatch: raises ``RuntimeError`` with the diff so a
          partial or unexpected native result is never silently accepted. No
          materialization is yielded before the full selected set validates.
        * Native Ferry execution errors propagate through Dagster's normal
          boundary. This method does not broad-wrap them.

        The mapping from selected asset keys to native sync names is read from
        the ``AssetsDefinition`` bound to the context. A custom
        ``DagsterFerryTranslator`` that changes asset keys still routes
        execution to the correct native sync. This method never infers a sync
        name from ``AssetKey.path[-1]``.

        Materialization metadata uses only fields directly exposed by the
        native ``SyncResult``. Ferry run UUIDs are rendered as text (they are
        not Dagster run ids). Genuine zero row counts are emitted. Metrics
        that Ferry does not expose (changed/skipped) and any invented status
        are omitted. See the README for the full key list.
        """
        # Lazy import avoids a circular import at module load time.
        from dagster_ferry._assets import key_to_sync_map

        assets_def = context.assets_def
        key_map = key_to_sync_map(assets_def)

        # Sort selected keys deterministically by path tuple. AssetKey.path is
        # a Sequence[str]; tuple() gives a stable orderable key.
        selected_keys: list[AssetKey] = sorted(
            context.selected_asset_keys, key=lambda k: tuple(k.path)
        )
        selected_sync_names: list[str] = []
        for key in selected_keys:
            sync_name = key_map.get(key)
            if sync_name is None:
                msg = (
                    f"Selected asset key {key.to_user_string()!r} is not mapped to a "
                    "Ferry sync name. This indicates the AssetsDefinition was not built "
                    "by ferry_assets or its metadata was stripped."
                )
                raise RuntimeError(msg)
            selected_sync_names.append(sync_name)

        if not selected_sync_names:
            # Empty selection: yield nothing and do not call Ferry. The
            # unreachable yield below keeps this function a generator even when
            # the selection is empty.
            return
            yield  # pragma: no cover - makes this a generator statically

        # Sort for deterministic native invocation order.
        sorted_sync_names = sorted(selected_sync_names)
        results = self.project.run(sync_names=sorted_sync_names)

        result_names = {r.sync_name for r in results}
        expected_names = set(sorted_sync_names)
        if result_names != expected_names:
            missing = sorted(expected_names - result_names)
            extra = sorted(result_names - expected_names)
            msg = (
                f"Ferry returned result names that do not exactly match the selected "
                f"sync names. missing={missing!r} extra={extra!r}. "
                "This indicates a partial or unexpected native run."
            )
            raise RuntimeError(msg)

        # The complete selected result set validated above. Index by sync name
        # so each selected key resolves to its matching result regardless of
        # the order Project.run returned them in. No materialization is yielded
        # before this point.
        results_by_name = {r.sync_name: r for r in results}

        # Yield one MaterializeResult per selected successful result, in
        # deterministic key order, with typed run metadata from SyncResult.
        for key in selected_keys:
            sync_name = key_map[key]
            result = results_by_name[sync_name]
            yield MaterializeResult(asset_key=key, metadata=_result_metadata(result))


def _result_metadata(result: ferry.SyncResult) -> dict[str, Any]:
    """Build typed Dagster metadata from a native Ferry ``SyncResult``.

    Uses only fields directly exposed by ``SyncResult``. Ferry run UUIDs are
    rendered as ``MetadataValue.text`` (they are foreign ids, not Dagster run
    ids). Genuine zero row counts are emitted as ``MetadataValue.int(0)``.
    Metrics Ferry does not expose (changed/skipped) and any invented status are
    omitted entirely rather than fabricated.
    """
    return {
        # Reuse the Dagster-conventional row-count key for delivered rows so
        # the UI renders it as a time series.
        "dagster/row_count": MetadataValue.int(result.rows_synced),
        # Ferry run id is a foreign UUID; render as text, never as a Dagster
        # run id.
        "ferry/run_id": MetadataValue.text(result.run_id),
        "ferry/rows_extracted": MetadataValue.int(result.rows_extracted),
        "ferry/rows_delivered": MetadataValue.int(result.rows_synced),
        "ferry/rows_failed": MetadataValue.int(result.rows_failed),
        "ferry/rows_pending": MetadataValue.int(result.rows_pending),
        "ferry/rows_retried": MetadataValue.int(result.rows_retried),
        # rows_dead is always exposed by SyncResult; emit it so operators can
        # distinguish dead-lettered rows from other failure counts.
        "ferry/rows_dead": MetadataValue.int(result.rows_dead),
        # Duration, mode, and dry_run are all directly exposed scalar fields.
        "ferry/duration_seconds": MetadataValue.float(result.duration_seconds),
        "ferry/mode": MetadataValue.text(result.mode),
        "ferry/dry_run": MetadataValue.bool(result.dry_run),
    }
