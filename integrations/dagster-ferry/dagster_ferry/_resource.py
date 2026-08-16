"""Dagster resource that wraps a native Ferry project."""

from __future__ import annotations

import os
from pathlib import Path

import ferry
from dagster import ConfigurableResource, InitResourceContext
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
