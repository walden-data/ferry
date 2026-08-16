"""Build, wheel contents, clean install, and published metadata tests.

The built wheel must include py.typed. The published metadata must declare
dagster and ferry-core as normal requirements with no local path markers.
A clean install of the built wheel must import from outside the checkout.
"""

from __future__ import annotations

import subprocess
import sys
import venv
from pathlib import Path

import pytest

INTEGRATION_DIR = Path(__file__).resolve().parents[1]


PY = sys.executable


def _run(
    cmd: list[str], cwd: Path, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, env=env, check=False)


def _wheel_names(wheel_path: Path) -> set[str]:
    proc = _run(
        [
            PY,
            "-c",
            "import zipfile,sys; z=zipfile.ZipFile(sys.argv[1]); print('\\n'.join(z.namelist()))",
            str(wheel_path),
        ],
        Path.cwd(),
    )
    assert proc.returncode == 0, proc.stderr
    return {name for name in proc.stdout.splitlines() if name}


def _read_wheel_metadata(wheel_path: Path) -> str:
    proc = _run(
        [
            PY,
            "-c",
            "import zipfile,sys; z=zipfile.ZipFile(sys.argv[1]); print(z.read([n for n in z.namelist() if n.endswith('METADATA')][0]).decode())",
            str(wheel_path),
        ],
        Path.cwd(),
    )
    assert proc.returncode == 0, proc.stderr
    return proc.stdout


@pytest.fixture(scope="module")
def built_wheel(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Build the integration wheel once for this test module."""
    out_dir = tmp_path_factory.mktemp("wheel-build")
    proc = _run(["uv", "build", "--out-dir", str(out_dir), "--wheel"], INTEGRATION_DIR)
    assert proc.returncode == 0, proc.stderr
    wheels = list(out_dir.glob("dagster_ferry-*.whl"))
    assert len(wheels) == 1, wheels
    return wheels[0]


@pytest.fixture(scope="module")
def built_sdist(tmp_path_factory: pytest.TempPathFactory) -> Path:
    out_dir = tmp_path_factory.mktemp("sdist-build")
    proc = _run(["uv", "build", "--out-dir", str(out_dir), "--sdist"], INTEGRATION_DIR)
    assert proc.returncode == 0, proc.stderr
    sdists = list(out_dir.glob("dagster_ferry-*.tar.gz"))
    assert len(sdists) == 1, sdists
    return sdists[0]


def test_wheel_contains_py_typed(built_wheel: Path) -> None:
    names = _wheel_names(built_wheel)
    assert any(name.endswith("dagster_ferry/py.typed") for name in names), names


def test_wheel_contains_resource_and_init(built_wheel: Path) -> None:
    names = _wheel_names(built_wheel)
    assert "dagster_ferry/__init__.py" in names
    assert "dagster_ferry/_resource.py" in names
    assert "dagster_ferry/_version.py" in names


def test_wheel_metadata_declares_dependencies(built_wheel: Path) -> None:
    metadata = _read_wheel_metadata(built_wheel)
    assert "Requires-Dist: dagster>=" in metadata
    assert "Requires-Dist: ferry-core>=" in metadata
    assert "Name: dagster-ferry" in metadata
    assert "Version: 0.1.0" in metadata


def test_wheel_metadata_has_no_local_path_markers(built_wheel: Path) -> None:
    metadata = _read_wheel_metadata(built_wheel)
    # Local editable installs (file://, path, @ ) must not leak into metadata.
    for marker in ("file://", "@ ", "../../crates", "path:", "editable"):
        assert marker not in metadata, (marker, metadata)


def test_clean_install_imports_from_outside_checkout(built_wheel: Path, tmp_path: Path) -> None:
    # Create a clean venv outside the source checkout and install only the
    # built wheel plus ferry-core (the native dep). Then import from a neutral
    # cwd to ensure the package works without the source tree on sys.path.
    venv_dir = tmp_path / "clean-venv"
    venv.create(venv_dir, with_pip=True, clear=True, symlinks=True)
    py = venv_dir / "bin" / "python"

    # Install the wheel. ferry-core is required for import, so install it from
    # the workspace wheel if a compatible one is available.
    ferry_wheels = list((INTEGRATION_DIR.parents[1] / "target" / "wheels").glob("ferry_core-*.whl"))
    # Filter to wheels compatible with this venv's interpreter tag.
    cp_tag = f"cp{sys.version_info.major}{sys.version_info.minor}"
    compatible = [w for w in ferry_wheels if cp_tag in w.name]

    install_cmd = [str(py), "-m", "pip", "install", "--no-input", str(built_wheel)]
    if compatible:
        install_cmd.append(str(compatible[0]))
    proc = _run(install_cmd, tmp_path)
    assert proc.returncode == 0, proc.stderr

    # Run from a neutral directory so the source checkout is not on sys.path.
    neutral = tmp_path / "neutral"
    neutral.mkdir()
    proc = _run(
        [
            str(py),
            "-c",
            "import dagster_ferry; print(dagster_ferry.__version__); print(','.join(dagster_ferry.__all__))",
        ],
        neutral,
    )
    assert proc.returncode == 0, (proc.stdout, proc.stderr)
    assert "0.1.0" in proc.stdout
    assert "DagsterFerryResource" in proc.stdout
