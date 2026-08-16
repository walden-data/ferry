"""Ferry - Reverse ETL engine with Python bindings."""

__version__ = "0.1.0"

try:
    from ferry._native import (
        Project,
        SyncResult,
        SyncMetadata,
        DiffPreview,
        DeadRow,
        FerryError,
        ConfigError,
        SourceError,
        DestinationError,
        CdcError,
        StateError,
        DeliveryError,
        ValidationError,
    )
except ImportError as e:
    raise ImportError(
        "The ferry native extension is not available. "
        "Please install with: pip install ferry-core\n"
        "Or build from source: maturin develop\n"
        f"Original error: {e}"
    ) from e

__all__ = [
    "Project",
    "SyncResult",
    "SyncMetadata",
    "DiffPreview",
    "DeadRow",
    "FerryError",
    "ConfigError",
    "SourceError",
    "DestinationError",
    "CdcError",
    "StateError",
    "DeliveryError",
    "ValidationError",
]
