# Type stubs for the ferry._native module.
# Auto-generated stub file — edit with care.

from typing import List, Optional

class Project:
    def __init__(self, project_dir: str) -> None: ...
    def list_syncs(self) -> List[str]: ...
    def list_syncs_metadata(self) -> List[SyncMetadata]: ...
    def run(
        self,
        sync_names: Optional[List[str]] = None,
        dry_run: bool = False,
        full_refresh: bool = False,
        retry_dead: bool = False,
    ) -> List[SyncResult]: ...
    def validate(self) -> List[str]: ...
    def diff(self, sync_name: str) -> DiffPreview: ...
    def dlq_list(self, sync_name: Optional[str] = None) -> List[DeadRow]: ...
    def dlq_retry(self, sync_name: Optional[str] = None) -> int: ...

class SyncResult:
    sync_name: str
    run_id: str
    rows_extracted: int
    rows_synced: int
    rows_failed: int
    rows_pending: int
    rows_retried: int
    rows_dead: int
    duration_seconds: float
    dry_run: bool
    mode: str
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...

class SyncMetadata:
    # Immutable, frozen dataclass-like type. Fields are read-only.
    name: str
    description: Optional[str]
    tags: List[str]
    destination_type: str
    dbt_model: Optional[DbtModelMetadata]
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...

class DbtModelMetadata:
    # Immutable, frozen dataclass-like type. Fields are read-only.
    unique_id: str
    name: str
    alias: Optional[str]
    package_name: Optional[str]
    schema: Optional[str]
    config_schema: Optional[str]
    database: Optional[str]
    fqn: Optional[List[str]]
    config_dagster_asset_key: Optional[List[str]]
    dagster_asset_key: Optional[List[str]]
    version: Optional[str]
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...

class DiffPreview:
    sync_name: str
    added: int
    changed: int
    removed: int
    total_rows: int
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...

class DeadRow:
    primary_key: str
    status: str
    attempts: int
    last_error: Optional[str]
    last_attempt_at: Optional[str]
    sync_name: str
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...

class FerryError(Exception): ...
class ConfigError(FerryError): ...
class SourceError(FerryError): ...
class DestinationError(FerryError): ...
class CdcError(FerryError): ...
class StateError(FerryError): ...
class DeliveryError(FerryError): ...
class ValidationError(FerryError): ...
