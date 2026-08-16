"""Public Dagster asset API for Ferry.

This module exposes:

* `ferry_assets`: a decorator that discovers Ferry syncs once at decoration
  time and returns one `multi_asset` with `can_subset=True`, one `AssetSpec`
  per discovered sync.
* `DagsterFerryTranslator`: a minimal, override-friendly translator that
  customizes asset key, description, group, tags, and kinds only.

The decorator body is never executed during discovery. It only runs at
materialization time, where it delegates to `DagsterFerryResource.run`.

Stable mapping contract
-----------------------

Each `AssetSpec` carries an internal metadata entry mapping its (possibly
translator-customized) `AssetKey` back to the native Ferry sync name. The
resource reads this mapping from the bound `AssetsDefinition` at runtime so a
custom translator can freely change keys without breaking execution.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from typing import Any, cast

import ferry
from dagster import (
    AssetKey,
    AssetsDefinition,
    AssetSpec,
    MaterializeResult,
    multi_asset,
)

__all__ = ["DagsterFerryTranslator", "ferry_assets"]

# Internal, stable metadata key holding the native Ferry sync name. The value
# is read by DagsterFerryResource.run to map selected asset keys back to native
# sync names. It avoids inferring from the (translator-customizable) AssetKey
# path.
_FERRY_SYNC_NAME_META = "ferry/sync_name"

# Dagster tag value character set: alphanumerics, `_`, `-`, `.` and <= 63 chars.
# Ferry tags are free-form strings, so they are joined with `.` (already in the
# allowed set) and validated eagerly. The join separator is deterministic and
# reload-stable.
_TAG_VALUE_MAX = 63


def _coerce_tag_value(parts: Sequence[str]) -> str:
    """Join Ferry tag parts into a single safe Dagster tag value.

    Raises ValueError when the result cannot be represented as a Dagster tag
    value. The failure surfaces at decoration time, not as an opaque Dagster
    error later.
    """
    joined = ".".join(p for p in parts if p)
    if not joined:
        return ""
    # Dagster allows alphanumerics, `_`, `-`, `.` and <= 63 chars.
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.")
    bad = sorted({c for c in joined if c not in allowed})
    if bad:
        msg = (
            f"Ferry tag value {joined!r} contains characters not allowed in a "
            f"Dagster tag value: {''.join(bad)!r}. Allowed characters are "
            "alphanumerics, '_', '-', and '.'."
        )
        raise ValueError(msg)
    if len(joined) > _TAG_VALUE_MAX:
        msg = (
            f"Ferry tag value {joined!r} exceeds the Dagster tag value limit of "
            f"{_TAG_VALUE_MAX} characters."
        )
        raise ValueError(msg)
    return joined


class DagsterFerryTranslator:
    """Translate Ferry sync metadata into Dagster asset properties.

    Override any of the methods below to customize asset shape. The base
    implementation provides the documented defaults.

    * key: `AssetKey(sync.name)`, unsanitized and unprefixed.
    * description: the configured description, with a deterministic fallback.
    * group: the first configured tag, or `default` when absent.
    * tags: the ordered Ferry tag list under `ferry/tags`.
    * kinds: `ferry` plus the destination type.

    Only key, description, group, tags, and kinds are customizable here. dbt
    dependencies and rich run metadata remain deferred.
    """

    def key(self, sync: ferry.SyncMetadata) -> AssetKey:
        """Return the default asset key for a sync."""
        return AssetKey(sync.name)

    def description(self, sync: ferry.SyncMetadata) -> str:
        """Return the asset description with a deterministic fallback."""
        if sync.description:
            return sync.description
        return f"Ferry sync: {sync.name}"

    def group_name(self, sync: ferry.SyncMetadata) -> str:
        """Return the asset group name. Defaults to the first tag or `default`."""
        if sync.tags:
            return sync.tags[0]
        return "default"

    def tags(self, sync: ferry.SyncMetadata) -> dict[str, str]:
        """Return Dagster tags for the sync.

        The ordered Ferry tag list is preserved under `ferry/tags` as a
        reload-stable, Dagster-safe value.
        """
        if not sync.tags:
            return {}
        return {"ferry/tags": _coerce_tag_value(sync.tags)}

    def kinds(self, sync: ferry.SyncMetadata) -> set[str]:
        """Return Dagster kinds for the sync: `ferry` plus the destination type."""
        return {"ferry", sync.destination_type}


def _build_spec(translator: DagsterFerryTranslator, sync: ferry.SyncMetadata) -> AssetSpec:
    """Build a single AssetSpec from a sync and translator, attaching the
    stable internal sync-name mapping metadata.

    Translator return values are checked at runtime. A subclass that returns
    an unexpected type fails at decoration time with a contextual error, not
    an opaque Dagster error later.
    """
    # Cast through Any so pyright does not narrow these to their declared
    # types and the runtime isinstance checks remain meaningful.
    key: Any = translator.key(sync)
    if not isinstance(key, AssetKey):
        msg = (
            f"DagsterFerryTranslator.key must return an AssetKey, got "
            f"{type(key).__name__} for sync {sync.name!r}."
        )
        raise TypeError(msg)

    description: Any = translator.description(sync)
    if not isinstance(description, str):
        msg = (
            f"DagsterFerryTranslator.description must return a str, got "
            f"{type(description).__name__} for sync {sync.name!r}."
        )
        raise TypeError(msg)

    group_name: Any = translator.group_name(sync)
    if not isinstance(group_name, str):
        msg = (
            f"DagsterFerryTranslator.group_name must return a str, got "
            f"{type(group_name).__name__} for sync {sync.name!r}."
        )
        raise TypeError(msg)

    tags: Any = translator.tags(sync)
    if not isinstance(tags, dict):
        msg = (
            f"DagsterFerryTranslator.tags must return a dict, got "
            f"{type(tags).__name__} for sync {sync.name!r}."
        )
        raise TypeError(msg)

    kinds: Any = translator.kinds(sync)
    if not isinstance(kinds, (set, frozenset)):
        msg = (
            f"DagsterFerryTranslator.kinds must return a set, got "
            f"{type(kinds).__name__} for sync {sync.name!r}."
        )
        raise TypeError(msg)

    # Cast the validated Any values to their concrete types so pyright accepts
    # them as AssetSpec constructor arguments.
    tags_typed = cast(dict[str, str], tags)
    kinds_typed = cast(set[str], kinds)
    return AssetSpec(
        key=key,
        description=description,
        group_name=group_name,
        tags=tags_typed,
        kinds=kinds_typed,
        metadata={_FERRY_SYNC_NAME_META: sync.name},
    )


def _discover_syncs(project_dir: str) -> list[ferry.SyncMetadata]:
    """Discover and return sorted sync metadata from a Ferry project directory.

    Native Ferry configuration errors propagate unchanged. This helper does
    not broad-wrap exceptions.
    """
    project = ferry.Project(project_dir)
    return list(project.list_syncs_metadata())


def ferry_assets(
    *,
    project_dir: str,
    translator: DagsterFerryTranslator | None = None,
) -> Callable[[Callable[..., Iterator[MaterializeResult[Any]]]], AssetsDefinition]:
    """Decorator that turns a decorated function into a multi-asset over all
    Ferry syncs discovered in `project_dir`.

    Discovery runs once at decoration time. The decorated function name
    becomes the Dagster definition/op name. The decorated body is not executed
    during discovery; it runs only at materialization time, where it typically
    delegates to `DagsterFerryResource.run(context)`.

    The decorated function must declare a context parameter and a parameter
    annotated as `DagsterFerryResource` (Dagster infers the resource key from
    the parameter name). For example::

        @ferry_assets(project_dir="path/to/ferry/project")
        def customer_syncs(context, ferry: DagsterFerryResource):
            yield from ferry.run(context)

    Args:
        project_dir: Path to the Ferry project directory containing
            `ferry.yml` and a `syncs/` directory.
        translator: Optional `DagsterFerryTranslator` subclass instance. When
            omitted, a default translator is used.

    Returns:
        A decorator that returns a single `AssetsDefinition` (one
        `multi_asset` with `can_subset=True`).

    Raises:
        ValueError: When no syncs are discovered or when two syncs translate
            to the same asset key.
        TypeError: When a translator method returns an unexpected type.
        ferry.FerryError: When native Ferry configuration loading fails.
        DagsterInvalidDefinitionError: When a translated group name is not a
            valid Dagster group name.
    """
    trans = translator or DagsterFerryTranslator()
    syncs = _discover_syncs(project_dir)
    if not syncs:
        msg = (
            f"No Ferry syncs discovered in {project_dir!r}. Define at least one "
            "sync YAML file under syncs/ before decorating with ferry_assets."
        )
        raise ValueError(msg)

    specs: list[AssetSpec] = []
    seen_keys: dict[AssetKey, str] = {}
    for sync in syncs:
        spec = _build_spec(trans, sync)
        existing = seen_keys.get(spec.key)
        if existing is not None:
            msg = (
                f"Two Ferry syncs translated to the same asset key "
                f"{spec.key.to_user_string()!r}: {existing!r} and {sync.name!r}. "
                "Override DagsterFerryTranslator.key to disambiguate."
            )
            raise ValueError(msg)
        seen_keys[spec.key] = sync.name
        specs.append(spec)

    def decorator(
        fn: Callable[..., Iterator[MaterializeResult[Any]]],
    ) -> AssetsDefinition:
        # The decorated function name is the Dagster definition/op name.
        # Resource requirements are inferred by Dagster from the function's
        # annotated parameters (e.g. `ferry: DagsterFerryResource`), so
        # `required_resource_keys` is intentionally not passed here.
        return multi_asset(
            specs=specs,
            can_subset=True,
            name=fn.__name__,
        )(fn)

    return decorator


def sync_name_for_key(assets_def: AssetsDefinition, key: AssetKey) -> str | None:
    """Return the native Ferry sync name attached to an asset key, or None.

    Reads the stable internal metadata stored on each AssetSpec at decoration
    time. Used by DagsterFerryResource.run to map selected keys back to native
    sync names deterministically.
    """
    for spec in assets_def.specs:
        if spec.key == key:
            value = spec.metadata.get(_FERRY_SYNC_NAME_META)
            if isinstance(value, str):
                return value
            return None
    return None


def key_to_sync_map(assets_def: AssetsDefinition) -> dict[AssetKey, str]:
    """Return a deterministic mapping of asset key to native sync name."""
    mapping: dict[AssetKey, str] = {}
    for spec in assets_def.specs:
        value = spec.metadata.get(_FERRY_SYNC_NAME_META)
        if isinstance(value, str):
            mapping[spec.key] = value
    return mapping
