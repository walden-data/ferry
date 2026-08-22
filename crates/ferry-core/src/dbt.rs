use std::collections::HashMap;
use std::path::Path;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde::Deserializer;
use tracing::warn;

use crate::error::FerryError;

// ---------------------------------------------------------------------------
// Manifest structs
// ---------------------------------------------------------------------------

/// A parsed dbt `manifest.json` artifact.
///
/// Only the fields Ferry needs are deserialized. Unknown fields are captured
/// via `#[serde(flatten)]` for forward compatibility with newer dbt versions.
#[derive(Debug, Deserialize)]
pub struct Manifest {
    pub metadata: ManifestMetadata,
    pub nodes: HashMap<String, ManifestNode>,
    #[serde(default)]
    pub sources: HashMap<String, serde_json::Value>,
    #[serde(flatten)]
    _extra: serde_json::Value,
}

/// Metadata about the dbt manifest artifact.
#[derive(Debug, Deserialize)]
pub struct ManifestMetadata {
    pub dbt_schema_version: String,
    pub generated_at: String,
    #[serde(default)]
    pub dbt_version: Option<String>,
    #[serde(flatten)]
    _extra: serde_json::Value,
}

/// A single node in the dbt manifest (model, seed, snapshot, etc.).
///
/// Fields beyond `compiled_code`/`raw_code` are deserialized additively so
/// Ferry can derive deterministic, typed model identity for Dagster asset-key
/// translation without re-executing dbt or importing `dagster-dbt`. Unknown
/// fields still flatten into `_extra` for forward compatibility.
#[derive(Debug, Deserialize)]
pub struct ManifestNode {
    pub unique_id: String,
    pub name: String,
    pub resource_type: String,
    #[serde(default)]
    pub compiled_code: Option<String>,
    #[serde(default)]
    pub raw_code: Option<String>,
    #[serde(default)]
    pub relation_name: Option<String>,
    #[serde(default)]
    pub config: Option<NodeConfig>,
    // Identity fields used for typed model metadata. All optional because dbt
    // manifests may omit them (e.g. minimal/test fixtures) and Ferry must keep
    // SQL-only projects unchanged.
    #[serde(default)]
    pub alias: Option<String>,
    #[serde(default)]
    pub package_name: Option<String>,
    #[serde(default)]
    pub schema: Option<String>,
    #[serde(default)]
    pub database: Option<String>,
    #[serde(default)]
    pub fqn: Option<Vec<String>>,
    #[serde(default)]
    pub meta: Option<NodeMeta>,
    /// Model version (dbt >= 1.5). Used for dagster-dbt-compatible asset-key
    /// mapping: versioned models resolve to `[alias]`, not `[schema, name]`.
    /// Accepts string, integer, or float per dbt's `Union[str, float]`.
    #[serde(default)]
    pub version: Option<NodeVersion>,
    #[serde(flatten)]
    _extra: serde_json::Value,
}

/// Configuration for a manifest node (materialization strategy, etc.).
///
/// `schema` and `meta` are deserialized so Ferry can match dagster-dbt's
/// `default_asset_key_fn` precedence. `config.meta` is checked before
/// top-level `meta`, and `config.schema` (the configured schema) is preferred
/// over the resolved top-level `schema` for the default key.
#[derive(Debug, Deserialize)]
pub struct NodeConfig {
    #[serde(default)]
    pub materialized: Option<String>,
    /// The configured schema (dbt `config.schema`). Differs from the
    /// resolved top-level `schema` when a custom `generate_schema_name`
    /// macro is in use.
    #[serde(default)]
    pub schema: Option<String>,
    /// Config-level meta mapping. dagster-dbt reads `config.meta` before
    /// top-level `meta` for `dagster.asset_key`.
    #[serde(default)]
    pub meta: Option<NodeMeta>,
    #[serde(flatten)]
    _extra: serde_json::Value,
}

/// Free-form `meta` mapping on a dbt node. Ferry only reads the
/// `dagster.asset_key` entry, if present, as a list of strings. Other entries
/// are ignored via `#[serde(flatten)]`.
#[derive(Debug, Default, Deserialize)]
pub struct NodeMeta {
    #[serde(default)]
    pub dagster: Option<DagsterMeta>,
    #[serde(flatten)]
    _extra: serde_json::Value,
}

/// dbt `meta.dagster` mapping. Only `asset_key` (a list of strings) is read.
#[derive(Debug, Default, Deserialize)]
pub struct DagsterMeta {
    #[serde(default)]
    pub asset_key: Option<Vec<String>>,
    #[serde(flatten)]
    _extra: serde_json::Value,
}

/// A dbt model version, accepting the `Union[str, float]` that dbt-core
/// serializes into manifests. dbt docs state the version identifier "can be
/// numeric (integer or float), or any string". Before the typed `version`
/// field was added, numeric versions flowed into `_extra` and parsed fine.
/// This type preserves that compatibility by accepting:
///
/// * JSON string → used as-is.
/// * JSON integer → stringified without a trailing `.0` (e.g. `2` → `"2"`).
/// * JSON float → stringified as-is (e.g. `2.5` → `"2.5"`).
///
/// Booleans, null, arrays, and objects are rejected with a clear error so a
/// malformed manifest does not silently produce a wrong key. The normalized
/// `String` is the human representation used only for Dagster key behavior
/// (versioned models resolve to `[alias]`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeVersion(String);

impl NodeVersion {
    /// Return the normalized string representation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for NodeVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for NodeVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        let normalized = match &value {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    i.to_string()
                } else if let Some(u) = n.as_u64() {
                    u.to_string()
                } else if let Some(f) = n.as_f64() {
                    // Keep floats as-is (e.g. `2.5`). f64::to_string preserves
                    // the decimal without trailing-zero inflation or the
                    // `2.0` → `"2"` collapse, matching dbt's `version_to_str`.
                    f.to_string()
                } else {
                    return Err(serde::de::Error::custom(format!(
                        "dbt model version must be a string or number, got: {value}"
                    )));
                }
            }
            _ => {
                return Err(serde::de::Error::custom(format!(
                    "dbt model version must be a string or number, got: {value}"
                )));
            }
        };
        Ok(NodeVersion(normalized))
    }
}

/// Immutable, deterministic identity metadata for a dbt model.
///
/// Built from a manifest node for a resolved `model.ref` sync. Exposed to
/// Python as a frozen pyclass so Dagster translators can derive an upstream
/// dbt `AssetKey` without importing `dagster-dbt` or re-parsing JSON. Fields
/// are optional because dbt manifests may omit them; consumers handle `None`
/// by falling back to the documented schema/name-compatible default.
///
/// The asset-key fields mirror dagster-dbt's `default_asset_key_fn`
/// precedence. `config.meta.dagster.asset_key` is checked before top-level
/// `meta.dagster.asset_key`. `config.schema` (the configured schema) is
/// preferred over the resolved top-level `schema` for the default key.
/// Versioned models resolve to `[alias]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbtModelMetadata {
    pub unique_id: String,
    pub name: String,
    pub alias: Option<String>,
    pub package_name: Option<String>,
    /// The resolved schema (top-level `node.schema`). May differ from
    /// `config_schema` when a custom `generate_schema_name` macro is in use.
    pub schema: Option<String>,
    /// The configured schema (`config.schema`). Preferred over `schema` for
    /// the default asset-key mapping, matching dagster-dbt.
    pub config_schema: Option<String>,
    pub database: Option<String>,
    pub fqn: Option<Vec<String>>,
    /// Asset key from `config.meta.dagster.asset_key`. Checked first.
    pub config_dagster_asset_key: Option<Vec<String>>,
    /// Asset key from top-level `meta.dagster.asset_key`. Checked second.
    pub dagster_asset_key: Option<Vec<String>>,
    /// Model version (dbt >= 1.5). Versioned models use `[alias]` as the key.
    pub version: Option<String>,
}

// ---------------------------------------------------------------------------
// Manifest methods
// ---------------------------------------------------------------------------

impl Manifest {
    /// Load and parse a dbt `manifest.json` from the given path.
    ///
    /// Returns a `FerryError::Config` if the file cannot be opened or parsed.
    pub fn load(path: &Path) -> Result<Self, FerryError> {
        let file = std::fs::File::open(path).map_err(|e| {
            FerryError::Config(format!(
                "Cannot open dbt manifest at {}: {e}",
                path.display()
            ))
        })?;

        let manifest: Manifest = serde_json::from_reader(file).map_err(|e| {
            FerryError::Config(format!(
                "Cannot parse dbt manifest at {}: {e}",
                path.display()
            ))
        })?;

        Ok(manifest)
    }

    /// Resolve a dbt model name to its compiled SQL.
    ///
    /// Looks up the model by `name` in the manifest's nodes. Returns the
    /// `compiled_code` if available, falling back to `raw_code`.
    ///
    /// # Errors
    ///
    /// - Returns an error if the model is ephemeral (cannot be used as a source).
    /// - Returns an error if the model is not found (includes available model names).
    /// - Returns an error if the model has neither `compiled_code` nor `raw_code`.
    pub fn resolve_ref(&self, model_name: &str) -> Result<String, FerryError> {
        // Find the node by name where resource_type == "model"
        let node = self
            .nodes
            .values()
            .find(|n| n.name == model_name && n.resource_type == "model")
            .ok_or_else(|| {
                let available = self.list_models();
                if available.is_empty() {
                    FerryError::Config(format!(
                        "Model '{model_name}' not found in dbt manifest (no models available)"
                    ))
                } else {
                    FerryError::Config(format!(
                        "Model '{model_name}' not found in dbt manifest. Available models: {}",
                        available.join(", ")
                    ))
                }
            })?;

        // Reject ephemeral models
        let is_ephemeral =
            node.config.as_ref().and_then(|c| c.materialized.as_deref()) == Some("ephemeral");

        if is_ephemeral {
            return Err(FerryError::Config(format!(
                "Model '{model_name}' is ephemeral — ephemeral models cannot be used as Ferry sources. \
                 Change the materialization to 'table' or 'view' in your dbt project."
            )));
        }

        // Prefer compiled_code, fall back to raw_code
        node.compiled_code
            .clone()
            .or_else(|| node.raw_code.clone())
            .ok_or_else(|| {
                FerryError::Config(format!(
                    "Model '{model_name}' has no compiled SQL — run 'dbt compile' to regenerate the manifest"
                ))
            })
    }

    /// Resolve deterministic, typed identity metadata for a dbt model name.
    ///
    /// This is a separate resolver from `resolve_ref`: it never reads
    /// `compiled_code`/`raw_code` and never produces SQL, so the compiled-SQL
    /// execution semantics are preserved. It only resolves identity fields
    /// (`unique_id`, `name`, `alias`, `package_name`, `schema`, `database`,
    /// `fqn`, `meta.dagster.asset_key`) for Dagster asset-key translation.
    ///
    /// Ambiguity policy: dbt manifests can contain multiple models with the
    /// same `name` across packages. Unlike `resolve_ref`, which selects the
    /// first match nondeterministically, this resolver rejects ambiguity and
    /// lists candidate `unique_id`s so operators can disambiguate by renaming
    /// or by configuring a single package.
    ///
    /// # Errors
    ///
    /// - Returns `FerryError::Config` if the model name matches zero or
    ///   multiple non-ephemeral model nodes, if the matched node is not a
    ///   model, or if the matched node is ephemeral. Each error message is
    ///   actionable and includes candidates or available model names.
    pub fn resolve_model_metadata(&self, model_name: &str) -> Result<DbtModelMetadata, FerryError> {
        // Collect every model node matching the requested name. Non-model
        // nodes (seeds, snapshots, tests) are excluded up front so a ref to a
        // non-model resource fails with a clear, contextual error.
        let matches: Vec<&ManifestNode> = self
            .nodes
            .values()
            .filter(|n| n.name == model_name && n.resource_type == "model")
            .collect();

        if matches.is_empty() {
            // Distinguish "name exists but is not a model" from "name absent".
            let non_model_kinds: Vec<&str> = self
                .nodes
                .values()
                .filter(|n| n.name == model_name && n.resource_type != "model")
                .map(|n| n.resource_type.as_str())
                .collect();
            let available = self.list_models();
            if !non_model_kinds.is_empty() {
                let kinds = non_model_kinds.join(", ");
                return Err(FerryError::Config(format!(
                    "dbt ref '{model_name}' resolves to a non-model node \
                     (resource_type: {kinds}). Ferry can only depend on dbt models. \
                     Available models: {}",
                    if available.is_empty() {
                        "(none)".to_string()
                    } else {
                        available.join(", ")
                    }
                )));
            }
            return Err(FerryError::Config(if available.is_empty() {
                format!("Model '{model_name}' not found in dbt manifest (no models available)")
            } else {
                format!(
                    "Model '{model_name}' not found in dbt manifest. Available models: {}",
                    available.join(", ")
                )
            }));
        }

        // Reject ephemeral matches before ambiguity: an ephemeral model is
        // never a valid Ferry source regardless of how many matches exist.
        let ephemeral_matches: Vec<&&ManifestNode> = matches
            .iter()
            .filter(|n| {
                n.config.as_ref().and_then(|c| c.materialized.as_deref()) == Some("ephemeral")
            })
            .collect();
        if !ephemeral_matches.is_empty() && ephemeral_matches.len() == matches.len() {
            // All matches are ephemeral: same contextual error as resolve_ref.
            return Err(FerryError::Config(format!(
                "Model '{model_name}' is ephemeral: ephemeral models cannot be used as Ferry sources. \
                 Change the materialization to 'table' or 'view' in your dbt project."
            )));
        }
        // If some-but-not-all matches are ephemeral, drop the ephemeral ones
        // before the ambiguity check so a single non-ephemeral model still
        // resolves. This matches dbt's own precedence (ephemeral models are
        // inlined, not materialized).
        let non_ephemeral: Vec<&ManifestNode> = matches
            .iter()
            .copied()
            .filter(|n| {
                n.config.as_ref().and_then(|c| c.materialized.as_deref()) != Some("ephemeral")
            })
            .collect();

        if non_ephemeral.len() > 1 {
            // Ambiguous: list candidate unique_ids so operators can
            // disambiguate deterministically rather than relying on HashMap
            // iteration order.
            let mut candidates: Vec<String> =
                non_ephemeral.iter().map(|n| n.unique_id.clone()).collect();
            candidates.sort();
            return Err(FerryError::Config(format!(
                "dbt model name '{model_name}' is ambiguous: matched {} models \
                 ({candidates:?}). Disambiguate by renaming the model in your dbt \
                 project or by configuring a single package manifest.",
                non_ephemeral.len()
            )));
        }

        let node = non_ephemeral[0];
        Ok(DbtModelMetadata {
            unique_id: node.unique_id.clone(),
            name: node.name.clone(),
            alias: node.alias.clone(),
            package_name: node.package_name.clone(),
            schema: node.schema.clone(),
            config_schema: node.config.as_ref().and_then(|c| c.schema.clone()),
            database: node.database.clone(),
            fqn: node.fqn.clone(),
            // dagster-dbt reads config.meta before top-level meta.
            config_dagster_asset_key: node
                .config
                .as_ref()
                .and_then(|c| c.meta.as_ref())
                .and_then(|m| m.dagster.as_ref())
                .and_then(|d| d.asset_key.clone()),
            dagster_asset_key: node
                .meta
                .as_ref()
                .and_then(|m| m.dagster.as_ref())
                .and_then(|d| d.asset_key.clone()),
            version: node.version.as_ref().map(|v| v.to_string()),
        })
    }

    /// Check if the manifest is stale (older than `max_age_hours`).
    ///
    /// This logs a warning but never returns an error — staleness is advisory.
    pub fn check_freshness(&self, max_age_hours: i64) -> Result<(), FerryError> {
        let generated_at =
            DateTime::parse_from_rfc3339(&self.metadata.generated_at).map_err(|e| {
                FerryError::Config(format!(
                    "Cannot parse manifest generated_at '{}': {e}",
                    self.metadata.generated_at
                ))
            })?;

        let age = Utc::now() - generated_at.with_timezone(&Utc);
        let age_hours = age.num_hours();

        if age_hours > max_age_hours {
            warn!(
                age_hours = age_hours,
                max_age_hours = max_age_hours,
                generated_at = %self.metadata.generated_at,
                "dbt manifest is {} hours old (max {} hours) — consider re-running 'dbt compile'",
                age_hours,
                max_age_hours,
            );
        }

        Ok(())
    }

    /// List all non-ephemeral model names in the manifest.
    ///
    /// This is useful for error messages and validation.
    pub fn list_models(&self) -> Vec<String> {
        let mut models: Vec<String> = self
            .nodes
            .values()
            .filter(|n| {
                n.resource_type == "model"
                    && n.config.as_ref().and_then(|c| c.materialized.as_deref())
                        != Some("ephemeral")
            })
            .map(|n| n.name.clone())
            .collect();

        models.sort();
        models
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Path to the sample manifest fixture.
    fn fixture_path(name: &str) -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join(name)
    }

    #[test]
    fn test_load_valid_manifest() {
        let path = fixture_path("sample_manifest.json");
        let manifest = Manifest::load(&path).expect("Should load valid manifest");
        assert_eq!(
            manifest.metadata.dbt_schema_version,
            "https://schemas.getdbt.com/dbt/manifest/v7.json"
        );
        assert_eq!(manifest.metadata.dbt_version, Some("1.7.0".to_string()));
        assert_eq!(manifest.nodes.len(), 3);
    }

    #[test]
    fn test_resolve_ref_table() {
        let path = fixture_path("sample_manifest.json");
        let manifest = Manifest::load(&path).expect("Should load valid manifest");
        let sql = manifest
            .resolve_ref("fct_users")
            .expect("Should resolve fct_users");
        assert_eq!(sql, "SELECT id, email, name FROM analytics.fct_users");
    }

    #[test]
    fn test_resolve_ref_view() {
        let path = fixture_path("sample_manifest.json");
        let manifest = Manifest::load(&path).expect("Should load valid manifest");
        let sql = manifest
            .resolve_ref("fct_orders")
            .expect("Should resolve fct_orders");
        assert_eq!(
            sql,
            "SELECT order_id, user_id, total FROM analytics.fct_orders WHERE status = 'completed'"
        );
    }

    #[test]
    fn test_resolve_ref_ephemeral_rejected() {
        let path = fixture_path("sample_manifest.json");
        let manifest = Manifest::load(&path).expect("Should load valid manifest");
        let result = manifest.resolve_ref("fct_ephemeral");
        assert!(result.is_err(), "Ephemeral models should be rejected");
        let err = result.unwrap_err();
        match &err {
            FerryError::Config(msg) => {
                assert!(
                    msg.contains("ephemeral"),
                    "Error should mention ephemeral: {msg}"
                );
            }
            other => panic!("Expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn test_resolve_ref_not_found() {
        let path = fixture_path("sample_manifest.json");
        let manifest = Manifest::load(&path).expect("Should load valid manifest");
        let result = manifest.resolve_ref("nonexistent");
        assert!(result.is_err(), "Nonexistent model should error");
        let err = result.unwrap_err();
        match &err {
            FerryError::Config(msg) => {
                assert!(
                    msg.contains("not found"),
                    "Error should mention 'not found': {msg}"
                );
                assert!(
                    msg.contains("fct_users"),
                    "Error should list available models: {msg}"
                );
                assert!(
                    msg.contains("fct_orders"),
                    "Error should list available models: {msg}"
                );
                // fct_ephemeral should NOT be in the list (it's ephemeral)
                assert!(
                    !msg.contains("fct_ephemeral"),
                    "Error should NOT list ephemeral models: {msg}"
                );
            }
            other => panic!("Expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn test_resolve_ref_no_compiled_code() {
        // Create a manifest with a node that has raw_code but no compiled_code
        let json = serde_json::json!({
            "metadata": {
                "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v7.json",
                "generated_at": "2026-06-21T10:00:00.000Z"
            },
            "nodes": {
                "model.test.fct_raw": {
                    "unique_id": "model.test.fct_raw",
                    "name": "fct_raw",
                    "resource_type": "model",
                    "raw_code": "SELECT * FROM raw_table",
                    "config": { "materialized": "table" }
                }
            },
            "sources": {}
        });

        let manifest: Manifest = serde_json::from_value(json).expect("Should deserialize manifest");
        let sql = manifest
            .resolve_ref("fct_raw")
            .expect("Should fall back to raw_code");
        assert_eq!(sql, "SELECT * FROM raw_table");
    }

    #[test]
    fn test_resolve_ref_no_code_at_all() {
        // Create a manifest with a node that has neither compiled_code nor raw_code
        let json = serde_json::json!({
            "metadata": {
                "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v7.json",
                "generated_at": "2026-06-21T10:00:00.000Z"
            },
            "nodes": {
                "model.test.fct_empty": {
                    "unique_id": "model.test.fct_empty",
                    "name": "fct_empty",
                    "resource_type": "model",
                    "config": { "materialized": "table" }
                }
            },
            "sources": {}
        });

        let manifest: Manifest = serde_json::from_value(json).expect("Should deserialize manifest");
        let result = manifest.resolve_ref("fct_empty");
        assert!(result.is_err(), "Model with no code should error");
        let err = result.unwrap_err();
        match &err {
            FerryError::Config(msg) => {
                assert!(
                    msg.contains("no compiled SQL"),
                    "Error should mention 'no compiled SQL': {msg}"
                );
            }
            other => panic!("Expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn test_check_freshness_fresh() {
        // Use the sample manifest which has generated_at = 2026-06-21T10:00:00.000Z
        // If we're running tests on 2026-06-21, this should be fresh
        let path = fixture_path("sample_manifest.json");
        let manifest = Manifest::load(&path).expect("Should load valid manifest");
        // Allow up to 48 hours — the fixture is from today
        let result = manifest.check_freshness(48);
        assert!(
            result.is_ok(),
            "Fresh manifest should not error: {:?}",
            result
        );
    }

    #[test]
    fn test_check_freshness_stale() {
        // Use the stale manifest which has generated_at = 2026-06-19T10:00:00.000Z
        // That's ~48 hours before 2026-06-21, so it should be stale with max_age_hours=24
        let path = fixture_path("stale_manifest.json");
        let manifest = Manifest::load(&path).expect("Should load valid manifest");
        // check_freshness should never error — it only warns
        let result = manifest.check_freshness(24);
        assert!(
            result.is_ok(),
            "Stale manifest should warn but not error: {:?}",
            result
        );
    }

    #[test]
    fn test_list_models() {
        let path = fixture_path("sample_manifest.json");
        let manifest = Manifest::load(&path).expect("Should load valid manifest");
        let models = manifest.list_models();
        assert_eq!(models.len(), 2, "Should have 2 non-ephemeral models");
        assert!(models.contains(&"fct_users".to_string()));
        assert!(models.contains(&"fct_orders".to_string()));
        assert!(
            !models.contains(&"fct_ephemeral".to_string()),
            "Ephemeral models should not be listed"
        );
    }

    #[test]
    fn test_malformed_json() {
        let dir = tempfile::tempdir().expect("Failed to create temp dir");
        let path = dir.path().join("bad_manifest.json");
        let mut file = std::fs::File::create(&path).expect("Failed to create file");
        write!(file, "this is not valid json").expect("Failed to write");

        let result = Manifest::load(&path);
        assert!(result.is_err(), "Malformed JSON should error");
        match result.unwrap_err() {
            FerryError::Config(msg) => {
                assert!(
                    msg.contains("Cannot parse"),
                    "Error should mention parsing: {msg}"
                );
            }
            other => panic!("Expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn test_file_not_found() {
        let path = std::path::PathBuf::from("/nonexistent/manifest.json");
        let result = Manifest::load(&path);
        assert!(result.is_err(), "Missing file should error");
        match result.unwrap_err() {
            FerryError::Config(msg) => {
                assert!(
                    msg.contains("Cannot open"),
                    "Error should mention 'Cannot open': {msg}"
                );
                assert!(
                    msg.contains("nonexistent"),
                    "Error should include the path: {msg}"
                );
            }
            other => panic!("Expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn test_forward_compatibility() {
        // Manifest with extra fields that we don't know about should still parse
        let json = serde_json::json!({
            "metadata": {
                "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
                "generated_at": "2026-06-21T10:00:00.000Z",
                "dbt_version": "1.8.0",
                "new_metadata_field": "some_value"
            },
            "nodes": {
                "model.test.fct_users": {
                    "unique_id": "model.test.fct_users",
                    "name": "fct_users",
                    "resource_type": "model",
                    "compiled_code": "SELECT * FROM users",
                    "config": { "materialized": "table", "new_config_field": true },
                    "new_node_field": "ignored"
                }
            },
            "sources": {},
            "metrics": {},
            "exposures": {},
            "new_top_level_field": "should_be_ignored"
        });

        let manifest: Manifest =
            serde_json::from_value(json).expect("Should parse manifest with unknown fields");
        let sql = manifest
            .resolve_ref("fct_users")
            .expect("Should resolve model");
        assert_eq!(sql, "SELECT * FROM users");
    }

    // -----------------------------------------------------------------------
    // resolve_model_metadata
    // -----------------------------------------------------------------------

    #[test]
    fn test_resolve_model_metadata_basic_fields() {
        let path = fixture_path("sample_manifest.json");
        let manifest = Manifest::load(&path).expect("Should load valid manifest");
        let meta = manifest
            .resolve_model_metadata("fct_users")
            .expect("Should resolve fct_users metadata");
        assert_eq!(meta.unique_id, "model.test.fct_users");
        assert_eq!(meta.name, "fct_users");
        assert_eq!(meta.alias.as_deref(), Some("fct_users"));
        assert_eq!(meta.package_name.as_deref(), Some("test"));
        assert_eq!(meta.schema.as_deref(), Some("analytics"));
        assert_eq!(meta.database.as_deref(), Some("warehouse"));
        assert_eq!(
            meta.fqn.as_deref(),
            Some(
                &[
                    "test".to_string(),
                    "models".to_string(),
                    "analytics".to_string(),
                    "fct_users.sql".to_string()
                ][..]
            )
        );
        assert_eq!(meta.dagster_asset_key, None);
        assert_eq!(meta.config_dagster_asset_key, None);
        // The fixture now includes config.schema matching the resolved schema.
        assert_eq!(meta.config_schema.as_deref(), Some("analytics"));
        assert_eq!(meta.version, None);
    }

    #[test]
    fn test_resolve_model_metadata_reads_dagster_asset_key() {
        let path = fixture_path("sample_manifest.json");
        let manifest = Manifest::load(&path).expect("Should load valid manifest");
        let meta = manifest
            .resolve_model_metadata("fct_orders")
            .expect("Should resolve fct_orders metadata");
        // fct_orders has the asset key at top-level meta, not config.meta.
        assert_eq!(
            meta.dagster_asset_key,
            Some(vec!["dbt".to_string(), "fct_orders".to_string()])
        );
        assert_eq!(meta.config_dagster_asset_key, None);
        // database is unset on fct_orders in the fixture, must stay None.
        assert_eq!(meta.database, None);
    }

    #[test]
    fn test_resolve_model_metadata_ephemeral_rejected() {
        let path = fixture_path("sample_manifest.json");
        let manifest = Manifest::load(&path).expect("Should load valid manifest");
        let result = manifest.resolve_model_metadata("fct_ephemeral");
        assert!(result.is_err());
        match result.unwrap_err() {
            FerryError::Config(msg) => assert!(msg.contains("ephemeral"), "{msg}"),
            other => panic!("Expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn test_resolve_model_metadata_not_found_lists_available() {
        let path = fixture_path("sample_manifest.json");
        let manifest = Manifest::load(&path).expect("Should load valid manifest");
        let result = manifest.resolve_model_metadata("does_not_exist");
        assert!(result.is_err());
        match result.unwrap_err() {
            FerryError::Config(msg) => {
                assert!(msg.contains("not found"), "{msg}");
                assert!(msg.contains("fct_users"), "{msg}");
                // Ephemeral models are excluded from the available list.
                assert!(!msg.contains("fct_ephemeral"), "{msg}");
            }
            other => panic!("Expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn test_resolve_model_metadata_non_model_ref_errors_contextually() {
        let json = serde_json::json!({
            "metadata": {
                "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v7.json",
                "generated_at": "2026-06-21T10:00:00.000Z"
            },
            "nodes": {
                "seed.test.raw_customers": {
                    "unique_id": "seed.test.raw_customers",
                    "name": "raw_customers",
                    "resource_type": "seed",
                    "config": { "materialized": "seed" }
                }
            },
            "sources": {}
        });
        let manifest: Manifest = serde_json::from_value(json).expect("Should deserialize");
        let result = manifest.resolve_model_metadata("raw_customers");
        assert!(result.is_err());
        match result.unwrap_err() {
            FerryError::Config(msg) => {
                assert!(msg.contains("non-model"), "{msg}");
                assert!(msg.contains("seed"), "{msg}");
            }
            other => panic!("Expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn test_resolve_model_metadata_ambiguous_lists_candidates() {
        let json = serde_json::json!({
            "metadata": {
                "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v7.json",
                "generated_at": "2026-06-21T10:00:00.000Z"
            },
            "nodes": {
                "model.a.fct_users": {
                    "unique_id": "model.a.fct_users",
                    "name": "fct_users",
                    "resource_type": "model",
                    "package_name": "a",
                    "config": { "materialized": "table" }
                },
                "model.b.fct_users": {
                    "unique_id": "model.b.fct_users",
                    "name": "fct_users",
                    "resource_type": "model",
                    "package_name": "b",
                    "config": { "materialized": "table" }
                }
            },
            "sources": {}
        });
        let manifest: Manifest = serde_json::from_value(json).expect("Should deserialize");
        let result = manifest.resolve_model_metadata("fct_users");
        assert!(result.is_err());
        match result.unwrap_err() {
            FerryError::Config(msg) => {
                assert!(msg.contains("ambiguous"), "{msg}");
                assert!(msg.contains("model.a.fct_users"), "{msg}");
                assert!(msg.contains("model.b.fct_users"), "{msg}");
            }
            other => panic!("Expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn test_resolve_model_metadata_single_non_ephemeral_among_ephemeral_resolves() {
        let json = serde_json::json!({
            "metadata": {
                "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v7.json",
                "generated_at": "2026-06-21T10:00:00.000Z"
            },
            "nodes": {
                "model.a.fct_users": {
                    "unique_id": "model.a.fct_users",
                    "name": "fct_users",
                    "resource_type": "model",
                    "package_name": "a",
                    "config": { "materialized": "ephemeral" }
                },
                "model.b.fct_users": {
                    "unique_id": "model.b.fct_users",
                    "name": "fct_users",
                    "resource_type": "model",
                    "package_name": "b",
                    "config": { "materialized": "table" }
                }
            },
            "sources": {}
        });
        let manifest: Manifest = serde_json::from_value(json).expect("Should deserialize");
        let meta = manifest
            .resolve_model_metadata("fct_users")
            .expect("Should resolve the single non-ephemeral match");
        assert_eq!(meta.unique_id, "model.b.fct_users");
    }

    #[test]
    fn test_resolve_model_metadata_does_not_read_compiled_code() {
        // A model with no compiled_code/raw_code at all should still resolve
        // metadata successfully: metadata resolution is independent of SQL.
        let json = serde_json::json!({
            "metadata": {
                "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v7.json",
                "generated_at": "2026-06-21T10:00:00.000Z"
            },
            "nodes": {
                "model.test.fct_no_code": {
                    "unique_id": "model.test.fct_no_code",
                    "name": "fct_no_code",
                    "resource_type": "model",
                    "package_name": "test",
                    "schema": "analytics",
                    "config": { "materialized": "table" }
                }
            },
            "sources": {}
        });
        let manifest: Manifest = serde_json::from_value(json).expect("Should deserialize");
        let meta = manifest
            .resolve_model_metadata("fct_no_code")
            .expect("Should resolve metadata without compiled SQL");
        assert_eq!(meta.name, "fct_no_code");
        // resolve_ref on the same node should still error (no compiled SQL),
        // proving the two resolvers are independent.
        assert!(manifest.resolve_ref("fct_no_code").is_err());
    }

    // -----------------------------------------------------------------------
    // dagster-dbt default_asset_key_fn parity tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_config_schema_differs_from_resolved_schema() {
        // When a custom generate_schema_name macro is in use, the top-level
        // resolved schema differs from config.schema. dagster-dbt uses
        // config.schema for the default key, not the resolved schema.
        let json = serde_json::json!({
            "metadata": {
                "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v7.json",
                "generated_at": "2026-06-21T10:00:00.000Z"
            },
            "nodes": {
                "model.test.fct_users": {
                    "unique_id": "model.test.fct_users",
                    "name": "fct_users",
                    "resource_type": "model",
                    "schema": "analytics_marts",
                    "config": {
                        "materialized": "table",
                        "schema": "marts"
                    }
                }
            },
            "sources": {}
        });
        let manifest: Manifest = serde_json::from_value(json).expect("Should deserialize");
        let meta = manifest
            .resolve_model_metadata("fct_users")
            .expect("Should resolve");
        // Resolved schema and configured schema differ.
        assert_eq!(meta.schema.as_deref(), Some("analytics_marts"));
        assert_eq!(meta.config_schema.as_deref(), Some("marts"));
    }

    #[test]
    fn test_config_meta_dagster_asset_key_precedes_top_level_meta() {
        // dagster-dbt reads config.meta first, then falls back to top-level
        // meta. When config.meta.dagster.asset_key is set, it wins.
        let json = serde_json::json!({
            "metadata": {
                "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v7.json",
                "generated_at": "2026-06-21T10:00:00.000Z"
            },
            "nodes": {
                "model.test.fct_users": {
                    "unique_id": "model.test.fct_users",
                    "name": "fct_users",
                    "resource_type": "model",
                    "meta": {
                        "dagster": { "asset_key": ["top_level_key"] }
                    },
                    "config": {
                        "materialized": "table",
                        "meta": {
                            "dagster": { "asset_key": ["config_key"] }
                        }
                    }
                }
            },
            "sources": {}
        });
        let manifest: Manifest = serde_json::from_value(json).expect("Should deserialize");
        let meta = manifest
            .resolve_model_metadata("fct_users")
            .expect("Should resolve");
        assert_eq!(
            meta.config_dagster_asset_key,
            Some(vec!["config_key".to_string()])
        );
        assert_eq!(
            meta.dagster_asset_key,
            Some(vec!["top_level_key".to_string()])
        );
    }

    #[test]
    fn test_top_level_meta_used_when_config_meta_absent() {
        // When config has no meta, the top-level meta.dagster.asset_key is
        // the fallback. This is the existing fixture behavior (fct_orders).
        let json = serde_json::json!({
            "metadata": {
                "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v7.json",
                "generated_at": "2026-06-21T10:00:00.000Z"
            },
            "nodes": {
                "model.test.fct_users": {
                    "unique_id": "model.test.fct_users",
                    "name": "fct_users",
                    "resource_type": "model",
                    "meta": {
                        "dagster": { "asset_key": ["top_only"] }
                    },
                    "config": { "materialized": "table" }
                }
            },
            "sources": {}
        });
        let manifest: Manifest = serde_json::from_value(json).expect("Should deserialize");
        let meta = manifest
            .resolve_model_metadata("fct_users")
            .expect("Should resolve");
        assert_eq!(meta.config_dagster_asset_key, None);
        assert_eq!(meta.dagster_asset_key, Some(vec!["top_only".to_string()]));
    }

    #[test]
    fn test_versioned_model_exposes_version_and_alias() {
        // dbt >= 1.5 versioned models: dagster-dbt uses [alias] as the key,
        // not [schema, name].
        let json = serde_json::json!({
            "metadata": {
                "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v9.json",
                "generated_at": "2026-06-21T10:00:00.000Z"
            },
            "nodes": {
                "model.test.fct_orders.v2": {
                    "unique_id": "model.test.fct_orders.v2",
                    "name": "fct_orders",
                    "resource_type": "model",
                    "alias": "fct_orders_v2",
                    "schema": "analytics",
                    "version": "2",
                    "config": { "materialized": "table" }
                }
            },
            "sources": {}
        });
        let manifest: Manifest = serde_json::from_value(json).expect("Should deserialize");
        let meta = manifest
            .resolve_model_metadata("fct_orders")
            .expect("Should resolve versioned model");
        assert_eq!(meta.version.as_deref(), Some("2"));
        assert_eq!(meta.alias.as_deref(), Some("fct_orders_v2"));
    }

    #[test]
    fn test_absent_config_schema_falls_back_to_name_only() {
        // When neither config.schema nor top-level schema is present,
        // dagster-dbt uses [name] as the default key.
        let json = serde_json::json!({
            "metadata": {
                "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v7.json",
                "generated_at": "2026-06-21T10:00:00.000Z"
            },
            "nodes": {
                "model.test.fct_no_schema": {
                    "unique_id": "model.test.fct_no_schema",
                    "name": "fct_no_schema",
                    "resource_type": "model",
                    "config": { "materialized": "table" }
                }
            },
            "sources": {}
        });
        let manifest: Manifest = serde_json::from_value(json).expect("Should deserialize");
        let meta = manifest
            .resolve_model_metadata("fct_no_schema")
            .expect("Should resolve");
        assert_eq!(meta.schema, None);
        assert_eq!(meta.config_schema, None);
        assert_eq!(meta.version, None);
    }

    // -----------------------------------------------------------------------
    // NodeVersion: string, integer, and float acceptance
    // -----------------------------------------------------------------------

    #[test]
    fn test_version_string_parses() {
        let json = serde_json::json!({
            "metadata": {
                "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v9.json",
                "generated_at": "2026-06-21T10:00:00.000Z"
            },
            "nodes": {
                "model.test.fct_orders.v2": {
                    "unique_id": "model.test.fct_orders.v2",
                    "name": "fct_orders",
                    "resource_type": "model",
                    "alias": "fct_orders_v2",
                    "schema": "analytics",
                    "version": "2",
                    "config": { "materialized": "table" }
                }
            },
            "sources": {}
        });
        let manifest: Manifest = serde_json::from_value(json).expect("Should deserialize");
        let meta = manifest
            .resolve_model_metadata("fct_orders")
            .expect("Should resolve versioned model");
        assert_eq!(meta.version.as_deref(), Some("2"));
        assert_eq!(meta.alias.as_deref(), Some("fct_orders_v2"));
    }

    #[test]
    fn test_version_integer_parses() {
        // dbt serializes model versions as raw JSON integers (e.g. `2`, not
        // `"2"`). Before the typed `version` field was added, these flowed into
        // `_extra` and parsed fine. The typed field must preserve that.
        let json = serde_json::json!({
            "metadata": {
                "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v9.json",
                "generated_at": "2026-06-21T10:00:00.000Z"
            },
            "nodes": {
                "model.test.fct_orders.v2": {
                    "unique_id": "model.test.fct_orders.v2",
                    "name": "fct_orders",
                    "resource_type": "model",
                    "alias": "fct_orders_v2",
                    "schema": "analytics",
                    "version": 2,
                    "config": { "materialized": "table" }
                }
            },
            "sources": {}
        });
        let manifest: Manifest = serde_json::from_value(json).expect("Should deserialize");
        let meta = manifest
            .resolve_model_metadata("fct_orders")
            .expect("Should resolve versioned model");
        assert_eq!(meta.version.as_deref(), Some("2"));
        assert_eq!(meta.alias.as_deref(), Some("fct_orders_v2"));
    }

    #[test]
    fn test_version_float_parses() {
        // dbt accepts float versions (e.g. `2.5`). These must parse and
        // normalize to the string representation without trailing-zero
        // inflation.
        let json = serde_json::json!({
            "metadata": {
                "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v9.json",
                "generated_at": "2026-06-21T10:00:00.000Z"
            },
            "nodes": {
                "model.test.fct_orders.v2_5": {
                    "unique_id": "model.test.fct_orders.v2_5",
                    "name": "fct_orders",
                    "resource_type": "model",
                    "alias": "fct_orders_v2_5",
                    "schema": "analytics",
                    "version": 2.5,
                    "config": { "materialized": "table" }
                }
            },
            "sources": {}
        });
        let manifest: Manifest = serde_json::from_value(json).expect("Should deserialize");
        let meta = manifest
            .resolve_model_metadata("fct_orders")
            .expect("Should resolve versioned model with float version");
        assert_eq!(meta.version.as_deref(), Some("2.5"));
        assert_eq!(meta.alias.as_deref(), Some("fct_orders_v2_5"));
    }

    #[test]
    fn test_versioned_model_triggers_alias_key_branch() {
        // Verify that a numeric version triggers the versioned `[alias]` key
        // branch in resolve_model_metadata (the version field is populated and
        // non-None, which the Python translator checks).
        let json = serde_json::json!({
            "metadata": {
                "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v9.json",
                "generated_at": "2026-06-21T10:00:00.000Z"
            },
            "nodes": {
                "model.test.fct_orders.v3": {
                    "unique_id": "model.test.fct_orders.v3",
                    "name": "fct_orders",
                    "resource_type": "model",
                    "alias": "fct_orders_v3",
                    "schema": "analytics",
                    "version": 3,
                    "config": { "materialized": "table", "schema": "analytics" }
                }
            },
            "sources": {}
        });
        let manifest: Manifest = serde_json::from_value(json).expect("Should deserialize");
        let meta = manifest
            .resolve_model_metadata("fct_orders")
            .expect("Should resolve");
        // version is set (not None) → the Python translator uses [alias].
        assert!(meta.version.is_some());
        assert_eq!(meta.alias.as_deref(), Some("fct_orders_v3"));
    }

    #[test]
    fn test_engine_load_path_accepts_numeric_version() {
        // Regression test: the pre-existing Engine/Manifest::load path (used by
        // resolve_ref for compiled-SQL execution) must still accept manifests
        // with numeric model versions. Before the typed `version` field, these
        // parsed fine via `_extra`. This test proves no regression.
        let json = serde_json::json!({
            "metadata": {
                "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v9.json",
                "generated_at": "2026-06-21T10:00:00.000Z"
            },
            "nodes": {
                "model.test.fct_orders.v2": {
                    "unique_id": "model.test.fct_orders.v2",
                    "name": "fct_orders",
                    "resource_type": "model",
                    "compiled_code": "SELECT 1",
                    "alias": "fct_orders_v2",
                    "schema": "analytics",
                    "version": 2,
                    "config": { "materialized": "table" }
                }
            },
            "sources": {}
        });
        let manifest: Manifest =
            serde_json::from_value(json).expect("Should parse manifest with numeric version");
        // The pre-existing resolve_ref path still works (compiled SQL).
        let sql = manifest
            .resolve_ref("fct_orders")
            .expect("Should resolve compiled SQL");
        assert_eq!(sql, "SELECT 1");
    }

    #[test]
    fn test_version_rejects_unsupported_types() {
        // Booleans, arrays, and objects are not valid dbt versions and must
        // be rejected with a clear error, not silently accepted. Note: `null`
        // is intentionally excluded because `#[serde(default)]` on
        // `Option<NodeVersion>` correctly maps `null` to `None` (no version),
        // which is valid.
        let bad_values = [
            serde_json::json!(true),
            serde_json::json!([1, 2]),
            serde_json::json!({"nested": true}),
        ];
        for bad in &bad_values {
            let json = serde_json::json!({
                "metadata": {
                    "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v9.json",
                    "generated_at": "2026-06-21T10:00:00.000Z"
                },
                "nodes": {
                    "model.test.bad": {
                        "unique_id": "model.test.bad",
                        "name": "bad",
                        "resource_type": "model",
                        "version": bad,
                        "config": { "materialized": "table" }
                    }
                },
                "sources": {}
            });
            let result: Result<Manifest, _> = serde_json::from_value(json);
            assert!(result.is_err(), "Should reject version: {bad}");
            let msg = result.unwrap_err().to_string();
            assert!(
                msg.contains("version"),
                "Error should mention version: {msg}"
            );
        }
    }
}
