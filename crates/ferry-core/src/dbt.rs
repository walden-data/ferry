use std::collections::HashMap;
use std::path::Path;

use chrono::{DateTime, Utc};
use serde::Deserialize;
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
    #[serde(flatten)]
    _extra: serde_json::Value,
}

/// Configuration for a manifest node (materialization strategy, etc.).
#[derive(Debug, Deserialize)]
pub struct NodeConfig {
    #[serde(default)]
    pub materialized: Option<String>,
    #[serde(flatten)]
    _extra: serde_json::Value,
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
}
