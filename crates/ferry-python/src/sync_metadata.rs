use pyo3::prelude::*;

use ferry_core::config::{DestinationConfig, SyncConfig};

use crate::dbt_model_metadata::DbtModelMetadata;

/// Stable metadata describing a single Ferry sync, returned by
/// `Project.list_syncs_metadata()`.
///
/// Fields are immutable and read-only from Python. The destination type is
/// normalized to a stable lowercase string so Dagster asset kinds and other
/// consumers can branch on it without importing Rust enums.
///
/// `dbt_model` is `Some` only for `model.ref` syncs when a dbt manifest is
/// configured and the referenced model resolves deterministically. SQL-only
/// syncs and projects without a manifest always carry `None`, preserving the
/// FERRY-8 behavior for projects that never use dbt.
#[pyclass(frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct SyncMetadata {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub description: Option<String>,
    #[pyo3(get)]
    pub tags: Vec<String>,
    #[pyo3(get)]
    pub destination_type: String,
    /// Resolved dbt model identity, or `None` for SQL-only syncs.
    #[pyo3(get)]
    pub dbt_model: Option<DbtModelMetadata>,
}

impl SyncMetadata {
    /// Build a `SyncMetadata` from a loaded `SyncConfig` and an optional,
    /// already-resolved dbt model metadata.
    ///
    /// Tags default to an empty vector when unset so callers can rely on a
    /// stable ordered sequence. The destination type is the lowercase variant
    /// name of `DestinationConfig` (`braze`, `slack`, `rest`, `file`,
    /// `google_sheets`). `dbt_model` is attached as-is; the caller is
    /// responsible for resolving it (or passing `None` for SQL syncs).
    pub fn from_sync_config(config: &SyncConfig, dbt_model: Option<DbtModelMetadata>) -> Self {
        Self {
            name: config.name.clone(),
            description: config.description.clone(),
            tags: config.tags.clone().unwrap_or_default(),
            destination_type: destination_type_name(&config.destination),
            dbt_model,
        }
    }
}

/// Return the stable lowercase destination type name for a `DestinationConfig`.
fn destination_type_name(dest: &DestinationConfig) -> String {
    let raw = match dest {
        DestinationConfig::Braze { .. } => "braze",
        DestinationConfig::Slack { .. } => "slack",
        DestinationConfig::Rest { .. } => "rest",
        DestinationConfig::File { .. } => "file",
        DestinationConfig::GoogleSheets { .. } => "google_sheets",
    };
    raw.to_string()
}

#[pymethods]
impl SyncMetadata {
    fn __repr__(&self) -> String {
        format!(
            "SyncMetadata(name={}, destination_type={})",
            self.name, self.destination_type
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferry_core::config::{DestinationConfig, ModelConfig, SyncConfig, SyncMode, SyncSettings};

    fn sample_config(name: &str, tags: Option<Vec<String>>) -> SyncConfig {
        SyncConfig {
            name: name.to_string(),
            description: Some("desc".to_string()),
            tags,
            model: ModelConfig::Sql {
                sql: "SELECT 1".to_string(),
            },
            destination: DestinationConfig::File {
                output_dir: "/tmp/out".to_string(),
                format: None,
            },
            sync: SyncSettings {
                mode: SyncMode::Incremental,
                cursor_field: None,
                cdc: None,
                delivery: None,
                full_refresh: None,
            },
            tests: None,
        }
    }

    #[test]
    fn test_from_sync_config_preserves_fields() {
        let config = sample_config(
            "users_sync",
            Some(vec!["team_a".to_string(), "team_b".to_string()]),
        );
        let meta = SyncMetadata::from_sync_config(&config, None);
        assert_eq!(meta.name, "users_sync");
        assert_eq!(meta.description.as_deref(), Some("desc"));
        assert_eq!(meta.tags, vec!["team_a", "team_b"]);
        assert_eq!(meta.destination_type, "file");
        assert!(meta.dbt_model.is_none());
    }

    #[test]
    fn test_from_sync_config_defaults_empty_tags() {
        let config = sample_config("users_sync", None);
        let meta = SyncMetadata::from_sync_config(&config, None);
        assert!(meta.tags.is_empty());
        assert_eq!(meta.destination_type, "file");
        assert!(meta.dbt_model.is_none());
    }

    #[test]
    fn test_destination_type_names() {
        let cases = [
            (DestinationConfig::Braze {
                api_key: "k".to_string(),
                endpoint: "e".to_string(),
                app_id: None,
            }),
            (DestinationConfig::Slack {
                webhook_url: "u".to_string(),
            }),
            (DestinationConfig::Rest {
                url: "u".to_string(),
                method: None,
                headers: None,
                auth: None,
                body_template: None,
                timeout_secs: None,
                connect_timeout_secs: None,
                max_response_bytes: None,
                allow_http: None,
                max_batch_size: None,
            }),
            (DestinationConfig::File {
                output_dir: "o".to_string(),
                format: None,
            }),
            (DestinationConfig::GoogleSheets {
                spreadsheet_id: "s".to_string(),
                sheet: "sh".to_string(),
                key_column: "k".to_string(),
                service_account_key_file: "f".to_string(),
                max_rows: 1,
                max_batch_size: None,
                timeout_secs: None,
                connect_timeout_secs: None,
                max_response_bytes: None,
            }),
        ];
        let expected = ["braze", "slack", "rest", "file", "google_sheets"];
        for (dest, want) in cases.iter().zip(expected.iter()) {
            assert_eq!(destination_type_name(dest), *want);
        }
    }

    #[test]
    fn test_repr_contains_name_and_destination_type() {
        let meta = SyncMetadata {
            name: "users_sync".to_string(),
            description: None,
            tags: vec![],
            destination_type: "file".to_string(),
            dbt_model: None,
        };
        let repr = meta.__repr__();
        assert!(repr.contains("users_sync"));
        assert!(repr.contains("file"));
    }
}
