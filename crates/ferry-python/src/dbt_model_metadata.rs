use pyo3::prelude::*;

/// Immutable, deterministic identity metadata for a dbt model.
///
/// Built from a dbt manifest node for a resolved `model.ref` sync. Exposed to
/// Python as a frozen pyclass so Dagster translators can derive an upstream
/// dbt `AssetKey` without importing `dagster-dbt` or re-parsing manifest JSON.
///
/// Fields are optional because dbt manifests may omit them (for example in
/// minimal or test fixtures). Consumers handle `None` by falling back to the
/// documented schema/name-compatible default asset-key mapping.
///
/// The asset-key fields mirror dagster-dbt's `default_asset_key_fn`
/// precedence. `config_dagster_asset_key` is checked before
/// `dagster_asset_key`, and `config_schema` (the configured schema) is
/// preferred over the resolved top-level `schema` for the default key.
/// Versioned models use `[alias]`.
#[pyclass(frozen, skip_from_py_object)]
#[derive(Clone, PartialEq, Eq)]
pub struct DbtModelMetadata {
    #[pyo3(get)]
    pub unique_id: String,
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub alias: Option<String>,
    #[pyo3(get)]
    pub package_name: Option<String>,
    /// The resolved schema (top-level `node.schema`). May differ from
    /// `config_schema` when a custom `generate_schema_name` macro is in use.
    #[pyo3(get)]
    pub schema: Option<String>,
    /// The configured schema (`config.schema`). Preferred over `schema` for
    /// the default asset-key mapping, matching dagster-dbt.
    #[pyo3(get)]
    pub config_schema: Option<String>,
    #[pyo3(get)]
    pub database: Option<String>,
    #[pyo3(get)]
    pub fqn: Option<Vec<String>>,
    /// Asset key from `config.meta.dagster.asset_key`. Checked first.
    #[pyo3(get)]
    pub config_dagster_asset_key: Option<Vec<String>>,
    /// Asset key from top-level `meta.dagster.asset_key`. Checked second.
    #[pyo3(get)]
    pub dagster_asset_key: Option<Vec<String>>,
    /// Model version (dbt >= 1.5). Versioned models use `[alias]` as the key.
    #[pyo3(get)]
    pub version: Option<String>,
}

impl DbtModelMetadata {
    /// Build a `DbtModelMetadata` from the core manifest model metadata.
    pub fn from_core(meta: ferry_core::dbt::DbtModelMetadata) -> Self {
        Self {
            unique_id: meta.unique_id,
            name: meta.name,
            alias: meta.alias,
            package_name: meta.package_name,
            schema: meta.schema,
            config_schema: meta.config_schema,
            database: meta.database,
            fqn: meta.fqn,
            config_dagster_asset_key: meta.config_dagster_asset_key,
            dagster_asset_key: meta.dagster_asset_key,
            version: meta.version,
        }
    }
}

#[pymethods]
impl DbtModelMetadata {
    fn __repr__(&self) -> String {
        format!(
            "DbtModelMetadata(unique_id={}, name={})",
            self.unique_id, self.name
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_core_preserves_fields() {
        let core = ferry_core::dbt::DbtModelMetadata {
            unique_id: "model.test.fct_users".to_string(),
            name: "fct_users".to_string(),
            alias: Some("fct_users".to_string()),
            package_name: Some("test".to_string()),
            schema: Some("analytics".to_string()),
            config_schema: Some("marts".to_string()),
            database: Some("warehouse".to_string()),
            fqn: Some(vec![
                "test".to_string(),
                "models".to_string(),
                "fct_users.sql".to_string(),
            ]),
            config_dagster_asset_key: Some(vec!["dbt".to_string(), "fct_users".to_string()]),
            dagster_asset_key: Some(vec!["top".to_string(), "fct_users".to_string()]),
            version: Some("2".to_string()),
        };
        let py_meta = DbtModelMetadata::from_core(core);
        assert_eq!(py_meta.unique_id, "model.test.fct_users");
        assert_eq!(py_meta.name, "fct_users");
        assert_eq!(py_meta.alias.as_deref(), Some("fct_users"));
        assert_eq!(py_meta.package_name.as_deref(), Some("test"));
        assert_eq!(py_meta.schema.as_deref(), Some("analytics"));
        assert_eq!(py_meta.config_schema.as_deref(), Some("marts"));
        assert_eq!(py_meta.database.as_deref(), Some("warehouse"));
        assert_eq!(
            py_meta.config_dagster_asset_key.as_deref(),
            Some(&["dbt".to_string(), "fct_users".to_string()][..])
        );
        assert_eq!(
            py_meta.dagster_asset_key.as_deref(),
            Some(&["top".to_string(), "fct_users".to_string()][..])
        );
        assert_eq!(py_meta.version.as_deref(), Some("2"));
    }

    #[test]
    fn test_repr_contains_unique_id_and_name() {
        let meta = DbtModelMetadata {
            unique_id: "model.test.x".to_string(),
            name: "x".to_string(),
            alias: None,
            package_name: None,
            schema: None,
            config_schema: None,
            database: None,
            fqn: None,
            config_dagster_asset_key: None,
            dagster_asset_key: None,
            version: None,
        };
        let repr = meta.__repr__();
        assert!(repr.contains("model.test.x"));
        assert!(repr.contains("x"));
    }
}
