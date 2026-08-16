use std::path::Path;
use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use tokio::runtime::Runtime;

use ferry_core::config::{FerryConfig, SyncConfig};
use ferry_core::engine::{Engine, RunOptions};
use ferry_core::state::DuckDbStateStore;
use ferry_core::traits::StateStore;

use crate::dead_row::DeadRow;
use crate::diff_preview::DiffPreview;
use crate::error::ferry_error_to_py_err;
use crate::factory;
use crate::sync_metadata::SyncMetadata;
use crate::sync_result::SyncResult;

/// A Ferry project, providing access to sync configuration and execution.
///
/// This is the main entry point for the Python bindings. Create a `Project`
/// by pointing it at a Ferry project directory containing `ferry.yml`.
#[pyclass]
pub struct Project {
    project_dir: String,
    runtime: Arc<Runtime>,
}

#[pymethods]
impl Project {
    /// Create a new Project from a Ferry project directory.
    ///
    /// Args:
    ///     project_dir: Path to the Ferry project directory (containing ferry.yml).
    #[new]
    fn new(project_dir: &str) -> PyResult<Self> {
        let runtime = Runtime::new().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create tokio runtime: {e}"
            ))
        })?;

        // Verify the project directory exists and has a valid config
        let dir = Path::new(project_dir);
        if !dir.exists() {
            return Err(PyErr::new::<PyValueError, _>(format!(
                "Project directory does not exist: {project_dir}"
            )));
        }

        // Try loading the config to validate early
        FerryConfig::load(dir).map_err(ferry_error_to_py_err)?;

        Ok(Self {
            project_dir: project_dir.to_string(),
            runtime: Arc::new(runtime),
        })
    }

    /// List all sync names in the project.
    fn list_syncs(&self) -> PyResult<Vec<String>> {
        let project_dir = Path::new(&self.project_dir);
        let syncs_dir = project_dir.join("syncs");
        let syncs = SyncConfig::load_all(&syncs_dir).map_err(ferry_error_to_py_err)?;
        Ok(syncs.into_iter().map(|s| s.name).collect())
    }

    /// List typed metadata for all syncs in the project.
    ///
    /// Returns one `SyncMetadata` per configured sync, sorted deterministically
    /// by sync name so downstream asset keys and ordering are reload-stable.
    /// Reuses the native `SyncConfig::load_all` path and does not reparse YAML
    /// on the Python side.
    fn list_syncs_metadata(&self) -> PyResult<Vec<SyncMetadata>> {
        let project_dir = Path::new(&self.project_dir);
        let syncs_dir = project_dir.join("syncs");
        let mut syncs = SyncConfig::load_all(&syncs_dir).map_err(ferry_error_to_py_err)?;
        // Sort by name for deterministic, reload-stable ordering. The native
        // loader already sorts by filename, but sorting by name makes the
        // contract explicit and resilient to filename changes.
        syncs.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(syncs.iter().map(SyncMetadata::from_sync_config).collect())
    }

    /// Run syncs.
    ///
    /// Args:
    ///     sync_names: Optional list of sync names to run. If None, runs all.
    ///     dry_run: If True, preview without writing.
    ///     full_refresh: If True, bypass CDC and sync all rows.
    ///     retry_dead: If True, retry dead rows before running.
    ///
    /// Returns:
    ///     List of SyncResult objects.
    #[pyo3(signature = (sync_names=None, dry_run=false, full_refresh=false, retry_dead=false))]
    fn run(
        &self,
        py: Python<'_>,
        sync_names: Option<Vec<String>>,
        dry_run: bool,
        full_refresh: bool,
        retry_dead: bool,
    ) -> PyResult<Vec<SyncResult>> {
        let project_dir = Path::new(&self.project_dir);
        let config = FerryConfig::load(project_dir).map_err(ferry_error_to_py_err)?;
        let syncs_dir = project_dir.join("syncs");
        let all_syncs = SyncConfig::load_all(&syncs_dir).map_err(ferry_error_to_py_err)?;

        // Extract state path before config is moved into Engine
        let state_path = config
            .state
            .path
            .clone()
            .unwrap_or_else(|| ".ferry/state.duckdb".to_string());

        // Filter syncs
        let selected_syncs: Vec<&SyncConfig> = if let Some(ref names) = sync_names {
            all_syncs
                .iter()
                .filter(|s| names.contains(&s.name))
                .collect()
        } else {
            all_syncs.iter().collect()
        };

        if selected_syncs.is_empty() {
            return Ok(Vec::new());
        }

        let engine = Engine::new(config).map_err(ferry_error_to_py_err)?;
        let rt = self.runtime.clone();

        // Clone data needed inside the closure
        let project_dir_buf = project_dir.to_path_buf();
        let state_path_clone = state_path.clone();
        let sync_names_clone = sync_names.clone();

        py.detach(move || {
            // We need to run syncs sequentially (not in parallel) to avoid
            // state store conflicts. Each sync creates its own source/destination.
            let mut results: Vec<ferry_core::engine::SyncResult> = Vec::new();

            for sync_config in &selected_syncs {
                // Create source
                let source =
                    match rt.block_on(factory::create_source(&project_dir_buf, sync_config)) {
                        Ok(s) => s,
                        Err(e) => {
                            return Err(ferry_error_to_py_err(e));
                        }
                    };

                // Create destination
                let destination =
                    match rt.block_on(factory::create_destination(&project_dir_buf, sync_config)) {
                        Ok(d) => d,
                        Err(e) => {
                            return Err(ferry_error_to_py_err(e));
                        }
                    };

                let options = RunOptions {
                    sync_names: sync_names_clone.clone(),
                    full_refresh,
                    dry_run,
                    retry_dead,
                };

                // If --retry-dead, retry dead rows before running
                if retry_dead && !dry_run {
                    if let Ok(state) =
                        DuckDbStateStore::new(project_dir_buf.join(&state_path_clone).as_path())
                    {
                        let _ = rt.block_on(state.retry_dead_rows(&sync_config.name, None));
                    }
                }

                let result = rt.block_on(engine.run_sync(
                    sync_config,
                    source.as_ref(),
                    destination.as_ref(),
                    &options,
                ));

                match result {
                    Ok(r) => results.push(r),
                    Err(e) => {
                        return Err(ferry_error_to_py_err(e));
                    }
                }
            }

            Ok(results.into_iter().map(SyncResult::from).collect())
        })
    }

    /// Validate all sync configurations.
    ///
    /// Returns a list of validation error messages. An empty list means
    /// everything is valid.
    fn validate(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let project_dir = Path::new(&self.project_dir);
        let config = FerryConfig::load(project_dir).map_err(ferry_error_to_py_err)?;
        let syncs_dir = project_dir.join("syncs");
        let engine = Engine::new(config).map_err(ferry_error_to_py_err)?;
        let rt = self.runtime.clone();
        let syncs_dir_buf = syncs_dir.to_path_buf();

        py.detach(move || {
            let errors = rt
                .block_on(engine.validate(&syncs_dir_buf))
                .map_err(ferry_error_to_py_err)?;
            Ok(errors.iter().map(|e| e.to_string()).collect())
        })
    }

    /// Preview what CDC would detect for a sync.
    ///
    /// Args:
    ///     sync_name: The name of the sync to diff.
    ///
    /// Returns:
    ///     A DiffPreview with added/changed/removed counts.
    fn diff(&self, py: Python<'_>, sync_name: &str) -> PyResult<DiffPreview> {
        let project_dir = Path::new(&self.project_dir);
        let config = FerryConfig::load(project_dir).map_err(ferry_error_to_py_err)?;
        let syncs_dir = project_dir.join("syncs");
        let all_syncs = SyncConfig::load_all(&syncs_dir).map_err(ferry_error_to_py_err)?;

        let sync_config = all_syncs
            .iter()
            .find(|s| s.name == sync_name)
            .ok_or_else(|| {
                PyErr::new::<crate::error::ConfigError, _>(format!("Sync '{sync_name}' not found"))
            })?;

        let rt = self.runtime.clone();
        let source = rt
            .block_on(factory::create_source(project_dir, sync_config))
            .map_err(ferry_error_to_py_err)?;
        let engine = Engine::new(config).map_err(ferry_error_to_py_err)?;
        let sync_name_owned = sync_name.to_string();

        py.detach(move || {
            let preview = rt
                .block_on(engine.diff(&sync_name_owned, source.as_ref(), sync_config))
                .map_err(ferry_error_to_py_err)?;
            Ok(DiffPreview::from(preview))
        })
    }

    /// List dead rows from the dead letter queue.
    ///
    /// Args:
    ///     sync_name: Optional sync name to filter by. If None, lists all.
    ///
    /// Returns:
    ///     List of DeadRow objects.
    #[pyo3(signature = (sync_name=None))]
    fn dlq_list(&self, py: Python<'_>, sync_name: Option<&str>) -> PyResult<Vec<DeadRow>> {
        let project_dir = Path::new(&self.project_dir);
        let config = FerryConfig::load(project_dir).map_err(ferry_error_to_py_err)?;
        let syncs_dir = project_dir.join("syncs");
        let all_syncs = SyncConfig::load_all(&syncs_dir).map_err(ferry_error_to_py_err)?;

        let state_path = config
            .state
            .path
            .as_deref()
            .unwrap_or(".ferry/state.duckdb");
        let state = DuckDbStateStore::new(project_dir.join(state_path).as_path())
            .map_err(ferry_error_to_py_err)?;

        let rt = self.runtime.clone();

        let sync_names: Vec<String> = if let Some(ref name) = sync_name {
            vec![name.to_string()]
        } else {
            all_syncs.iter().map(|s| s.name.clone()).collect()
        };

        py.detach(move || {
            let mut dead_rows = Vec::new();
            for name in &sync_names {
                let entries = rt
                    .block_on(state.get_dead_rows(name))
                    .map_err(ferry_error_to_py_err)?;
                for entry in &entries {
                    dead_rows.push(DeadRow::from_entry(entry, name));
                }
            }
            Ok(dead_rows)
        })
    }

    /// Retry dead rows from the dead letter queue.
    ///
    /// Args:
    ///     sync_name: Optional sync name to filter by. If None, retries all.
    ///
    /// Returns:
    ///     Number of rows retried.
    #[pyo3(signature = (sync_name=None))]
    fn dlq_retry(&self, py: Python<'_>, sync_name: Option<&str>) -> PyResult<usize> {
        let project_dir = Path::new(&self.project_dir);
        let config = FerryConfig::load(project_dir).map_err(ferry_error_to_py_err)?;
        let syncs_dir = project_dir.join("syncs");
        let all_syncs = SyncConfig::load_all(&syncs_dir).map_err(ferry_error_to_py_err)?;

        let state_path = config
            .state
            .path
            .as_deref()
            .unwrap_or(".ferry/state.duckdb");
        let state = DuckDbStateStore::new(project_dir.join(state_path).as_path())
            .map_err(ferry_error_to_py_err)?;

        let rt = self.runtime.clone();

        let sync_names: Vec<String> = if let Some(ref name) = sync_name {
            vec![name.to_string()]
        } else {
            all_syncs.iter().map(|s| s.name.clone()).collect()
        };

        py.detach(move || {
            let mut total = 0usize;
            for name in &sync_names {
                let count = rt
                    .block_on(state.retry_dead_rows(name, None))
                    .map_err(ferry_error_to_py_err)?;
                total += count;
            }
            Ok(total)
        })
    }
}
