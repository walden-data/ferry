use pyo3::prelude::*;

/// Result of a single sync run, exposed to Python.
#[pyclass(skip_from_py_object)]
#[derive(Clone)]
pub struct SyncResult {
    #[pyo3(get)]
    pub sync_name: String,
    #[pyo3(get)]
    pub run_id: String,
    #[pyo3(get)]
    pub rows_extracted: usize,
    #[pyo3(get)]
    pub rows_synced: usize,
    #[pyo3(get)]
    pub rows_failed: usize,
    #[pyo3(get)]
    pub rows_pending: usize,
    #[pyo3(get)]
    pub rows_retried: usize,
    #[pyo3(get)]
    pub rows_dead: usize,
    #[pyo3(get)]
    pub duration_seconds: f64,
    #[pyo3(get)]
    pub dry_run: bool,
    #[pyo3(get)]
    pub mode: String,
}

impl From<ferry_core::engine::SyncResult> for SyncResult {
    fn from(r: ferry_core::engine::SyncResult) -> Self {
        Self {
            sync_name: r.sync_name,
            run_id: r.run_id,
            rows_extracted: r.rows_extracted,
            rows_synced: r.rows_synced,
            rows_failed: r.rows_failed,
            rows_pending: r.rows_pending,
            rows_retried: r.rows_retried,
            rows_dead: r.rows_dead,
            duration_seconds: r.duration_seconds,
            dry_run: r.dry_run,
            mode: r.mode,
        }
    }
}

#[pymethods]
impl SyncResult {
    fn __repr__(&self) -> String {
        format!(
            "SyncResult(sync_name={}, rows_synced={}, duration={}s)",
            self.sync_name, self.rows_synced, self.duration_seconds
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
    fn test_sync_result_from_engine() {
        let engine_result = ferry_core::engine::SyncResult {
            sync_name: "test_sync".to_string(),
            run_id: "run-001".to_string(),
            rows_extracted: 100,
            rows_synced: 95,
            rows_failed: 5,
            rows_pending: 3,
            rows_retried: 2,
            rows_dead: 1,
            duration_seconds: 12.5,
            dry_run: false,
            mode: "incremental".to_string(),
        };

        let py_result = SyncResult::from(engine_result);
        assert_eq!(py_result.sync_name, "test_sync");
        assert_eq!(py_result.rows_synced, 95);
        assert_eq!(py_result.rows_failed, 5);
        assert_eq!(py_result.duration_seconds, 12.5);
        assert!(!py_result.dry_run);
    }

    #[test]
    fn test_sync_result_repr() {
        let engine_result = ferry_core::engine::SyncResult {
            sync_name: "my_sync".to_string(),
            run_id: "run-002".to_string(),
            rows_extracted: 50,
            rows_synced: 50,
            rows_failed: 0,
            rows_pending: 0,
            rows_retried: 0,
            rows_dead: 0,
            duration_seconds: 3.2,
            dry_run: true,
            mode: "full_refresh".to_string(),
        };

        let py_result = SyncResult::from(engine_result);
        let repr = py_result.__repr__();
        assert!(repr.contains("my_sync"));
        assert!(repr.contains("50"));
        assert!(repr.contains("3.2"));
    }
}
