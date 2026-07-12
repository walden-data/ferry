use pyo3::prelude::*;

/// A dead row from the dead letter queue, exposed to Python.
#[pyclass(skip_from_py_object)]
#[derive(Clone)]
pub struct DeadRow {
    #[pyo3(get)]
    pub primary_key: String,
    #[pyo3(get)]
    pub status: String,
    #[pyo3(get)]
    pub attempts: i32,
    #[pyo3(get)]
    pub last_error: Option<String>,
    #[pyo3(get)]
    pub last_attempt_at: Option<String>,
    #[pyo3(get)]
    pub sync_name: String,
}

impl DeadRow {
    pub fn from_entry(entry: &ferry_core::traits::RowEntry, sync_name: &str) -> Self {
        Self {
            primary_key: entry.primary_key.clone(),
            status: entry.status.clone(),
            attempts: entry.attempts,
            last_error: entry.last_error.clone(),
            last_attempt_at: entry
                .last_attempt_at
                .map(|t| t.format("%Y-%m-%d %H:%M:%S").to_string()),
            sync_name: sync_name.to_string(),
        }
    }
}

#[pymethods]
impl DeadRow {
    fn __repr__(&self) -> String {
        format!(
            "DeadRow(sync_name={}, pk={}, attempts={})",
            self.sync_name, self.primary_key, self.attempts
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}
