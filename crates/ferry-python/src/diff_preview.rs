use pyo3::prelude::*;

/// Preview of a CDC diff (no delivery), exposed to Python.
#[pyclass(skip_from_py_object)]
#[derive(Clone)]
pub struct DiffPreview {
    #[pyo3(get)]
    pub sync_name: String,
    #[pyo3(get)]
    pub added: usize,
    #[pyo3(get)]
    pub changed: usize,
    #[pyo3(get)]
    pub removed: usize,
    #[pyo3(get)]
    pub total_rows: usize,
}

impl From<ferry_core::engine::DiffPreview> for DiffPreview {
    fn from(d: ferry_core::engine::DiffPreview) -> Self {
        Self {
            sync_name: d.sync_name,
            added: d.added,
            changed: d.changed,
            removed: d.removed,
            total_rows: d.total_rows,
        }
    }
}

#[pymethods]
impl DiffPreview {
    fn __repr__(&self) -> String {
        format!(
            "DiffPreview(sync_name={}, added={}, changed={}, removed={}, total={})",
            self.sync_name, self.added, self.changed, self.removed, self.total_rows
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}
