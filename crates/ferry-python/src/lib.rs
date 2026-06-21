mod dead_row;
mod diff_preview;
mod error;
mod factory;
mod project;
mod sync_result;

use pyo3::prelude::*;

use dead_row::DeadRow;
use diff_preview::DiffPreview;
use error::*;
use project::Project;
use sync_result::SyncResult;

/// Ferry - Reverse ETL engine with Python bindings.
#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Register pyclasses
    m.add_class::<Project>()?;
    m.add_class::<SyncResult>()?;
    m.add_class::<DiffPreview>()?;
    m.add_class::<DeadRow>()?;

    // Register exceptions
    m.add("FerryError", m.py().get_type::<FerryPyError>())?;
    m.add("ConfigError", m.py().get_type::<ConfigError>())?;
    m.add("SourceError", m.py().get_type::<SourceError>())?;
    m.add("DestinationError", m.py().get_type::<DestinationError>())?;
    m.add("CdcError", m.py().get_type::<CdcError>())?;
    m.add("StateError", m.py().get_type::<StateError>())?;
    m.add("DeliveryError", m.py().get_type::<DeliveryError>())?;
    m.add("ValidationError", m.py().get_type::<ValidationError>())?;

    Ok(())
}
