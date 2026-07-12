use pyo3::create_exception;
use pyo3::exceptions::PyException;

use ferry_core::error::FerryError;

// Exception hierarchy mirroring FerryError variants
create_exception!(ferry._native, FerryPyError, PyException);
create_exception!(ferry._native, ConfigError, FerryPyError);
create_exception!(ferry._native, SourceError, FerryPyError);
create_exception!(ferry._native, DestinationError, FerryPyError);
create_exception!(ferry._native, CdcError, FerryPyError);
create_exception!(ferry._native, StateError, FerryPyError);
create_exception!(ferry._native, DeliveryError, FerryPyError);
create_exception!(ferry._native, ValidationError, FerryPyError);

/// Convert a `FerryError` into a `PyErr`.
///
/// This is used instead of `impl From<FerryError> for PyErr` because the orphan
/// rule prevents implementing a foreign trait (`From`) for a foreign type (`PyErr`)
/// with a foreign type (`FerryError`).
pub fn ferry_error_to_py_err(err: FerryError) -> pyo3::PyErr {
    match err {
        FerryError::Config(msg) => ConfigError::new_err(msg),
        FerryError::Source(msg) => SourceError::new_err(msg),
        FerryError::Destination(msg) => DestinationError::new_err(msg),
        FerryError::Cdc(msg) => CdcError::new_err(msg),
        FerryError::State(msg) => StateError::new_err(msg),
        FerryError::Delivery(msg) => DeliveryError::new_err(msg),
        FerryError::Validation(msg) => ValidationError::new_err(msg),
    }
}
