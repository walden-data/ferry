use thiserror::Error;

#[derive(Error, Debug)]
pub enum FerryError {
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Source error: {0}")]
    Source(String),

    #[error("Destination error: {0}")]
    Destination(String),

    #[error("CDC error: {0}")]
    Cdc(String),

    #[error("State error: {0}")]
    State(String),

    #[error("Delivery error: {0}")]
    Delivery(String),

    #[error("Validation error: {0}")]
    Validation(String),
}

impl From<anyhow::Error> for FerryError {
    fn from(err: anyhow::Error) -> Self {
        FerryError::Source(err.to_string())
    }
}
