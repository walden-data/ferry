pub mod file;
pub mod rest;

// Re-export ferry-core types for convenience.
pub use ferry_core::*;

pub use file::{FileDestination, FileFormat};
pub use rest::{MockBehavior, MockRestDestination};
