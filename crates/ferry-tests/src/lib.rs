//! Integration tests for the Ferry reverse ETL engine.
//!
//! This crate contains workspace-level integration tests that verify
//! cross-cutting scenarios spanning multiple crates (ferry-core,
//! ferry-sources, ferry-destinations).
//!
//! The actual tests live in the `tests/` directory as separate integration
//! test files. This lib.rs exists only to satisfy Cargo's requirement for
//! a target in the crate.
