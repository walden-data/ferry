use std::path::{Path, PathBuf};

use ferry_core::config::{DestinationConfig, FerryConfig, FileFormat, SourceConfig, SyncConfig};
use ferry_core::error::FerryError;
use ferry_core::traits::{Destination, Source};
use ferry_destinations::FileDestination;
use ferry_sources::duckdb::DuckDbSource;
use ferry_sources::postgres::PostgresSource;

/// Create a source connector from a sync config.
pub async fn create_source(
    project_dir: &Path,
    _sync_config: &SyncConfig,
) -> Result<Box<dyn Source>, FerryError> {
    // For now, we use the project-level source config from ferry.yml
    // In Phase 2, sync-level source overrides will be supported.
    let config = FerryConfig::load(project_dir)?;
    match &config.source {
        SourceConfig::DuckDB { path, query: _ } => {
            let resolved_path = if Path::new(path).is_relative() {
                project_dir.join(path)
            } else {
                PathBuf::from(path)
            };
            let source = DuckDbSource::new(
                resolved_path
                    .to_str()
                    .ok_or_else(|| FerryError::Config("Invalid source path".to_string()))?,
            )?;
            Ok(Box::new(source))
        }
        SourceConfig::Postgres {
            connection_string,
            query,
        } => {
            let source = if let Some(q) = query {
                PostgresSource::with_query(connection_string, q.clone()).await?
            } else {
                PostgresSource::new(connection_string).await?
            };
            Ok(Box::new(source))
        }
    }
}

/// Create a destination connector from a sync config.
pub fn create_destination(
    project_dir: &Path,
    sync_config: &SyncConfig,
) -> Result<Box<dyn Destination>, FerryError> {
    match &sync_config.destination {
        DestinationConfig::File { output_dir, format } => {
            let resolved_dir = if Path::new(output_dir).is_relative() {
                project_dir.join(output_dir)
            } else {
                PathBuf::from(output_dir)
            };
            let file_format = match format {
                Some(f) => match f {
                    FileFormat::Csv => ferry_destinations::FileFormat::Csv,
                    FileFormat::Json => ferry_destinations::FileFormat::Json,
                },
                None => ferry_destinations::FileFormat::Csv,
            };
            let dest = FileDestination::new(&resolved_dir, file_format, &sync_config.name);
            Ok(Box::new(dest))
        }
        DestinationConfig::Rest { .. } => Err(FerryError::Config(
            "REST destination not yet implemented in Phase 1".to_string(),
        )),
        DestinationConfig::Braze { .. } => Err(FerryError::Config(
            "Braze destination not yet implemented in Phase 1".to_string(),
        )),
        DestinationConfig::Slack { .. } => Err(FerryError::Config(
            "Slack destination not yet implemented in Phase 1".to_string(),
        )),
    }
}
