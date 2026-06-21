use crate::config::{CdcMethod, DestinationConfig, FerryConfig, SyncConfig, SyncMode};

/// A single validation error with context about which field and why.
#[derive(Debug, Clone)]
pub struct ValidationError {
    pub field: String,
    pub message: String,
    pub context: String,
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}: {}", self.context, self.field, self.message)
    }
}

/// Validate a top-level `FerryConfig`.
///
/// Collects ALL errors and returns them as a `Vec` (not fail-fast).
pub fn validate_ferry_config(config: &FerryConfig) -> Result<(), Vec<ValidationError>> {
    let mut errors: Vec<ValidationError> = Vec::new();

    // name must be non-empty
    if config.name.trim().is_empty() {
        errors.push(ValidationError {
            field: "name".to_string(),
            message: "Project name must not be empty".to_string(),
            context: "ferry.yml".to_string(),
        });
    }

    // state backend must be valid (at minimum, it must be one of the known backends)
    // Since StateBackend is an enum, serde already validates it's a known value.
    // We just check that the path is present for DuckDB backend (optional but recommended).
    if config.state.path.as_deref().unwrap_or("").is_empty() {
        errors.push(ValidationError {
            field: "state.path".to_string(),
            message: "State path should be specified for state backend".to_string(),
            context: "ferry.yml".to_string(),
        });
    }

    // source config must be present (serde already ensures this via the enum)
    // Validate source-specific fields
    match &config.source {
        crate::config::SourceConfig::DuckDB { path, query: _ } => {
            if path.trim().is_empty() {
                errors.push(ValidationError {
                    field: "source.DuckDB.path".to_string(),
                    message: "DuckDB source path must not be empty".to_string(),
                    context: "ferry.yml".to_string(),
                });
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// Validate a single `SyncConfig`.
///
/// Collects ALL errors and returns them as a `Vec` (not fail-fast).
pub fn validate_sync_config(config: &SyncConfig) -> Result<(), Vec<ValidationError>> {
    let mut errors: Vec<ValidationError> = Vec::new();

    // name must be non-empty
    if config.name.trim().is_empty() {
        errors.push(ValidationError {
            field: "name".to_string(),
            message: "Sync name must not be empty".to_string(),
            context: format!("sync:{}", config.name),
        });
    }

    // model must be present (serde ensures this via the enum)
    match &config.model {
        crate::config::ModelConfig::Sql { sql } => {
            if sql.trim().is_empty() {
                errors.push(ValidationError {
                    field: "model.sql".to_string(),
                    message: "SQL model must have a non-empty SQL query".to_string(),
                    context: format!("sync:{}", config.name),
                });
            }
        }
        crate::config::ModelConfig::Ref { r#ref } => {
            if r#ref.trim().is_empty() {
                errors.push(ValidationError {
                    field: "model.ref".to_string(),
                    message: "Ref model must have a non-empty reference".to_string(),
                    context: format!("sync:{}", config.name),
                });
            }
        }
    }

    // destination must be present (serde ensures this via the enum)
    match &config.destination {
        DestinationConfig::Braze {
            api_key, endpoint, ..
        } => {
            if api_key.trim().is_empty() {
                errors.push(ValidationError {
                    field: "destination.Braze.api_key".to_string(),
                    message: "Braze API key must not be empty".to_string(),
                    context: format!("sync:{}", config.name),
                });
            }
            if endpoint.trim().is_empty() {
                errors.push(ValidationError {
                    field: "destination.Braze.endpoint".to_string(),
                    message: "Braze endpoint must not be empty".to_string(),
                    context: format!("sync:{}", config.name),
                });
            }
        }
        DestinationConfig::Slack { webhook_url } => {
            if webhook_url.trim().is_empty() {
                errors.push(ValidationError {
                    field: "destination.Slack.webhook_url".to_string(),
                    message: "Slack webhook URL must not be empty".to_string(),
                    context: format!("sync:{}", config.name),
                });
            }
        }
        DestinationConfig::Rest { url, .. } => {
            if url.trim().is_empty() {
                errors.push(ValidationError {
                    field: "destination.Rest.url".to_string(),
                    message: "REST destination URL must not be empty".to_string(),
                    context: format!("sync:{}", config.name),
                });
            }
        }
        DestinationConfig::File { output_dir, .. } => {
            if output_dir.trim().is_empty() {
                errors.push(ValidationError {
                    field: "destination.File.output_dir".to_string(),
                    message: "File destination output directory must not be empty".to_string(),
                    context: format!("sync:{}", config.name),
                });
            }
        }
    }

    // Validate sync settings
    let sync = &config.sync;

    // batch_size > 0 if delivery is configured
    if let Some(delivery) = &sync.delivery {
        if delivery.batch_size == 0 {
            errors.push(ValidationError {
                field: "sync.delivery.batch_size".to_string(),
                message: "batch_size must be greater than 0".to_string(),
                context: format!("sync:{}", config.name),
            });
        }

        // max_attempts > 0 if retry is configured
        if let Some(retry) = &delivery.retry {
            if retry.max_attempts == 0 {
                errors.push(ValidationError {
                    field: "sync.delivery.retry.max_attempts".to_string(),
                    message: "max_attempts must be greater than 0".to_string(),
                    context: format!("sync:{}", config.name),
                });
            }
        }
    }

    // Validate mode / CDC combinations
    match &sync.mode {
        SyncMode::Incremental => {
            if let Some(cdc) = &sync.cdc {
                match &cdc.method {
                    CdcMethod::Cursor => {
                        // Cursor mode requires cursor_field
                        if sync.cursor_field.is_none()
                            || sync.cursor_field.as_deref().unwrap_or("").trim().is_empty()
                        {
                            errors.push(ValidationError {
                                field: "sync.cursor_field".to_string(),
                                message: "Cursor mode requires a non-empty cursor_field"
                                    .to_string(),
                                context: format!("sync:{}", config.name),
                            });
                        }
                    }
                    CdcMethod::Hash => {
                        // Hash mode is valid without extra fields
                    }
                }
            }
        }
        SyncMode::FullRefresh => {
            // Full refresh doesn't require CDC config
        }
        SyncMode::Mirror => {
            // Mirror mode doesn't require CDC config
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::*;

    fn valid_ferry_config() -> FerryConfig {
        FerryConfig {
            name: "test_project".to_string(),
            version: Some("1.0".to_string()),
            source: SourceConfig::DuckDB {
                path: "/data/db.duckdb".to_string(),
                query: Some("SELECT * FROM users".to_string()),
            },
            state: StateConfig {
                backend: StateBackend::DuckDB,
                path: Some(".ferry/state.db".to_string()),
            },
            dbt: None,
            defaults: None,
        }
    }

    fn valid_sync_config(name: &str) -> SyncConfig {
        SyncConfig {
            name: name.to_string(),
            description: Some("Test sync".to_string()),
            tags: Some(vec!["test".to_string()]),
            model: ModelConfig::Sql {
                sql: "SELECT * FROM users".to_string(),
            },
            destination: DestinationConfig::Rest {
                url: "https://api.example.com/users".to_string(),
                method: Some("POST".to_string()),
                headers: None,
            },
            sync: SyncSettings {
                mode: SyncMode::Incremental,
                cursor_field: Some("updated_at".to_string()),
                cdc: Some(CdcConfig {
                    method: CdcMethod::Cursor,
                    hash_columns: None,
                }),
                delivery: Some(DeliveryConfig {
                    batch_size: 100,
                    retry: Some(RetryConfig {
                        max_attempts: 3,
                        backoff: BackoffStrategy::Exponential,
                        initial_delay_secs: 5,
                        max_delay_secs: 300,
                    }),
                    on_reject: None,
                    dead_letter: None,
                    allow_redelivery: false,
                }),
                full_refresh: None,
            },
            tests: None,
        }
    }

    #[test]
    fn test_validate_valid_ferry_config() {
        let config = valid_ferry_config();
        let result = validate_ferry_config(&config);
        assert!(result.is_ok(), "Expected OK, got errors: {:?}", result);
    }

    #[test]
    fn test_validate_valid_sync_config() {
        let config = valid_sync_config("test_sync");
        let result = validate_sync_config(&config);
        assert!(result.is_ok(), "Expected OK, got errors: {:?}", result);
    }

    #[test]
    fn test_validate_missing_name() {
        let config = FerryConfig {
            name: "   ".to_string(),
            ..valid_ferry_config()
        };
        let result = validate_ferry_config(&config);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.field == "name"));
    }

    #[test]
    fn test_validate_cursor_without_cursor_field() {
        let config = SyncConfig {
            name: "cursor_sync".to_string(),
            sync: SyncSettings {
                mode: SyncMode::Incremental,
                cursor_field: None,
                cdc: Some(CdcConfig {
                    method: CdcMethod::Cursor,
                    hash_columns: None,
                }),
                delivery: Some(DeliveryConfig {
                    batch_size: 100,
                    retry: None,
                    on_reject: None,
                    dead_letter: None,
                    allow_redelivery: false,
                }),
                full_refresh: None,
            },
            ..valid_sync_config("cursor_sync")
        };
        let result = validate_sync_config(&config);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(
            errors.iter().any(|e| e.field == "sync.cursor_field"),
            "Expected error about cursor_field, got: {:?}",
            errors
        );
    }

    #[test]
    fn test_validate_collects_all_errors() {
        // Multiple errors: empty name, empty SQL, empty destination URL, batch_size=0
        let config = SyncConfig {
            name: "".to_string(),
            model: ModelConfig::Sql {
                sql: "".to_string(),
            },
            destination: DestinationConfig::Rest {
                url: "".to_string(),
                method: None,
                headers: None,
            },
            sync: SyncSettings {
                mode: SyncMode::Incremental,
                cursor_field: None,
                cdc: None,
                delivery: Some(DeliveryConfig {
                    batch_size: 0,
                    retry: Some(RetryConfig {
                        max_attempts: 0,
                        backoff: BackoffStrategy::Exponential,
                        initial_delay_secs: 5,
                        max_delay_secs: 300,
                    }),
                    on_reject: None,
                    dead_letter: None,
                    allow_redelivery: false,
                }),
                full_refresh: None,
            },
            ..valid_sync_config("")
        };
        let result = validate_sync_config(&config);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        // We expect at least 3 errors: empty name, empty sql, empty url, batch_size=0, max_attempts=0
        assert!(
            errors.len() >= 4,
            "Expected at least 4 errors, got {}: {:?}",
            errors.len(),
            errors
        );
    }

    #[test]
    fn test_validate_empty_sync_name() {
        let config = valid_sync_config("");
        let result = validate_sync_config(&config);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.field == "name"));
    }

    #[test]
    fn test_validate_batch_size_zero() {
        let config = SyncConfig {
            sync: SyncSettings {
                mode: SyncMode::Incremental,
                cursor_field: Some("id".to_string()),
                cdc: None,
                delivery: Some(DeliveryConfig {
                    batch_size: 0,
                    retry: None,
                    on_reject: None,
                    dead_letter: None,
                    allow_redelivery: false,
                }),
                full_refresh: None,
            },
            ..valid_sync_config("batch_test")
        };
        let result = validate_sync_config(&config);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.field == "sync.delivery.batch_size"));
    }

    #[test]
    fn test_validate_max_attempts_zero() {
        let config = SyncConfig {
            sync: SyncSettings {
                mode: SyncMode::Incremental,
                cursor_field: Some("id".to_string()),
                cdc: None,
                delivery: Some(DeliveryConfig {
                    batch_size: 100,
                    retry: Some(RetryConfig {
                        max_attempts: 0,
                        backoff: BackoffStrategy::Exponential,
                        initial_delay_secs: 5,
                        max_delay_secs: 300,
                    }),
                    on_reject: None,
                    dead_letter: None,
                    allow_redelivery: false,
                }),
                full_refresh: None,
            },
            ..valid_sync_config("attempts_test")
        };
        let result = validate_sync_config(&config);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(
            errors
                .iter()
                .any(|e| e.field == "sync.delivery.retry.max_attempts")
        );
    }
}
