use crate::config::{
    AuthConfig, CdcMethod, DestinationConfig, FerryConfig, ModelConfig, SyncConfig, SyncMode,
};
use crate::dbt::Manifest;

/// Max response body size accepted by the REST destination (64 MiB).
const MAX_RESPONSE_BYTES_CAP: usize = 64 * 1024 * 1024;
/// Default max response body size (1 MiB).
pub const DEFAULT_MAX_RESPONSE_BYTES: usize = 1024 * 1024;
/// Default total request timeout (30 s).
pub const DEFAULT_TIMEOUT_SECS: u64 = 30;
/// Default connect timeout (10 s).
pub const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 10;
/// Default per-destination max batch size (100 rows).
pub const DEFAULT_MAX_BATCH_SIZE: usize = 100;

/// Maximum per-cell character count enforced by the Google Sheets API.
pub const GOOGLE_SHEETS_MAX_CELL_CHARS: usize = 50_000;
/// Regex pattern for a valid Google Spreadsheet ID.
pub const GOOGLE_SHEETS_SPREADSHEET_ID_REGEX: &str = r"^[A-Za-z0-9_-]+$";

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
        crate::config::SourceConfig::Postgres {
            connection_string,
            query: _,
        } => {
            if connection_string.trim().is_empty() {
                errors.push(ValidationError {
                    field: "source.Postgres.connection_string".to_string(),
                    message: "PostgreSQL connection string must not be empty".to_string(),
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
        DestinationConfig::Rest {
            url,
            method,
            headers,
            auth,
            body_template,
            timeout_secs,
            connect_timeout_secs,
            max_response_bytes,
            allow_http,
            max_batch_size,
        } => {
            let ctx = format!("sync:{}", config.name);
            if url.trim().is_empty() {
                errors.push(ValidationError {
                    field: "destination.Rest.url".to_string(),
                    message: "REST destination URL must not be empty".to_string(),
                    context: ctx.clone(),
                });
            } else {
                // Parse and validate URL scheme.
                let parsed = url::Url::parse(url).map_err(|e| ValidationError {
                    field: "destination.Rest.url".to_string(),
                    message: format!("invalid URL: {e}"),
                    context: ctx.clone(),
                });
                match parsed {
                    Ok(u) => {
                        // Reject embedded URL userinfo — secrets must live in
                        // secrets.toml, never in the manifest URL. A URL with
                        // userinfo (`scheme://user:pass@host`) is a secret-leak
                        // vector via logs, Debug output, and reqwest error
                        // Display strings.
                        if !u.username().is_empty() || u.password().is_some() {
                            errors.push(ValidationError {
                                field: "destination.Rest.url".to_string(),
                                message: "REST URL must not contain userinfo (user:password@); store credentials in secrets.toml via the auth config".to_string(),
                                context: ctx.clone(),
                            });
                        }
                        let allow_http = allow_http.unwrap_or(false);
                        let scheme = u.scheme();
                        let scheme_ok = match scheme {
                            "https" => true,
                            "http" => allow_http,
                            _ => false,
                        };
                        if !scheme_ok {
                            errors.push(ValidationError {
                                field: "destination.Rest.url".to_string(),
                                message: if scheme == "http" {
                                    "REST URL scheme 'http' is not allowed by default; set allow_http: true to opt in (intended for localhost testing only)".to_string()
                                } else {
                                    format!("REST URL scheme '{scheme}' is not supported; use https (or http with allow_http: true for localhost)")
                                },
                                context: ctx.clone(),
                            });
                        }
                    }
                    Err(e) => {
                        errors.push(e);
                    }
                }
            }

            // Method validation (default POST).
            let method_str = method.as_deref().unwrap_or("POST").to_uppercase();
            if !matches!(
                method_str.as_str(),
                "GET" | "POST" | "PUT" | "PATCH" | "DELETE"
            ) {
                errors.push(ValidationError {
                    field: "destination.Rest.method".to_string(),
                    message: format!(
                        "REST method '{method_str}' is not supported; expected one of GET, POST, PUT, PATCH, DELETE"
                    ),
                    context: ctx.clone(),
                });
            }

            // Header names legality + values no CRLF.
            if let Some(headers) = headers {
                for h in headers {
                    if http::HeaderName::from_bytes(h.name.as_bytes()).is_err() {
                        errors.push(ValidationError {
                            field: format!("destination.Rest.headers[{}]", h.name),
                            message: format!("invalid HTTP header name '{}'", h.name),
                            context: ctx.clone(),
                        });
                    }
                    if h.value.contains(['\r', '\n']) {
                        errors.push(ValidationError {
                            field: format!("destination.Rest.headers[{}]", h.name),
                            message: format!(
                                "header value for '{}' contains CRLF — header injection is not allowed",
                                h.name
                            ),
                            context: ctx.clone(),
                        });
                    }
                }
            }

            // Auth validation.
            if let Some(auth) = auth {
                match auth {
                    AuthConfig::ApiKey { header_name, .. } => {
                        if header_name.trim().is_empty() {
                            errors.push(ValidationError {
                                field: "destination.Rest.auth.api_key.header_name".to_string(),
                                message: "api_key auth requires a non-empty header_name"
                                    .to_string(),
                                context: ctx.clone(),
                            });
                        } else if http::HeaderName::from_bytes(header_name.as_bytes()).is_err() {
                            errors.push(ValidationError {
                                field: "destination.Rest.auth.api_key.header_name".to_string(),
                                message: format!(
                                    "invalid HTTP header name for api_key auth: '{header_name}'"
                                ),
                                context: ctx.clone(),
                            });
                        }
                    }
                    AuthConfig::Bearer { .. } | AuthConfig::Basic { .. } | AuthConfig::None => {}
                }
            }

            // Body template parse check (fail fast at config load).
            if let Some(template) = body_template {
                if let Err(e) = minijinja::Environment::new().add_template("body", template) {
                    errors.push(ValidationError {
                        field: "destination.Rest.body_template".to_string(),
                        message: format!("invalid minijinja body_template: {e}"),
                        context: ctx.clone(),
                    });
                }
            }

            // Timeout sanity.
            if let Some(t) = timeout_secs {
                if *t == 0 {
                    errors.push(ValidationError {
                        field: "destination.Rest.timeout_secs".to_string(),
                        message: "timeout_secs must be greater than 0".to_string(),
                        context: ctx.clone(),
                    });
                }
            }
            if let Some(t) = connect_timeout_secs {
                if *t == 0 {
                    errors.push(ValidationError {
                        field: "destination.Rest.connect_timeout_secs".to_string(),
                        message: "connect_timeout_secs must be greater than 0".to_string(),
                        context: ctx.clone(),
                    });
                }
            }
            if let Some(m) = max_response_bytes {
                if *m == 0 {
                    errors.push(ValidationError {
                        field: "destination.Rest.max_response_bytes".to_string(),
                        message: "max_response_bytes must be greater than 0".to_string(),
                        context: ctx.clone(),
                    });
                } else if *m > MAX_RESPONSE_BYTES_CAP {
                    errors.push(ValidationError {
                        field: "destination.Rest.max_response_bytes".to_string(),
                        message: format!(
                            "max_response_bytes ({m}) exceeds the maximum allowed cap of {MAX_RESPONSE_BYTES_CAP} bytes"
                        ),
                        context: ctx.clone(),
                    });
                }
            }
            if let Some(m) = max_batch_size {
                if *m == 0 {
                    errors.push(ValidationError {
                        field: "destination.Rest.max_batch_size".to_string(),
                        message: "max_batch_size must be at least 1".to_string(),
                        context: ctx.clone(),
                    });
                }
            }
            let _ = allow_http; // already consumed by URL scheme check
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
        DestinationConfig::GoogleSheets {
            spreadsheet_id,
            sheet,
            key_column,
            service_account_key_file,
            max_rows,
            max_batch_size,
            timeout_secs,
            connect_timeout_secs,
            max_response_bytes,
        } => {
            let ctx = format!("sync:{}", config.name);

            // Spreadsheet ID: required + regex.
            if spreadsheet_id.trim().is_empty() {
                errors.push(ValidationError {
                    field: "destination.GoogleSheets.spreadsheet_id".to_string(),
                    message: "Google Sheets destination spreadsheet_id must not be empty"
                        .to_string(),
                    context: ctx.clone(),
                });
            } else {
                let re = regex::Regex::new(GOOGLE_SHEETS_SPREADSHEET_ID_REGEX)
                    .expect("static regex is valid");
                if !re.is_match(spreadsheet_id) {
                    errors.push(ValidationError {
                        field: "destination.GoogleSheets.spreadsheet_id".to_string(),
                        message: format!(
                            "spreadsheet_id '{spreadsheet_id}' contains characters outside [A-Za-z0-9_-]; this is unsafe for URL interpolation"
                        ),
                        context: ctx.clone(),
                    });
                }
            }

            // Sheet/tab name.
            if sheet.trim().is_empty() {
                errors.push(ValidationError {
                    field: "destination.GoogleSheets.sheet".to_string(),
                    message: "Google Sheets destination sheet must not be empty".to_string(),
                    context: ctx.clone(),
                });
            }

            // Key column name.
            if key_column.trim().is_empty() {
                errors.push(ValidationError {
                    field: "destination.GoogleSheets.key_column".to_string(),
                    message: "Google Sheets destination key_column must not be empty".to_string(),
                    context: ctx.clone(),
                });
            }

            // Credential file path. Resolution from secrets.toml happens in
            // `SyncConfig::resolve_secrets` (after env-substitution). At
            // validation time the field may still be empty if a `secrets.toml`
            // resolution was not performed (e.g. loading `SyncConfig` directly
            // without a secrets file). We surface an empty path as an error so
            // operators do not discover this at first write.
            if service_account_key_file.trim().is_empty() {
                errors.push(ValidationError {
                    field: "destination.GoogleSheets.service_account_key_file".to_string(),
                    message: "Google Sheets destination service_account_key_file must not be empty (set it directly, via env var, or in [destination.google_sheets] of secrets.toml)".to_string(),
                    context: ctx.clone(),
                });
            }

            // max_rows: row 1 is the header, so at least 2 rows are required
            // for any data write to be possible.
            if *max_rows < 2 {
                errors.push(ValidationError {
                    field: "destination.GoogleSheets.max_rows".to_string(),
                    message: "max_rows must be at least 2 (one header row + one data row)"
                        .to_string(),
                    context: ctx.clone(),
                });
            }

            if let Some(m) = max_batch_size {
                if *m == 0 {
                    errors.push(ValidationError {
                        field: "destination.GoogleSheets.max_batch_size".to_string(),
                        message: "max_batch_size must be at least 1".to_string(),
                        context: ctx.clone(),
                    });
                }
            }

            if let Some(t) = timeout_secs {
                if *t == 0 {
                    errors.push(ValidationError {
                        field: "destination.GoogleSheets.timeout_secs".to_string(),
                        message: "timeout_secs must be greater than 0".to_string(),
                        context: ctx.clone(),
                    });
                }
            }
            if let Some(t) = connect_timeout_secs {
                if *t == 0 {
                    errors.push(ValidationError {
                        field: "destination.GoogleSheets.connect_timeout_secs".to_string(),
                        message: "connect_timeout_secs must be greater than 0".to_string(),
                        context: ctx.clone(),
                    });
                }
            }
            if let Some(m) = max_response_bytes {
                if *m == 0 {
                    errors.push(ValidationError {
                        field: "destination.GoogleSheets.max_response_bytes".to_string(),
                        message: "max_response_bytes must be greater than 0".to_string(),
                        context: ctx.clone(),
                    });
                } else if *m > MAX_RESPONSE_BYTES_CAP {
                    errors.push(ValidationError {
                        field: "destination.GoogleSheets.max_response_bytes".to_string(),
                        message: format!(
                            "max_response_bytes ({m}) exceeds the maximum allowed cap of {MAX_RESPONSE_BYTES_CAP} bytes"
                        ),
                        context: ctx.clone(),
                    });
                }
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
            // Mirror mode doesn't require CDC config. However, the Google
            // Sheets destination does not implement `replace_all` (it is a
            // key-based upsert only) and `RemoveCapability::None`, so mirror
            // dispatch would either call an unsupported `replace_all` or fall
            // back to `write` while leaving target-only rows untouched —
            // contradicting the user-visible mirror contract. Reject mirror
            // mode for Google Sheets destinations at validation time.
            if matches!(config.destination, DestinationConfig::GoogleSheets { .. }) {
                errors.push(ValidationError {
                    field: "sync.mode".to_string(),
                    message: "Google Sheets destination does not support mirror mode (it is key-based upsert only with no remove/replace_all capability); use incremental or full_refresh".to_string(),
                    context: format!("sync:{}", config.name),
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

/// Validate dbt refs across all sync configs.
///
/// Checks that:
/// 1. If any sync uses `ModelConfig::Ref`, `dbt.manifest_path` is configured.
/// 2. If a manifest is loaded, all refs resolve to actual models.
pub fn validate_dbt_refs(
    config: &FerryConfig,
    manifest: Option<&Manifest>,
    sync_configs: &[SyncConfig],
) -> Result<(), Vec<ValidationError>> {
    let mut errors: Vec<ValidationError> = Vec::new();

    let has_refs = sync_configs
        .iter()
        .any(|s| matches!(s.model, ModelConfig::Ref { .. }));

    if !has_refs {
        return Ok(());
    }

    // Check that dbt.manifest_path is configured
    let manifest_configured = config
        .dbt
        .as_ref()
        .and_then(|d| d.manifest_path.as_deref())
        .map(|p| !p.is_empty())
        .unwrap_or(false);

    if !manifest_configured {
        errors.push(ValidationError {
            field: "dbt.manifest_path".to_string(),
            message: "One or more syncs use model.ref but dbt.manifest_path is not configured in ferry.yml".to_string(),
            context: "ferry.yml".to_string(),
        });
        return Err(errors);
    }

    // If manifest is loaded, verify all refs resolve
    if let Some(manifest) = manifest {
        for sync_config in sync_configs {
            if let ModelConfig::Ref { r#ref } = &sync_config.model {
                if let Err(e) = manifest.resolve_ref(r#ref) {
                    errors.push(ValidationError {
                        field: format!("model.ref: {}", r#ref),
                        message: e.to_string(),
                        context: format!("sync:{}", sync_config.name),
                    });
                }
            }
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
                auth: None,
                body_template: None,
                timeout_secs: None,
                connect_timeout_secs: None,
                max_response_bytes: None,
                allow_http: None,
                max_batch_size: None,
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
                auth: None,
                body_template: None,
                timeout_secs: None,
                connect_timeout_secs: None,
                max_response_bytes: None,
                allow_http: None,
                max_batch_size: None,
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
