use std::path::Path;

use serde::Deserialize;
use serde::Deserializer;
use serde::de;

use crate::env_sub::substitute_env_vars;
use crate::error::FerryError;
use crate::secrets::Secrets;
use crate::validation::{validate_ferry_config, validate_sync_config};

// ---------------------------------------------------------------------------
// Top-level config
// ---------------------------------------------------------------------------

/// The root ferry project configuration, loaded from `ferry.yml`.
#[derive(Debug, Deserialize)]
pub struct FerryConfig {
    pub name: String,
    pub version: Option<String>,
    pub source: SourceConfig,
    pub state: StateConfig,
    pub dbt: Option<DbtConfig>,
    pub defaults: Option<SyncSettings>,
}

// ---------------------------------------------------------------------------
// Source config
// ---------------------------------------------------------------------------

/// Source connector configuration, tagged by `type`.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SourceConfig {
    DuckDB {
        path: String,
        query: Option<String>,
    },
    Postgres {
        connection_string: String,
        query: Option<String>,
    },
}

// ---------------------------------------------------------------------------
// State config
// ---------------------------------------------------------------------------

/// State backend configuration.
#[derive(Debug, Deserialize)]
pub struct StateConfig {
    pub backend: StateBackend,
    pub path: Option<String>,
}

/// Supported state backends.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StateBackend {
    DuckDB,
    Warehouse,
}

// ---------------------------------------------------------------------------
// DBT config
// ---------------------------------------------------------------------------

/// Optional dbt integration configuration.
#[derive(Debug, Deserialize)]
pub struct DbtConfig {
    pub manifest_path: Option<String>,
}

// ---------------------------------------------------------------------------
// Sync config
// ---------------------------------------------------------------------------

/// A single sync configuration, loaded from `syncs/*.yml`.
#[derive(Debug, Deserialize)]
pub struct SyncConfig {
    pub name: String,
    pub description: Option<String>,
    pub tags: Option<Vec<String>>,
    pub model: ModelConfig,
    pub destination: DestinationConfig,
    pub sync: SyncSettings,
    pub tests: Option<serde_json::Value>,
}

// ---------------------------------------------------------------------------
// Model config
// ---------------------------------------------------------------------------

/// Model definition — either inline SQL or a reference to a dbt model.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum ModelConfig {
    Sql { sql: String },
    Ref { r#ref: String },
}

// ---------------------------------------------------------------------------
// Destination config
// ---------------------------------------------------------------------------

/// Destination connector configuration, tagged by `type`.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DestinationConfig {
    Braze {
        api_key: String,
        endpoint: String,
        #[serde(default)]
        app_id: Option<String>,
    },
    Slack {
        webhook_url: String,
    },
    Rest {
        url: String,
        #[serde(default)]
        method: Option<String>,
        #[serde(default)]
        headers: Option<Vec<HeaderConfig>>,
        #[serde(default)]
        auth: Option<AuthConfig>,
        #[serde(default)]
        body_template: Option<String>,
        #[serde(default)]
        timeout_secs: Option<u64>,
        #[serde(default)]
        connect_timeout_secs: Option<u64>,
        #[serde(default)]
        max_response_bytes: Option<usize>,
        #[serde(default)]
        allow_http: Option<bool>,
        #[serde(default)]
        max_batch_size: Option<usize>,
    },
    File {
        output_dir: String,
        format: Option<FileFormat>,
    },
    /// Google Sheets v4 destination. Authenticated via a service-account
    /// JSON key (path resolved through env vars / `secrets.toml`). Writes are
    /// key-based upserts against explicit A1 ranges using
    /// `spreadsheets.values.batchUpdate` with `valueInputOption=RAW`.
    GoogleSheets {
        spreadsheet_id: String,
        sheet: String,
        key_column: String,
        service_account_key_file: String,
        max_rows: usize,
        #[serde(default)]
        max_batch_size: Option<usize>,
        #[serde(default)]
        timeout_secs: Option<u64>,
        #[serde(default)]
        connect_timeout_secs: Option<u64>,
        #[serde(default)]
        max_response_bytes: Option<usize>,
    },
}

/// An HTTP header for REST destinations.
#[derive(Debug, Clone, Deserialize)]
pub struct HeaderConfig {
    pub name: String,
    pub value: String,
}

/// Authentication configuration for REST destinations.
///
/// Secrets (token, password, api key value) are masked in `Debug` output to
/// avoid leaking them through `FerryConfig`'s derived `Debug`.
#[derive(Deserialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthConfig {
    /// `Authorization: Bearer <token>` header.
    Bearer {
        #[serde(default)]
        token: String,
    },
    /// `Authorization: Basic <base64(user:pass)>` header.
    Basic {
        #[serde(default)]
        username: String,
        #[serde(default)]
        password: String,
    },
    /// A custom header (e.g. `X-Api-Key: <value>`).
    ApiKey {
        #[serde(default)]
        header_name: String,
        #[serde(default)]
        value: String,
    },
    /// No auth (default).
    None,
}

impl std::fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthConfig::Bearer { .. } => f.debug_struct("Bearer").field("token", &"***").finish(),
            AuthConfig::Basic { username, .. } => f
                .debug_struct("Basic")
                .field("username", username)
                .field("password", &"***")
                .finish(),
            AuthConfig::ApiKey { header_name, .. } => f
                .debug_struct("ApiKey")
                .field("header_name", header_name)
                .field("value", &"***")
                .finish(),
            AuthConfig::None => f.write_str("None"),
        }
    }
}

/// Output file format for file destinations.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileFormat {
    Csv,
    Json,
}

// ---------------------------------------------------------------------------
// Sync settings
// ---------------------------------------------------------------------------

/// Sync-specific settings.
#[derive(Debug, Deserialize)]
pub struct SyncSettings {
    pub mode: SyncMode,
    #[serde(default)]
    pub cursor_field: Option<String>,
    #[serde(default)]
    pub cdc: Option<CdcConfig>,
    #[serde(default)]
    pub delivery: Option<DeliveryConfig>,
    #[serde(default)]
    pub full_refresh: Option<FullRefreshConfig>,
}

/// Sync mode.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SyncMode {
    Incremental,
    FullRefresh,
    Mirror,
}

// ---------------------------------------------------------------------------
// CDC config
// ---------------------------------------------------------------------------

/// Change data capture configuration.
#[derive(Debug, Deserialize)]
pub struct CdcConfig {
    pub method: CdcMethod,
    #[serde(default)]
    pub hash_columns: Option<HashColumns>,
}

/// CDC method.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CdcMethod {
    Hash,
    Cursor,
}

/// Which columns to include in hash-based CDC.
///
/// In YAML config, this can be:
/// - `hash_columns: all` → `HashColumns::All`
/// - `hash_columns: [col1, col2]` → `HashColumns::Explicit([col1, col2])`
///
/// Uses a custom deserializer because `#[serde(untagged)]` would match the
/// string `"all"` as `Explicit(vec!["all"])` instead of `All`.
#[derive(Debug)]
pub enum HashColumns {
    All,
    Explicit(Vec<String>),
}

impl<'de> Deserialize<'de> for HashColumns {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = yaml_serde::Value::deserialize(deserializer)?;
        match value {
            yaml_serde::Value::String(s) if s == "all" => Ok(HashColumns::All),
            yaml_serde::Value::Sequence(seq) => {
                let cols: Vec<String> = seq
                    .into_iter()
                    .map(|v| serde::de::Deserialize::deserialize(v).map_err(de::Error::custom))
                    .collect::<Result<_, _>>()?;
                Ok(HashColumns::Explicit(cols))
            }
            _ => Err(de::Error::custom(
                "hash_columns must be 'all' or a list of column names",
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// Delivery config
// ---------------------------------------------------------------------------

/// Delivery pipeline configuration.
#[derive(Debug, Deserialize)]
pub struct DeliveryConfig {
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default)]
    pub retry: Option<RetryConfig>,
    #[serde(default)]
    pub on_reject: Option<RejectConfig>,
    #[serde(default)]
    pub dead_letter: Option<DeadLetterConfig>,
    #[serde(default)]
    pub allow_redelivery: bool,
}

fn default_batch_size() -> usize {
    1000
}

// ---------------------------------------------------------------------------
// Retry config
// ---------------------------------------------------------------------------

/// Retry policy configuration.
#[derive(Debug, Deserialize)]
pub struct RetryConfig {
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default)]
    pub backoff: BackoffStrategy,
    #[serde(default = "default_initial_delay_secs")]
    pub initial_delay_secs: u64,
    #[serde(default = "default_max_delay_secs")]
    pub max_delay_secs: u64,
}

fn default_max_attempts() -> u32 {
    3
}

fn default_initial_delay_secs() -> u64 {
    5
}

fn default_max_delay_secs() -> u64 {
    300
}

/// Backoff strategy.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackoffStrategy {
    Exponential,
    Linear,
    Fixed,
}

impl Default for BackoffStrategy {
    fn default() -> Self {
        Self::Exponential
    }
}

// ---------------------------------------------------------------------------
// Reject config
// ---------------------------------------------------------------------------

/// Error classification and action configuration.
#[derive(Debug, Deserialize)]
pub struct RejectConfig {
    pub classify: Vec<RejectRule>,
}

/// A single reject classification rule.
#[derive(Debug, Deserialize)]
pub struct RejectRule {
    #[serde(rename = "match")]
    pub match_: RejectMatch,
    pub action: RejectAction,
}

/// Conditions for matching a reject rule.
#[derive(Debug, Deserialize)]
pub struct RejectMatch {
    pub status_code: Option<u16>,
    pub body_contains: Option<String>,
}

/// Action to take when a reject rule matches.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectAction {
    Retry,
    DeadLetter,
    Skip,
    FailSync,
}

// ---------------------------------------------------------------------------
// Dead letter config
// ---------------------------------------------------------------------------

/// Dead letter queue configuration.
#[derive(Debug, Deserialize)]
pub struct DeadLetterConfig {
    #[serde(default = "default_max_age_secs")]
    pub max_age_secs: u64,
    #[serde(default)]
    pub alert: bool,
}

fn default_max_age_secs() -> u64 {
    604800 // 7 days
}

// ---------------------------------------------------------------------------
// Full refresh config
// ---------------------------------------------------------------------------

/// Full refresh configuration.
#[derive(Debug, Deserialize)]
pub struct FullRefreshConfig {
    #[serde(default)]
    pub schedule: Option<String>,
}

// ---------------------------------------------------------------------------
// Loading methods
// ---------------------------------------------------------------------------

impl FerryConfig {
    /// Load a `FerryConfig` from a project directory.
    ///
    /// Reads `ferry.yml` from the project directory, substitutes environment
    /// variables, parses the YAML, loads `secrets.toml` if present (checking
    /// permissions 600), and validates the config.
    pub fn load(project_dir: &Path) -> Result<Self, FerryError> {
        let ferry_yml_path = project_dir.join("ferry.yml");
        if !ferry_yml_path.exists() {
            return Err(FerryError::Config(format!(
                "ferry.yml not found in {}",
                project_dir.display()
            )));
        }

        let raw = std::fs::read_to_string(&ferry_yml_path)
            .map_err(|e| FerryError::Config(format!("Cannot read ferry.yml: {e}")))?;

        let substituted = substitute_env_vars(&raw)?;

        let mut config: FerryConfig = yaml_serde::from_str(&substituted)
            .map_err(|e| FerryError::Config(format!("Cannot parse ferry.yml: {e}")))?;

        // Load secrets.toml if present
        let secrets_path = project_dir.join("secrets.toml");
        if let Some(secrets) = Secrets::load(&secrets_path)? {
            // Resolve secrets into config fields
            config.resolve_secrets(&secrets);
        }

        // Validate
        validate_ferry_config(&config).map_err(|errors| {
            let msgs: Vec<String> = errors.iter().map(|e| e.to_string()).collect();
            FerryError::Validation(msgs.join("\n"))
        })?;

        Ok(config)
    }

    /// Resolve secret values from the secrets file into config fields.
    fn resolve_secrets(&mut self, secrets: &Secrets) {
        // Resolve source secrets
        match &mut self.source {
            SourceConfig::DuckDB { path, query: _ } => {
                if let Some(resolved) = secrets.resolve("source.duckdb", "path") {
                    *path = resolved;
                }
            }
            SourceConfig::Postgres {
                connection_string,
                query: _,
            } => {
                if let Some(resolved) = secrets.resolve("source.postgres", "connection_string") {
                    *connection_string = resolved;
                }
            }
        }
    }
}

impl SyncConfig {
    /// Load a single sync configuration from a YAML file path.
    ///
    /// Substitutes environment variables before parsing. If a `secrets.toml`
    /// exists in the project directory (the parent of the file's directory, or
    /// the file's directory itself), destination secrets are resolved into
    /// the config before validation.
    pub fn load(path: &Path) -> Result<Self, FerryError> {
        let raw = std::fs::read_to_string(path).map_err(|e| {
            FerryError::Config(format!("Cannot read sync file {}: {e}", path.display()))
        })?;

        let substituted = substitute_env_vars(&raw)?;

        let mut config: SyncConfig = yaml_serde::from_str(&substituted).map_err(|e| {
            FerryError::Config(format!("Cannot parse sync file {}: {e}", path.display()))
        })?;

        // Resolve destination secrets from secrets.toml if present in the
        // project directory (parent of the sync file's directory, or the
        // directory itself for direct loads).
        if let Some(secrets) = find_secrets_for(path)? {
            config.resolve_secrets(&secrets);
        }

        // Validate
        validate_sync_config(&config).map_err(|errors| {
            let msgs: Vec<String> = errors.iter().map(|e| e.to_string()).collect();
            FerryError::Validation(msgs.join("\n"))
        })?;

        Ok(config)
    }

    /// Load all sync configurations from a directory of `*.yml` files.
    ///
    /// Reads every file matching `*.yml` in the given directory, parses each
    /// as a `SyncConfig`, resolves destination secrets from `secrets.toml`
    /// in the project directory (parent of the syncs dir), and returns them
    /// in a `Vec`.
    pub fn load_all(syncs_dir: &Path) -> Result<Vec<Self>, FerryError> {
        if !syncs_dir.exists() {
            return Err(FerryError::Config(format!(
                "Syncs directory not found: {}",
                syncs_dir.display()
            )));
        }

        let mut configs = Vec::new();

        let dir_entries = std::fs::read_dir(syncs_dir)
            .map_err(|e| FerryError::Config(format!("Cannot read syncs directory: {e}")))?;

        let mut yml_paths: Vec<std::path::PathBuf> = dir_entries
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) == Some("yml") {
                    Some(path)
                } else {
                    None
                }
            })
            .collect();

        // Sort for deterministic ordering
        yml_paths.sort();

        for path in yml_paths {
            let config = SyncConfig::load(&path)?;
            configs.push(config);
        }

        Ok(configs)
    }

    /// Resolve destination secret values from `secrets.toml` into this sync's
    /// destination config. Source secrets are handled at the `FerryConfig`
    /// level (`FerryConfig::resolve_secrets`).
    pub fn resolve_secrets(&mut self, secrets: &Secrets) {
        match &mut self.destination {
            DestinationConfig::Rest { auth, headers, .. } => {
                // Auth resolution
                if let Some(auth) = auth.as_mut() {
                    resolve_auth_secrets(auth, secrets);
                }
                // Raw header value resolution: each header value is treated as
                // a secret key under `destination.rest` (e.g. `header.Authorization`
                // or simply the header name lowercased). The header value in YAML
                // is interpreted as a key into `destination.rest`.
                if let Some(headers) = headers.as_mut() {
                    for h in headers.iter_mut() {
                        let key = h.value.trim();
                        if key.is_empty() {
                            continue;
                        }
                        // First try the raw key, then `header.{key}`.
                        if let Some(v) = secrets.resolve("destination.rest", key).or_else(|| {
                            secrets.resolve("destination.rest", &format!("header.{key}"))
                        }) {
                            h.value = v;
                        }
                    }
                }
            }
            DestinationConfig::GoogleSheets {
                service_account_key_file,
                ..
            } => {
                // An empty `service_account_key_file` in YAML signals "resolve
                // from secrets.toml". Direct paths and env-substituted values
                // remain valid and are left untouched.
                if service_account_key_file.trim().is_empty() {
                    if let Some(v) =
                        secrets.resolve("destination.google_sheets", "service_account_key_file")
                    {
                        *service_account_key_file = v;
                    }
                }
            }
            DestinationConfig::Braze { .. }
            | DestinationConfig::Slack { .. }
            | DestinationConfig::File { .. } => {}
        }
    }
}

/// Resolve a secret value from `secrets.toml`'s `[destination.rest]` section.
fn resolve_auth_secrets(auth: &mut AuthConfig, secrets: &Secrets) {
    let section = "destination.rest";
    match auth {
        AuthConfig::Bearer { token } => {
            if token.trim().is_empty() {
                if let Some(v) = secrets.resolve(section, "bearer_token") {
                    *token = v;
                } else if let Some(v) = secrets.resolve(section, "token") {
                    *token = v;
                }
            }
        }
        AuthConfig::Basic { username, password } => {
            if username.trim().is_empty() {
                if let Some(v) = secrets.resolve(section, "basic_username") {
                    *username = v;
                } else if let Some(v) = secrets.resolve(section, "username") {
                    *username = v;
                }
            }
            if password.trim().is_empty() {
                if let Some(v) = secrets.resolve(section, "basic_password") {
                    *password = v;
                } else if let Some(v) = secrets.resolve(section, "password") {
                    *password = v;
                }
            }
        }
        AuthConfig::ApiKey { header_name, value } => {
            if header_name.trim().is_empty() {
                if let Some(v) = secrets.resolve(section, "api_key_header_name") {
                    *header_name = v;
                }
            }
            if value.trim().is_empty() {
                if let Some(v) = secrets.resolve(section, "api_key") {
                    *value = v;
                }
            }
        }
        AuthConfig::None => {}
    }
}

/// Locate a `secrets.toml` file relevant to the given path.
///
/// Looks in the parent of the file's directory (the project dir for the
/// typical `syncs/*.yml` layout), then the file's directory itself. Returns
/// `None` if no secrets file exists (no error — secrets are optional).
/// Propagates errors from `Secrets::load` (e.g. insecure permissions) so
/// operators learn about misconfigured secrets files rather than silently
/// missing resolution.
fn find_secrets_for(path: &Path) -> Result<Option<Secrets>, FerryError> {
    let file_dir = match path.parent() {
        Some(d) => d,
        None => return Ok(None),
    };
    let project_dir = file_dir.parent().unwrap_or(file_dir);
    let candidates = [
        project_dir.join("secrets.toml"),
        file_dir.join("secrets.toml"),
    ];
    for cand in candidates {
        if cand.exists() {
            return Secrets::load(&cand);
        }
    }
    Ok(None)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_load_valid_ferry_yml() {
        let dir = tempfile::tempdir().unwrap();
        let ferry_yml = dir.path().join("ferry.yml");
        let mut file = std::fs::File::create(&ferry_yml).unwrap();
        write!(
            file,
            r#"
name: test_project
version: "1.0"
source:
  type: duckdb
  path: /data/db.duckdb
  query: SELECT * FROM users
state:
  backend: duckdb
  path: .ferry/state.db
"#
        )
        .unwrap();

        let config = FerryConfig::load(dir.path()).unwrap();
        assert_eq!(config.name, "test_project");
        assert_eq!(config.version, Some("1.0".to_string()));
        match &config.source {
            SourceConfig::DuckDB { path, query } => {
                assert_eq!(path, "/data/db.duckdb");
                assert_eq!(query.as_deref(), Some("SELECT * FROM users"));
            }
            SourceConfig::Postgres { .. } => {
                panic!("Expected DuckDB source config, got Postgres");
            }
        }
    }

    #[test]
    fn test_load_valid_sync_yml() {
        let dir = tempfile::tempdir().unwrap();
        let sync_yml = dir.path().join("test_sync.yml");
        let mut file = std::fs::File::create(&sync_yml).unwrap();
        write!(
            file,
            r#"
name: test_sync
description: A test sync
tags:
  - test
model:
  sql: SELECT id, name FROM users
destination:
  type: rest
  url: https://api.example.com/users
  method: POST
sync:
  mode: incremental
  cursor_field: updated_at
  cdc:
    method: cursor
  delivery:
    batch_size: 100
    retry:
      max_attempts: 3
      backoff: exponential
      initial_delay_secs: 5
      max_delay_secs: 300
"#
        )
        .unwrap();

        let config = SyncConfig::load(&sync_yml).unwrap();
        assert_eq!(config.name, "test_sync");
        assert_eq!(config.description, Some("A test sync".to_string()));
        assert_eq!(config.tags, Some(vec!["test".to_string()]));
        match &config.model {
            ModelConfig::Sql { sql } => {
                assert_eq!(sql, "SELECT id, name FROM users");
            }
            _ => panic!("Expected Sql model"),
        }
        match &config.destination {
            DestinationConfig::Rest { url, method, .. } => {
                assert_eq!(url, "https://api.example.com/users");
                assert_eq!(method.as_deref(), Some("POST"));
            }
            _ => panic!("Expected Rest destination"),
        }
        assert_eq!(config.sync.mode, SyncMode::Incremental);
        assert_eq!(config.sync.cursor_field, Some("updated_at".to_string()));
    }

    #[test]
    fn test_load_sync_with_env_vars() {
        // SAFETY: test-only env var manipulation, single-threaded
        unsafe {
            std::env::set_var("FERRY_TEST_URL", "https://api.test.com/endpoint");
            std::env::set_var("FERRY_TEST_BATCH_SIZE", "250");
        }

        let dir = tempfile::tempdir().unwrap();
        let sync_yml = dir.path().join("env_sync.yml");
        let mut file = std::fs::File::create(&sync_yml).unwrap();
        write!(
            file,
            r#"
name: env_sync
model:
  sql: SELECT * FROM items
destination:
  type: rest
  url: ${{FERRY_TEST_URL}}
  method: POST
sync:
  mode: incremental
  cursor_field: id
  delivery:
    batch_size: ${{FERRY_TEST_BATCH_SIZE}}
"#
        )
        .unwrap();

        let config = SyncConfig::load(&sync_yml).unwrap();
        match &config.destination {
            DestinationConfig::Rest { url, .. } => {
                assert_eq!(url, "https://api.test.com/endpoint");
            }
            _ => panic!("Expected Rest destination"),
        }
        assert_eq!(config.sync.delivery.as_ref().unwrap().batch_size, 250);

        unsafe {
            std::env::remove_var("FERRY_TEST_URL");
            std::env::remove_var("FERRY_TEST_BATCH_SIZE");
        }
    }

    #[test]
    fn test_load_all_syncs() {
        let dir = tempfile::tempdir().unwrap();
        let syncs_dir = dir.path().join("syncs");
        std::fs::create_dir(&syncs_dir).unwrap();

        // Create two sync files
        let sync1 = syncs_dir.join("users.yml");
        let mut f1 = std::fs::File::create(&sync1).unwrap();
        write!(
            f1,
            r#"
name: users_sync
model:
  sql: SELECT * FROM users
destination:
  type: rest
  url: https://api.example.com/users
sync:
  mode: incremental
  cursor_field: id
"#
        )
        .unwrap();

        let sync2 = syncs_dir.join("orders.yml");
        let mut f2 = std::fs::File::create(&sync2).unwrap();
        write!(
            f2,
            r#"
name: orders_sync
model:
  sql: SELECT * FROM orders
destination:
  type: rest
  url: https://api.example.com/orders
sync:
  mode: incremental
  cursor_field: id
"#
        )
        .unwrap();

        // Create a non-yml file that should be ignored
        let _readme = syncs_dir.join("README.md");
        std::fs::write(&_readme, "not a config").unwrap();

        let configs = SyncConfig::load_all(&syncs_dir).unwrap();
        assert_eq!(configs.len(), 2);
        let names: Vec<&str> = configs.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"orders_sync"));
        assert!(names.contains(&"users_sync"));
    }

    #[test]
    fn test_load_missing_ferry_yml() {
        let dir = tempfile::tempdir().unwrap();
        // No ferry.yml in this dir
        let result = FerryConfig::load(dir.path());
        assert!(result.is_err());
        match result.unwrap_err() {
            FerryError::Config(msg) => {
                assert!(msg.contains("ferry.yml not found"));
            }
            other => panic!("Expected Config error, got {:?}", other),
        }
    }

    #[test]
    fn test_load_missing_syncs_dir() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("nonexistent_syncs");
        let result = SyncConfig::load_all(&missing);
        assert!(result.is_err());
        match result.unwrap_err() {
            FerryError::Config(msg) => {
                assert!(msg.contains("Syncs directory not found"));
            }
            other => panic!("Expected Config error, got {:?}", other),
        }
    }

    #[test]
    fn test_load_invalid_yaml() {
        let dir = tempfile::tempdir().unwrap();
        let sync_yml = dir.path().join("bad.yml");
        std::fs::write(&sync_yml, "name: [invalid yaml: unclosed").unwrap();
        let result = SyncConfig::load(&sync_yml);
        assert!(result.is_err());
    }

    #[test]
    fn test_hash_columns_all_from_string() {
        // Deserialize YAML with hash_columns: all → should be HashColumns::All
        let yaml_str = r#"
method: hash
hash_columns: all
"#;
        let config: CdcConfig = yaml_serde::from_str(yaml_str).unwrap();
        assert!(
            matches!(config.hash_columns, Some(HashColumns::All)),
            "hash_columns: all should deserialize to HashColumns::All, got {:?}",
            config.hash_columns
        );
    }

    #[test]
    fn test_hash_columns_explicit_list() {
        // Deserialize YAML with hash_columns: [col1, col2] → should be HashColumns::Explicit
        let yaml_str = r#"
method: hash
hash_columns:
  - col1
  - col2
"#;
        let config: CdcConfig = yaml_serde::from_str(yaml_str).unwrap();
        match config.hash_columns {
            Some(HashColumns::Explicit(cols)) => {
                assert_eq!(cols, vec!["col1", "col2"]);
            }
            other => panic!("Expected Explicit([\"col1\", \"col2\"]), got {:?}", other),
        }
    }

    #[test]
    fn test_hash_columns_invalid_value() {
        // Deserialize YAML with hash_columns: 123 → should error
        let yaml_str = r#"
method: hash
hash_columns: 123
"#;
        let result: Result<CdcConfig, _> = yaml_serde::from_str(yaml_str);
        assert!(
            result.is_err(),
            "hash_columns: 123 should fail to deserialize"
        );
    }

    #[test]
    fn test_rest_config_minimal_yaml_backcompat() {
        // Existing minimal REST YAML (url only) must still parse with all
        // optional fields defaulting to None.
        let yaml_str = r#"
name: t
model:
  sql: SELECT 1
destination:
  type: rest
  url: https://api.example.com/users
sync:
  mode: incremental
  cursor_field: id
"#;
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("syncs");
        std::fs::create_dir_all(&p).unwrap();
        let f = p.join("t.yml");
        std::fs::write(&f, yaml_str).unwrap();
        let cfg = SyncConfig::load(&f).unwrap();
        match cfg.destination {
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
                assert_eq!(url, "https://api.example.com/users");
                assert!(method.is_none());
                assert!(headers.is_none());
                assert!(auth.is_none());
                assert!(body_template.is_none());
                assert!(timeout_secs.is_none());
                assert!(connect_timeout_secs.is_none());
                assert!(max_response_bytes.is_none());
                assert!(!allow_http.unwrap_or(false));
                assert!(max_batch_size.is_none());
            }
            _ => panic!("Expected Rest destination"),
        }
    }

    #[test]
    fn test_rest_config_full_fields_parse() {
        let yaml_str = r#"
name: t
model:
  sql: SELECT 1
destination:
  type: rest
  url: https://api.example.com/users
  method: PUT
  headers:
    - name: X-Trace-Id
      value: abc
  auth:
    type: bearer
    token: ""
  body_template: '{"events": {{ rows | tojson }}}'
  timeout_secs: 10
  connect_timeout_secs: 5
  max_response_bytes: 1024
  allow_http: false
  max_batch_size: 50
sync:
  mode: incremental
  cursor_field: id
"#;
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("syncs");
        std::fs::create_dir_all(&p).unwrap();
        let f = p.join("t.yml");
        std::fs::write(&f, yaml_str).unwrap();
        let cfg = SyncConfig::load(&f).unwrap();
        if let DestinationConfig::Rest {
            method,
            headers,
            auth,
            body_template,
            timeout_secs,
            connect_timeout_secs,
            max_response_bytes,
            allow_http,
            max_batch_size,
            ..
        } = cfg.destination
        {
            assert_eq!(method.as_deref(), Some("PUT"));
            assert_eq!(headers.as_ref().unwrap().len(), 1);
            assert!(matches!(auth, Some(AuthConfig::Bearer { .. })));
            assert!(body_template.is_some());
            assert_eq!(timeout_secs, Some(10));
            assert_eq!(connect_timeout_secs, Some(5));
            assert_eq!(max_response_bytes, Some(1024));
            assert_eq!(allow_http, Some(false));
            assert_eq!(max_batch_size, Some(50));
        } else {
            panic!("Expected Rest");
        }
    }

    #[test]
    fn test_rest_config_rejects_http_scheme_by_default() {
        let yaml_str = r#"
name: t
model:
  sql: SELECT 1
destination:
  type: rest
  url: http://localhost:8080/ingest
sync:
  mode: incremental
  cursor_field: id
"#;
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("syncs");
        std::fs::create_dir_all(&p).unwrap();
        let f = p.join("t.yml");
        std::fs::write(&f, yaml_str).unwrap();
        let err = SyncConfig::load(&f).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("http") || msg.contains("allow_http"),
            "expected http-scheme rejection, got: {msg}"
        );
    }

    #[test]
    fn test_rest_config_allows_http_with_flag() {
        let yaml_str = r#"
name: t
model:
  sql: SELECT 1
destination:
  type: rest
  url: http://localhost:8080/ingest
  allow_http: true
sync:
  mode: incremental
  cursor_field: id
"#;
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("syncs");
        std::fs::create_dir_all(&p).unwrap();
        let f = p.join("t.yml");
        std::fs::write(&f, yaml_str).unwrap();
        let cfg = SyncConfig::load(&f).unwrap();
        if let DestinationConfig::Rest { allow_http, .. } = cfg.destination {
            assert_eq!(allow_http, Some(true));
        }
    }

    #[test]
    fn test_rest_config_rejects_bad_method() {
        let yaml_str = r#"
name: t
model:
  sql: SELECT 1
destination:
  type: rest
  url: https://x.example.com
  method: TRACE
sync:
  mode: incremental
  cursor_field: id
"#;
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("syncs");
        std::fs::create_dir_all(&p).unwrap();
        let f = p.join("t.yml");
        std::fs::write(&f, yaml_str).unwrap();
        let err = SyncConfig::load(&f).unwrap_err();
        assert!(err.to_string().contains("method"), "got: {err}");
    }

    #[test]
    fn test_rest_config_rejects_bad_template() {
        let yaml_str = r#"
name: t
model:
  sql: SELECT 1
destination:
  type: rest
  url: https://x.example.com
  body_template: '{% for x in rows %}{{ x | tojson }}{% endif %}'
sync:
  mode: incremental
  cursor_field: id
"#;
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("syncs");
        std::fs::create_dir_all(&p).unwrap();
        let f = p.join("t.yml");
        std::fs::write(&f, yaml_str).unwrap();
        let err = SyncConfig::load(&f).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("body_template") || msg.contains("minijinja"),
            "expected template error, got: {msg}"
        );
    }

    #[test]
    fn test_rest_secrets_resolution_bearer() {
        // secrets.toml in project dir with [destination.rest] bearer_token.
        let dir = tempfile::tempdir().unwrap();
        let syncs = dir.path().join("syncs");
        std::fs::create_dir_all(&syncs).unwrap();
        let yaml_str = r#"
name: t
model:
  sql: SELECT 1
destination:
  type: rest
  url: https://x.example.com
  auth:
    type: bearer
    token: ""
sync:
  mode: incremental
  cursor_field: id
"#;
        std::fs::write(syncs.join("t.yml"), yaml_str).unwrap();

        let secrets_path = dir.path().join("secrets.toml");
        std::fs::write(
            &secrets_path,
            "[destination.rest]\nbearer_token = \"supersecret\"\n",
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&secrets_path, std::fs::Permissions::from_mode(0o600))
                .unwrap();
        }

        let cfg = SyncConfig::load(&syncs.join("t.yml")).unwrap();
        if let DestinationConfig::Rest {
            auth: Some(AuthConfig::Bearer { token }),
            ..
        } = cfg.destination
        {
            assert_eq!(token, "supersecret");
        } else {
            panic!("Expected bearer auth with resolved token");
        }
    }

    #[test]
    fn test_auth_config_debug_redacts_secrets() {
        let auth = AuthConfig::Bearer {
            token: "live-secret-token".to_string(),
        };
        let s = format!("{auth:?}");
        assert!(!s.contains("live-secret-token"), "Debug leaked token: {s}");
        assert!(s.contains("***"));

        let auth = AuthConfig::Basic {
            username: "alice".to_string(),
            password: "p@ss".to_string(),
        };
        let s = format!("{auth:?}");
        assert!(!s.contains("p@ss"));
        assert!(s.contains("alice")); // username is not secret

        let auth = AuthConfig::ApiKey {
            header_name: "X-Api-Key".to_string(),
            value: "val-secret".to_string(),
        };
        let s = format!("{auth:?}");
        assert!(!s.contains("val-secret"));
        assert!(s.contains("X-Api-Key"));
    }

    #[test]
    fn test_rest_config_rejects_url_userinfo() {
        let yaml_str = r#"
name: t
model:
  sql: SELECT 1
destination:
  type: rest
  url: https://user:pass@api.example.com/users
sync:
  mode: incremental
  cursor_field: id
"#;
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("syncs");
        std::fs::create_dir_all(&p).unwrap();
        let f = p.join("t.yml");
        std::fs::write(&f, yaml_str).unwrap();
        let err = SyncConfig::load(&f).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("userinfo"),
            "expected userinfo rejection, got: {msg}"
        );
    }
}
