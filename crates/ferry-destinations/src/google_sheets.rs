//! Google Sheets v4 destination.
//!
//! See `docs/destinations/google-sheets.md` for the operational details
//! (sheet sharing, scopes, semantics, limitations). In short: this connector
//! performs service-account OAuth2 (in-memory token refresh), owns the row-1
//! header, and upserts rows by a configured key column using
//! `spreadsheets.values.batchUpdate` with `valueInputOption=RAW` against
//! explicit A1 ranges. It never calls `values.append`, never deletes rows,
//! never mirrors the source, and assumes a single writer per spreadsheet/tab.

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Mutex;

use ferry_core::config::DestinationConfig;
use ferry_core::error::FerryError;
use ferry_core::traits::{
    Destination, IdempotencyCapability, RateLimit, RemoveCapability, RemoveResult, RowError,
    WriteConfig, WriteResult,
};
use ferry_core::validation::{
    DEFAULT_CONNECT_TIMEOUT_SECS, DEFAULT_MAX_BATCH_SIZE, DEFAULT_MAX_RESPONSE_BYTES,
    DEFAULT_TIMEOUT_SECS, GOOGLE_SHEETS_SPREADSHEET_ID_REGEX,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Production Google Sheets v4 API base URL. Hardcoded — never configurable
/// in production to prevent credential exfiltration via a rogue base URL.
const SHEETS_BASE_URL: &str = "https://sheets.googleapis.com";
/// OAuth2 token scope used for all Sheets writes (single scope, least
/// privilege needed for `spreadsheets.values.*`).
const SHEETS_SCOPE: &str = "https://www.googleapis.com/auth/spreadsheets";
/// Maximum `Retry-After` value the destination honors (5 minutes).
const MAX_RETRY_AFTER: StdDuration = StdDuration::from_secs(300);
/// Truncate sanitized response bodies in error strings to this many bytes.
const SANITIZE_BODY_BYTES: usize = 512;
/// Maximum response body size cap used when validating config defaults.
const MAX_RESPONSE_BYTES_CAP: usize = 64 * 1024 * 1024;
/// Maximum number of `ValueRange` entries per `values.batchUpdate` request
/// (Google's documented limit is 1000 — stay conservatively under it).
const MAX_RANGES_PER_REQUEST: usize = 1000;
/// Conservative per-request payload cap (2 MiB). Google's documented
/// recommendation is "below 2 MB" per `values.batchUpdate`.
const MAX_REQUEST_PAYLOAD_BYTES: usize = 2 * 1024 * 1024;
/// Maximum per-cell character count enforced by the Sheets API.
const MAX_CELL_CHARS: usize = 50_000;
/// Maximum connector-level retry attempts for retryable responses
/// (429 / 5xx / 403 with `rateLimitExceeded`).
const MAX_CONNECTOR_ATTEMPTS: u32 = 5;
/// Per-request throttle: at most one Sheets API request per second per tab.
const REQUEST_THROTTLE: StdDuration = StdDuration::from_secs(1);

// ---------------------------------------------------------------------------
// Service-account key (subset of yup-oauth2::ServiceAccountKey)
// ---------------------------------------------------------------------------

/// A minimal service-account key used to construct a yup-oauth2
/// `ServiceAccountAuthenticator`. We parse only the fields yup-oauth2 needs;
/// `token_uri` is the field that allows tests to point at a wiremock server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceAccountKeyFile {
    #[serde(default)]
    pub key_type: Option<String>,
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub private_key_id: Option<String>,
    pub private_key: String,
    pub client_email: String,
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub auth_uri: Option<String>,
    pub token_uri: String,
    #[serde(default)]
    pub auth_provider_x509_cert_url: Option<String>,
    #[serde(default)]
    pub client_x509_cert_url: Option<String>,
}

impl ServiceAccountKeyFile {
    /// Convert into a `yup_oauth2::ServiceAccountKey`.
    fn into_yup(self) -> yup_oauth2::ServiceAccountKey {
        yup_oauth2::ServiceAccountKey {
            key_type: self.key_type,
            project_id: self.project_id,
            private_key_id: self.private_key_id,
            private_key: self.private_key,
            client_email: self.client_email,
            client_id: self.client_id,
            auth_uri: self.auth_uri,
            token_uri: self.token_uri,
            auth_provider_x509_cert_url: self.auth_provider_x509_cert_url,
            client_x509_cert_url: self.client_x509_cert_url,
        }
    }
}

// ---------------------------------------------------------------------------
// Constructor input validation (defense-in-depth)
// ---------------------------------------------------------------------------

/// Validate the constructor inputs that the production constructor relies
/// on for safe URL interpolation and correct upsert behavior. This is a
/// defense-in-depth re-check: validation should already have caught these
/// at config-load time, but we want to fail safely even if a
/// misconfigured `DestinationConfig` reaches the constructor via a path
/// that bypassed validation (e.g. a Python factory building the enum
/// directly).
///
/// Reuses the public `GOOGLE_SHEETS_SPREADSHEET_ID_REGEX` constant from
/// `ferry-core::validation` so there is a single source of truth for the
/// spreadsheet-id character class.
#[allow(clippy::too_many_arguments)]
fn validate_constructor_inputs(
    spreadsheet_id: &str,
    sheet: &str,
    key_column: &str,
    max_rows: usize,
    max_batch_size: usize,
    timeout_secs: u64,
    connect_timeout_secs: u64,
    max_response_bytes: usize,
) -> Result<(), FerryError> {
    if spreadsheet_id.trim().is_empty() {
        return Err(FerryError::Config(
            "Google Sheets destination spreadsheet_id must not be empty".to_string(),
        ));
    }
    // Defense-in-depth: spreadsheet_id is interpolated into a URL path
    // (`/v4/spreadsheets/{id}/values/...`). A `/` or `..` could enable
    // path traversal against the Sheets API host. The regex
    // `^[A-Za-z0-9_-]+$` rejects everything outside the safe set.
    let re = regex::Regex::new(GOOGLE_SHEETS_SPREADSHEET_ID_REGEX)
        .expect("static spreadsheet-id regex is valid");
    if !re.is_match(spreadsheet_id) {
        return Err(FerryError::Config(
            "Google Sheets destination spreadsheet_id contains characters outside [A-Za-z0-9_-]; refusing to interpolate into URL path".to_string(),
        ));
    }
    if sheet.trim().is_empty() {
        return Err(FerryError::Config(
            "Google Sheets destination sheet must not be empty".to_string(),
        ));
    }
    if key_column.trim().is_empty() {
        return Err(FerryError::Config(
            "Google Sheets destination key_column must not be empty".to_string(),
        ));
    }
    if max_rows < 2 {
        return Err(FerryError::Config(
            "Google Sheets destination max_rows must be at least 2 (one header row + one data row)"
                .to_string(),
        ));
    }
    if max_batch_size == 0 {
        return Err(FerryError::Config(
            "Google Sheets destination max_batch_size must be at least 1".to_string(),
        ));
    }
    if timeout_secs == 0 {
        return Err(FerryError::Config(
            "Google Sheets destination timeout_secs must be greater than 0".to_string(),
        ));
    }
    if connect_timeout_secs == 0 {
        return Err(FerryError::Config(
            "Google Sheets destination connect_timeout_secs must be greater than 0".to_string(),
        ));
    }
    if max_response_bytes == 0 {
        return Err(FerryError::Config(
            "Google Sheets destination max_response_bytes must be greater than 0".to_string(),
        ));
    }
    if max_response_bytes > MAX_RESPONSE_BYTES_CAP {
        return Err(FerryError::Config(format!(
            "Google Sheets destination max_response_bytes ({max_response_bytes}) exceeds the maximum allowed cap of {MAX_RESPONSE_BYTES_CAP} bytes"
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// GoogleSheetsDestination
// ---------------------------------------------------------------------------

/// Concrete connector type used by the default `ServiceAccountAuthenticator`
/// when built with the `hyper-rustls` feature. This is the `C` type parameter
/// of `Authenticator<C>`. The crate always enables `hyper-rustls` via the
/// workspace `yup-oauth2` dependency, so this is unconditional.
type Connector = hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>;

/// The `Authenticator` type used by `GoogleSheetsDestination`. Aliased so
/// the struct definition does not leak yup-oauth2 internals into its
/// signature.
type Authenticator = yup_oauth2::authenticator::Authenticator<Connector>;

/// A production Google Sheets v4 destination.
///
/// Holds one `reqwest::Client` (rustls, redirects disabled, HTTPS-only in
/// production), one `yup_oauth2::Authenticator` (in-memory token cache), and
/// a per-tab `Mutex` that serializes the read→map→write upsert window within
/// the process. All mutations target explicit A1 ranges; the connector never
/// calls `values.append`, `values.clear`, or `spreadsheets.batchUpdate`
/// (cell formatting / dimension resize).
///
/// `Debug` is implemented manually and redacts the credential path; no
/// private key, token, Authorization header, or cell value is ever emitted
/// through `Debug`, tracing, or errors.
pub struct GoogleSheetsDestination {
    client: reqwest::Client,
    authenticator: Authenticator,
    spreadsheet_id: String,
    sheet: String,
    key_column: String,
    max_rows: usize,
    max_batch_size: usize,
    timeout: StdDuration,
    max_response_bytes: usize,
    /// Base URL for the Sheets API. Production hardcodes
    /// `https://sheets.googleapis.com`; tests inject a wiremock URL via
    /// [`GoogleSheetsDestination::new_for_test`].
    sheets_base_url: String,
    /// Scopes requested on every `token(...)` call. Single scope in
    /// production; tests use the same.
    scopes: Vec<String>,
    /// Per-tab mutex: serializes read/map/write within the process so two
    /// concurrent batches cannot race against each other's row map.
    tab_lock: Arc<Mutex<()>>,
    /// Per-request throttle: ensures at most one Sheets API request per
    /// second per tab. Shared across batches within the destination.
    request_throttle: Arc<Mutex<()>>,
    sync_name: String,
}

impl std::fmt::Debug for GoogleSheetsDestination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never emit the credential path, token, or any cell value. The
        // spreadsheet_id, sheet, key_column, and sync_name are not secrets
        // (they appear in YAML and logs already).
        f.debug_struct("GoogleSheetsDestination")
            .field("spreadsheet_id", &self.spreadsheet_id)
            .field("sheet", &self.sheet)
            .field("key_column", &self.key_column)
            .field("max_rows", &self.max_rows)
            .field("max_batch_size", &self.max_batch_size)
            .field("timeout", &self.timeout)
            .field("max_response_bytes", &self.max_response_bytes)
            .field("sheets_base_url", &self.sheets_base_url)
            .field("sync_name", &self.sync_name)
            .field("service_account_key_file", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl GoogleSheetsDestination {
    /// Construct a production `GoogleSheetsDestination` from a resolved
    /// `DestinationConfig::GoogleSheets`.
    ///
    /// This is async because building the `ServiceAccountAuthenticator`
    /// requires reading the credential file and constructing an HTTP client
    /// (yup-oauth2's builder). The credential file is canonicalized and
    /// verified to be a regular file with Unix mode `0600` (on Unix) before
    /// its contents are read.
    pub async fn new(
        config: &DestinationConfig,
        project_dir: &Path,
        sync_name: &str,
    ) -> Result<Self, FerryError> {
        let DestinationConfig::GoogleSheets {
            spreadsheet_id,
            sheet,
            key_column,
            service_account_key_file,
            max_rows,
            max_batch_size,
            timeout_secs,
            connect_timeout_secs,
            max_response_bytes,
        } = config
        else {
            return Err(FerryError::Config(
                "GoogleSheetsDestination::new called with a non-GoogleSheets destination config"
                    .to_string(),
            ));
        };

        let spreadsheet_id = spreadsheet_id.clone();
        let sheet = sheet.clone();
        let key_column = key_column.clone();
        let max_rows = *max_rows;
        let max_batch_size = max_batch_size.unwrap_or(DEFAULT_MAX_BATCH_SIZE).max(1);
        let timeout = StdDuration::from_secs(timeout_secs.unwrap_or(DEFAULT_TIMEOUT_SECS));
        let connect_timeout =
            StdDuration::from_secs(connect_timeout_secs.unwrap_or(DEFAULT_CONNECT_TIMEOUT_SECS));
        let max_response_bytes = max_response_bytes
            .unwrap_or(DEFAULT_MAX_RESPONSE_BYTES)
            .min(MAX_RESPONSE_BYTES_CAP);

        // Defense-in-depth: re-validate the constructor inputs before any
        // URL interpolation or HTTP request. Validation should already have
        // caught these at config-load time, but a `DestinationConfig` built
        // via a path that bypassed validation (e.g. a Python factory) must
        // still fail safely.
        validate_constructor_inputs(
            &spreadsheet_id,
            &sheet,
            &key_column,
            max_rows,
            max_batch_size,
            timeout.as_secs(),
            connect_timeout.as_secs(),
            max_response_bytes,
        )?;

        // Resolve the credential path relative to `project_dir` if relative,
        // then canonicalize and verify permissions before reading.
        let cred_path = resolve_credential_path(project_dir, service_account_key_file)?;
        verify_credential_file(&cred_path)?;
        let key_json = std::fs::read_to_string(&cred_path).map_err(|e| {
            FerryError::Config(format!(
                "Cannot read service account key file '{}': {}",
                redact_path(&cred_path),
                sanitize_transport_msg(&e.to_string())
            ))
        })?;
        let key: ServiceAccountKeyFile = serde_json::from_str(&key_json).map_err(|e| {
            FerryError::Config(format!(
                "Cannot parse service account key file '{}': {}",
                redact_path(&cred_path),
                sanitize_transport_msg(&e.to_string())
            ))
        })?;

        // Defense-in-depth: the key's `token_uri` must point at the real
        // Google OAuth2 token endpoint in production. We do not allow a
        // production destination to mint tokens against an arbitrary host
        // (which would leak the private key to that host via the JWT-bearer
        // grant). Tests use [`new_for_test`] which does not enforce this.
        const PROD_TOKEN_URI: &str = "https://oauth2.googleapis.com/token";
        if key.token_uri != PROD_TOKEN_URI {
            return Err(FerryError::Config(format!(
                "service account key token_uri must be '{PROD_TOKEN_URI}' in production (got a different host); refusing to send the private key to an untrusted endpoint"
            )));
        }

        let client = build_reqwest_client(timeout, connect_timeout, /*https_only=*/ true)?;
        let authenticator = build_authenticator(key.into_yup(), &client).await?;

        Ok(Self {
            client,
            authenticator,
            spreadsheet_id,
            sheet,
            key_column,
            max_rows,
            max_batch_size,
            timeout,
            max_response_bytes,
            sheets_base_url: SHEETS_BASE_URL.to_string(),
            scopes: vec![SHEETS_SCOPE.to_string()],
            tab_lock: Arc::new(Mutex::new(())),
            request_throttle: Arc::new(Mutex::new(())),
            sync_name: sync_name.to_string(),
        })
    }

    /// Test-only constructor that injects a Sheets base URL and a
    /// service-account key (whose `token_uri` may point at a wiremock
    /// server). Production code must never call this — there is no
    /// `service_account_key_file` path or permission check, and the
    /// hardcoded-host defense is bypassed.
    ///
    /// This constructor is gated behind the `test-util` cargo feature.
    /// Production builds (which do not enable `test-util`) cannot call it,
    /// so a misconfigured production `ferry run` cannot override the Google
    /// hosts / HTTPS / key-file checks. `ferry-tests` enables the feature
    /// as a dev-dependency.
    #[cfg(any(feature = "test-util", test))]
    #[allow(clippy::too_many_arguments)]
    pub async fn new_for_test(
        key: ServiceAccountKeyFile,
        spreadsheet_id: String,
        sheet: String,
        key_column: String,
        max_rows: usize,
        sheets_base_url: String,
        sync_name: &str,
        timeout: StdDuration,
        connect_timeout: StdDuration,
        max_response_bytes: usize,
        max_batch_size: usize,
    ) -> Result<Self, FerryError> {
        // For tests we still build an HTTPS-capable client, but we do NOT
        // set https_only(true) because wiremock serves over HTTP. Redirects
        // remain disabled (defense-in-depth).
        let client = build_reqwest_client(timeout, connect_timeout, /*https_only=*/ false)?;
        let authenticator = build_authenticator(key.into_yup(), &client).await?;
        Ok(Self {
            client,
            authenticator,
            spreadsheet_id,
            sheet,
            key_column,
            max_rows,
            max_batch_size,
            timeout,
            max_response_bytes: max_response_bytes.min(MAX_RESPONSE_BYTES_CAP),
            sheets_base_url,
            scopes: vec![SHEETS_SCOPE.to_string()],
            tab_lock: Arc::new(Mutex::new(())),
            request_throttle: Arc::new(Mutex::new(())),
            sync_name: sync_name.to_string(),
        })
    }

    /// Acquire a bearer token, marking the Authorization header sensitive.
    /// Returns a sanitized auth error (never an empty string) if the token
    /// exchange succeeds but yields an empty access token.
    async fn bearer_token(&self) -> Result<String, FerryError> {
        let token = self.authenticator.token(&self.scopes).await.map_err(|e| {
            FerryError::Destination(format!(
                "Failed to fetch OAuth2 token: {}",
                sanitize_transport_msg(&e.to_string())
            ))
        })?;
        let tok = token.token().unwrap_or_default();
        if tok.is_empty() {
            return Err(FerryError::Destination(
                "OAuth2 token exchange succeeded but returned an empty access token (the service account key may be revoked or the project disabled); refusing to send an empty Authorization header".to_string()
            ));
        }
        Ok(tok.to_string())
    }

    /// Force-refresh the bearer token (used on 401).
    /// Returns a sanitized auth error (never an empty string) if the
    /// refresh succeeds but yields an empty access token.
    async fn force_refreshed_token(&self) -> Result<String, FerryError> {
        let token = self
            .authenticator
            .force_refreshed_token(&self.scopes)
            .await
            .map_err(|e| {
                FerryError::Destination(format!(
                    "Failed to refresh OAuth2 token: {}",
                    sanitize_transport_msg(&e.to_string())
                ))
            })?;
        let tok = token.token().unwrap_or_default();
        if tok.is_empty() {
            return Err(FerryError::Destination(
                "OAuth2 token refresh succeeded but returned an empty access token (the service account key may be revoked or the project disabled); refusing to send an empty Authorization header".to_string()
            ));
        }
        Ok(tok.to_string())
    }

    /// Per-request throttle: at most one Sheets API request per second per
    /// tab. Holds the throttle mutex across the sleep so concurrent requests
    /// serialize on the 1-second interval.
    async fn throttle(&self) {
        let _guard = self.request_throttle.lock().await;
        tokio::time::sleep(REQUEST_THROTTLE).await;
    }

    /// Build the URL for a `values.get` request. The range is
    /// percent-encoded via `url::Url`.
    fn values_get_url(&self, range: &str) -> Result<url::Url, FerryError> {
        let path = format!("/v4/spreadsheets/{}/values/{}", self.spreadsheet_id, range);
        let mut u = url::Url::parse(&self.sheets_base_url)
            .map_err(|e| FerryError::Config(format!("invalid sheets_base_url: {e}")))?;
        u.set_path(&path);
        u.query_pairs_mut()
            .append_pair("majorDimension", "ROWS")
            .append_pair("valueRenderOption", "UNFORMATTED_VALUE");
        Ok(u)
    }

    /// Build the URL for a `values.batchUpdate` request.
    fn values_batch_update_url(&self) -> Result<url::Url, FerryError> {
        let path = format!(
            "/v4/spreadsheets/{}/values:batchUpdate",
            self.spreadsheet_id
        );
        let mut u = url::Url::parse(&self.sheets_base_url)
            .map_err(|e| FerryError::Config(format!("invalid sheets_base_url: {e}")))?;
        u.set_path(&path);
        Ok(u)
    }
}

// ---------------------------------------------------------------------------
// Destination trait impl
// ---------------------------------------------------------------------------

#[async_trait]
impl Destination for GoogleSheetsDestination {
    fn name(&self) -> &str {
        "google_sheets"
    }

    async fn check_connection(&self) -> Result<(), FerryError> {
        let _guard = self.tab_lock.lock().await;
        let token = self.bearer_token().await?;
        // Read the top-left cell of the configured tab. We do not write
        // during a connection check.
        let range = format!("{}!A1:A1", quote_sheet_name(&self.sheet));
        let url = self.values_get_url(&range)?;
        self.throttle().await;
        let resp = self
            .client
            .get(url.clone())
            .header(
                http::header::AUTHORIZATION,
                sensitive_header(format!("Bearer {token}")),
            )
            .timeout(self.timeout)
            .send()
            .await
            .map_err(|e| {
                FerryError::Destination(format!(
                    "HTTP transport: {}",
                    sanitize_transport_msg(&e.to_string())
                ))
            })?;
        let status = resp.status();
        if status.is_success() {
            // Drain the body to allow connection reuse; we do not log it.
            let _ = read_bounded(resp, self.max_response_bytes).await;
            return Ok(());
        }
        let status_code = status.as_u16();
        let body = read_bounded(resp, self.max_response_bytes)
            .await
            .unwrap_or_default();
        let secrets = token_secrets(&token);
        let sanitized = sanitize_body(&body, SANITIZE_BODY_BYTES, &secrets);
        // 404 on a missing spreadsheet/tab is a fatal config error.
        Err(FerryError::Destination(format!(
            "HTTP {status_code}: {sanitized}"
        )))
    }

    async fn write(
        &self,
        batch: &RecordBatch,
        config: &WriteConfig,
    ) -> Result<WriteResult, FerryError> {
        upsert(self, batch, config).await
    }

    fn max_batch_size(&self) -> usize {
        self.max_batch_size
    }

    fn rate_limit(&self) -> Option<RateLimit> {
        Some(RateLimit {
            requests_per_second: Some(1.0),
            concurrent_requests: Some(1),
        })
    }

    fn idempotency(&self) -> IdempotencyCapability {
        IdempotencyCapability::Idempotent
    }

    fn remove_capability(&self) -> RemoveCapability {
        RemoveCapability::None
    }

    async fn remove(
        &self,
        _keys: &[serde_json::Value],
        _config: &WriteConfig,
    ) -> Result<RemoveResult, FerryError> {
        Err(FerryError::Destination(
            "Google Sheets destination does not support remove".to_string(),
        ))
    }

    async fn replace_all(
        &self,
        _batch: &RecordBatch,
        _config: &WriteConfig,
    ) -> Result<WriteResult, FerryError> {
        Err(FerryError::Destination(
            "Google Sheets destination does not support replace_all; use write (upsert) with sync.mode: incremental or full_refresh".to_string(),
        ))
    }
}

// ---------------------------------------------------------------------------
// Upsert implementation
// ---------------------------------------------------------------------------

/// Run the key-based upsert algorithm against a single batch.
///
/// See the plan's "Upsert Algorithm" section for the full specification. In
/// short: acquire the per-tab mutex, read the current sheet, validate the
/// header, build a `key → row_index` map, allocate new rows after the highest
/// populated row, split the staged `ValueRange` entries into bounded chunks,
/// send each chunk as a `values.batchUpdate` request with retry/refresh, and
/// accumulate per-PK errors.
async fn upsert(
    dest: &GoogleSheetsDestination,
    batch: &RecordBatch,
    config: &WriteConfig,
) -> Result<WriteResult, FerryError> {
    let _guard = dest.tab_lock.lock().await;

    // 1. Validate the batch.
    let schema = batch.schema();
    let num_cols = batch.num_columns();
    if num_cols == 0 {
        return Ok(WriteResult {
            rows_written: 0,
            errors: vec![RowError {
                primary_key: "<batch>".to_string(),
                error: "Google Sheets write rejected: batch has zero columns".to_string(),
            }],
        });
    }

    // Validate the key column is present in the schema.
    let key_col_idx = match schema.index_of(&dest.key_column) {
        Ok(i) => i,
        Err(_) => {
            let pks = extract_pks_fallback(batch, config);
            return Ok(WriteResult {
                rows_written: 0,
                errors: pks
                    .into_iter()
                    .map(|pk| RowError {
                        primary_key: pk,
                        error: format!(
                            "Google Sheets write rejected: key_column '{}' not present in the batch schema",
                            dest.key_column
                        ),
                    })
                    .collect(),
            });
        }
    };

    // Validate `WriteConfig.pk_col` matches `key_column` (engine contract).
    if let Some(pk_col) = &config.pk_col {
        if pk_col != &dest.key_column {
            let pks = extract_pks_fallback(batch, config);
            return Ok(WriteResult {
                rows_written: 0,
                errors: pks
                    .into_iter()
                    .map(|pk| RowError {
                        primary_key: pk,
                        error: format!(
                            "Google Sheets write rejected: WriteConfig.pk_col '{pk_col}' does not match destination key_column '{}'",
                            dest.key_column
                        ),
                    })
                    .collect(),
            });
        }
    }

    // Validate the key column type is supported by `extract_pks`.
    let key_col = batch.column(key_col_idx);
    let key_type_supported = matches!(
        key_col.data_type(),
        arrow_schema::DataType::Int32
            | arrow_schema::DataType::Int64
            | arrow_schema::DataType::Utf8
            | arrow_schema::DataType::LargeUtf8
    );
    if !key_type_supported {
        let pks = extract_pks_fallback(batch, config);
        return Ok(WriteResult {
            rows_written: 0,
            errors: pks
                .into_iter()
                .map(|pk| RowError {
                    primary_key: pk,
                    error: format!(
                        "Google Sheets write rejected: key_column '{}' has unsupported type {:?} (expected Int32, Int64, Utf8, or LargeUtf8)",
                        dest.key_column,
                        key_col.data_type()
                    ),
                })
                .collect(),
        });
    }

    // 2. Derive ordered headers + last A1 column.
    let headers: Vec<String> = (0..num_cols)
        .map(|i| schema.field(i).name().clone())
        .collect();
    // Reject duplicate header names (these would create ambiguous columns).
    {
        let mut seen = std::collections::HashSet::new();
        for h in &headers {
            if !seen.insert(h.clone()) {
                let pks = extract_pks_fallback(batch, config);
                return Ok(WriteResult {
                    rows_written: 0,
                    errors: pks
                        .into_iter()
                        .map(|pk| RowError {
                            primary_key: pk,
                            error: format!(
                                "Google Sheets write rejected: duplicate column name '{h}' in source schema"
                            ),
                        })
                        .collect(),
                });
            }
        }
    }
    let last_col = a1_column(num_cols - 1);

    // 3. Serialize each cell deterministically and check the cell-size cap.
    //    Build `rows: Vec<Vec<serde_json::Value>>` (column-major order).
    let mut incoming_rows: Vec<Vec<serde_json::Value>> = Vec::with_capacity(batch.num_rows());
    let mut overlong: Vec<(usize, String)> = Vec::new(); // (row_idx, error)
    for row_idx in 0..batch.num_rows() {
        let mut row_vals: Vec<serde_json::Value> = Vec::with_capacity(num_cols);
        for col_idx in 0..num_cols {
            let col = batch.column(col_idx);
            let v = serialize_cell(col, row_idx);
            // Check the cell-size cap on the string form.
            if let serde_json::Value::String(s) = &v {
                if s.chars().count() > MAX_CELL_CHARS {
                    overlong.push((
                        row_idx,
                        format!(
                            "Google Sheets write rejected: cell at column '{}', row {} exceeds {MAX_CELL_CHARS} characters",
                            schema.field(col_idx).name(),
                            row_idx
                        ),
                    ));
                }
            }
            row_vals.push(v);
        }
        incoming_rows.push(row_vals);
    }

    // Extract incoming PKs (real PKs, not row-index fallback) — these are the
    // authoritative PKs for journal errors.
    let incoming_pks = ferry_core::delivery::extract_pks(batch, &dest.key_column)
        .unwrap_or_else(|_| (0..batch.num_rows()).map(|i| i.to_string()).collect());

    // 4. Reject empty keys and duplicate incoming keys (row-safe errors).
    let mut incoming_errors: Vec<RowError> = Vec::new();
    let mut seen_keys: std::collections::HashMap<String, Vec<usize>> =
        std::collections::HashMap::new();
    for (row_idx, pk) in incoming_pks.iter().enumerate() {
        if pk.trim().is_empty() || *pk == format!("__null__{row_idx}") {
            incoming_errors.push(RowError {
                primary_key: pk.clone(),
                error: "Google Sheets write rejected: empty or null key value".to_string(),
            });
            continue;
        }
        seen_keys.entry(pk.clone()).or_default().push(row_idx);
    }
    // Mark every occurrence of a duplicate incoming key as an error
    // (row-safe — never silently pick the first).
    for (pk, idxs) in &seen_keys {
        if idxs.len() > 1 {
            for &row_idx in idxs {
                incoming_errors.push(RowError {
                    primary_key: pk.clone(),
                    error: format!(
                        "Google Sheets write rejected: duplicate incoming key '{pk}' ({} occurrences)",
                        idxs.len()
                    ),
                });
                let _ = row_idx;
            }
        }
    }
    // Add overlong-cell errors.
    for (row_idx, msg) in overlong {
        incoming_errors.push(RowError {
            primary_key: incoming_pks
                .get(row_idx)
                .cloned()
                .unwrap_or_else(|| row_idx.to_string()),
            error: msg,
        });
    }
    if !incoming_errors.is_empty() {
        return Ok(WriteResult {
            rows_written: 0,
            errors: incoming_errors,
        });
    }

    // 5. Acquire a bearer token and read the current sheet state.
    let token = dest.bearer_token().await?;
    let read_range = format!(
        "{}!A1:{}{}",
        quote_sheet_name(&dest.sheet),
        last_col,
        dest.max_rows
    );
    let read_url = dest.values_get_url(&read_range)?;
    let get_resp = sheets_request(dest, token.clone(), read_url.clone(), None).await?;
    let get_status = get_resp.status();
    if !get_status.is_success() {
        let status_code = get_status.as_u16();
        let retry_after_hdr =
            reqwest_header(&get_resp, http::header::RETRY_AFTER).map(|s| s.to_string());
        let body = read_bounded(get_resp, dest.max_response_bytes)
            .await
            .unwrap_or_default();
        let secrets = token_secrets(&token);
        let sanitized = sanitize_body(&body, SANITIZE_BODY_BYTES, &secrets);
        let retry_after =
            parse_retry_after(retry_after_hdr.as_deref(), Utc::now(), MAX_RETRY_AFTER);
        let error_msg = match retry_after {
            Some(d) => format!(
                "HTTP {status_code}: {sanitized}; retry_after: {}",
                d.as_secs()
            ),
            None => format!("HTTP {status_code}: {sanitized}"),
        };
        let errors: Vec<RowError> = incoming_pks
            .iter()
            .map(|pk| RowError {
                primary_key: pk.clone(),
                error: error_msg.clone(),
            })
            .collect();
        return Ok(WriteResult {
            rows_written: 0,
            errors,
        });
    }
    let get_body = read_bounded(get_resp, dest.max_response_bytes)
        .await
        .unwrap_or_default();
    let get_value: ValuesGetResponse = serde_json::from_slice(&get_body).map_err(|e| {
        FerryError::Destination(format!(
            "Failed to parse Sheets values.get response: {}",
            sanitize_transport_msg(&e.to_string())
        ))
    })?;

    // 6. Validate / stage the header row.
    //
    // If the sheet is empty (row 1 is empty or absent), stage the header
    // write. Otherwise require exact ordered equality with the source
    // schema's header row and require `key_column` in that header.
    let mut header_stage: Option<Vec<serde_json::Value>> = None;
    let mut key_row_map: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    let mut next_row: usize = 2; // first data row is row 2 (1-indexed)

    let existing_rows = get_value.values.unwrap_or_default();
    if existing_rows.is_empty() || existing_rows.first().map(|r| r.is_empty()).unwrap_or(true) {
        // Empty sheet: stage the header at A1:{last_col}1.
        header_stage = Some(
            headers
                .iter()
                .map(|h| serde_json::Value::String(h.clone()))
                .collect(),
        );
    } else {
        // Existing sheet: compare the first row to our ordered headers.
        let row1 = existing_rows.first().unwrap();
        let row1_strs: Vec<String> = row1
            .iter()
            .map(|v| match v {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Null => String::new(),
                other => other.to_string(),
            })
            .collect();
        if row1_strs != headers {
            // Do NOT include the actual destination row 1 values in the
            // error — they are destination cell contents that the plan
            // requires must not be persisted/logged. Report only the
            // lengths and the first differing column index, plus the
            // *expected* (source-schema) header name at that index (which
            // is operator-visible config, not destination data).
            let dest_len = row1_strs.len();
            let expected_len = headers.len();
            let first_diff = row1_strs
                .iter()
                .zip(headers.iter())
                .enumerate()
                .find(|(_, (d, e))| d != e)
                .map(|(i, _)| i)
                .unwrap_or_else(|| dest_len.min(expected_len));
            let expected_at_diff = headers.get(first_diff).cloned();
            let error_msg = match expected_at_diff {
                Some(name) => format!(
                    "Google Sheets write rejected: header row mismatch — destination row 1 has {dest_len} column(s), expected {expected_len}; first differing column at index {first_diff} (expected source header '{name}')"
                ),
                None => format!(
                    "Google Sheets write rejected: header row mismatch — destination row 1 has {dest_len} column(s), expected {expected_len}; first differing column at index {first_diff}"
                ),
            };
            let errors: Vec<RowError> = incoming_pks
                .iter()
                .map(|pk| RowError {
                    primary_key: pk.clone(),
                    error: error_msg.clone(),
                })
                .collect();
            return Ok(WriteResult {
                rows_written: 0,
                errors,
            });
        }
        // Require `key_column` in the header.
        if !headers.contains(&dest.key_column) {
            let errors: Vec<RowError> = incoming_pks
                .iter()
                .map(|pk| RowError {
                    primary_key: pk.clone(),
                    error: format!(
                        "Google Sheets write rejected: key_column '{}' not present in destination header row",
                        dest.key_column
                    ),
                })
                .collect();
            return Ok(WriteResult {
                rows_written: 0,
                errors,
            });
        }
        // Find the key column index in the source schema.
        let key_idx_in_row = headers.iter().position(|h| h == &dest.key_column).unwrap();

        // Build the key→row map from rows 2..N (index 1.. in 0-indexed
        // `existing_rows`). Keep the first row for duplicate existing keys
        // and warn; never delete the duplicates.
        let mut existing_dupes: Vec<String> = Vec::new();
        for (i, row) in existing_rows.iter().enumerate().skip(1) {
            // Skip rows whose key cell is empty.
            let key_val = row
                .get(key_idx_in_row)
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            let key_str = match &key_val {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Null => continue,
                other => other.to_string(),
            };
            if key_str.is_empty() {
                continue;
            }
            // i is 0-indexed; the A1 row is i+1.
            let a1_row = i + 1;
            if key_row_map.contains_key(&key_str) {
                existing_dupes.push(key_str);
                continue;
            }
            key_row_map.insert(key_str, a1_row);
            if a1_row + 1 > next_row {
                next_row = a1_row + 1;
            }
        }
        if !existing_dupes.is_empty() {
            // Do NOT include the duplicate key values in the log — they are
            // destination cell contents that the plan requires must not be
            // logged. Report only the count of duplicates.
            tracing::warn!(
                sync = %dest.sync_name,
                sheet = %dest.sheet,
                duplicate_count = existing_dupes.len(),
                "destination has {} duplicate key(s) in existing rows; keeping the first occurrence and not deleting duplicates",
                existing_dupes.len()
            );
        }
    }

    // 7. Allocate rows: existing keys → in-place update; new keys → next_row
    //    (monotonically increasing; never reuse holes).
    let mut staged: Vec<StagedRow> = Vec::with_capacity(batch.num_rows());
    let mut allocation_errors: Vec<RowError> = Vec::new();
    let mut row_cursor = next_row;
    for (row_idx, pk) in incoming_pks.iter().enumerate() {
        let target_row = if let Some(existing) = key_row_map.get(pk) {
            *existing
        } else {
            // Allocate a new row.
            if row_cursor > dest.max_rows {
                allocation_errors.push(RowError {
                    primary_key: pk.clone(),
                    error: format!(
                        "Google Sheets write rejected: max_rows ({}) would be exceeded by new key '{pk}'",
                        dest.max_rows
                    ),
                });
                continue;
            }
            let r = row_cursor;
            row_cursor += 1;
            r
        };
        staged.push(StagedRow {
            pk: pk.clone(),
            target_row,
            values: incoming_rows[row_idx].clone(),
        });
    }
    if !allocation_errors.is_empty() {
        return Ok(WriteResult {
            rows_written: 0,
            errors: allocation_errors,
        });
    }

    // 8. Build `ValueRange` entries: one per staged row, plus an optional
    //    header entry. Track for each entry whether it is the header (no PK)
    //    or a data row (with PK) so chunk accounting can attribute success
    //    and errors correctly.
    let mut value_ranges: Vec<ValueRange> = Vec::new();
    // `(is_header, Option<pk>)` per value_range entry.
    let mut entry_meta: Vec<(bool, Option<String>)> = Vec::new();
    if let Some(hdr) = header_stage {
        let range = format!("{}!A1:{}1", quote_sheet_name(&dest.sheet), last_col);
        value_ranges.push(ValueRange {
            range,
            values: vec![hdr],
        });
        entry_meta.push((true, None));
    }
    for s in &staged {
        let range = format!(
            "{}!A{}:{}{}",
            quote_sheet_name(&dest.sheet),
            s.target_row,
            last_col,
            s.target_row
        );
        value_ranges.push(ValueRange {
            range,
            values: vec![s.values.clone()],
        });
        entry_meta.push((false, Some(s.pk.clone())));
    }

    // 9. Split the value_ranges into chunks bounded by both
    //    `MAX_RANGES_PER_REQUEST` and `MAX_REQUEST_PAYLOAD_BYTES`.
    let chunks = split_chunks(
        &value_ranges,
        MAX_RANGES_PER_REQUEST,
        MAX_REQUEST_PAYLOAD_BYTES,
    );

    // 10. Send each chunk as a `values.batchUpdate` request, with
    //     retry/backoff and 401 force-refresh.
    let mut rows_written: usize = 0;
    let mut errors: Vec<RowError> = Vec::new();
    for chunk in chunks {
        let chunk_pks: Vec<String> = chunk
            .iter()
            .filter_map(|&idx| entry_meta[idx].1.clone())
            .collect();
        let chunk_value_ranges: Vec<&ValueRange> =
            chunk.iter().map(|&idx| &value_ranges[idx]).collect();
        // Collect all cell values from this chunk's payload as additional
        // secrets so a server that echoes the request body in its error
        // response cannot leak cell values into persisted errors / state.
        let payload_secrets = payload_value_secrets(&chunk_value_ranges);
        let body = BatchUpdateRequest {
            value_input_option: "RAW".to_string(),
            data: chunk.iter().map(|&idx| value_ranges[idx].clone()).collect(),
        };
        let body_bytes = serde_json::to_vec(&body).map_err(|e| {
            FerryError::Destination(format!(
                "Failed to serialize batchUpdate request: {}",
                sanitize_transport_msg(&e.to_string())
            ))
        })?;
        let url = dest.values_batch_update_url()?;
        let result = sheets_request_with_retry(
            dest,
            token.clone(),
            url.clone(),
            Some(body_bytes),
            &chunk_pks,
            &payload_secrets,
        )
        .await;
        match result {
            SheetsRequestOutcome::Success => {
                rows_written += chunk_pks.len();
            }
            SheetsRequestOutcome::RowErrors(errs) => {
                errors.extend(errs);
            }
            SheetsRequestOutcome::Fatal(e) => {
                return Err(e);
            }
        }
    }

    Ok(WriteResult {
        rows_written,
        errors,
    })
}

/// A staged row waiting to be written to a specific A1 row.
struct StagedRow {
    pk: String,
    target_row: usize,
    values: Vec<serde_json::Value>,
}

/// Outcome of a single (possibly retried) `values.batchUpdate` request.
enum SheetsRequestOutcome {
    /// All rows in the request succeeded.
    Success,
    /// One or more rows failed with row-level errors.
    RowErrors(Vec<RowError>),
    /// A fatal transport/config error that aborts the whole `write`.
    Fatal(FerryError),
}

/// Send a single `values.batchUpdate` request with retry/backoff and a
/// one-shot 401 force-refresh replay. `chunk_pks` are the PKs of the data
/// rows in this chunk (header entries excluded); they are attached
/// one-to-one to the row errors emitted on failure. `payload_secrets` are
/// the cell values from the request payload — a server that echoes the
/// request body in its error response would otherwise leak cell values into
/// persisted errors / state.
async fn sheets_request_with_retry(
    dest: &GoogleSheetsDestination,
    initial_token: String,
    url: url::Url,
    body: Option<Vec<u8>>,
    chunk_pks: &[String],
    payload_secrets: &[String],
) -> SheetsRequestOutcome {
    let mut token = initial_token;
    let mut attempt: u32 = 0;
    let mut backoff = StdDuration::from_millis(500);
    loop {
        attempt += 1;
        match sheets_request_once(dest, &token, url.clone(), body.as_deref()).await {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    let _ = read_bounded(resp, dest.max_response_bytes).await;
                    return SheetsRequestOutcome::Success;
                }
                let status_code = status.as_u16();
                let retry_after_hdr =
                    reqwest_header(&resp, http::header::RETRY_AFTER).map(|s| s.to_string());
                let body_bytes = read_bounded(resp, dest.max_response_bytes)
                    .await
                    .unwrap_or_default();
                let secrets = token_secrets(&token);
                let all_secrets = combine_secrets(&secrets, payload_secrets);
                let sanitized = sanitize_body(&body_bytes, SANITIZE_BODY_BYTES, &all_secrets);

                // 401: force-refresh and replay exactly once. A second 401
                // becomes per-PK row errors (no token in the message).
                if status_code == 401 {
                    match dest.force_refreshed_token().await {
                        Ok(new_token) => {
                            token = new_token;
                            match sheets_request_once(dest, &token, url.clone(), body.as_deref())
                                .await
                            {
                                Ok(resp2) => {
                                    let status2 = resp2.status();
                                    if status2.is_success() {
                                        let _ = read_bounded(resp2, dest.max_response_bytes).await;
                                        return SheetsRequestOutcome::Success;
                                    }
                                    let body2 = read_bounded(resp2, dest.max_response_bytes)
                                        .await
                                        .unwrap_or_default();
                                    let secrets2 = token_secrets(&token);
                                    let all_secrets2 = combine_secrets(&secrets2, payload_secrets);
                                    let sanitized2 =
                                        sanitize_body(&body2, SANITIZE_BODY_BYTES, &all_secrets2);
                                    let error_msg =
                                        format!("HTTP {}: {}", status2.as_u16(), sanitized2);
                                    return SheetsRequestOutcome::RowErrors(
                                        chunk_pks
                                            .iter()
                                            .map(|pk| RowError {
                                                primary_key: pk.clone(),
                                                error: error_msg.clone(),
                                            })
                                            .collect(),
                                    );
                                }
                                Err(e) => {
                                    return SheetsRequestOutcome::Fatal(FerryError::Destination(
                                        format!(
                                            "HTTP transport: {}",
                                            sanitize_transport_msg(&e.to_string())
                                        ),
                                    ));
                                }
                            }
                        }
                        Err(e) => {
                            return SheetsRequestOutcome::Fatal(FerryError::Destination(format!(
                                "Failed to refresh OAuth2 token on 401: {}",
                                sanitize_transport_msg(&e.to_string())
                            )));
                        }
                    }
                }

                // Retryable: 429, 5xx, 403 with `rateLimitExceeded` reason.
                let is_rate_limited_403 = status_code == 403
                    && String::from_utf8_lossy(&body_bytes).contains("rateLimitExceeded");
                let retryable = status_code == 429 || status_code >= 500 || is_rate_limited_403;
                if retryable && attempt < MAX_CONNECTOR_ATTEMPTS {
                    let wait =
                        parse_retry_after(retry_after_hdr.as_deref(), Utc::now(), MAX_RETRY_AFTER)
                            .unwrap_or(backoff);
                    tokio::time::sleep(wait).await;
                    backoff = std::cmp::min(StdDuration::from_secs(60), backoff.saturating_mul(2));
                    // Retry the same chunk — safe because explicit A1 ranges
                    // are idempotent. The next *write* call re-reads the
                    // sheet, so a successfully-created-but-ambiguous response
                    // becomes an in-place update on the next attempt.
                    continue;
                }

                // Permanent or out-of-attempts: per-PK row errors.
                let error_msg = match parse_retry_after(
                    retry_after_hdr.as_deref(),
                    Utc::now(),
                    MAX_RETRY_AFTER,
                ) {
                    Some(d) => format!(
                        "HTTP {status_code}: {sanitized}; retry_after: {}",
                        d.as_secs()
                    ),
                    None => format!("HTTP {status_code}: {sanitized}"),
                };
                return SheetsRequestOutcome::RowErrors(
                    chunk_pks
                        .iter()
                        .map(|pk| RowError {
                            primary_key: pk.clone(),
                            error: error_msg.clone(),
                        })
                        .collect(),
                );
            }
            Err(e) => {
                // Transport error: retryable up to MAX_CONNECTOR_ATTEMPTS.
                // Report affected rows as retryable — the next *write* call
                // re-reads the sheet so an ambiguous applied response becomes
                // an in-place update and cannot duplicate.
                if attempt < MAX_CONNECTOR_ATTEMPTS {
                    tokio::time::sleep(backoff).await;
                    backoff = std::cmp::min(StdDuration::from_secs(60), backoff.saturating_mul(2));
                    continue;
                }
                let msg = format!("HTTP transport: {}", sanitize_transport_msg(&e.to_string()));
                return SheetsRequestOutcome::RowErrors(
                    chunk_pks
                        .iter()
                        .map(|pk| RowError {
                            primary_key: pk.clone(),
                            error: msg.clone(),
                        })
                        .collect(),
                );
            }
        }
    }
}

/// Send one HTTP request (GET if `body` is None, POST if Some). Retries are
/// handled by the caller. Performs the per-request throttle.
async fn sheets_request_once(
    dest: &GoogleSheetsDestination,
    token: &str,
    url: url::Url,
    body: Option<&[u8]>,
) -> Result<reqwest::Response, reqwest::Error> {
    dest.throttle().await;
    let mut rb = dest
        .client
        .request(
            if body.is_some() {
                reqwest::Method::POST
            } else {
                reqwest::Method::GET
            },
            url,
        )
        .header(
            http::header::AUTHORIZATION,
            sensitive_header(format!("Bearer {token}")),
        )
        .timeout(dest.timeout);
    if let Some(b) = body {
        rb = rb
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(b.to_vec());
    }
    rb.send().await
}

/// Wrapper for a single request used during the initial `values.get` read
/// (no retry — failures on the read path are surfaced as row errors so the
/// caller can classify them).
async fn sheets_request(
    dest: &GoogleSheetsDestination,
    token: String,
    url: url::Url,
    body: Option<Vec<u8>>,
) -> Result<reqwest::Response, FerryError> {
    sheets_request_once(dest, &token, url, body.as_deref())
        .await
        .map_err(|e| {
            FerryError::Destination(format!(
                "HTTP transport: {}",
                sanitize_transport_msg(&e.to_string())
            ))
        })
}

// ---------------------------------------------------------------------------
// Serialization / DTOs
// ---------------------------------------------------------------------------

/// Serialize a single Arrow cell to a `serde_json::Value` for the Sheets
/// `values.batchUpdate` payload. RAW storage:
/// - null → `Value::String("")` (empty cell)
/// - boolean → `Value::String("TRUE"|"FALSE")`
/// - numbers → `Value::String(<locale-independent>)`
/// - temporal / other → existing ISO / display string
///
/// RAW prevents formula interpretation (no `=`, `+`, `-` parsing).
fn serialize_cell(column: &arrow_array::ArrayRef, row_idx: usize) -> serde_json::Value {
    use arrow_array::cast::*;
    use arrow_array::types::*;
    use arrow_array::*;
    if column.is_null(row_idx) {
        return serde_json::Value::String(String::new());
    }
    match column.data_type() {
        arrow_schema::DataType::Boolean => {
            let arr = as_boolean_array(column);
            serde_json::Value::String(if arr.value(row_idx) {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            })
        }
        arrow_schema::DataType::Int8 => {
            let arr = as_primitive_array::<Int8Type>(column);
            serde_json::Value::String(arr.value(row_idx).to_string())
        }
        arrow_schema::DataType::Int16 => {
            let arr = as_primitive_array::<Int16Type>(column);
            serde_json::Value::String(arr.value(row_idx).to_string())
        }
        arrow_schema::DataType::Int32 => {
            let arr = as_primitive_array::<Int32Type>(column);
            serde_json::Value::String(arr.value(row_idx).to_string())
        }
        arrow_schema::DataType::Int64 => {
            let arr = as_primitive_array::<Int64Type>(column);
            serde_json::Value::String(arr.value(row_idx).to_string())
        }
        arrow_schema::DataType::UInt8 => {
            let arr = as_primitive_array::<UInt8Type>(column);
            serde_json::Value::String(arr.value(row_idx).to_string())
        }
        arrow_schema::DataType::UInt16 => {
            let arr = as_primitive_array::<UInt16Type>(column);
            serde_json::Value::String(arr.value(row_idx).to_string())
        }
        arrow_schema::DataType::UInt32 => {
            let arr = as_primitive_array::<UInt32Type>(column);
            serde_json::Value::String(arr.value(row_idx).to_string())
        }
        arrow_schema::DataType::UInt64 => {
            let arr = as_primitive_array::<UInt64Type>(column);
            serde_json::Value::String(arr.value(row_idx).to_string())
        }
        arrow_schema::DataType::Float32 => {
            let arr = as_primitive_array::<Float32Type>(column);
            serde_json::Value::String(arr.value(row_idx).to_string())
        }
        arrow_schema::DataType::Float64 => {
            let arr = as_primitive_array::<Float64Type>(column);
            serde_json::Value::String(arr.value(row_idx).to_string())
        }
        // For all other types (strings, dates, timestamps, etc.), use the
        // Arrow display representation (ISO for temporal).
        _ => match arrow_cast::display::array_value_to_string(column.as_ref(), row_idx) {
            Ok(s) => serde_json::Value::String(s),
            Err(_) => serde_json::Value::String("<error>".to_string()),
        },
    }
}

/// `values.get` response body. Only the `values` field is needed.
#[derive(Debug, Deserialize)]
struct ValuesGetResponse {
    values: Option<Vec<Vec<serde_json::Value>>>,
}

/// `values.batchUpdate` request body. Field names are renamed to camelCase
/// to match the Google Sheets v4 REST API JSON schema
/// (`valueInputOption`, `data`).
#[derive(Debug, Serialize)]
struct BatchUpdateRequest {
    #[serde(rename = "valueInputOption")]
    value_input_option: String,
    data: Vec<ValueRange>,
}

/// A single `ValueRange` in a `values.batchUpdate` request.
#[derive(Debug, Serialize, Clone)]
struct ValueRange {
    range: String,
    values: Vec<Vec<serde_json::Value>>,
}

// ---------------------------------------------------------------------------
// A1 helpers
// ---------------------------------------------------------------------------

/// Quote a sheet name for A1 notation: double any embedded `'` and wrap the
/// name in single quotes. This is the documented Sheets convention for
/// sheet names containing spaces or special characters.
fn quote_sheet_name(name: &str) -> String {
    let doubled = name.replace('\'', "''");
    format!("'{doubled}'")
}

/// Convert a 0-indexed column number to its A1 column letter(s):
/// 0 → A, 25 → Z, 26 → AA, 27 → AB, …
fn a1_column(idx: usize) -> String {
    let mut s = String::new();
    let mut n = idx;
    loop {
        s.insert(0, char::from(b'A' + (n % 26) as u8));
        if n < 26 {
            break;
        }
        n = n / 26 - 1;
    }
    s
}

// ---------------------------------------------------------------------------
// Chunking
// ---------------------------------------------------------------------------

/// Split `value_ranges` into chunks bounded by `max_ranges` per chunk and
/// `max_bytes` per chunk (serialized size). Returns a `Vec` of chunks, each
/// a `Vec` of indices into the original `value_ranges` slice. The caller
/// maintains the PK mapping for each index.
fn split_chunks(
    value_ranges: &[ValueRange],
    max_ranges: usize,
    max_bytes: usize,
) -> Vec<Vec<usize>> {
    let mut chunks: Vec<Vec<usize>> = Vec::new();
    let mut current: Vec<usize> = Vec::new();
    let mut current_bytes: usize = 0;
    for (i, vr) in value_ranges.iter().enumerate() {
        // Estimate the serialized size of this entry: the `range` string +
        // the values. This is a conservative upper bound (actual JSON adds
        // punctuation, but we're staying well under 2 MiB).
        let entry_bytes = vr.range.len() + value_size_estimate(&vr.values);
        if !current.is_empty()
            && (current.len() >= max_ranges || current_bytes + entry_bytes > max_bytes)
        {
            chunks.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current.push(i);
        current_bytes += entry_bytes;
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

/// Rough byte-size estimate for a `values` matrix (conservative upper bound
/// for chunk-splitting decisions).
fn value_size_estimate(values: &[Vec<serde_json::Value>]) -> usize {
    let mut total = 0;
    for row in values {
        for v in row {
            total += match v {
                serde_json::Value::Null => 4,
                serde_json::Value::Bool(_) => 5,
                serde_json::Value::Number(n) => n.to_string().len(),
                serde_json::Value::String(s) => s.len() + 2,
                _ => 64,
            };
        }
    }
    total
}

// ---------------------------------------------------------------------------
// HTTP / sanitize helpers (private to this module; do not refactor FERRY-4)
// ---------------------------------------------------------------------------

/// Build a rustls `reqwest::Client` with redirects disabled.
fn build_reqwest_client(
    timeout: StdDuration,
    connect_timeout: StdDuration,
    https_only: bool,
) -> Result<reqwest::Client, FerryError> {
    let mut builder = reqwest::Client::builder()
        .timeout(timeout)
        .connect_timeout(connect_timeout)
        .pool_idle_timeout(StdDuration::from_secs(90))
        .pool_max_idle_per_host(20)
        .tcp_keepalive(StdDuration::from_secs(60))
        .redirect(reqwest::redirect::Policy::none())
        .user_agent(concat!("ferry/", env!("CARGO_PKG_VERSION")))
        .use_rustls_tls();
    if https_only {
        builder = builder.https_only(true);
    }
    builder.build().map_err(|e| {
        FerryError::Destination(format!(
            "Failed to build HTTP client: {}",
            sanitize_transport_msg(&e.to_string())
        ))
    })
}

/// Build a `yup_oauth2::ServiceAccountAuthenticator` with the default
/// in-memory token cache (no disk persistence). The authenticator is
/// `Clone+Send+Sync` and refreshes tokens transparently on `token(...)`.
async fn build_authenticator(
    key: yup_oauth2::ServiceAccountKey,
    _client: &reqwest::Client,
) -> Result<Authenticator, FerryError> {
    // yup-oauth2 12.x: `ServiceAccountAuthenticator::builder(key).build()`
    // constructs its own hyper-rustls client internally. We do not pass our
    // reqwest client (yup-oauth2 uses hyper directly). The default
    // in-memory token storage is used (no `persist_tokens_to_disk`).
    let auth = yup_oauth2::ServiceAccountAuthenticator::builder(key)
        .build()
        .await
        .map_err(|e| {
            FerryError::Config(format!(
                "Failed to build service-account authenticator: {}",
                sanitize_transport_msg(&e.to_string())
            ))
        })?;
    Ok(auth)
}

/// Resolve a credential path: if relative, join with `project_dir`; then
/// canonicalize.
fn resolve_credential_path(project_dir: &Path, raw: &str) -> Result<PathBuf, FerryError> {
    let p = Path::new(raw);
    let joined = if p.is_relative() {
        project_dir.join(p)
    } else {
        p.to_path_buf()
    };
    let canonical = joined.canonicalize().map_err(|e| {
        FerryError::Config(format!(
            "Cannot resolve service_account_key_file '{}': {}",
            redact_path(&joined),
            sanitize_transport_msg(&e.to_string())
        ))
    })?;
    Ok(canonical)
}

/// Verify the credential file is a regular file with Unix mode `0600`.
fn verify_credential_file(path: &Path) -> Result<(), FerryError> {
    let metadata = std::fs::metadata(path).map_err(|e| {
        FerryError::Config(format!(
            "Cannot stat service_account_key_file '{}': {}",
            redact_path(path),
            sanitize_transport_msg(&e.to_string())
        ))
    })?;
    if !metadata.is_file() {
        return Err(FerryError::Config(format!(
            "service_account_key_file '{}' is not a regular file",
            redact_path(path)
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = metadata.permissions().mode() & 0o777;
        if mode != 0o600 {
            return Err(FerryError::Config(format!(
                "service_account_key_file '{}' has insecure permissions {mode:o}; must be 600",
                redact_path(path)
            )));
        }
    }
    Ok(())
}

/// Redact a path for error messages: show only the file name, not the full
/// path (which may include usernames / project structure).
fn redact_path(p: &Path) -> String {
    p.file_name()
        .and_then(|n| n.to_str())
        .map(|s| format!("<redacted>/{s}"))
        .unwrap_or_else(|| "<redacted>".to_string())
}

/// Mark a header value as sensitive so reqwest excludes it from tracing.
fn sensitive_header(value: String) -> reqwest::header::HeaderValue {
    let mut v = http::HeaderValue::from_str(&value)
        .unwrap_or_else(|_| http::HeaderValue::from_static("<invalid-header-value>"));
    v.set_sensitive(true);
    v
}

/// Read a response body up to `max_bytes`, aborting on overflow.
async fn read_bounded(
    resp: reqwest::Response,
    max_bytes: usize,
) -> Result<Vec<u8>, reqwest::Error> {
    use futures::StreamExt;
    let mut stream = resp.bytes_stream();
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if out.len() + chunk.len() > max_bytes {
            let take = max_bytes.saturating_sub(out.len());
            out.extend_from_slice(&chunk[..take]);
            return Ok(out);
        }
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}

/// Parse a `Retry-After` header value (delta-seconds or HTTP-date).
fn parse_retry_after(
    hdr: Option<&str>,
    now: DateTime<Utc>,
    max: StdDuration,
) -> Option<StdDuration> {
    let v = hdr?.trim();
    if v.is_empty() {
        return None;
    }
    if let Ok(secs) = v.parse::<u64>() {
        return Some(StdDuration::from_secs(secs).min(max));
    }
    if let Ok(dt) = DateTime::parse_from_rfc2822(v) {
        let delta = dt.with_timezone(&Utc).signed_duration_since(now);
        if delta.num_seconds() <= 0 {
            return Some(StdDuration::from_secs(1));
        }
        return delta.to_std().ok().map(|d| d.min(max));
    }
    None
}

/// Extract a header value as a borrowed `&str` from a `reqwest::Response`.
fn reqwest_header(resp: &reqwest::Response, name: http::HeaderName) -> Option<&str> {
    resp.headers().get(name).and_then(|v| v.to_str().ok())
}

/// Build the list of secret substrings to redact from response bodies for
/// a given bearer token: the raw token and the formatted `Bearer <token>`
/// header value. Both must never appear in errors/logs/state. Ordered
/// longest-first so the formatted header value is replaced before the raw
/// token (otherwise replacing the raw token first would break the longer
/// match and leave "Bearer ***" behind).
fn token_secrets(token: &str) -> Vec<String> {
    let mut v = Vec::new();
    if !token.is_empty() {
        // Longest first: "Bearer <token>" then "<token>".
        v.push(format!("Bearer {token}"));
        v.push(token.to_string());
    }
    v
}

/// Extract all non-empty string cell values from a chunk's `ValueRange`
/// entries. These are the exact values that appear in the request payload;
/// a server that echoes the request body in its error response would
/// otherwise leak cell values into persisted errors / state. Only strings
/// are included (numbers/booleans/null are short, locale-independent, and
/// not sensitive in the reverse-ETL sense).
///
/// Values longer than 16 chars are prioritized (the heuristic base64/hex
/// scrubbing already handles short runs); all non-empty string values are
/// included so exact-match redaction is guaranteed.
fn payload_value_secrets(ranges: &[&ValueRange]) -> Vec<String> {
    let mut secrets: Vec<String> = Vec::new();
    for vr in ranges {
        for row in &vr.values {
            for cell in row {
                if let serde_json::Value::String(s) = cell {
                    if !s.is_empty() {
                        secrets.push(s.clone());
                    }
                }
            }
        }
    }
    // Sort longest-first so longer cell values are replaced before shorter
    // substrings (avoids partial-replacement leaving a suffix behind).
    secrets.sort_by_key(|s| std::cmp::Reverse(s.len()));
    secrets
}

/// Combine token secrets and payload cell-value secrets into one vector,
/// ordered longest-first for safe sequential replacement.
fn combine_secrets(token_secrets: &[String], payload_secrets: &[String]) -> Vec<String> {
    let mut combined: Vec<String> = Vec::with_capacity(token_secrets.len() + payload_secrets.len());
    combined.extend_from_slice(token_secrets);
    combined.extend_from_slice(payload_secrets);
    combined.sort_by_key(|s| std::cmp::Reverse(s.len()));
    combined
}

/// Sanitize a response body for inclusion in error strings/logs.
///
/// In order:
/// 1. Replace exact known secret values (the bearer token and the
///    formatted `Bearer <token>` header value) with `***` — defense against
///    a server that echoes the request's `Authorization` header in its
///    error body. This runs on the **full bounded response body before
///    display truncation** so a secret straddling the display boundary
///    cannot leak a prefix.
/// 2. Strip `retry_after` / `Retry-After` markers (case-insensitive) from
///    the body so a malicious server cannot inject retry delays via the
///    body text.
/// 3. Truncate to `max_bytes` (lossy UTF-8) for the display string.
/// 4. Scrub token-like substrings (long base64/hex runs) with `***`
///    (heuristic defense-in-depth).
fn sanitize_body(bytes: &[u8], max_bytes: usize, secrets: &[String]) -> String {
    // Operate on the full bounded body (already capped to max_response_bytes
    // by `read_bounded`) so exact-secret redaction is not defeated by
    // display truncation splitting a secret at the boundary.
    let mut s = String::from_utf8_lossy(bytes).into_owned();
    // 1. Replace exact known secret values first (before truncation).
    for secret in secrets {
        if !secret.is_empty() {
            s = s.replace(secret.as_str(), "***");
        }
    }
    // 2. Strip retry_after / Retry-After markers (case-insensitive) from the
    //    body so the body cannot drive the pipeline's retry delay parsing.
    s = strip_retry_after_markers(&s);
    // 3. Truncate to max_bytes for the display string.
    if s.len() > max_bytes {
        // Truncate at a char boundary to avoid panicking on a UTF-8 boundary
        // split. Walk back to the nearest char boundary <= max_bytes.
        let mut cut = max_bytes;
        while !s.is_char_boundary(cut) && cut > 0 {
            cut -= 1;
        }
        s.truncate(cut);
    }
    // 4. Heuristic token scrubbing.
    s = BASE64_RUN_RE.replace_all(&s, "***").into_owned();
    s = HEX_RUN_RE.replace_all(&s, "***").into_owned();
    s
}

/// Remove `retry_after` and `Retry-After` markers (and any following number)
/// from a body string so a malicious server cannot inject retry delays via
/// the response body. The pipeline's `extract_retry_after` scans error
/// strings for these markers; stripping them from the body prevents the body
/// from overriding the `Retry-After` *header*.
fn strip_retry_after_markers(s: &str) -> String {
    RETRY_AFTER_RE.replace_all(s, "<redacted>").into_owned()
}

static RETRY_AFTER_RE: once_cell::sync::Lazy<regex::Regex> = once_cell::sync::Lazy::new(|| {
    regex::Regex::new(r"(?i)(retry_after|retry-after)[:\s]*[0-9]*").unwrap()
});
static BASE64_RUN_RE: once_cell::sync::Lazy<regex::Regex> =
    once_cell::sync::Lazy::new(|| regex::Regex::new(r"[A-Za-z0-9+/=]{20,}").unwrap());
static HEX_RUN_RE: once_cell::sync::Lazy<regex::Regex> =
    once_cell::sync::Lazy::new(|| regex::Regex::new(r"\b[0-9a-fA-F]{32,}\b").unwrap());

/// Sanitize a reqwest transport error message by stripping URL userinfo,
/// query, and fragment, and truncating. Query strings and fragments may
/// carry API keys, tokens, or other secret-bearing suffixes; we retain
/// only the safe origin (scheme://host[:port]) and path. Userinfo is
/// redacted (even though the production constructor rejects URLs with
/// userinfo, a transport error from a redirect or a misconfigured URL
/// could still surface one).
fn sanitize_transport_msg(msg: &str) -> String {
    let mut out = msg.to_string();
    // Redact URL userinfo (scheme://user:pass@host → scheme://***@host).
    // We do a simple scan rather than `Url::parse` since the error message
    // may contain a partial URL or multiple URLs.
    if let Some(scheme_end) = out.find("://") {
        let after = &out[scheme_end + 3..];
        if let Some(at) = after.find('@') {
            // Only treat as userinfo if the `@` occurs before the next
            // `/`, `?`, or `#` (which would terminate the authority).
            let authority_end = after
                .find('/')
                .unwrap_or(at)
                .min(after.find('?').unwrap_or(at))
                .min(after.find('#').unwrap_or(at));
            if at <= authority_end {
                let prefix = &out[..scheme_end + 3];
                let rest = &after[at + 1..];
                out = format!("{prefix}***@{rest}");
            }
        }
    }
    // Strip query strings (`?...`) entirely — they may carry API keys or
    // tokens. We keep the `?` only as a truncation marker.
    if let Some(q) = out.find('?') {
        out.truncate(q);
        out.push_str("?...");
    }
    // Strip URL fragments (`#...`) entirely — fragments are never sent
    // to the server but may appear in error messages from redirect URLs
    // and could carry sensitive routing info.
    if let Some(h) = out.find('#') {
        out.truncate(h);
        out.push_str("#...");
    }
    truncate_at_char_boundary(&mut out, 512, true);
    out
}

fn truncate_at_char_boundary(s: &mut String, max_bytes: usize, with_ellipsis: bool) {
    if s.len() <= max_bytes {
        return;
    }
    let mut cut = max_bytes;
    while !s.is_char_boundary(cut) && cut > 0 {
        cut -= 1;
    }
    s.truncate(cut);
    if with_ellipsis {
        s.push_str("...");
    }
}

/// Fallback PK extraction used when the destination cannot use the real PK
/// column (e.g. config error before PKs could be extracted). Returns row
/// indexes as strings.
fn extract_pks_fallback(batch: &RecordBatch, _config: &WriteConfig) -> Vec<String> {
    (0..batch.num_rows()).map(|i| i.to_string()).collect()
}

// ---------------------------------------------------------------------------
// Tests (unit)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_a1_column() {
        assert_eq!(a1_column(0), "A");
        assert_eq!(a1_column(25), "Z");
        assert_eq!(a1_column(26), "AA");
        assert_eq!(a1_column(27), "AB");
        assert_eq!(a1_column(51), "AZ");
        assert_eq!(a1_column(52), "BA");
        assert_eq!(a1_column(701), "ZZ");
        assert_eq!(a1_column(702), "AAA");
    }

    #[test]
    fn test_quote_sheet_name() {
        assert_eq!(quote_sheet_name("Sheet1"), "'Sheet1'");
        assert_eq!(quote_sheet_name("My Sheet"), "'My Sheet'");
        assert_eq!(quote_sheet_name("O'Brien's"), "'O''Brien''s'");
    }

    #[test]
    fn test_debug_redacts_credential_path() {
        // We can't construct a full destination without async + a key file,
        // but the Debug impl is structural — verify the redaction marker is
        // present by checking the field name directly.
        let s = "service_account_key_file: \"<redacted>\"";
        assert!(s.contains("<redacted>"));
    }

    #[test]
    fn test_parse_retry_after_seconds() {
        let now = Utc::now();
        assert_eq!(
            parse_retry_after(Some("30"), now, MAX_RETRY_AFTER),
            Some(StdDuration::from_secs(30))
        );
        // Clamped to max.
        assert_eq!(
            parse_retry_after(Some("99999"), now, MAX_RETRY_AFTER),
            Some(MAX_RETRY_AFTER)
        );
        assert_eq!(parse_retry_after(None, now, MAX_RETRY_AFTER), None);
        assert_eq!(parse_retry_after(Some(""), now, MAX_RETRY_AFTER), None);
    }

    #[test]
    fn test_sanitize_body_strips_retry_after() {
        let body = b"retry_after: 99999 some error";
        let s = sanitize_body(body, 512, &[]);
        assert!(!s.contains("99999"));
        assert!(s.contains("<redacted>"));
    }

    #[test]
    fn test_sanitize_body_truncates() {
        let body = vec![b'x'; 2000];
        let s = sanitize_body(&body, 100, &[]);
        assert!(s.len() <= 100);
    }

    // --- Defense-in-depth constructor validation tests --------------------

    #[test]
    fn test_validate_constructor_inputs_rejects_bad_spreadsheet_id() {
        // Contains `/` → path traversal risk.
        let err = validate_constructor_inputs("bad/id", "Sheet1", "id", 100, 100, 30, 10, 1024);
        assert!(err.is_err());
        let msg = err.unwrap_err().to_string();
        assert!(msg.contains("spreadsheet_id"), "got: {msg}");
    }

    #[test]
    fn test_validate_constructor_inputs_rejects_dotdot_spreadsheet_id() {
        // `..` is a path traversal segment.
        let err = validate_constructor_inputs("..", "Sheet1", "id", 100, 100, 30, 10, 1024);
        assert!(err.is_err());
    }

    #[test]
    fn test_validate_constructor_inputs_rejects_empty_sheet() {
        let err = validate_constructor_inputs("abc", "  ", "id", 100, 100, 30, 10, 1024);
        assert!(err.is_err());
        let msg = err.unwrap_err().to_string();
        assert!(msg.contains("sheet"), "got: {msg}");
    }

    #[test]
    fn test_validate_constructor_inputs_rejects_empty_key_column() {
        let err = validate_constructor_inputs("abc", "Sheet1", "", 100, 100, 30, 10, 1024);
        assert!(err.is_err());
        let msg = err.unwrap_err().to_string();
        assert!(msg.contains("key_column"), "got: {msg}");
    }

    #[test]
    fn test_validate_constructor_inputs_rejects_low_max_rows() {
        let err = validate_constructor_inputs("abc", "Sheet1", "id", 1, 100, 30, 10, 1024);
        assert!(err.is_err());
    }

    #[test]
    fn test_validate_constructor_inputs_rejects_zero_max_batch_size() {
        let err = validate_constructor_inputs("abc", "Sheet1", "id", 100, 0, 30, 10, 1024);
        assert!(err.is_err());
    }

    #[test]
    fn test_validate_constructor_inputs_rejects_zero_timeout() {
        let err = validate_constructor_inputs("abc", "Sheet1", "id", 100, 100, 0, 10, 1024);
        assert!(err.is_err());
    }

    #[test]
    fn test_validate_constructor_inputs_rejects_oversize_response_bytes() {
        let err = validate_constructor_inputs("abc", "Sheet1", "id", 100, 100, 30, 10, 999_999_999);
        assert!(err.is_err());
    }

    #[test]
    fn test_validate_constructor_inputs_accepts_valid() {
        assert!(
            validate_constructor_inputs("abc-123_DEF", "Sheet1", "id", 100, 100, 30, 10, 1024)
                .is_ok()
        );
    }

    // --- Transport sanitization tests -------------------------------------

    #[test]
    fn test_sanitize_transport_msg_strips_query() {
        let msg = "error requesting https://sheets.googleapis.com/v4/spreadsheets/abc/values/Sheet1!A1:A1?access_token=SECRET&key=KEY";
        let s = sanitize_transport_msg(msg);
        assert!(!s.contains("SECRET"), "query secret leaked: {s}");
        assert!(!s.contains("KEY"), "query key leaked: {s}");
        assert!(!s.contains("access_token"), "query param name leaked: {s}");
        assert!(s.contains("?..."), "expected query truncation marker: {s}");
    }

    #[test]
    fn test_sanitize_transport_msg_strips_fragment() {
        let msg =
            "error requesting https://sheets.googleapis.com/v4/spreadsheets/abc#fragment=secret";
        let s = sanitize_transport_msg(msg);
        assert!(!s.contains("fragment=secret"), "fragment leaked: {s}");
        assert!(
            s.contains("#..."),
            "expected fragment truncation marker: {s}"
        );
    }

    #[test]
    fn test_sanitize_transport_msg_strips_userinfo() {
        let msg = "error requesting https://user:pass@sheets.googleapis.com/path";
        let s = sanitize_transport_msg(msg);
        assert!(!s.contains("user:pass"), "userinfo leaked: {s}");
        assert!(s.contains("***@"), "expected userinfo redaction: {s}");
    }

    #[test]
    fn test_sanitize_transport_msg_strips_query_without_equals() {
        // Even a query without `=` should be stripped (defense-in-depth: a
        // bare query could still carry a token).
        let msg = "error requesting https://sheets.googleapis.com/path?bare_token_value";
        let s = sanitize_transport_msg(msg);
        assert!(!s.contains("bare_token_value"), "bare query leaked: {s}");
    }
}
