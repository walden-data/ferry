use std::sync::Arc;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use chrono::Duration;
use serde_json::Value;

use ferry_core::error::FerryError;
use ferry_core::traits::{
    Destination, IdempotencyCapability, RateLimit, RemoveCapability, RemoveResult, RowError,
    WriteConfig, WriteResult,
};

/// Configurable behavior for the mock REST destination.
pub enum MockBehavior {
    /// All rows succeed.
    Success,
    /// All rows fail with HTTP 429 (rate limited).
    RateLimited {
        /// Duration to suggest as Retry-After.
        retry_after: Duration,
    },
    /// All rows fail with HTTP 500 (server error).
    ServerError,
    /// First N rows succeed, remaining rows fail.
    PartialSuccess {
        /// Number of rows to succeed before failing.
        fail_after: usize,
    },
    /// Custom behavior via a closure.
    Custom(Arc<dyn Fn(&RecordBatch) -> WriteResult + Send + Sync>),
}

impl std::fmt::Debug for MockBehavior {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MockBehavior::Success => f.write_str("Success"),
            MockBehavior::RateLimited { retry_after } => f
                .debug_struct("RateLimited")
                .field("retry_after", retry_after)
                .finish(),
            MockBehavior::ServerError => f.write_str("ServerError"),
            MockBehavior::PartialSuccess { fail_after } => f
                .debug_struct("PartialSuccess")
                .field("fail_after", fail_after)
                .finish(),
            MockBehavior::Custom(_) => f.write_str("Custom(<fn>)"),
        }
    }
}

/// A mock REST API destination for testing.
///
/// Does not make real HTTP calls. Simulates responses based on
/// configurable [`MockBehavior`].
pub struct MockRestDestination {
    behavior: MockBehavior,
    sync_name: String,
}

impl MockRestDestination {
    /// Create a new `MockRestDestination` with the given behavior.
    pub fn new(behavior: MockBehavior, sync_name: &str) -> Self {
        Self {
            behavior,
            sync_name: sync_name.to_string(),
        }
    }

    /// Create a `MockRestDestination` that always succeeds.
    pub fn success() -> Self {
        Self::new(MockBehavior::Success, "mock_rest")
    }

    /// Create a `MockRestDestination` that rate-limits all rows.
    pub fn rate_limited(retry_after_secs: u64) -> Self {
        Self::new(
            MockBehavior::RateLimited {
                retry_after: Duration::seconds(retry_after_secs as i64),
            },
            "mock_rest",
        )
    }

    /// Create a `MockRestDestination` that returns server errors for all rows.
    pub fn server_error() -> Self {
        Self::new(MockBehavior::ServerError, "mock_rest")
    }

    /// Create a `MockRestDestination` that succeeds for the first N rows,
    /// then fails the rest.
    pub fn partial_success(fail_after: usize) -> Self {
        Self::new(MockBehavior::PartialSuccess { fail_after }, "mock_rest")
    }
}

#[async_trait]
impl Destination for MockRestDestination {
    fn name(&self) -> &str {
        &self.sync_name
    }

    async fn check_connection(&self) -> Result<(), FerryError> {
        Ok(())
    }

    async fn write(
        &self,
        batch: &RecordBatch,
        _config: &WriteConfig,
    ) -> Result<WriteResult, FerryError> {
        match &self.behavior {
            MockBehavior::Success => Ok(WriteResult {
                rows_written: batch.num_rows(),
                errors: vec![],
            }),
            MockBehavior::RateLimited { retry_after } => {
                let errors: Vec<RowError> = (0..batch.num_rows())
                    .map(|i| RowError {
                        primary_key: i.to_string(),
                        error: format!(
                            "HTTP 429 Too Many Requests (retry after {}s)",
                            retry_after.num_seconds()
                        ),
                    })
                    .collect();
                Ok(WriteResult {
                    rows_written: 0,
                    errors,
                })
            }
            MockBehavior::ServerError => {
                let errors: Vec<RowError> = (0..batch.num_rows())
                    .map(|i| RowError {
                        primary_key: i.to_string(),
                        error: "HTTP 500 Internal Server Error".to_string(),
                    })
                    .collect();
                Ok(WriteResult {
                    rows_written: 0,
                    errors,
                })
            }
            MockBehavior::PartialSuccess { fail_after } => {
                let num_rows = batch.num_rows();
                let fail_after = *fail_after;
                let rows_written = fail_after.min(num_rows);
                let errors: Vec<RowError> = (rows_written..num_rows)
                    .map(|i| RowError {
                        primary_key: i.to_string(),
                        error: "HTTP 500 Internal Server Error".to_string(),
                    })
                    .collect();
                Ok(WriteResult {
                    rows_written,
                    errors,
                })
            }
            MockBehavior::Custom(f) => Ok(f(batch)),
        }
    }

    fn max_batch_size(&self) -> usize {
        75
    }

    fn rate_limit(&self) -> Option<RateLimit> {
        Some(RateLimit {
            requests_per_second: Some(10.0),
            concurrent_requests: None,
        })
    }

    fn idempotency(&self) -> IdempotencyCapability {
        IdempotencyCapability::Idempotent
    }

    fn remove_capability(&self) -> RemoveCapability {
        RemoveCapability::RemoveByKey
    }

    async fn remove(
        &self,
        keys: &[Value],
        _config: &WriteConfig,
    ) -> Result<RemoveResult, FerryError> {
        Ok(RemoveResult {
            rows_removed: keys.len(),
            errors: vec![],
        })
    }

    async fn replace_all(
        &self,
        batch: &RecordBatch,
        config: &WriteConfig,
    ) -> Result<WriteResult, FerryError> {
        // Same as write for mock
        self.write(batch, config).await
    }
}

// ---------------------------------------------------------------------------
// Production REST destination
// ---------------------------------------------------------------------------

use std::time::Duration as StdDuration;

use chrono::{DateTime, Utc};
use ferry_core::config::{AuthConfig, DestinationConfig, HeaderConfig};
use ferry_core::validation::{
    DEFAULT_CONNECT_TIMEOUT_SECS, DEFAULT_MAX_BATCH_SIZE, DEFAULT_MAX_RESPONSE_BYTES,
    DEFAULT_TIMEOUT_SECS,
};
use http::header::AUTHORIZATION;
use minijinja::Environment;
use reqwest::redirect::Policy;
use reqwest::{Client, Method};
use url::Url;

use crate::util::batch_to_json_rows;

/// Maximum `Retry-After` value the destination honors (5 minutes). Servers may
/// return absurd values; we always clamp.
const MAX_RETRY_AFTER: StdDuration = StdDuration::from_secs(300);
/// Truncate sanitized response bodies in error strings to this many bytes.
const SANITIZE_BODY_BYTES: usize = 512;
/// Maximum response body size cap used when validating config defaults.
const MAX_RESPONSE_BYTES_CAP: usize = 64 * 1024 * 1024;

/// A production REST API destination that issues real HTTP requests.
///
/// One `reqwest::Client` is constructed in [`RestDestination::new`] and reused
/// for all requests (per-host connection pooling, TLS session reuse). The
/// connector converts each `RecordBatch` slice to a JSON array of row objects
/// (or renders an optional minijinja `body_template`) and POSTs (or other
/// configured method) it to the configured URL.
///
/// Responses are classified into:
/// - **2xx** → all rows succeed;
/// - **408, 425, 429, 5xx** → retryable; all rows get a single `RowError`
///   carrying `"HTTP {code}: {sanitized_body}; retry_after: {secs}"`;
/// - **other 4xx** → permanent; same shape, no `retry_after`;
/// - **network/transport error** → retryable; error string has no `"HTTP NNN"`,
///   so the pipeline falls back to its default retry classification.
///
/// Per-row request mode and per-row response→row-status mapping are deferred
/// (see FERRY-4 plan). PKs are row-index strings, matching `MockRestDestination`.
pub struct RestDestination {
    client: Client,
    url: Url,
    method: Method,
    headers: Vec<HeaderConfig>,
    auth: AuthConfig,
    body_template: Option<Environment<'static>>,
    body_template_name: Option<String>,
    timeout: StdDuration,
    max_response_bytes: usize,
    max_batch_size: usize,
    allow_http: bool,
    sync_name: String,
}

impl std::fmt::Debug for RestDestination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RestDestination")
            .field("url", &redact_url(&self.url))
            .field("method", &self.method.as_str())
            .field("timeout", &self.timeout)
            .field("max_response_bytes", &self.max_response_bytes)
            .field("max_batch_size", &self.max_batch_size)
            .field("allow_http", &self.allow_http)
            .field("sync_name", &self.sync_name)
            .field("auth", &self.auth) // redacting Debug
            .finish_non_exhaustive()
    }
}

impl RestDestination {
    /// Construct a new `RestDestination` from a resolved `DestinationConfig::Rest`.
    ///
    /// The constructor is synchronous (the CLI/Python factories are sync) and
    /// builds a single `reqwest::Client` for the lifetime of the destination.
    /// If `body_template` is set, it is compiled once into a minijinja
    /// `Environment` stored on the struct (compile-once, render-many).
    pub fn new(config: &DestinationConfig, sync_name: &str) -> Result<Self, FerryError> {
        let DestinationConfig::Rest {
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
        } = config
        else {
            return Err(FerryError::Config(
                "RestDestination::new called with a non-Rest destination config".to_string(),
            ));
        };

        let allow_http = allow_http.unwrap_or(false);

        // Defense-in-depth: re-validate the URL scheme here. Validation should
        // already have caught this, but we want to fail safely even if the
        // config reaches us without validation (e.g. via Python factory).
        let parsed_url = Url::parse(url)
            .map_err(|e| FerryError::Config(format!("REST destination URL is invalid: {e}")))?;
        // Reject embedded userinfo (user:pass@) — secrets must live in
        // secrets.toml, not the URL. This prevents leaks via logs/Debug/errors.
        if !parsed_url.username().is_empty() || parsed_url.password().is_some() {
            return Err(FerryError::Config(
                "REST destination URL must not contain userinfo (user:password@); store credentials in secrets.toml via the auth config".to_string(),
            ));
        }
        let scheme_ok = match parsed_url.scheme() {
            "https" => true,
            "http" => allow_http,
            other => {
                return Err(FerryError::Config(format!(
                    "REST destination URL scheme '{other}' is not supported; use https (or http with allow_http: true for localhost testing)"
                )));
            }
        };
        if !scheme_ok {
            return Err(FerryError::Config(
                "REST destination URL scheme 'http' is not allowed by default; set allow_http: true to opt in (intended for localhost testing only)".to_string(),
            ));
        }

        // Method (default POST).
        let method_str = method.as_deref().unwrap_or("POST").to_uppercase();
        let method = match method_str.as_str() {
            "GET" => Method::GET,
            "POST" => Method::POST,
            "PUT" => Method::PUT,
            "PATCH" => Method::PATCH,
            "DELETE" => Method::DELETE,
            other => {
                return Err(FerryError::Config(format!(
                    "REST method '{other}' is not supported; expected one of GET, POST, PUT, PATCH, DELETE"
                )));
            }
        };

        // Body template: compile once.
        let (body_template_env, body_template_name) = if let Some(template) = body_template {
            let mut env = Environment::new();
            // Autoescape is None by default for non-HTML; templates must use
            // `| tojson` for proper JSON output.
            let name = "body".to_string();
            env.add_template_owned(name.clone(), template.clone())
                .map_err(|e| FerryError::Config(format!("Invalid body_template: {e}")))?;
            (Some(env), Some(name))
        } else {
            (None, None)
        };

        let timeout = StdDuration::from_secs(timeout_secs.unwrap_or(DEFAULT_TIMEOUT_SECS));
        let connect_timeout =
            StdDuration::from_secs(connect_timeout_secs.unwrap_or(DEFAULT_CONNECT_TIMEOUT_SECS));
        let max_response_bytes = max_response_bytes
            .unwrap_or(DEFAULT_MAX_RESPONSE_BYTES)
            .min(MAX_RESPONSE_BYTES_CAP);
        let max_batch_size = max_batch_size.unwrap_or(DEFAULT_MAX_BATCH_SIZE).max(1);

        let mut builder = Client::builder()
            .timeout(timeout)
            .connect_timeout(connect_timeout)
            .pool_idle_timeout(StdDuration::from_secs(90))
            .pool_max_idle_per_host(20)
            .tcp_keepalive(StdDuration::from_secs(60))
            // Disable redirects entirely. A reverse-ETL connector should not
            // silently follow redirects to attacker-controlled hosts (SSRF /
            // credential leakage). reqwest does not strip `Authorization` /
            // sensitive headers across redirect hosts, so following redirects
            // could leak API keys. The connector surfaces 3xx as a non-2xx
            // response to be classified by the pipeline.
            .redirect(Policy::none())
            .user_agent(concat!("ferry/", env!("CARGO_PKG_VERSION")))
            .use_rustls_tls();
        if !allow_http {
            builder = builder.https_only(true);
        }
        let client = builder.build().map_err(|e| {
            FerryError::Destination(format!(
                "Failed to build HTTP client: {}",
                sanitize_transport_msg(&e.to_string())
            ))
        })?;

        let headers = headers.clone().unwrap_or_default();
        let auth = auth.clone().unwrap_or(AuthConfig::None);

        Ok(Self {
            client,
            url: parsed_url,
            method,
            headers,
            auth,
            body_template: body_template_env,
            body_template_name,
            timeout,
            max_response_bytes,
            max_batch_size,
            allow_http,
            sync_name: sync_name.to_string(),
        })
    }

    /// Build the request body bytes for a batch.
    ///
    /// Default body is a JSON array of row objects. If a `body_template` is
    /// configured, it is rendered with a minijinja context `{"rows": [...],
    /// "sync": {"name": ..., "batch_index": ..., "total_batches": ...}}`.
    fn render_body(
        &self,
        batch: &RecordBatch,
        write_config: &WriteConfig,
    ) -> Result<Vec<u8>, FerryError> {
        let rows = batch_to_json_rows(batch);
        if let (Some(env), Some(name)) = (&self.body_template, &self.body_template_name) {
            let ctx = minijinja::context! {
                rows => rows,
                sync => minijinja::context! {
                    name => write_config.sync_name,
                    batch_index => write_config.batch_index,
                    total_batches => write_config.total_batches,
                },
            };
            let rendered = env
                .get_template(name)
                .map_err(|e| FerryError::Destination(format!("Template lookup failed: {e}")))?
                .render(ctx)
                .map_err(|e| {
                    FerryError::Destination(format!("body_template render failed: {e}"))
                })?;
            Ok(rendered.into_bytes())
        } else {
            serde_json::to_vec(&rows).map_err(|e| {
                FerryError::Destination(format!("Failed to serialize batch JSON: {e}"))
            })
        }
    }

    /// Apply configured static headers + per-request auth to a `RequestBuilder`.
    fn apply_headers_and_auth(
        &self,
        mut rb: reqwest::RequestBuilder,
    ) -> Result<reqwest::RequestBuilder, FerryError> {
        // Static headers (from YAML). CRLF already rejected at validation; we
        // still use `HeaderValue::from_str` for defense-in-depth. Header
        // values are marked sensitive so reqwest excludes them from
        // tracing/debug output.
        for h in &self.headers {
            let name = http::HeaderName::from_bytes(h.name.as_bytes()).map_err(|e| {
                FerryError::Config(format!("Invalid header name '{}': {}", h.name, e))
            })?;
            let mut value = http::HeaderValue::from_str(&h.value).map_err(|e| {
                FerryError::Config(format!("Invalid header value for '{}': {}", h.name, e))
            })?;
            value.set_sensitive(true);
            rb = rb.header(name, value);
        }
        // Auth: applied per-request (NOT in default_headers on the shared
        // client) to avoid leaking to redirect hosts.
        rb = apply_auth(rb, &self.auth)?;
        Ok(rb)
    }

    /// Build the set of exact secret substrings that must never appear in
    /// persisted/loggable error messages. Used to scrub response bodies that
    /// echo request credentials.
    fn secret_values(&self) -> Vec<String> {
        let mut secrets = Vec::new();
        match &self.auth {
            AuthConfig::Bearer { token } => {
                if !token.is_empty() {
                    secrets.push(token.clone());
                    // Also the formatted header value.
                    secrets.push(format!("Bearer {token}"));
                }
            }
            AuthConfig::Basic { username, password } => {
                // base64(user:pass) as it appears on the wire.
                use base64::{Engine as _, engine::general_purpose};
                let creds = format!("{username}:{password}");
                secrets.push(creds.clone());
                secrets.push(general_purpose::STANDARD.encode(&creds));
                // Also include raw password in case the server echoes just it.
                if !password.is_empty() {
                    secrets.push(password.clone());
                }
            }
            AuthConfig::ApiKey { value, .. } => {
                if !value.is_empty() {
                    secrets.push(value.clone());
                }
            }
            AuthConfig::None => {}
        }
        // Static configured header values (treated as sensitive on the wire).
        for h in &self.headers {
            if !h.value.is_empty() {
                secrets.push(h.value.clone());
            }
        }
        secrets
    }
}

/// Apply `AuthConfig` to a `RequestBuilder`, marking header values sensitive.
fn apply_auth(
    rb: reqwest::RequestBuilder,
    auth: &AuthConfig,
) -> Result<reqwest::RequestBuilder, FerryError> {
    match auth {
        AuthConfig::None => Ok(rb),
        AuthConfig::Bearer { token } => {
            if token.is_empty() {
                return Err(FerryError::Config(
                    "Bearer auth configured but token is empty (not resolved from secrets.toml?)"
                        .to_string(),
                ));
            }
            // Format as "Bearer <token>" and mark sensitive.
            let formatted = format!("Bearer {token}");
            let mut v = http::HeaderValue::from_str(&formatted).map_err(|e| {
                FerryError::Config(format!("Bearer token is not a valid header value: {e}"))
            })?;
            v.set_sensitive(true);
            Ok(rb.header(AUTHORIZATION, v))
        }
        AuthConfig::Basic { username, password } => {
            if username.is_empty() {
                return Err(FerryError::Config(
                    "Basic auth configured but username is empty".to_string(),
                ));
            }
            // Use reqwest's built-in basic_auth which handles base64 + sensitive header.
            Ok(rb.basic_auth(username, Some(password)))
        }
        AuthConfig::ApiKey { header_name, value } => {
            if header_name.is_empty() {
                return Err(FerryError::Config(
                    "ApiKey auth configured but header_name is empty".to_string(),
                ));
            }
            if value.is_empty() {
                return Err(FerryError::Config(
                    "ApiKey auth configured but value is empty (not resolved from secrets.toml?)"
                        .to_string(),
                ));
            }
            let name = http::HeaderName::from_bytes(header_name.as_bytes()).map_err(|e| {
                FerryError::Config(format!(
                    "Invalid api_key header name '{}': {}",
                    header_name, e
                ))
            })?;
            let mut v = http::HeaderValue::from_str(value).map_err(|e| {
                FerryError::Config(format!(
                    "Invalid api_key value for '{}': {}",
                    header_name, e
                ))
            })?;
            v.set_sensitive(true);
            Ok(rb.header(name, v))
        }
    }
}

#[async_trait]
impl Destination for RestDestination {
    fn name(&self) -> &str {
        &self.sync_name
    }

    async fn check_connection(&self) -> Result<(), FerryError> {
        // Issue a HEAD to the URL (fallback to GET via .send). Treat 2xx/3xx,
        // 404, 405 as "reachable". 401/403/5xx → error (sanitized).
        let rb = self.client.request(Method::HEAD, self.url.clone());
        let rb = self.apply_headers_and_auth(rb)?;
        let resp = rb.timeout(self.timeout).send().await.map_err(|e| {
            FerryError::Destination(format!(
                "HTTP transport: {}",
                sanitize_transport_msg(&e.to_string())
            ))
        })?;
        let status = resp.status().as_u16();
        if (200..400).contains(&status) || status == 404 || status == 405 {
            Ok(())
        } else {
            // Read a small slice of the body for a sanitized message.
            let body = read_bounded(resp, self.max_response_bytes)
                .await
                .unwrap_or_default();
            let secrets = self.secret_values();
            Err(FerryError::Destination(format!(
                "HTTP {}: {}",
                status,
                sanitize_body(&body, SANITIZE_BODY_BYTES, &secrets)
            )))
        }
    }

    async fn write(
        &self,
        batch: &RecordBatch,
        config: &WriteConfig,
    ) -> Result<WriteResult, FerryError> {
        let body = self.render_body(batch, config)?;
        let rb = self.client.request(self.method.clone(), self.url.clone());
        let rb = self.apply_headers_and_auth(rb)?;
        let rb = rb
            .header(http::header::CONTENT_TYPE, "application/json")
            .timeout(self.timeout)
            .body(body);

        let resp = rb.send().await.map_err(|e| {
            // Network/transport error: no "HTTP NNN" → pipeline default retry.
            FerryError::Destination(format!(
                "HTTP transport: {}",
                sanitize_transport_msg(&e.to_string())
            ))
        })?;
        let status = resp.status();

        // 2xx → success.
        if status.is_success() {
            // Drain the body to allow connection reuse; ignore contents.
            let _ = read_bounded(resp, self.max_response_bytes).await;
            return Ok(WriteResult {
                rows_written: batch.num_rows(),
                errors: vec![],
            });
        }

        // Non-2xx → all rows share the batch fate (per-row mapping deferred).
        let status_code = status.as_u16();
        let retry_after = parse_retry_after(
            resp.headers()
                .get(http::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok()),
            Utc::now(),
            MAX_RETRY_AFTER,
        );
        let body = read_bounded(resp, self.max_response_bytes)
            .await
            .unwrap_or_default();
        // Sanitize the body for inclusion in the error string:
        // 1. Strip `retry_after` / `Retry-After` markers from the body so a
        //    malicious server cannot inject retry delays via the body text
        //    (B4).
        // 2. Replace exact known secret values (auth token, Basic base64,
        //    api key, configured header values) with `***` in case the server
        //    echoes the request's credentials (B3).
        // 3. Truncate and apply heuristic token scrubbing as defense-in-depth.
        let secrets = self.secret_values();
        let sanitized = sanitize_body(&body, SANITIZE_BODY_BYTES, &secrets);
        let error_msg = match retry_after {
            Some(d) => format!(
                "HTTP {}: {}; retry_after: {}",
                status_code,
                sanitized,
                d.as_secs()
            ),
            None => format!("HTTP {}: {}", status_code, sanitized),
        };

        // Build per-row errors. Prefer real PKs (via `pk_col`) so the pipeline's
        // PK-based journal dedup works; fall back to row-index strings (matching
        // MockRestDestination) when the PK column is unknown.
        let pks: Vec<String> = match &config.pk_col {
            Some(col) => ferry_core::delivery::extract_pks(batch, col)
                .unwrap_or_else(|_| (0..batch.num_rows()).map(|i| i.to_string()).collect()),
            None => (0..batch.num_rows()).map(|i| i.to_string()).collect(),
        };
        let errors: Vec<RowError> = pks
            .into_iter()
            .map(|pk| RowError {
                primary_key: pk,
                error: error_msg.clone(),
            })
            .collect();

        Ok(WriteResult {
            rows_written: 0,
            errors,
        })
    }

    fn max_batch_size(&self) -> usize {
        self.max_batch_size
    }

    fn rate_limit(&self) -> Option<RateLimit> {
        // Pipeline-level governor handles rate limiting; per-destination rate
        // limit is deferred.
        None
    }

    fn idempotency(&self) -> IdempotencyCapability {
        match self.method {
            Method::PUT | Method::DELETE => IdempotencyCapability::Idempotent,
            _ => IdempotencyCapability::NotIdempotent, // POST default
        }
    }

    fn remove_capability(&self) -> RemoveCapability {
        RemoveCapability::None
    }

    async fn remove(
        &self,
        _keys: &[Value],
        _config: &WriteConfig,
    ) -> Result<RemoveResult, FerryError> {
        Err(FerryError::Destination(
            "REST destination does not support remove".to_string(),
        ))
    }

    async fn replace_all(
        &self,
        batch: &RecordBatch,
        config: &WriteConfig,
    ) -> Result<WriteResult, FerryError> {
        // For generic REST, replace_all is just a write.
        self.write(batch, config).await
    }
}

/// Read a response body up to `max_bytes`, aborting on overflow.
///
/// Streams the body chunk-by-chunk and stops as soon as `max_bytes` is
/// reached, even if `Content-Length` is absent or lies. This bounds memory
/// and prevents OOM from malicious/buggy servers. The connection is not
/// returned to the pool when the body is truncated (the stream is dropped).
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
            // Drop the stream; remaining chunks (and the connection) are discarded.
            return Ok(out);
        }
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}

/// Parse a `Retry-After` header value.
///
/// Two forms (RFC 7231 §7.1.3):
/// - delta-seconds: an integer;
/// - HTTP-date (RFC 2822 IMF-fixdate).
///
/// Always clamps to `max`. Malformed/absent → `None`.
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
            // Past/now → retry immediately with minimal delay.
            return Some(StdDuration::from_secs(1));
        }
        return delta.to_std().ok().map(|d| d.min(max));
    }
    None
}

/// Sanitize a response body for inclusion in error strings/logs.
///
/// In order:
/// 1. Replace exact known secret values (auth token, Basic base64, api key,
///    configured header values) with `***` — defense against a server that
///    echoes the request's credentials in its error body. This runs on the
///    **full bounded response body before display truncation** so a secret
///    straddling the display boundary cannot leak a prefix.
/// 2. Strip `retry_after` / `Retry-After` markers (case-insensitive) from
///    the body so a malicious server cannot inject retry delays via the body
///    text that `delivery.rs::extract_retry_after` would parse.
/// 3. Truncate to `max_bytes` (lossy UTF-8) for the display string.
/// 4. Scrub token-like substrings (long base64/hex runs) with `***`
///    (heuristic defense-in-depth).
///
/// `max_bytes` bounds the *display* string length; the response body itself
/// was already bounded to the configured `max_response_bytes` by `read_bounded`
/// before this function is called.
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
    s = RETRY_AFTER_RE.replace_all(&s, "<redacted>").into_owned();
    s = BASE64_RUN_RE.replace_all(&s, "***").into_owned();
    s = HEX_RUN_RE.replace_all(&s, "***").into_owned();
    s
}

// Compile-once regexes for `sanitize_body`. Using `once_cell::sync::Lazy`
// keeps them static (compile-once) and avoids re-compiling per call.
// `(?i)` makes the pattern case-insensitive so `retry_after`, `Retry-After`,
// `RETRY_AFTER`, etc. all match. `\s*` handles optional whitespace; the
// optional trailing digits/colons are consumed so the marker cannot drive
// `extract_retry_after`.
static RETRY_AFTER_RE: once_cell::sync::Lazy<regex::Regex> = once_cell::sync::Lazy::new(|| {
    // Match "retry_after" or "retry-after" (case-insensitive) followed by
    // optional colon/space and digits, replacing the whole match.
    regex::Regex::new(r"(?i)(retry_after|retry-after)[:\s]*[0-9]*").unwrap()
});
static BASE64_RUN_RE: once_cell::sync::Lazy<regex::Regex> =
    once_cell::sync::Lazy::new(|| regex::Regex::new(r"[A-Za-z0-9+/=]{20,}").unwrap());
static HEX_RUN_RE: once_cell::sync::Lazy<regex::Regex> =
    once_cell::sync::Lazy::new(|| regex::Regex::new(r"\b[0-9a-fA-F]{32,}\b").unwrap());

/// Truncate a string to at most `max_bytes` bytes, walking back to the
/// nearest UTF-8 character boundary to avoid panicking on a multi-byte
/// sequence split. Appends `...` only when `with_ellipsis` is true and the
/// string was actually truncated.
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

/// Remove `retry_after` and `Retry-After` markers (and any following number)
/// from a body string so a malicious server cannot inject retry delays via
/// the response body. The pipeline's `extract_retry_after` scans error
/// strings for these markers; stripping them from the body prevents the body
/// from overriding the `Retry-After` *header*.
///
/// This uses a compile-once case-insensitive regex that redacts **all**
/// occurrences (not just the first) and is panic-free under Unicode content
/// (it operates on `&str` with no manual byte-index mutation).
fn strip_retry_after_markers(s: &str) -> String {
    RETRY_AFTER_RE.replace_all(s, "<redacted>").into_owned()
}

/// Sanitize a reqwest transport error message by stripping URL userinfo and
/// query, and truncating. Avoids leaking embedded credentials or query
/// secrets in `reqwest::Error` Display strings.
fn sanitize_transport_msg(msg: &str) -> String {
    let mut out = msg.to_string();
    // Strip URL userinfo (scheme://user:pass@host → scheme://***@host) and
    // query (scheme://host/path?secret=... → scheme://host/path?...) from any
    // embedded URLs. We do a simple scan rather than `Url::parse` since the
    // error message may contain a partial URL.
    if let Some(scheme_end) = out.find("://") {
        let after = &out[scheme_end + 3..];
        if let Some(at) = after.find('@') {
            // Only treat as userinfo if it occurs before the next '/' or '?'.
            let userinfo_end = at
                .min(after.find('/').unwrap_or(at))
                .min(after.find('?').unwrap_or(at));
            if at == userinfo_end {
                let prefix = &out[..scheme_end + 3];
                let rest = &after[at + 1..];
                out = format!("{prefix}***@{rest}");
            }
        }
    }
    // Redact query strings (which may carry API keys/secrets).
    if let Some(q) = out.find('?') {
        let before = &out[..q];
        let after_query = &out[q..];
        // Keep the '?' but replace the rest with '...' if it looks like a query.
        if after_query.contains('=') {
            out = format!("{before}?...");
        }
    }
    // Truncate to 512 bytes, walking back to the nearest UTF-8 char
    // boundary to avoid panicking when byte 512 splits a multi-byte
    // sequence. Reuses the same UTF-8-safe helper as `sanitize_body`.
    truncate_at_char_boundary(&mut out, 512, true);
    out
}

/// Return a redacted form of a URL for Debug/logging: strips userinfo and
/// query. Query strings may carry API keys/tokens; userinfo is rejected at
/// construction but we redact defensively.
fn redact_url(u: &Url) -> String {
    let mut s = String::new();
    s.push_str(u.scheme());
    s.push_str("://");
    if !u.username().is_empty() || u.password().is_some() {
        s.push_str("***@");
    }
    if let Some(host_str) = u.host_str() {
        s.push_str(host_str);
    }
    if let Some(port) = u.port() {
        s.push_str(&format!(":{port}"));
    }
    s.push_str(u.path());
    if u.query().is_some() && !u.query().unwrap().is_empty() {
        s.push_str("?...");
    }
    s
}

#[cfg(test)]
mod rest_destination_tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use ferry_core::config::{AuthConfig, DestinationConfig};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use base64::Engine as _;

    fn rest_config(url: String) -> DestinationConfig {
        // wiremock serves over HTTP, so opt in to allow_http for tests.
        DestinationConfig::Rest {
            url,
            method: Some("POST".to_string()),
            headers: None,
            auth: None,
            body_template: None,
            timeout_secs: Some(5),
            connect_timeout_secs: Some(2),
            max_response_bytes: Some(1024),
            allow_http: Some(true),
            max_batch_size: Some(50),
        }
    }

    fn rest_config_with_http(url: String) -> DestinationConfig {
        rest_config(url)
    }

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let ids = Int32Array::from(vec![1, 2, 3]);
        let names = StringArray::from(vec!["A", "B", "C"]);
        RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(names)]).unwrap()
    }

    fn write_config() -> WriteConfig {
        WriteConfig {
            sync_name: "test".to_string(),
            batch_index: 0,
            total_batches: 1,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_write_200_success() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let dest = RestDestination::new(&rest_config(server.uri()), "test").unwrap();
        let batch = create_test_batch();
        let result = dest.write(&batch, &write_config()).await.unwrap();
        assert_eq!(result.rows_written, 3);
        assert!(result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_write_201_success() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(201))
            .mount(&server)
            .await;
        let dest = RestDestination::new(&rest_config(server.uri()), "test").unwrap();
        let batch = create_test_batch();
        let result = dest.write(&batch, &write_config()).await.unwrap();
        assert_eq!(result.rows_written, 3);
    }

    #[tokio::test]
    async fn test_write_400_permanent_no_retry_after() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(400).set_body_string("bad request body"))
            .mount(&server)
            .await;
        let dest = RestDestination::new(&rest_config(server.uri()), "test").unwrap();
        let batch = create_test_batch();
        let result = dest.write(&batch, &write_config()).await.unwrap();
        assert_eq!(result.rows_written, 0);
        assert_eq!(result.errors.len(), 3);
        for e in &result.errors {
            assert!(e.error.contains("HTTP 400"), "got: {}", e.error);
            assert!(
                !e.error.contains("retry_after"),
                "4xx must not carry retry_after: {}",
                e.error
            );
        }
    }

    #[tokio::test]
    async fn test_write_401_permanent() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;
        let dest = RestDestination::new(&rest_config(server.uri()), "test").unwrap();
        let batch = create_test_batch();
        let result = dest.write(&batch, &write_config()).await.unwrap();
        assert_eq!(result.rows_written, 0);
        assert!(result.errors[0].error.contains("HTTP 401"));
    }

    #[tokio::test]
    async fn test_write_429_with_retry_after_seconds() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "30"))
            .mount(&server)
            .await;
        let dest = RestDestination::new(&rest_config(server.uri()), "test").unwrap();
        let batch = create_test_batch();
        let result = dest.write(&batch, &write_config()).await.unwrap();
        assert_eq!(result.rows_written, 0);
        for e in &result.errors {
            assert!(e.error.contains("HTTP 429"), "{}", e.error);
            assert!(e.error.contains("retry_after: 30"), "{}", e.error);
        }
    }

    #[tokio::test]
    async fn test_write_429_with_retry_after_http_date() {
        let server = MockServer::start().await;
        // HTTP-date 60s in the future.
        let future = Utc::now() + chrono::Duration::seconds(60);
        let date_str = future.format("%a, %d %b %Y %H:%M:%S GMT").to_string();
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(429).insert_header("Retry-After", date_str.as_str()),
            )
            .mount(&server)
            .await;
        let dest = RestDestination::new(&rest_config(server.uri()), "test").unwrap();
        let batch = create_test_batch();
        let result = dest.write(&batch, &write_config()).await.unwrap();
        for e in &result.errors {
            // Should be clamped to <=300 and present.
            assert!(e.error.contains("retry_after: "), "{}", e.error);
            // Extract number
            let n: u64 = e
                .error
                .split("retry_after: ")
                .nth(1)
                .and_then(|s| s.split(|c: char| !c.is_ascii_digit()).next())
                .unwrap()
                .parse()
                .unwrap();
            assert!(n <= 300);
        }
    }

    #[tokio::test]
    async fn test_write_500_retryable() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500).set_body_string("oops"))
            .mount(&server)
            .await;
        let dest = RestDestination::new(&rest_config(server.uri()), "test").unwrap();
        let batch = create_test_batch();
        let result = dest.write(&batch, &write_config()).await.unwrap();
        for e in &result.errors {
            assert!(e.error.contains("HTTP 500"), "{}", e.error);
            assert!(
                !e.error.contains("retry_after"),
                "500 without header must not carry retry_after: {}",
                e.error
            );
        }
    }

    #[tokio::test]
    async fn test_write_503_with_retry_after() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(503).insert_header("Retry-After", "10"))
            .mount(&server)
            .await;
        let dest = RestDestination::new(&rest_config(server.uri()), "test").unwrap();
        let batch = create_test_batch();
        let result = dest.write(&batch, &write_config()).await.unwrap();
        for e in &result.errors {
            assert!(e.error.contains("HTTP 503"), "{}", e.error);
            assert!(e.error.contains("retry_after: 10"), "{}", e.error);
        }
    }

    #[tokio::test]
    async fn test_write_timeout_classified_as_transport() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_delay(StdDuration::from_secs(3)))
            .mount(&server)
            .await;
        // Use a 1s timeout.
        let mut cfg = rest_config(server.uri());
        if let DestinationConfig::Rest { timeout_secs, .. } = &mut cfg {
            *timeout_secs = Some(1);
        }
        let dest = RestDestination::new(&cfg, "test").unwrap();
        let batch = create_test_batch();
        let result = dest.write(&batch, &write_config()).await;
        assert!(result.is_err(), "expected Err on timeout");
        let msg = result.unwrap_err().to_string();
        // Network/transport errors must NOT contain "HTTP NNN" (a status code)
        // so the pipeline's extract_status_code returns None → default retry.
        assert!(
            !regex::Regex::new(r"HTTP \d{3}").unwrap().is_match(&msg),
            "transport error must not look like HTTP NNN: {msg}"
        );
        assert!(
            msg.contains("transport") || msg.contains("timeout") || msg.contains("error"),
            "expected transport-ish error, got: {msg}"
        );
    }

    #[tokio::test]
    async fn test_body_cap_rejects_oversize() {
        let server = MockServer::start().await;
        let big = "x".repeat(2 * 1024 * 1024); // 2 MiB
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500).set_body_string(big))
            .mount(&server)
            .await;
        let mut cfg = rest_config(server.uri());
        if let DestinationConfig::Rest {
            max_response_bytes, ..
        } = &mut cfg
        {
            *max_response_bytes = Some(1024 * 1024); // 1 MiB
        }
        let dest = RestDestination::new(&cfg, "test").unwrap();
        let batch = create_test_batch();
        let result = dest.write(&batch, &write_config()).await.unwrap();
        // Body was capped; we still classify status 500.
        for e in &result.errors {
            assert!(e.error.contains("HTTP 500"), "{}", e.error);
            // Body should be short (truncated) — not contain 2 MiB of 'x'.
            assert!(
                e.error.len() < 1024,
                "error string should be small: {}",
                e.error.len()
            );
        }
    }

    #[tokio::test]
    async fn test_bearer_auth_header_sent() {
        let server = MockServer::start().await;
        let received = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let received_clone = received.clone();
        Mock::given(method("POST"))
            .respond_with(move |req: &wiremock::Request| {
                let auth = req
                    .headers
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string());
                received_clone.try_lock().unwrap().push(auth);
                ResponseTemplate::new(200)
            })
            .mount(&server)
            .await;
        let mut cfg = rest_config(server.uri());
        if let DestinationConfig::Rest { auth, .. } = &mut cfg {
            *auth = Some(AuthConfig::Bearer {
                token: "testtoken123".to_string(),
            });
        }
        let dest = RestDestination::new(&cfg, "test").unwrap();
        let batch = create_test_batch();
        let result = dest.write(&batch, &write_config()).await.unwrap();
        assert_eq!(result.rows_written, 3);
        let received = received.lock().await.clone();
        assert_eq!(received, vec![Some("Bearer testtoken123".to_string())]);
    }

    #[tokio::test]
    async fn test_no_secret_in_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500).set_body_string("token: abc123longbody"))
            .mount(&server)
            .await;
        let mut cfg = rest_config(server.uri());
        if let DestinationConfig::Rest { auth, .. } = &mut cfg {
            *auth = Some(AuthConfig::Bearer {
                token: "SUPERSECRETTOKEN".to_string(),
            });
        }
        let dest = RestDestination::new(&cfg, "test").unwrap();
        let batch = create_test_batch();
        let result = dest.write(&batch, &write_config()).await.unwrap();
        for e in &result.errors {
            assert!(
                !e.error.contains("SUPERSECRETTOKEN"),
                "token leaked: {}",
                e.error
            );
        }
    }

    #[tokio::test]
    async fn test_template_render() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(|req: &wiremock::Request| {
                let body = String::from_utf8_lossy(&req.body);
                // Body should be the template output containing events array.
                assert!(body.contains("\"events\""), "body: {body}");
                assert!(
                    body.contains("\"id\":1") || body.contains("\"id\": 1"),
                    "body: {body}"
                );
                ResponseTemplate::new(200)
            })
            .mount(&server)
            .await;
        let mut cfg = rest_config(server.uri());
        if let DestinationConfig::Rest { body_template, .. } = &mut cfg {
            *body_template = Some("{\"events\": {{ rows | tojson }}}".to_string());
        }
        let dest = RestDestination::new(&cfg, "test").unwrap();
        let batch = create_test_batch();
        let result = dest.write(&batch, &write_config()).await.unwrap();
        assert_eq!(result.rows_written, 3);
    }

    #[tokio::test]
    async fn test_check_connection_ok_200() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let dest = RestDestination::new(&rest_config(server.uri()), "test").unwrap();
        assert!(dest.check_connection().await.is_ok());
    }

    #[tokio::test]
    async fn test_check_connection_err_500() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let dest = RestDestination::new(&rest_config(server.uri()), "test").unwrap();
        let r = dest.check_connection().await;
        assert!(r.is_err());
        assert!(r.unwrap_err().to_string().contains("HTTP 500"));
    }

    #[tokio::test]
    async fn test_http_scheme_rejected_without_flag() {
        // wiremock serves http; we must NOT opt in via allow_http.
        let server = MockServer::start().await;
        let mut cfg = rest_config(server.uri());
        if let DestinationConfig::Rest { allow_http, .. } = &mut cfg {
            *allow_http = Some(false);
        }
        let r = RestDestination::new(&cfg, "test");
        assert!(
            r.is_err(),
            "http URL without allow_http must fail at construction"
        );
    }

    #[tokio::test]
    async fn test_http_scheme_allowed_with_flag() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let dest = RestDestination::new(&rest_config_with_http(server.uri()), "test").unwrap();
        let batch = create_test_batch();
        let result = dest.write(&batch, &write_config()).await.unwrap();
        assert_eq!(result.rows_written, 3);
    }

    #[test]
    fn test_parse_retry_after_delta_seconds() {
        let now = Utc::now();
        assert_eq!(
            parse_retry_after(Some("30"), now, MAX_RETRY_AFTER),
            Some(StdDuration::from_secs(30))
        );
        assert_eq!(parse_retry_after(None, now, MAX_RETRY_AFTER), None);
        assert_eq!(parse_retry_after(Some(""), now, MAX_RETRY_AFTER), None);
    }

    #[test]
    fn test_parse_retry_after_clamps() {
        let now = Utc::now();
        assert_eq!(
            parse_retry_after(Some("999999"), now, MAX_RETRY_AFTER),
            Some(StdDuration::from_secs(300))
        );
    }

    #[test]
    fn test_parse_retry_after_http_date() {
        let now = Utc::now();
        let future = now + chrono::Duration::seconds(45);
        let date_str = future.format("%a, %d %b %Y %H:%M:%S GMT").to_string();
        let d = parse_retry_after(Some(&date_str), now, MAX_RETRY_AFTER).unwrap();
        // ~45s ± a few seconds.
        assert!(
            d.as_secs() <= 45 && d.as_secs() >= 40,
            "got {}s",
            d.as_secs()
        );
    }

    #[test]
    fn test_parse_retry_after_past_http_date() {
        let now = Utc::now();
        let past = now - chrono::Duration::seconds(60);
        let date_str = past.format("%a, %d %b %Y %H:%M:%S GMT").to_string();
        let d = parse_retry_after(Some(&date_str), now, MAX_RETRY_AFTER).unwrap();
        assert_eq!(d, StdDuration::from_secs(1));
    }

    #[test]
    fn test_sanitize_body_truncates() {
        let big = "x".repeat(1024);
        let s = sanitize_body(big.as_bytes(), 100, &[]);
        assert!(s.len() <= 100, "got len {}", s.len());
    }

    #[test]
    fn test_sanitize_body_scrubs_tokens() {
        let body = b"token: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.payload.sig";
        let s = sanitize_body(body, 512, &[]);
        assert!(
            !s.contains("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"),
            "leaked JWT: {s}"
        );
        assert!(s.contains("***"));
    }

    #[test]
    fn test_sanitize_body_scrubs_hex() {
        let body = b"sha: a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";
        let s = sanitize_body(body, 512, &[]);
        assert!(s.contains("***"), "hex not scrubbed: {s}");
    }

    #[test]
    fn test_strip_retry_after_markers_multiple_mixed() {
        // Both markers, mixed case, multiple occurrences, and trailing digits.
        // Must redact ALL occurrences without panicking.
        let body = "retry_after: 99999999999 and Retry-After: 30 and RETRY_AFTER:5";
        let s = strip_retry_after_markers(body);
        assert!(!s.contains("99999999999"), "digits leaked: {s}");
        assert!(!s.contains("retry_after: 30"), "second marker leaked: {s}");
        assert!(!s.contains("Retry-After"), "case-variant leaked: {s}");
        assert!(!s.contains("RETRY_AFTER"), "upper variant leaked: {s}");
        // All three should be redacted.
        let count = s.matches("<redacted>").count();
        assert_eq!(count, 3, "expected 3 redactions, got {count} in: {s}");
    }

    #[test]
    fn test_strip_retry_after_markers_unicode_adjacent() {
        // Unicode content around markers must not panic and must still redact.
        let body = "héllo 世界 retry_after: 7 café Retry-After: 12 ☕";
        let s = strip_retry_after_markers(body);
        assert!(!s.contains("retry_after: 7"), "marker+digit leaked: {s}");
        assert!(!s.contains("Retry-After: 12"), "second marker leaked: {s}");
        // Unicode preserved.
        assert!(s.contains("héllo"), "unicode lost: {s}");
        assert!(s.contains("☕"), "emoji lost: {s}");
    }

    #[test]
    fn test_sanitize_body_secret_straddles_boundary() {
        // A short/hyphenated secret straddles the 512-byte display boundary.
        // Exact secret redaction must run on the FULL body before truncation
        // so no prefix of the secret survives into the display string.
        let secret = "short-hyph-tok";
        let prefix = "x".repeat(506);
        let suffix = "y".repeat(50);
        // Body: [506 x's][secret][50 y's] → secret spans bytes 506..519,
        // and the display is truncated to 512 bytes (so "short-" + "hyph"
        // would be in the first 512 without prior redaction).
        let body = format!("{prefix}{secret}{suffix}");
        let s = sanitize_body(body.as_bytes(), 512, &[secret.to_string()]);
        assert!(!s.contains("short-hyph-tok"), "full secret leaked: {s}");
        assert!(
            !s.contains("short-hyph"),
            "secret prefix leaked across boundary: {s}"
        );
        assert!(!s.contains("hyph-tok"), "secret suffix leaked: {s}");
        assert!(s.contains("***"), "expected redaction marker: {s}");
    }

    #[test]
    fn test_sanitize_body_secret_at_exact_boundary() {
        // Secret starts exactly at the 512-byte boundary.
        let secret = "boundary-secret";
        let prefix = "x".repeat(512);
        let body = format!("{prefix}{secret}");
        let s = sanitize_body(body.as_bytes(), 512, &[secret.to_string()]);
        assert!(!s.contains("boundary-secret"), "secret leaked: {s}");
        assert!(!s.contains("boundary"), "secret prefix leaked: {s}");
    }

    #[test]
    fn test_sanitize_transport_msg_truncates_unicode_boundary() {
        // Regression: `sanitize_transport_msg` previously called
        // `String::truncate(512)`, which panics if byte 512 falls inside a
        // multi-byte UTF-8 sequence. Build a message whose 512th byte splits
        // a 4-byte sequence so the UTF-8-safe path is exercised.
        //
        // `é` is 2 bytes (0xC3 0xA9). We construct [509 ASCII bytes][é][é][é]
        // = 509 + 6 = 515 bytes. Byte 512 is the second byte of the second
        // `é`, i.e. inside a UTF-8 sequence → unsafe truncate would panic.
        let prefix = "a".repeat(509);
        let msg = format!("{prefix}ééé");
        assert_eq!(
            msg.len(),
            515,
            "setup: expected 515 bytes, got {}",
            msg.len()
        );
        // Must not panic and must be <= 512 bytes (plus the "..." suffix).
        let sanitized = sanitize_transport_msg(&msg);
        assert!(
            sanitized.len() <= 512 + 3,
            "sanitized length {} exceeds cap+ellipsis",
            sanitized.len()
        );
        // Must be valid UTF-8 (no panic, no broken boundary).
        assert!(sanitized.is_char_boundary(sanitized.len()));
        // The "..." suffix is appended only when truncation actually occurred.
        assert!(
            sanitized.ends_with("..."),
            "expected ellipsis suffix: {sanitized}"
        );
        // No partial `é` byte leak (no stray 0xA9 continuation byte without
        // its lead byte).
        assert!(
            sanitized.contains('é') || sanitized.ends_with("..."),
            "expected valid UTF-8 content, got: {sanitized}"
        );
    }

    #[test]
    fn test_sanitize_transport_msg_short_passthrough() {
        // Short ASCII message with no URL: should pass through unchanged.
        let msg = "connection reset by peer";
        assert_eq!(sanitize_transport_msg(msg), msg);
    }

    #[test]
    fn test_debug_redacts_auth() {
        let cfg = DestinationConfig::Rest {
            url: "https://x.example.com".to_string(),
            method: Some("POST".to_string()),
            headers: None,
            auth: Some(AuthConfig::Bearer {
                token: "secrettoken".to_string(),
            }),
            body_template: None,
            timeout_secs: Some(5),
            connect_timeout_secs: Some(2),
            max_response_bytes: Some(1024),
            allow_http: None,
            max_batch_size: Some(50),
        };
        let dest = RestDestination::new(&cfg, "test").unwrap();
        let s = format!("{dest:?}");
        assert!(!s.contains("secrettoken"), "Debug leaked token: {s}");
    }

    #[test]
    fn test_debug_redacts_url_query() {
        // URL with a query string (e.g. ?api_key=...) must be redacted in Debug.
        let cfg = DestinationConfig::Rest {
            url: "https://x.example.com/ingest?api_key=verysecret".to_string(),
            method: Some("POST".to_string()),
            headers: None,
            auth: None,
            body_template: None,
            timeout_secs: Some(5),
            connect_timeout_secs: Some(2),
            max_response_bytes: Some(1024),
            allow_http: None,
            max_batch_size: Some(50),
        };
        let dest = RestDestination::new(&cfg, "test").unwrap();
        let s = format!("{dest:?}");
        assert!(!s.contains("verysecret"), "Debug leaked query secret: {s}");
        assert!(s.contains("?..."), "Debug should redact query: {s}");
    }

    #[test]
    fn test_constructor_rejects_url_userinfo() {
        let cfg = DestinationConfig::Rest {
            url: "https://user:pass@x.example.com/ingest".to_string(),
            method: Some("POST".to_string()),
            headers: None,
            auth: None,
            body_template: None,
            timeout_secs: Some(5),
            connect_timeout_secs: Some(2),
            max_response_bytes: Some(1024),
            allow_http: None,
            max_batch_size: Some(50),
        };
        let r = RestDestination::new(&cfg, "test");
        assert!(r.is_err(), "URL with userinfo must be rejected");
        let msg = r.unwrap_err().to_string();
        assert!(
            msg.contains("userinfo"),
            "expected userinfo error, got: {msg}"
        );
    }

    #[tokio::test]
    async fn test_no_redirect_followed() {
        // Server returns a 302 redirect; with Policy::none(), reqwest returns
        // the 3xx response without following. The destination must NOT follow
        // the redirect (which would leak auth headers to the redirect target).
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(302)
                    .insert_header("Location", "http://evil.example.com/steal"),
            )
            .mount(&server)
            .await;
        let dest = RestDestination::new(&rest_config(server.uri()), "test").unwrap();
        let batch = create_test_batch();
        let result = dest.write(&batch, &write_config()).await.unwrap();
        // 302 is non-2xx → all rows error. The redirect must NOT be followed.
        assert_eq!(result.rows_written, 0);
        for e in &result.errors {
            assert!(e.error.contains("HTTP 302"), "got: {}", e.error);
        }
    }

    #[tokio::test]
    async fn test_apikey_auth_header_not_forwarded_on_redirect() {
        // Regression: even if a redirect were followed, the API key header
        // must not leak to the redirect host. With Policy::none() the redirect
        // is not followed at all; this test asserts the 3xx surfaces as an
        // error and the API key never appears in the error string.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(302)
                    .insert_header("Location", "http://evil.example.com/steal")
                    // Evil host echoes the request's API key in its body.
                    .set_body_string("echo: APIKEY-secret-value"),
            )
            .mount(&server)
            .await;
        let mut cfg = rest_config(server.uri());
        if let DestinationConfig::Rest { auth, .. } = &mut cfg {
            *auth = Some(AuthConfig::ApiKey {
                header_name: "X-Api-Key".to_string(),
                value: "APIKEY-secret-value".to_string(),
            });
        }
        let dest = RestDestination::new(&cfg, "test").unwrap();
        let batch = create_test_batch();
        let result = dest.write(&batch, &write_config()).await.unwrap();
        for e in &result.errors {
            assert!(
                !e.error.contains("APIKEY-secret-value"),
                "API key leaked: {}",
                e.error
            );
        }
    }

    #[tokio::test]
    async fn test_basic_auth_header_sent() {
        let server = MockServer::start().await;
        let received = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let received_clone = received.clone();
        Mock::given(method("POST"))
            .respond_with(move |req: &wiremock::Request| {
                let auth = req
                    .headers
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string());
                received_clone.try_lock().unwrap().push(auth);
                ResponseTemplate::new(200)
            })
            .mount(&server)
            .await;
        let mut cfg = rest_config(server.uri());
        if let DestinationConfig::Rest { auth, .. } = &mut cfg {
            *auth = Some(AuthConfig::Basic {
                username: "alice".to_string(),
                password: "wonderland".to_string(),
            });
        }
        let dest = RestDestination::new(&cfg, "test").unwrap();
        let batch = create_test_batch();
        let result = dest.write(&batch, &write_config()).await.unwrap();
        assert_eq!(result.rows_written, 3);
        let received = received.lock().await.clone();
        assert_eq!(received.len(), 1);
        let auth = received[0].as_ref().unwrap();
        // Basic auth header is "Basic <base64(user:pass)>".
        assert!(auth.starts_with("Basic "), "got: {auth}");
        let b64 = &auth[6..];
        let decoded = String::from_utf8(
            base64::engine::general_purpose::STANDARD
                .decode(b64)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(decoded, "alice:wonderland");
    }

    #[tokio::test]
    async fn test_apikey_auth_header_sent() {
        let server = MockServer::start().await;
        let received = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let received_clone = received.clone();
        Mock::given(method("POST"))
            .respond_with(move |req: &wiremock::Request| {
                let v = req
                    .headers
                    .get("x-api-key")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string());
                received_clone.try_lock().unwrap().push(v);
                ResponseTemplate::new(200)
            })
            .mount(&server)
            .await;
        let mut cfg = rest_config(server.uri());
        if let DestinationConfig::Rest { auth, .. } = &mut cfg {
            *auth = Some(AuthConfig::ApiKey {
                header_name: "X-Api-Key".to_string(),
                value: "key123".to_string(),
            });
        }
        let dest = RestDestination::new(&cfg, "test").unwrap();
        let batch = create_test_batch();
        let result = dest.write(&batch, &write_config()).await.unwrap();
        assert_eq!(result.rows_written, 3);
        let received = received.lock().await.clone();
        assert_eq!(received, vec![Some("key123".to_string())]);
    }

    #[tokio::test]
    async fn test_server_echoes_bearer_token_redacted() {
        // A misconfigured server echoes the request's Authorization header in
        // its 500 error body. The exact token must be redacted from RowError.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(500)
                    .set_body_string("error: you sent Bearer short-token-xyz"),
            )
            .mount(&server)
            .await;
        let mut cfg = rest_config(server.uri());
        if let DestinationConfig::Rest { auth, .. } = &mut cfg {
            *auth = Some(AuthConfig::Bearer {
                token: "short-token-xyz".to_string(),
            });
        }
        let dest = RestDestination::new(&cfg, "test").unwrap();
        let batch = create_test_batch();
        let result = dest.write(&batch, &write_config()).await.unwrap();
        for e in &result.errors {
            assert!(
                !e.error.contains("short-token-xyz"),
                "token leaked: {}",
                e.error
            );
            assert!(
                !e.error.contains("Bearer short-token-xyz"),
                "formatted token leaked: {}",
                e.error
            );
        }
    }

    #[tokio::test]
    async fn test_server_echoes_basic_credentials_redacted() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(500)
                    // Echo the base64 credentials and the raw password.
                    .set_body_string("creds: YWxpY2U6cGFzc3dvcmQ= password"),
            )
            .mount(&server)
            .await;
        let mut cfg = rest_config(server.uri());
        if let DestinationConfig::Rest { auth, .. } = &mut cfg {
            *auth = Some(AuthConfig::Basic {
                username: "alice".to_string(),
                password: "password".to_string(),
            });
        }
        let dest = RestDestination::new(&cfg, "test").unwrap();
        let batch = create_test_batch();
        let result = dest.write(&batch, &write_config()).await.unwrap();
        for e in &result.errors {
            assert!(
                !e.error.contains("YWxpY2U6cGFzc3dvcmQ="),
                "base64 creds leaked: {}",
                e.error
            );
            assert!(
                !e.error.contains("password"),
                "raw password leaked: {}",
                e.error
            );
        }
    }

    #[tokio::test]
    async fn test_retry_after_injection_from_body_rejected() {
        // A malicious server puts "retry_after: 99999999999" in its 500 body.
        // The body's retry_after marker must be stripped; only the header's
        // Retry-After (if any) is honored. Here there's no header, so no
        // retry_after in the error string at all.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500).set_body_string("retry_after: 99999999999"))
            .mount(&server)
            .await;
        let dest = RestDestination::new(&rest_config(server.uri()), "test").unwrap();
        let batch = create_test_batch();
        let result = dest.write(&batch, &write_config()).await.unwrap();
        for e in &result.errors {
            assert!(e.error.contains("HTTP 500"), "{}", e.error);
            // The body's "retry_after: 99999999999" must be stripped — the
            // error string must NOT contain "retry_after: 99999999999".
            assert!(
                !e.error.contains("retry_after: 99999999999"),
                "body injected retry_after: {}",
                e.error
            );
            // No header → no retry_after suffix at all.
            assert!(
                !e.error.contains("retry_after:"),
                "unexpected retry_after: {}",
                e.error
            );
        }
    }

    #[tokio::test]
    async fn test_write_408_retryable() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(408).insert_header("Retry-After", "5"))
            .mount(&server)
            .await;
        let dest = RestDestination::new(&rest_config(server.uri()), "test").unwrap();
        let batch = create_test_batch();
        let result = dest.write(&batch, &write_config()).await.unwrap();
        for e in &result.errors {
            assert!(e.error.contains("HTTP 408"), "{}", e.error);
            assert!(e.error.contains("retry_after: 5"), "{}", e.error);
        }
    }

    #[tokio::test]
    async fn test_write_425_retryable() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(425))
            .mount(&server)
            .await;
        let dest = RestDestination::new(&rest_config(server.uri()), "test").unwrap();
        let batch = create_test_batch();
        let result = dest.write(&batch, &write_config()).await.unwrap();
        for e in &result.errors {
            assert!(e.error.contains("HTTP 425"), "{}", e.error);
        }
    }

    #[tokio::test]
    async fn test_real_pks_in_row_errors() {
        // Assert that real PK values (not row indices) appear in RowError when
        // pk_col is set. This validates the WriteConfig.pk_col change.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let dest = RestDestination::new(&rest_config(server.uri()), "test").unwrap();
        let batch = create_test_batch_with_pks();
        let mut wc = write_config();
        wc.pk_col = Some("id".to_string());
        let result = dest.write(&batch, &wc).await.unwrap();
        assert_eq!(result.errors.len(), 3);
        assert_eq!(result.errors[0].primary_key, "1001");
        assert_eq!(result.errors[1].primary_key, "1002");
        assert_eq!(result.errors[2].primary_key, "1003");
    }

    #[tokio::test]
    async fn test_bounded_streaming_body_cap() {
        // Server streams a body larger than max_response_bytes; the reader
        // must stop at the cap rather than OOMing.
        let server = MockServer::start().await;
        let big = "x".repeat(2 * 1024 * 1024); // 2 MiB, no Content-Length
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500).set_body_string(big))
            .mount(&server)
            .await;
        let mut cfg = rest_config(server.uri());
        if let DestinationConfig::Rest {
            max_response_bytes, ..
        } = &mut cfg
        {
            *max_response_bytes = Some(1024); // 1 KiB
        }
        let dest = RestDestination::new(&cfg, "test").unwrap();
        let batch = create_test_batch();
        let result = dest.write(&batch, &write_config()).await.unwrap();
        for e in &result.errors {
            assert!(e.error.contains("HTTP 500"), "{}", e.error);
            // Error string should be small (body truncated to 512 bytes).
            assert!(e.error.len() < 1024, "error too large: {}", e.error.len());
        }
    }

    #[tokio::test]
    async fn test_template_missing_field() {
        // Template references a field that doesn't exist in the batch.
        // minijinja default (undefined → empty string) should not panic;
        // the render should produce valid output (the field becomes empty).
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let mut cfg = rest_config(server.uri());
        if let DestinationConfig::Rest { body_template, .. } = &mut cfg {
            // Template references `nonexistent` which isn't in the row.
            *body_template = Some("{{ rows | tojson }}".to_string());
        }
        let dest = RestDestination::new(&cfg, "test").unwrap();
        let batch = create_test_batch();
        let result = dest.write(&batch, &write_config()).await.unwrap();
        assert_eq!(result.rows_written, 3);
    }

    fn create_test_batch_with_pks() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let ids = Int32Array::from(vec![1001, 1002, 1003]);
        let names = StringArray::from(vec!["A", "B", "C"]);
        RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(names)]).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let ids = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let names = StringArray::from(vec!["A", "B", "C", "D", "E"]);

        RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(names)]).unwrap()
    }

    #[tokio::test]
    async fn test_success() {
        let dest = MockRestDestination::success();
        let batch = create_test_batch();
        let config = WriteConfig {
            sync_name: "test".to_string(),
            batch_index: 0,
            total_batches: 1,
            ..Default::default()
        };

        let result = dest.write(&batch, &config).await.unwrap();
        assert_eq!(result.rows_written, 5);
        assert!(result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_rate_limited() {
        let dest = MockRestDestination::rate_limited(30);
        let batch = create_test_batch();
        let config = WriteConfig {
            sync_name: "test".to_string(),
            batch_index: 0,
            total_batches: 1,
            ..Default::default()
        };

        let result = dest.write(&batch, &config).await.unwrap();
        assert_eq!(result.rows_written, 0);
        assert_eq!(result.errors.len(), 5);
        for err in &result.errors {
            assert!(
                err.error.contains("429"),
                "Expected 429 error, got: {}",
                err.error
            );
        }
    }

    #[tokio::test]
    async fn test_server_error() {
        let dest = MockRestDestination::server_error();
        let batch = create_test_batch();
        let config = WriteConfig {
            sync_name: "test".to_string(),
            batch_index: 0,
            total_batches: 1,
            ..Default::default()
        };

        let result = dest.write(&batch, &config).await.unwrap();
        assert_eq!(result.rows_written, 0);
        assert_eq!(result.errors.len(), 5);
        for err in &result.errors {
            assert!(
                err.error.contains("500"),
                "Expected 500 error, got: {}",
                err.error
            );
        }
    }

    #[tokio::test]
    async fn test_partial_success() {
        let dest = MockRestDestination::partial_success(3);
        let batch = create_test_batch();
        let config = WriteConfig {
            sync_name: "test".to_string(),
            batch_index: 0,
            total_batches: 1,
            ..Default::default()
        };

        let result = dest.write(&batch, &config).await.unwrap();
        assert_eq!(result.rows_written, 3);
        assert_eq!(result.errors.len(), 2);
        for err in &result.errors {
            assert!(
                err.error.contains("500"),
                "Expected 500 error, got: {}",
                err.error
            );
        }
    }

    #[tokio::test]
    async fn test_max_batch_size() {
        let dest = MockRestDestination::success();
        assert_eq!(dest.max_batch_size(), 75);
    }

    #[tokio::test]
    async fn test_idempotency() {
        let dest = MockRestDestination::success();
        assert_eq!(dest.idempotency(), IdempotencyCapability::Idempotent);
    }

    #[tokio::test]
    async fn test_remove_capability() {
        let dest = MockRestDestination::success();
        assert_eq!(dest.remove_capability(), RemoveCapability::RemoveByKey);
    }

    #[tokio::test]
    async fn test_remove() {
        let dest = MockRestDestination::success();
        let config = WriteConfig {
            sync_name: "test".to_string(),
            batch_index: 0,
            total_batches: 1,
            ..Default::default()
        };

        let keys = vec![
            serde_json::json!({"id": 1}),
            serde_json::json!({"id": 2}),
            serde_json::json!({"id": 3}),
        ];
        let result = dest.remove(&keys, &config).await.unwrap();
        assert_eq!(result.rows_removed, 3);
        assert!(result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_replace_all() {
        let dest = MockRestDestination::success();
        let batch = create_test_batch();
        let config = WriteConfig {
            sync_name: "test".to_string(),
            batch_index: 0,
            total_batches: 1,
            ..Default::default()
        };

        let result = dest.replace_all(&batch, &config).await.unwrap();
        assert_eq!(result.rows_written, 5);
        assert!(result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_rate_limit_config() {
        let dest = MockRestDestination::success();
        let rl = dest.rate_limit().unwrap();
        assert_eq!(rl.requests_per_second, Some(10.0));
        assert_eq!(rl.concurrent_requests, None);
    }

    #[tokio::test]
    async fn test_check_connection() {
        let dest = MockRestDestination::success();
        assert!(dest.check_connection().await.is_ok());
    }

    #[tokio::test]
    async fn test_custom_behavior() {
        let dest = MockRestDestination::new(
            MockBehavior::Custom(Arc::new(|batch| WriteResult {
                rows_written: batch.num_rows(),
                errors: vec![],
            })),
            "custom",
        );
        let batch = create_test_batch();
        let config = WriteConfig {
            sync_name: "test".to_string(),
            batch_index: 0,
            total_batches: 1,
            ..Default::default()
        };

        let result = dest.write(&batch, &config).await.unwrap();
        assert_eq!(result.rows_written, 5);
        assert!(result.errors.is_empty());
    }
}
