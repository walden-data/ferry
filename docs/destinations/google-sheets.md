# Google Sheets Destination

The `google_sheets` destination writes rows from a Ferry sync to a Google
Sheets spreadsheet tab using the Sheets v4 API. It performs key-based
upserts against explicit A1 ranges — never appends, never deletes, never
mirrors the source. Each row is written with `valueInputOption=RAW` so the
source values are stored verbatim (no formula interpretation, no
locale-dependent parsing).

## Setup

### 1. Enable the Google Sheets API

In the [Google Cloud Console](https://console.cloud.google.com/), enable the
**Google Sheets API** for your project.

### 2. Create a service-account key

1. Go to **IAM & Admin → Service Accounts → Create service account**.
2. Give it a name (e.g. `ferry-sheets-writer`).
3. After creation, open the service account → **Keys → Add key → Create new
   key → JSON**. A JSON key file downloads.
4. Save the key file to a secure path on the host that runs Ferry (e.g.
   `/etc/ferry/sheets-sa.json`).
5. On Unix, set the file permissions to `0600`:
   ```sh
   chmod 600 /etc/ferry/sheets-sa.json
   ```
   Ferry refuses to load the key if the permissions are more permissive.

### 3. Share the spreadsheet with the service account

Open the JSON key file and copy the `client_email` (e.g.
`ferry-sheets-writer@my-project.iam.gserviceaccount.com`). In the target
Google Sheet, click **Share** and add that email as an **Editor**.

### 4. Configure the destination

In your sync YAML:

```yaml
destination:
  type: google_sheets
  spreadsheet_id: "1ABC...your-spreadsheet-id..."
  sheet: "Customers"
  key_column: "id"
  service_account_key_file: "${GOOGLE_SHEETS_SERVICE_ACCOUNT_KEY_FILE}"
  max_rows: 10000
  max_batch_size: 100       # optional; default 100
  timeout_secs: 30          # optional
  connect_timeout_secs: 10  # optional
  max_response_bytes: 1048576  # optional
```

### 5. Resolve the credential path from `secrets.toml` (optional)

If you prefer not to put the path in YAML (or want to keep it out of env
vars), leave `service_account_key_file` empty in YAML and add it to
`secrets.toml`:

```toml
[destination.google_sheets]
service_account_key_file = "/etc/ferry/sheets-sa.json"
```

Remember to `chmod 600 secrets.toml`.

## Configuration reference

| Field                         | Required | Default | Description |
|-------------------------------|----------|---------|-------------|
| `spreadsheet_id`              | yes      | —       | The spreadsheet ID from the sheet URL (`/d/<id>/`). Must match `^[A-Za-z0-9_-]+$`. |
| `sheet`                       | yes      | —       | The tab name (e.g. `Customers`). Quoted automatically in A1 notation. |
| `key_column`                  | yes      | —       | The source column used as the upsert key. Must match `WriteConfig.pk_col`. Supported types: `Int32`, `Int64`, `Utf8`, `LargeUtf8`. |
| `service_account_key_file`    | yes\*    | —       | Path to the service-account JSON key. Relative paths are resolved against the project directory. Can be left empty in YAML if resolved from `secrets.toml`. |
| `max_rows`                    | yes      | —       | The maximum row count the tab may grow to (including the header row). Must be `>= 2`. New rows that would exceed this are rejected with a per-row error. |
| `max_batch_size`              | no       | `100`   | Per-batch row cap. The internal byte/range splitting is authoritative. |
| `timeout_secs`                | no       | `30`    | Per-request timeout. |
| `connect_timeout_secs`        | no       | `10`    | Connect timeout. |
| `max_response_bytes`          | no       | `1 MiB` | Response body cap. Hard-capped at 64 MiB. |

\* `service_account_key_file` is required at validation time. It may be
empty in YAML only if `secrets.toml` resolves it.

## Semantics

- **Row 1 is owned by Ferry.** Ferry writes the ordered column names from
  the source schema as row 1 the first time it sees an empty tab. On every
  subsequent write, Ferry compares the existing row 1 to the source schema
  and rejects the batch with a per-row "header row mismatch" error if they
  differ. Do not edit row 1 manually.
- **Key-based upsert.** Each incoming row is matched against the
  `key_column` in the existing sheet. Existing keys are updated in place at
  their current A1 row. New keys are written to the next free row
  (monotonically increasing — Ferry never reuses holes left by deleted
  rows). Ferry never calls `values.append` (the heuristic append endpoint is
  non-idempotent and can create duplicate rows on retry).
- **Orphan rows are left untouched.** Keys present in the sheet but absent
  from the incoming batch are not deleted. Ferry does not support row
  deletion (`remove` returns an unsupported-operation error).
- **No mirror mode.** The `google_sheets` destination does not implement
  `replace_all` (it is a key-based upsert only). `sync.mode: mirror` is
  rejected at validation time. Use `incremental` or `full_refresh`.
- **RAW values.** All cells are written with `valueInputOption=RAW`:
  - null → empty string
  - boolean → `"TRUE"` / `"FALSE"`
  - numbers → locale-independent string form
  - temporal / other → ISO-8601 / Arrow display string
  - Strings starting with `=`, `+`, `-`, or `@` are stored as text (RAW
    prevents formula interpretation — no formula injection).
- **Single writer.** Ferry assumes it is the only writer to the tab during
  a sync. Concurrent human or process edits during the read→map→write
  window are unsupported and can produce incorrect row placement.

## Authentication

- **Service-account OAuth2 only.** Ferry reads the JSON key, builds a
  `yup-oauth2::ServiceAccountAuthenticator` with an in-memory token cache,
  and requests the single scope
  `https://www.googleapis.com/auth/spreadsheets`. Tokens are refreshed
  transparently by the authenticator (1-hour TTL); no pre-minted token, no
  disk persistence, no ADC, no user OAuth.
- **401 handling.** On a 401 response, Ferry calls
  `force_refreshed_token` and replays the request exactly once. A second
  401 becomes a per-PK row error (without exposing the token).
- **Hardcoded production hosts.** The Sheets API base URL
  (`https://sheets.googleapis.com`) and the OAuth2 token endpoint
  (`https://oauth2.googleapis.com/token`) are hardcoded in production. A
  key whose `token_uri` points elsewhere is rejected at construction
  (defense against credential exfiltration via a rogue token endpoint).
  Test-only construction injects wiremock URLs through a separate
  constructor.

## Retry and quota behavior

- **Per-request throttle:** at most one Sheets API request per second per
  tab (enforced inside the destination). `Destination::rate_limit` reports
  the same as advisory metadata.
- **Retryable responses:** 429, 5xx, and 403 with Google reason
  `rateLimitExceeded` are retried up to 5 times with capped exponential
  backoff and jitter. `Retry-After` is honored and clamped to 5 minutes.
- **Non-retryable 4xx:** 400, 403 (non-rate-limit), 404, etc. are surfaced
  as per-PK row errors in the existing `HTTP NNN: ...; retry_after: N`
  convention so the delivery pipeline's `on_reject` classification and
  pending/dead-letter state continue to work. Configure `on_reject` rules
  to dead-letter permanent 400/403/404 responses — the pipeline default
  retries unclassified errors.
- **Ambiguous transport failures.** If a `values.batchUpdate` request
  fails with a transport error after Google may have applied it, the
  affected rows are reported as retryable. The next delivery attempt
  re-reads the sheet and resolves every key to its existing A1 row,
  converting a successfully-created row into an in-place update — no
  duplicate rows.

## Limits

- **Per-cell:** 50,000 characters. Ferry rejects rows with overlong cells
  before any HTTP request.
- **Per-request:** at most 1000 `ValueRange` entries and conservatively
  below 2 MiB per `values.batchUpdate` request. Ferry splits staged writes
  across multiple requests when either bound would be crossed.
- **Per-tab:** `max_rows` (configured). New rows that would exceed this
  are rejected with a per-row error before any HTTP request.
- **Quotas:** 300 reads/min, 300 writes/min per project; 60/min per
  service account. The 1 req/sec per-tab throttle keeps Ferry well under
  these.

## Security

- The credential file is canonicalized, verified to be a regular file,
  and checked to have Unix mode `0600` before its contents are read.
- The `Debug` impl of `GoogleSheetsDestination` redacts the credential
  path; no private key, token, `Authorization` header, or cell value is
  ever emitted through `Debug`, tracing, or errors.
- Response bodies are sanitized before inclusion in error strings: exact
  known secret values (the bearer token and the formatted `Bearer <token>`
  header value) are replaced with `***`, `retry_after` markers are stripped
  (so a malicious body cannot drive the pipeline's retry delay), and the
  display string is truncated.
- The `reqwest::Client` uses rustls, disables redirects, and is HTTPS-only
  in production. The `Authorization` header is marked sensitive so reqwest
  excludes it from tracing.

## Unsupported behavior

- `values.append` (non-idempotent; creates duplicate rows on retry).
- Row deletion (`DeleteDimensionRequest`) — index-shifting hazard.
- `values.clear` / `replace_all` / mirror mode.
- `spreadsheets.batchUpdate` (cell formatting, formulas, dimension resize).
- `USER_ENTERED` value input (formula injection, locale-dependent parsing).
- Sheet auto-creation / auto-resize.
- Multiple tabs per destination.
- Concurrent writers to the same tab.
- Application Default Credentials / Workload Identity Federation.

## Manual smoke test

Automated tests are credential-free and run against a wiremock server. To
smoke-test against a real spreadsheet:

1. Follow the setup steps above to create a service-account key and share
   a scratch spreadsheet with it.
2. Set `GOOGLE_SHEETS_SERVICE_ACCOUNT_KEY_FILE` to the key path.
3. Configure a sync pointing at the scratch spreadsheet.
4. Run `ferry run --select <sync-name>`.
5. Inspect the sheet: row 1 should be the header, subsequent rows the
   data. Re-running should update in place (no duplicates).