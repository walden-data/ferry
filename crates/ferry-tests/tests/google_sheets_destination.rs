//! Integration tests for the production Google Sheets destination.
//!
//! These tests drive the full `Engine` + `DeliveryPipeline` against a
//! `wiremock` HTTP server standing in for both the Google OAuth2 token
//! endpoint and the Sheets v4 API. They cover:
//!
//! - OAuth2 service-account token exchange (stubbed) and `Authorization:
//!   Bearer` header on every Sheets request.
//! - Empty-sheet upsert: row-1 headers + explicit A1 row writes via
//!   `values:batchUpdate` with `valueInputOption=RAW`.
//! - Existing-sheet upsert: same-key rows update in place; new-key rows
//!   land at `max populated row + 1`.
//! - Retry idempotency: an ambiguous applied response does not create a
//!   duplicate row on the next write (the read→map→write cycle resolves
//!   the key to its existing A1 row).
//! - A1 quoting / URL encoding for sheet names containing spaces.
//! - Error classification: 401 (one force-refresh replay), 429 with
//!   `Retry-After`, retryable 503, permanent 400/404, transport timeout.
//! - Secret non-disclosure: the service-account private key, the bearer
//!   token, and cell values never appear in errors, `Debug`, or the state
//!   DB's `last_error` column.
//! - Validation: mirror-mode rejection, malformed spreadsheet IDs,
//!   empty credential path.

mod common;

use std::time::Duration as StdDuration;

use ferry_core::engine::RunOptions;
use ferry_core::traits::Destination;
use ferry_destinations::GoogleSheetsDestination;
use wiremock::matchers::{body_string_contains, method, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

use common::*;

/// Stub the OAuth2 token endpoint to return a canned bearer token. Returns
/// the token string so tests can assert on it.
async fn stub_token_endpoint(server: &MockServer, token: &str) {
    Mock::given(method("POST"))
        .and(path_regex(r".*/token$"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            r#"{{"access_token":"{token}","token_type":"Bearer","expires_in":3600}}"#
        )))
        .up_to_n_times(u64::MAX)
        .mount(server)
        .await;
}

/// Stub `values.get` to return the given JSON body.
async fn stub_values_get(server: &MockServer, body: &str) {
    Mock::given(method("GET"))
        .and(path_regex(r".*/values/.*"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .mount(server)
        .await;
}

/// Stub `values:batchUpdate` to return 200.
async fn stub_batch_update_200(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path_regex(r".*/values:batchUpdate$"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"totalUpdatedRows":1,"totalUpdatedCells":1}"#),
        )
        .mount(server)
        .await;
}

/// Build a `GoogleSheetsDestination` wired to a wiremock server.
async fn build_dest(
    server: &MockServer,
    sheet: &str,
    key_column: &str,
    max_rows: usize,
) -> GoogleSheetsDestination {
    let key = test_service_account_key(format!("{}/token", server.uri()));
    GoogleSheetsDestination::new_for_test(
        key,
        "test-spreadsheet-id".to_string(),
        sheet.to_string(),
        key_column.to_string(),
        max_rows,
        server.uri(),
        "test_sync",
        StdDuration::from_secs(5),
        StdDuration::from_secs(2),
        1024 * 1024,
        100,
    )
    .await
    .expect("failed to build test destination")
}

#[tokio::test]
async fn test_check_connection_ok() {
    let server = MockServer::start().await;
    stub_token_endpoint(&server, "test-token").await;
    // values.get on A1:A1 returns 200.
    Mock::given(method("GET"))
        .and(path_regex(r".*/values/.*A1:A1.*"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"values":[["x"]]}"#))
        .mount(&server)
        .await;
    let dest = build_dest(&server, "Sheet1", "id", 100).await;
    dest.check_connection().await.expect("connection OK");
}

#[tokio::test]
async fn test_check_connection_404_returns_error() {
    let server = MockServer::start().await;
    stub_token_endpoint(&server, "test-token").await;
    Mock::given(method("GET"))
        .and(path_regex(r".*/values/.*A1:A1.*"))
        .respond_with(ResponseTemplate::new(404).set_body_string("not found"))
        .mount(&server)
        .await;
    let dest = build_dest(&server, "Sheet1", "id", 100).await;
    let err = dest.check_connection().await.unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("HTTP 404"), "got: {msg}");
    // Token must not leak.
    assert!(!msg.contains("test-token"), "token leaked: {msg}");
}

#[tokio::test]
async fn test_bearer_header_sent_on_sheets_request() {
    let server = MockServer::start().await;
    stub_token_endpoint(&server, "bearer-test-123").await;
    // The connection check does a values.get; assert it carries the bearer.
    let received = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let received_clone = received.clone();
    Mock::given(method("GET"))
        .and(path_regex(r".*/values/.*"))
        .respond_with(move |req: &wiremock::Request| {
            let auth = req
                .headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
            received_clone.try_lock().unwrap().push(auth);
            ResponseTemplate::new(200).set_body_string(r#"{"values":[["x"]]}"#)
        })
        .mount(&server)
        .await;
    let dest = build_dest(&server, "Sheet1", "id", 100).await;
    dest.check_connection().await.unwrap();
    let received = received.lock().await.clone();
    assert_eq!(
        received,
        vec![Some("Bearer bearer-test-123".to_string())],
        "Sheets request must carry Authorization: Bearer <token>"
    );
}

#[tokio::test]
async fn test_empty_sheet_writes_headers_and_rows() {
    let server = MockServer::start().await;
    stub_token_endpoint(&server, "tok").await;
    // values.get returns empty (no values).
    stub_values_get(&server, r#"{"values":[]}"#).await;
    // Capture the batchUpdate body.
    let captured = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let captured_clone = captured.clone();
    Mock::given(method("POST"))
        .and(path_regex(r".*/values:batchUpdate$"))
        .respond_with(move |req: &wiremock::Request| {
            captured_clone
                .try_lock()
                .unwrap()
                .push(String::from_utf8_lossy(&req.body).to_string());
            ResponseTemplate::new(200).set_body_string(r#"{}"#)
        })
        .mount(&server)
        .await;

    let (_db_dir, db_path) = setup_test_db(create_test_table(), &insert_test_rows(3));
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest = build_dest(&server, "Sheet1", "id", 100).await;
    let dest_cfg = google_sheets_dest_config("test-spreadsheet-id", "Sheet1", "id", 100);
    let sync_config = create_google_sheets_sync_config(
        "gs_empty",
        "SELECT id, name, value FROM test_table ORDER BY id",
        dest_cfg,
    );

    let result = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("sync should succeed");
    assert_eq!(result.rows_synced, 3, "all 3 rows should sync");
    assert_eq!(count_journal_synced(&engine, "gs_empty"), 3);

    let captured = captured.lock().await.clone();
    assert!(!captured.is_empty(), "expected at least one batchUpdate");
    let body = captured.join("\n");
    // Must be RAW.
    assert!(
        body.contains("\"valueInputOption\":\"RAW\""),
        "expected RAW, got: {body}"
    );
    // Must contain the header row (id, name, value) and an A1 range ending in 1.
    assert!(
        body.contains("'Sheet1'!A1:C1"),
        "missing header range: {body}"
    );
    // Must contain data rows with explicit A1 ranges (A2, A3, A4).
    assert!(body.contains("'Sheet1'!A2:C2"), "missing row 2: {body}");
    assert!(body.contains("'Sheet1'!A3:C3"), "missing row 3: {body}");
    assert!(body.contains("'Sheet1'!A4:C4"), "missing row 4: {body}");
    // Must NOT use append.
    assert!(!body.contains(":append"), "append endpoint used: {body}");
    // Must NOT use USER_ENTERED.
    assert!(!body.contains("USER_ENTERED"), "USER_ENTERED used: {body}");
}

#[tokio::test]
async fn test_existing_sheet_updates_in_place_and_appends_new() {
    let server = MockServer::start().await;
    stub_token_endpoint(&server, "tok").await;
    // values.get returns an existing sheet with header + 2 rows (keys pk-0000, pk-0001).
    stub_values_get(
        &server,
        r#"{"values":[["id","name","value"],["pk-0000","name-0","0"],["pk-0001","name-1","10"]]}"#,
    )
    .await;
    let captured = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let captured_clone = captured.clone();
    Mock::given(method("POST"))
        .and(path_regex(r".*/values:batchUpdate$"))
        .respond_with(move |req: &wiremock::Request| {
            captured_clone
                .try_lock()
                .unwrap()
                .push(String::from_utf8_lossy(&req.body).to_string());
            ResponseTemplate::new(200).set_body_string(r#"{}"#)
        })
        .mount(&server)
        .await;

    let (_db_dir, db_path) = setup_test_db(create_test_table(), &insert_test_rows(3));
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest = build_dest(&server, "Sheet1", "id", 100).await;
    let dest_cfg = google_sheets_dest_config("test-spreadsheet-id", "Sheet1", "id", 100);
    let sync_config = create_google_sheets_sync_config(
        "gs_existing",
        "SELECT id, name, value FROM test_table ORDER BY id",
        dest_cfg,
    );

    let result = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("sync should succeed");
    assert_eq!(result.rows_synced, 3);

    let captured = captured.lock().await.clone();
    let body = captured.join("\n");
    // pk-0000 → A2 (in-place), pk-0001 → A3 (in-place), pk-0002 → A4 (new).
    assert!(
        body.contains("'Sheet1'!A2:C2"),
        "missing in-place A2: {body}"
    );
    assert!(
        body.contains("'Sheet1'!A3:C3"),
        "missing in-place A3: {body}"
    );
    assert!(body.contains("'Sheet1'!A4:C4"), "missing new A4: {body}");
    // Must NOT contain a header write (sheet already has the right header).
    assert!(
        !body.contains("'Sheet1'!A1:C1"),
        "unexpected header write: {body}"
    );
}

#[tokio::test]
async fn test_header_mismatch_is_fatal() {
    let server = MockServer::start().await;
    stub_token_endpoint(&server, "tok").await;
    // Existing header does NOT match the source schema. The actual
    // destination row-1 cell values ("wrong", "headers", "here") are
    // sensitive destination data — they must NOT leak into the journal.
    stub_values_get(
        &server,
        r#"{"values":[["wrong","headers","here"],["pk-0000","a","b"]]}"#,
    )
    .await;
    stub_batch_update_200(&server).await;

    let (_db_dir, db_path) = setup_test_db(create_test_table(), &insert_test_rows(2));
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest = build_dest(&server, "Sheet1", "id", 100).await;
    let dest_cfg = google_sheets_dest_config("test-spreadsheet-id", "Sheet1", "id", 100);
    let sync_config = create_google_sheets_sync_config(
        "gs_mismatch",
        "SELECT id, name, value FROM test_table ORDER BY id",
        dest_cfg,
    );

    let result = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("sync should not hard-fail");
    assert_eq!(
        result.rows_synced, 0,
        "no rows should sync on header mismatch"
    );
    // Journal should have rows with header mismatch errors.
    assert!(
        count_journal_errors_containing(&engine, "gs_mismatch", "dead", "header row mismatch") > 0
            || count_journal_errors_containing(
                &engine,
                "gs_mismatch",
                "pending",
                "header row mismatch"
            ) > 0,
        "expected header mismatch errors in journal"
    );
    // The actual destination cell values ("wrong", "headers", "here") must
    // NOT appear in the journal — they are destination data, not config.
    let conn = engine.state().get_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT last_error FROM row_journal WHERE sync_name = ? AND last_error IS NOT NULL",
        )
        .unwrap();
    let errors: Vec<String> = stmt
        .query_map(duckdb::params!["gs_mismatch"], |row| {
            row.get::<_, String>(0)
        })
        .unwrap()
        .filter_map(|r| r.ok())
        .filter(|s| !s.is_empty())
        .collect();
    for e in &errors {
        assert!(
            !e.contains("wrong"),
            "destination cell value 'wrong' leaked into journal: {e}"
        );
        assert!(
            !e.contains("headers"),
            "destination cell value 'headers' leaked into journal: {e}"
        );
        assert!(
            !e.contains("here"),
            "destination cell value 'here' leaked into journal: {e}"
        );
        // The error should report counts and a first-differing column
        // index, not the full actual row.
        assert!(
            e.contains("3 column") || e.contains("expected 3"),
            "expected column count in mismatch error, got: {e}"
        );
    }
}

#[tokio::test]
async fn test_sheet_name_with_space_is_quoted() {
    let server = MockServer::start().await;
    stub_token_endpoint(&server, "tok").await;
    stub_values_get(&server, r#"{"values":[]}"#).await;
    let captured = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let captured_clone = captured.clone();
    Mock::given(method("POST"))
        .and(path_regex(r".*/values:batchUpdate$"))
        .respond_with(move |req: &wiremock::Request| {
            captured_clone
                .try_lock()
                .unwrap()
                .push(String::from_utf8_lossy(&req.body).to_string());
            ResponseTemplate::new(200).set_body_string(r#"{}"#)
        })
        .mount(&server)
        .await;

    let (_db_dir, db_path) = setup_test_db(create_test_table(), &insert_test_rows(1));
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest = build_dest(&server, "My Sheet", "id", 100).await;
    let dest_cfg = google_sheets_dest_config("test-spreadsheet-id", "My Sheet", "id", 100);
    let sync_config = create_google_sheets_sync_config(
        "gs_quote",
        "SELECT id, name, value FROM test_table ORDER BY id",
        dest_cfg,
    );

    let result = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("sync should succeed");
    assert_eq!(result.rows_synced, 1);
    let captured = captured.lock().await.clone();
    let body = captured.join("\n");
    // Sheet name must be single-quoted: 'My Sheet'!A1:C1
    assert!(
        body.contains("'My Sheet'!A1:C1"),
        "expected quoted sheet name, got: {body}"
    );
}

#[tokio::test]
async fn test_401_force_refresh_then_success() {
    let server = MockServer::start().await;
    // Token endpoint: returns "first-token" then "refreshed-token" (force_refreshed_token
    // always fetches a new one, so both calls hit the stub).
    stub_token_endpoint(&server, "any-token").await;
    // values.get returns empty.
    stub_values_get(&server, r#"{"values":[]}"#).await;
    // batchUpdate: first call 401, second call 200.
    let call_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let call_count_clone = call_count.clone();
    Mock::given(method("POST"))
        .and(path_regex(r".*/values:batchUpdate$"))
        .respond_with(move |_req: &wiremock::Request| {
            let n = call_count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n == 0 {
                ResponseTemplate::new(401).set_body_string("unauthorized")
            } else {
                ResponseTemplate::new(200).set_body_string(r#"{}"#)
            }
        })
        .mount(&server)
        .await;

    let (_db_dir, db_path) = setup_test_db(create_test_table(), &insert_test_rows(1));
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest = build_dest(&server, "Sheet1", "id", 100).await;
    let dest_cfg = google_sheets_dest_config("test-spreadsheet-id", "Sheet1", "id", 100);
    let sync_config = create_google_sheets_sync_config(
        "gs_401",
        "SELECT id, name, value FROM test_table ORDER BY id",
        dest_cfg,
    );

    let result = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("sync should succeed after refresh");
    // The 401 was refreshed and retried → row syncs.
    assert_eq!(result.rows_synced, 1, "row should sync after 401 refresh");
    assert!(
        call_count.load(std::sync::atomic::Ordering::SeqCst) >= 2,
        "expected at least 2 batchUpdate calls (401 + replay)"
    );
}

#[tokio::test]
async fn test_429_marks_rows_pending_with_retry_after() {
    let server = MockServer::start().await;
    stub_token_endpoint(&server, "tok").await;
    stub_values_get(&server, r#"{"values":[]}"#).await;
    Mock::given(method("POST"))
        .and(path_regex(r".*/values:batchUpdate$"))
        .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "1"))
        .mount(&server)
        .await;

    let (_db_dir, db_path) = setup_test_db(create_test_table(), &insert_test_rows(2));
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest = build_dest(&server, "Sheet1", "id", 100).await;
    let dest_cfg = google_sheets_dest_config("test-spreadsheet-id", "Sheet1", "id", 100);
    let sync_config = create_google_sheets_sync_config(
        "gs_429",
        "SELECT id, name, value FROM test_table ORDER BY id",
        dest_cfg,
    );

    let result = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("sync should not hard-fail");
    assert_eq!(result.rows_synced, 0);
    assert!(
        count_journal_errors_containing(&engine, "gs_429", "pending", "HTTP 429") > 0,
        "expected pending rows with HTTP 429"
    );
}

#[tokio::test]
async fn test_500_marks_rows_pending() {
    let server = MockServer::start().await;
    stub_token_endpoint(&server, "tok").await;
    stub_values_get(&server, r#"{"values":[]}"#).await;
    Mock::given(method("POST"))
        .and(path_regex(r".*/values:batchUpdate$"))
        .respond_with(ResponseTemplate::new(500).set_body_string("server error"))
        .mount(&server)
        .await;

    let (_db_dir, db_path) = setup_test_db(create_test_table(), &insert_test_rows(2));
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest = build_dest(&server, "Sheet1", "id", 100).await;
    let dest_cfg = google_sheets_dest_config("test-spreadsheet-id", "Sheet1", "id", 100);
    let sync_config = create_google_sheets_sync_config(
        "gs_500",
        "SELECT id, name, value FROM test_table ORDER BY id",
        dest_cfg,
    );

    let result = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("sync should not hard-fail");
    assert_eq!(result.rows_synced, 0);
    assert!(
        count_journal_errors_containing(&engine, "gs_500", "pending", "HTTP 500") > 0,
        "expected pending rows with HTTP 500"
    );
}

#[tokio::test]
async fn test_400_marks_rows_dead_letter() {
    let server = MockServer::start().await;
    stub_token_endpoint(&server, "tok").await;
    stub_values_get(&server, r#"{"values":[]}"#).await;
    Mock::given(method("POST"))
        .and(path_regex(r".*/values:batchUpdate$"))
        .respond_with(ResponseTemplate::new(400).set_body_string("bad request"))
        .mount(&server)
        .await;

    let (_db_dir, db_path) = setup_test_db(create_test_table(), &insert_test_rows(2));
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest = build_dest(&server, "Sheet1", "id", 100).await;
    let dest_cfg = google_sheets_dest_config("test-spreadsheet-id", "Sheet1", "id", 100);
    let mut sync_config = create_google_sheets_sync_config(
        "gs_400",
        "SELECT id, name, value FROM test_table ORDER BY id",
        dest_cfg,
    );
    // Classify 400 as DeadLetter (default would retry).
    if let Some(delivery) = sync_config.sync.delivery.as_mut() {
        delivery.on_reject = Some(ferry_core::config::RejectConfig {
            classify: vec![ferry_core::config::RejectRule {
                match_: ferry_core::config::RejectMatch {
                    status_code: Some(400),
                    body_contains: None,
                },
                action: ferry_core::config::RejectAction::DeadLetter,
            }],
        });
    }

    let result = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("sync should not hard-fail");
    assert_eq!(result.rows_synced, 0);
    assert!(
        count_journal_errors_containing(&engine, "gs_400", "dead", "HTTP 400") > 0,
        "expected dead rows with HTTP 400"
    );
}

#[tokio::test]
async fn test_token_never_leaks_in_errors_or_debug() {
    let server = MockServer::start().await;
    stub_token_endpoint(&server, "SECRET-TOKEN-XYZ").await;
    stub_values_get(&server, r#"{"values":[]}"#).await;
    // Server echoes the Authorization header back in a 500 body.
    Mock::given(method("POST"))
        .and(path_regex(r".*/values:batchUpdate$"))
        .respond_with(|req: &wiremock::Request| {
            let auth = req
                .headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            ResponseTemplate::new(500).set_body_string(format!("echo: {auth}"))
        })
        .mount(&server)
        .await;

    let (_db_dir, db_path) = setup_test_db(create_test_table(), &insert_test_rows(1));
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest = build_dest(&server, "Sheet1", "id", 100).await;

    // Debug output must not contain the token or the private key.
    let dbg = format!("{dest:?}");
    assert!(
        !dbg.contains("SECRET-TOKEN-XYZ"),
        "Debug leaked token: {dbg}"
    );
    assert!(
        !dbg.contains("BEGIN PRIVATE KEY"),
        "Debug leaked private key: {dbg}"
    );

    let dest_cfg = google_sheets_dest_config("test-spreadsheet-id", "Sheet1", "id", 100);
    let sync_config = create_google_sheets_sync_config(
        "gs_leak",
        "SELECT id, name, value FROM test_table ORDER BY id",
        dest_cfg,
    );

    let _ = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("sync should not hard-fail");

    // Inspect the state DB's last_error column for the token / private key.
    let conn = engine.state().get_conn().unwrap();
    let mut stmt = conn
        .prepare("SELECT last_error FROM row_journal WHERE sync_name = ?")
        .unwrap();
    let errors: Vec<String> = stmt
        .query_map(duckdb::params!["gs_leak"], |row| row.get::<_, String>(0))
        .unwrap()
        .filter_map(|r| r.ok())
        .filter(|s| !s.is_empty())
        .collect();
    for e in &errors {
        assert!(
            !e.contains("SECRET-TOKEN-XYZ"),
            "token leaked into journal: {e}"
        );
        assert!(
            !e.contains("Bearer"),
            "Bearer prefix leaked into journal: {e}"
        );
        assert!(
            !e.contains("BEGIN PRIVATE KEY"),
            "private key leaked into journal: {e}"
        );
    }
}

#[tokio::test]
async fn test_no_append_or_clear_or_user_entered_in_requests() {
    let server = MockServer::start().await;
    stub_token_endpoint(&server, "tok").await;
    stub_values_get(&server, r#"{"values":[]}"#).await;
    let captured = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let captured_clone = captured.clone();
    Mock::given(method("POST"))
        .and(path_regex(r".*/values:batchUpdate$"))
        .respond_with(move |req: &wiremock::Request| {
            captured_clone
                .try_lock()
                .unwrap()
                .push(req.url.path().to_string());
            ResponseTemplate::new(200).set_body_string(r#"{}"#)
        })
        .mount(&server)
        .await;

    let (_db_dir, db_path) = setup_test_db(create_test_table(), &insert_test_rows(1));
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest = build_dest(&server, "Sheet1", "id", 100).await;
    let dest_cfg = google_sheets_dest_config("test-spreadsheet-id", "Sheet1", "id", 100);
    let sync_config = create_google_sheets_sync_config(
        "gs_no_append",
        "SELECT id, name, value FROM test_table ORDER BY id",
        dest_cfg,
    );

    let result = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("sync should succeed");
    assert_eq!(result.rows_synced, 1);
    let captured = captured.lock().await.clone();
    for path in &captured {
        assert!(!path.contains(":append"), "append endpoint used: {path}");
        assert!(!path.contains(":clear"), "clear endpoint used: {path}");
    }
}

/// Validation tests live in `ferry-core::validation`, but we exercise the
/// full config-load path here to assert mirror-mode rejection end-to-end.
#[tokio::test]
async fn test_mirror_mode_rejected_for_google_sheets() {
    let dir = tempfile::tempdir().unwrap();
    let syncs = dir.path().join("syncs");
    std::fs::create_dir_all(&syncs).unwrap();
    let yaml = r#"
name: gs_mirror
model:
  sql: SELECT 1
destination:
  type: google_sheets
  spreadsheet_id: test-id
  sheet: Sheet1
  key_column: id
  service_account_key_file: /tmp/key.json
  max_rows: 100
sync:
  mode: mirror
"#;
    std::fs::write(syncs.join("gs_mirror.yml"), yaml).unwrap();
    let err = ferry_core::config::SyncConfig::load(&syncs.join("gs_mirror.yml")).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("mirror") && msg.to_lowercase().contains("google"),
        "expected mirror-mode rejection for Google Sheets, got: {msg}"
    );
}

#[tokio::test]
async fn test_malformed_spreadsheet_id_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let syncs = dir.path().join("syncs");
    std::fs::create_dir_all(&syncs).unwrap();
    // Spreadsheet ID contains a '/' — unsafe for URL interpolation.
    let yaml = r#"
name: gs_bad_id
model:
  sql: SELECT 1
destination:
  type: google_sheets
  spreadsheet_id: "bad/id"
  sheet: Sheet1
  key_column: id
  service_account_key_file: /tmp/key.json
  max_rows: 100
sync:
  mode: full_refresh
"#;
    std::fs::write(syncs.join("gs_bad_id.yml"), yaml).unwrap();
    let err = ferry_core::config::SyncConfig::load(&syncs.join("gs_bad_id.yml")).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("spreadsheet_id"),
        "expected spreadsheet_id validation error, got: {msg}"
    );
}

#[tokio::test]
async fn test_empty_credential_path_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let syncs = dir.path().join("syncs");
    std::fs::create_dir_all(&syncs).unwrap();
    let yaml = r#"
name: gs_no_cred
model:
  sql: SELECT 1
destination:
  type: google_sheets
  spreadsheet_id: test-id
  sheet: Sheet1
  key_column: id
  service_account_key_file: ""
  max_rows: 100
sync:
  mode: full_refresh
"#;
    std::fs::write(syncs.join("gs_no_cred.yml"), yaml).unwrap();
    let err = ferry_core::config::SyncConfig::load(&syncs.join("gs_no_cred.yml")).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("service_account_key_file"),
        "expected empty credential path error, got: {msg}"
    );
}

#[tokio::test]
async fn test_full_refresh_upsert_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let syncs = dir.path().join("syncs");
    std::fs::create_dir_all(&syncs).unwrap();
    let yaml = r#"
name: gs_full
model:
  sql: SELECT 1
destination:
  type: google_sheets
  spreadsheet_id: test-id
  sheet: Sheet1
  key_column: id
  service_account_key_file: /tmp/key.json
  max_rows: 100
sync:
  mode: full_refresh
"#;
    std::fs::write(syncs.join("gs_full.yml"), yaml).unwrap();
    // Should parse and validate without error (factory would still fail at
    // `GoogleSheetsDestination::new` because /tmp/key.json doesn't exist,
    // but validation itself passes).
    let cfg = ferry_core::config::SyncConfig::load(&syncs.join("gs_full.yml"))
        .expect("full_refresh + google_sheets should validate");
    assert!(matches!(
        cfg.destination,
        ferry_core::config::DestinationConfig::GoogleSheets { .. }
    ));
}

#[tokio::test]
async fn test_secrets_resolution_fills_credential_path() {
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let syncs = dir.path().join("syncs");
    std::fs::create_dir_all(&syncs).unwrap();
    let yaml = r#"
name: gs_secrets
model:
  sql: SELECT 1
destination:
  type: google_sheets
  spreadsheet_id: test-id
  sheet: Sheet1
  key_column: id
  service_account_key_file: ""
  max_rows: 100
sync:
  mode: full_refresh
"#;
    std::fs::write(syncs.join("gs_secrets.yml"), yaml).unwrap();

    let secrets_path = dir.path().join("secrets.toml");
    let mut f = std::fs::File::create(&secrets_path).unwrap();
    write!(
        f,
        "[destination.google_sheets]\nservice_account_key_file = \"/resolved/from/secrets.json\"\n"
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&secrets_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }

    let cfg = ferry_core::config::SyncConfig::load(&syncs.join("gs_secrets.yml"))
        .expect("should load with secrets resolution");
    if let ferry_core::config::DestinationConfig::GoogleSheets {
        service_account_key_file,
        ..
    } = cfg.destination
    {
        assert_eq!(
            service_account_key_file, "/resolved/from/secrets.json",
            "secrets.toml should fill the empty credential path"
        );
    } else {
        panic!("expected GoogleSheets destination");
    }
}

#[tokio::test]
async fn test_a1_column_aa_boundary() {
    // This is a unit test that belongs next to the impl, but we keep all
    // Google Sheets tests in this one file. Assert the A1 column helper
    // across the Z/AA boundary by inspecting a batchUpdate body for a
    // 27-column schema.
    let server = MockServer::start().await;
    stub_token_endpoint(&server, "tok").await;
    stub_values_get(&server, r#"{"values":[]}"#).await;
    let captured = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let captured_clone = captured.clone();
    Mock::given(method("POST"))
        .and(path_regex(r".*/values:batchUpdate$"))
        .respond_with(move |req: &wiremock::Request| {
            captured_clone
                .try_lock()
                .unwrap()
                .push(String::from_utf8_lossy(&req.body).to_string());
            ResponseTemplate::new(200).set_body_string(r#"{}"#)
        })
        .mount(&server)
        .await;

    // 27 columns → last A1 column is "AA".
    let tables_sql = "CREATE TABLE wide (id VARCHAR PRIMARY KEY";
    let mut cols = String::new();
    for i in 0..26 {
        cols.push_str(&format!(", c{i} VARCHAR"));
    }
    cols.push_str(");");
    let tables_sql = format!("{tables_sql}{cols}");
    let mut insert = String::from("INSERT INTO wide VALUES ('pk-0'");
    for i in 0..26 {
        insert.push_str(&format!(", 'v{i}'"));
    }
    insert.push_str(");");

    let (_db_dir, db_path) = setup_test_db(&tables_sql, &insert);
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest = build_dest(&server, "Sheet1", "id", 100).await;
    let dest_cfg = google_sheets_dest_config("test-spreadsheet-id", "Sheet1", "id", 100);
    // SELECT all 27 columns.
    let sync_config = create_google_sheets_sync_config(
        "gs_wide",
        "SELECT id, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25 FROM wide",
        dest_cfg,
    );

    let result = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("sync should succeed");
    assert_eq!(result.rows_synced, 1);
    let captured = captured.lock().await.clone();
    let body = captured.join("\n");
    // 27 columns → A through AA. Header range should be A1:AA1.
    assert!(
        body.contains("'Sheet1'!A1:AA1"),
        "expected A1:AA1 for 27 columns, got: {body}"
    );
}

#[tokio::test]
async fn test_max_rows_exhaustion_for_new_keys() {
    let server = MockServer::start().await;
    stub_token_endpoint(&server, "tok").await;
    // Existing sheet has 1 header + 1 data row at row 2. max_rows=3 → only
    // row 3 is available for new keys; a second new key must fail.
    stub_values_get(
        &server,
        r#"{"values":[["id","name","value"],["pk-existing","name","0"]]}"#,
    )
    .await;
    stub_batch_update_200(&server).await;

    let (_db_dir, db_path) = setup_test_db(create_test_table(), &insert_test_rows(2));
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    // max_rows=3: row 1 header, row 2 existing, row 3 free. We have 2
    // incoming rows: pk-0000 (new) and pk-0001 (new). Only one can land.
    let dest = build_dest(&server, "Sheet1", "id", 3).await;
    let dest_cfg = google_sheets_dest_config("test-spreadsheet-id", "Sheet1", "id", 3);
    let sync_config = create_google_sheets_sync_config(
        "gs_max_rows",
        "SELECT id, name, value FROM test_table ORDER BY id",
        dest_cfg,
    );

    let result = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("sync should not hard-fail");
    // At least one row should fail with max_rows exhaustion.
    assert!(
        count_journal_errors_containing(&engine, "gs_max_rows", "dead", "max_rows") > 0
            || count_journal_errors_containing(&engine, "gs_max_rows", "pending", "max_rows") > 0,
        "expected max_rows exhaustion errors, got synced={} pending={} failed={}",
        result.rows_synced,
        result.rows_pending,
        result.rows_failed
    );
}

#[tokio::test]
async fn test_duplicate_incoming_keys_rejected() {
    let server = MockServer::start().await;
    stub_token_endpoint(&server, "tok").await;
    stub_values_get(&server, r#"{"values":[["id","name","value"]]}"#).await;
    stub_batch_update_200(&server).await;

    // Insert two rows with the SAME id. Drop the PK constraint so DuckDB
    // accepts the duplicate (the source is allowed to have duplicates; the
    // destination must reject them row-safely).
    let tables = "CREATE TABLE test_table (\n    id VARCHAR,\n    name VARCHAR NOT NULL,\n    value INTEGER\n);";
    let insert = "INSERT INTO test_table VALUES ('dup', 'a', 1);\nINSERT INTO test_table VALUES ('dup', 'b', 2);\n";
    let (_db_dir, db_path) = setup_test_db(tables, insert);
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest = build_dest(&server, "Sheet1", "id", 100).await;
    let dest_cfg = google_sheets_dest_config("test-spreadsheet-id", "Sheet1", "id", 100);
    let sync_config = create_google_sheets_sync_config(
        "gs_dup",
        "SELECT id, name, value FROM test_table",
        dest_cfg,
    );

    let result = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("sync should not hard-fail");
    assert_eq!(
        result.rows_synced, 0,
        "no rows should sync on duplicate keys"
    );
    // The duplicate-key error has no HTTP status code, so the pipeline
    // default classifies it as Retry → pending.
    let pending =
        count_journal_errors_containing(&engine, "gs_dup", "pending", "duplicate incoming key");
    let dead = count_journal_errors_containing(&engine, "gs_dup", "dead", "duplicate incoming key");
    assert!(
        pending > 0 || dead > 0,
        "expected duplicate incoming key errors in journal (pending={pending}, dead={dead})"
    );
}

#[tokio::test]
async fn test_null_and_boolean_serialization() {
    let server = MockServer::start().await;
    stub_token_endpoint(&server, "tok").await;
    stub_values_get(&server, r#"{"values":[]}"#).await;
    let captured = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let captured_clone = captured.clone();
    Mock::given(method("POST"))
        .and(path_regex(r".*/values:batchUpdate$"))
        .respond_with(move |req: &wiremock::Request| {
            captured_clone
                .try_lock()
                .unwrap()
                .push(String::from_utf8_lossy(&req.body).to_string());
            ResponseTemplate::new(200).set_body_string(r#"{}"#)
        })
        .mount(&server)
        .await;

    // Schema: id (VARCHAR), flag (BOOLEAN), note (VARCHAR nullable).
    let tables = "CREATE TABLE typed (id VARCHAR PRIMARY KEY, flag BOOLEAN, note VARCHAR);";
    let insert = "INSERT INTO typed VALUES ('a', true, 'hello');\nINSERT INTO typed VALUES ('b', false, NULL);\n";
    let (_db_dir, db_path) = setup_test_db(tables, insert);
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest = build_dest(&server, "Sheet1", "id", 100).await;
    let dest_cfg = google_sheets_dest_config("test-spreadsheet-id", "Sheet1", "id", 100);
    let sync_config =
        create_google_sheets_sync_config("gs_types", "SELECT id, flag, note FROM typed", dest_cfg);

    let result = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("sync should succeed");
    assert_eq!(result.rows_synced, 2);
    let captured = captured.lock().await.clone();
    let body = captured.join("\n");
    // Booleans → "TRUE" / "FALSE" strings.
    assert!(body.contains("TRUE"), "expected TRUE in body: {body}");
    assert!(body.contains("FALSE"), "expected FALSE in body: {body}");
    // Null → empty string (RAW).
    // The body should have an empty string value for the null cell.
    // We check for `"\"\""` somewhere in the values.
    assert!(
        body.contains("\"\""),
        "expected empty-string for null cell: {body}"
    );
}

#[tokio::test]
async fn test_oversized_response_rejected() {
    let server = MockServer::start().await;
    stub_token_endpoint(&server, "tok").await;
    // values.get returns a 2 MiB body; max_response_bytes is 1 MiB.
    let big = "x".repeat(2 * 1024 * 1024);
    Mock::given(method("GET"))
        .and(path_regex(r".*/values/.*"))
        .respond_with(ResponseTemplate::new(200).set_body_string(big))
        .mount(&server)
        .await;
    stub_batch_update_200(&server).await;

    let (_db_dir, db_path) = setup_test_db(create_test_table(), &insert_test_rows(1));
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest = build_dest(&server, "Sheet1", "id", 100).await;
    let dest_cfg = google_sheets_dest_config("test-spreadsheet-id", "Sheet1", "id", 100);
    let sync_config = create_google_sheets_sync_config(
        "gs_oversize",
        "SELECT id, name, value FROM test_table ORDER BY id",
        dest_cfg,
    );

    // The oversized response should result in a parse error or row errors,
    // but not a hard crash.
    let _ = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("sync should not hard-fail even on oversized response");
}

#[tokio::test]
async fn test_transport_timeout_classified_as_transport() {
    let server = MockServer::start().await;
    stub_token_endpoint(&server, "tok").await;
    // values.get delays 3s; destination timeout is 1s.
    Mock::given(method("GET"))
        .and(path_regex(r".*/values/.*"))
        .respond_with(ResponseTemplate::new(200).set_delay(StdDuration::from_secs(3)))
        .mount(&server)
        .await;
    stub_batch_update_200(&server).await;

    // Build a destination with a 1s timeout.
    let key = test_service_account_key(format!("{}/token", server.uri()));
    let dest = GoogleSheetsDestination::new_for_test(
        key,
        "test-spreadsheet-id".to_string(),
        "Sheet1".to_string(),
        "id".to_string(),
        100,
        server.uri(),
        "gs_timeout",
        StdDuration::from_secs(1),
        StdDuration::from_secs(1),
        1024 * 1024,
        100,
    )
    .await
    .unwrap();

    let (_db_dir, db_path) = setup_test_db(create_test_table(), &insert_test_rows(1));
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest_cfg = google_sheets_dest_config("test-spreadsheet-id", "Sheet1", "id", 100);
    let sync_config = create_google_sheets_sync_config(
        "gs_timeout",
        "SELECT id, name, value FROM test_table ORDER BY id",
        dest_cfg,
    );

    let _ = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("sync should not hard-fail on timeout");
    // The error should be classified as transport (no HTTP NNN).
    let conn = engine.state().get_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT last_error FROM row_journal WHERE sync_name = ? AND last_error IS NOT NULL",
        )
        .unwrap();
    let errors: Vec<String> = stmt
        .query_map(duckdb::params!["gs_timeout"], |row| row.get::<_, String>(0))
        .unwrap()
        .filter_map(|r| r.ok())
        .filter(|s| !s.is_empty())
        .collect();
    let http_re = regex::Regex::new(r"HTTP \d{3}").unwrap();
    for e in &errors {
        assert!(
            !http_re.is_match(e) || e.contains("transport"),
            "timeout error should look like transport, not HTTP NNN: {e}"
        );
    }
}

#[tokio::test]
async fn test_retry_after_applied_on_429() {
    let server = MockServer::start().await;
    stub_token_endpoint(&server, "tok").await;
    stub_values_get(&server, r#"{"values":[]}"#).await;
    Mock::given(method("POST"))
        .and(path_regex(r".*/values:batchUpdate$"))
        .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "2"))
        .mount(&server)
        .await;

    let (_db_dir, db_path) = setup_test_db(create_test_table(), &insert_test_rows(1));
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest = build_dest(&server, "Sheet1", "id", 100).await;
    let dest_cfg = google_sheets_dest_config("test-spreadsheet-id", "Sheet1", "id", 100);
    let sync_config = create_google_sheets_sync_config(
        "gs_retry_after",
        "SELECT id, name, value FROM test_table ORDER BY id",
        dest_cfg,
    );

    let _ = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("sync should not hard-fail");
    // The journal should have a pending row whose error contains "retry_after: 2".
    assert!(
        count_journal_errors_containing(&engine, "gs_retry_after", "pending", "retry_after: 2") > 0,
        "expected retry_after: 2 in pending errors"
    );
}

#[tokio::test]
async fn test_real_pks_in_journal_errors() {
    // When a write fails, the journal's primary_key column should contain
    // the real source PK (e.g. "pk-0000"), not a row index.
    let server = MockServer::start().await;
    stub_token_endpoint(&server, "tok").await;
    stub_values_get(&server, r#"{"values":[]}"#).await;
    Mock::given(method("POST"))
        .and(path_regex(r".*/values:batchUpdate$"))
        .respond_with(ResponseTemplate::new(400).set_body_string("bad"))
        .mount(&server)
        .await;

    let (_db_dir, db_path) = setup_test_db(create_test_table(), &insert_test_rows(2));
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest = build_dest(&server, "Sheet1", "id", 100).await;
    let dest_cfg = google_sheets_dest_config("test-spreadsheet-id", "Sheet1", "id", 100);
    let mut sync_config = create_google_sheets_sync_config(
        "gs_realpk",
        "SELECT id, name, value FROM test_table ORDER BY id",
        dest_cfg,
    );
    if let Some(delivery) = sync_config.sync.delivery.as_mut() {
        delivery.on_reject = Some(ferry_core::config::RejectConfig {
            classify: vec![ferry_core::config::RejectRule {
                match_: ferry_core::config::RejectMatch {
                    status_code: Some(400),
                    body_contains: None,
                },
                action: ferry_core::config::RejectAction::DeadLetter,
            }],
        });
    }

    let _ = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .unwrap();
    let conn = engine.state().get_conn().unwrap();
    let mut stmt = conn
        .prepare("SELECT primary_key FROM row_journal WHERE sync_name = ? AND status = 'dead'")
        .unwrap();
    let pks: Vec<String> = stmt
        .query_map(duckdb::params!["gs_realpk"], |row| row.get::<_, String>(0))
        .unwrap()
        .filter_map(|r| r.ok())
        .collect();
    assert!(!pks.is_empty(), "expected dead rows with real PKs");
    for pk in &pks {
        assert!(
            pk.starts_with("pk-"),
            "expected real PK like 'pk-0000', got '{pk}'"
        );
    }
}

#[tokio::test]
async fn test_batch_update_body_contains_explicit_a1_ranges() {
    let server = MockServer::start().await;
    stub_token_endpoint(&server, "tok").await;
    stub_values_get(&server, r#"{"values":[]}"#).await;
    // Use body_string_contains to assert the body has explicit A1 ranges.
    Mock::given(method("POST"))
        .and(path_regex(r".*/values:batchUpdate$"))
        .and(body_string_contains("'Sheet1'!A1:C1"))
        .and(body_string_contains("'Sheet1'!A2:C2"))
        .and(body_string_contains("\"valueInputOption\":\"RAW\""))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{}"#))
        .mount(&server)
        .await;

    let (_db_dir, db_path) = setup_test_db(create_test_table(), &insert_test_rows(1));
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest = build_dest(&server, "Sheet1", "id", 100).await;
    let dest_cfg = google_sheets_dest_config("test-spreadsheet-id", "Sheet1", "id", 100);
    let sync_config = create_google_sheets_sync_config(
        "gs_a1",
        "SELECT id, name, value FROM test_table ORDER BY id",
        dest_cfg,
    );

    let result = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("sync should succeed when body matches A1 expectations");
    assert_eq!(result.rows_synced, 1);
}

// ---------------------------------------------------------------------------
// Quality-gate tests
// ---------------------------------------------------------------------------

/// End-to-end ambiguous-write recovery test.
///
/// Scenario: A sync writes one new key (`pk-0000`) to an empty sheet. The
/// first `values.batchUpdate` is applied by the server but returns a 503
/// (ambiguous transport failure). The delivery pipeline marks the row
/// pending. On the second sync, the re-read exposes the applied key at its
/// row (row 2). The retry must update exactly that same A1 row — never
/// allocate another row — and the journal PK must transition pending→synced
/// with no dead rows and no duplicates.
#[tokio::test]
async fn test_ambiguous_write_recovery_two_runs() {
    let server = MockServer::start().await;
    stub_token_endpoint(&server, "tok").await;

    // Shared state: track how many batchUpdate calls we've seen and what
    // the sheet currently contains (simulating the server applying writes).
    let sheet_state = std::sync::Arc::new(tokio::sync::Mutex::new(SheetState {
        values: Vec::new(),
        batch_update_calls: 0,
        first_sync_done: false,
    }));
    let sheet_state_clone = sheet_state.clone();

    // values.get: return the current sheet state. Set `first_sync_done`
    // when we observe a non-empty sheet (i.e., the first sync's applied
    // write is visible on the second sync's read).
    Mock::given(method("GET"))
        .and(path_regex(r".*/values/.*"))
        .respond_with(move |_req: &wiremock::Request| {
            let mut state = sheet_state_clone.try_lock().unwrap();
            // If the sheet is non-empty and we haven't marked the first
            // sync done yet, this read is the start of the second sync.
            if !state.values.is_empty() && !state.first_sync_done {
                state.first_sync_done = true;
            }
            let body = serde_json::to_string(&serde_json::json!({
                "values": state.values.clone(),
            }))
            .unwrap();
            ResponseTemplate::new(200).set_body_string(body)
        })
        .mount(&server)
        .await;

    // batchUpdate: on the first batchUpdate call ever (across all sync
    // runs), return 503 (applied but response lost). On every subsequent
    // call, return 200 and apply. The connector retries up to 5 times
    // within a single `write`, so we must return 503 for ALL connector
    // retries during the first sync run. We use the sheet state's
    // `first_sync_done` flag (set true by the GET handler seeing a non-
    // empty sheet on the second sync) to switch to 200.
    let sheet_state_clone2 = sheet_state.clone();
    Mock::given(method("POST"))
        .and(path_regex(r".*/values:batchUpdate$"))
        .respond_with(move |req: &wiremock::Request| {
            let mut state = sheet_state_clone2.try_lock().unwrap();
            state.batch_update_calls += 1;
            // If the first sync's GET saw an empty sheet, this is the
            // first sync run → always 503 (applied but response lost).
            // If the second sync's GET saw a non-empty sheet (the applied
            // row), we're in the second run → 200.
            if state.first_sync_done {
                apply_batch_update_to_state(&req.body, &mut state);
                ResponseTemplate::new(200).set_body_string(r#"{}"#)
            } else {
                // First sync: apply the write (simulating a real server that
                // processes the request before the connection drops) but
                // return 503 on every connector retry within this run.
                apply_batch_update_to_state(&req.body, &mut state);
                ResponseTemplate::new(503).set_body_string("server error (ambiguous)")
            }
        })
        .mount(&server)
        .await;

    let (_db_dir, db_path) = setup_test_db(create_test_table(), &insert_test_rows(1));
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest = build_dest(&server, "Sheet1", "id", 100).await;
    let dest_cfg = google_sheets_dest_config("test-spreadsheet-id", "Sheet1", "id", 100);
    let sync_config = create_google_sheets_sync_config(
        "gs_ambiguous",
        "SELECT id, name, value FROM test_table ORDER BY id",
        dest_cfg,
    );

    // First sync: batchUpdate returns 503 → row goes pending.
    let result1 = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("first sync should not hard-fail");
    assert_eq!(
        result1.rows_synced, 0,
        "first sync should not mark any rows synced (503)"
    );
    assert!(
        count_journal_errors_containing(&engine, "gs_ambiguous", "pending", "HTTP 503") > 0,
        "expected pending rows with HTTP 503 after first sync"
    );

    // The server applied the write despite the 503. The sheet now has
    // the header + 1 data row at row 2.
    {
        let state = sheet_state.lock().await;
        assert_eq!(
            state.values.len(),
            2,
            "sheet should have header + 1 data row"
        );
        // The connector retried up to 5 times within the first write call,
        // all returning 503. Each retry re-applied the same A1 write (no
        // duplicate rows). The exact count depends on MAX_CONNECTOR_ATTEMPTS
        // but must be >= 1.
        assert!(
            state.batch_update_calls >= 1,
            "expected at least 1 batchUpdate call, got {}",
            state.batch_update_calls
        );
    }

    // Second sync: re-read exposes the applied key at row 2. The retry
    // must update exactly that same A1 row — never allocate another row.
    let result2 = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("second sync should succeed");
    assert_eq!(
        result2.rows_synced, 1,
        "second sync should mark the row synced"
    );

    // Journal: the PK transitioned pending→synced.
    assert_eq!(
        count_journal_synced(&engine, "gs_ambiguous"),
        1,
        "exactly one row should be synced"
    );
    assert_eq!(
        count_journal_errors_containing(&engine, "gs_ambiguous", "dead", ""),
        0,
        "no dead rows after recovery"
    );

    // The sheet should have exactly 2 rows (header + 1 data row), NOT 3+.
    // If the retry had allocated a new row instead of updating in place,
    // we'd see 3 rows.
    {
        let state = sheet_state.lock().await;
        assert_eq!(
            state.values.len(),
            2,
            "sheet must still have 2 rows after retry (no duplicate row)"
        );
    }
}

/// Helper state for the ambiguous-write test. Tracks the simulated sheet
/// contents and the number of batchUpdate calls received.
struct SheetState {
    /// `values[row]` is a `Vec<serde_json::Value>` (one per column).
    values: Vec<Vec<serde_json::Value>>,
    batch_update_calls: usize,
    /// Set to `true` when the GET handler observes a non-empty sheet
    /// (i.e., the first sync's applied write is visible on the second
    /// sync's read). Used to switch the batchUpdate handler from "always
    /// 503" (first sync) to "200" (second sync).
    first_sync_done: bool,
}

/// Apply a `values.batchUpdate` request body to the simulated sheet state.
/// Parses the JSON body, extracts the `ValueRange` entries, and writes each
/// to its A1 range.
fn apply_batch_update_to_state(body: &[u8], state: &mut SheetState) {
    let parsed: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => return,
    };
    let data = parsed.get("data").and_then(|d| d.as_array());
    if let Some(data) = data {
        for vr in data {
            let range = vr.get("range").and_then(|r| r.as_str()).unwrap_or("");
            let values = vr.get("values").and_then(|v| v.as_array());
            if let Some(values) = values {
                // Parse the A1 row number from the range (e.g.
                // "'Sheet1'!A2:C2" → row 2).
                if let Some(row_num) = parse_a1_row(range) {
                    // Ensure the sheet has enough rows.
                    while state.values.len() < row_num {
                        state.values.push(Vec::new());
                    }
                    // Take the first row of values (we write one row per range).
                    if let Some(first_row) = values.first() {
                        if let Some(row_arr) = first_row.as_array() {
                            state.values[row_num - 1] = row_arr.to_vec();
                        }
                    }
                }
            }
        }
    }
}

/// Extract the first row number from an A1 range string like
/// `'Sheet1'!A2:C2` or `Sheet1!A1:C1`. Returns the 1-based row index.
fn parse_a1_row(range: &str) -> Option<usize> {
    // Find the first digit sequence after the last `!` and before any `:`.
    let after_bang = range.rsplit('!').next()?;
    // The A1 part starts with letters then digits. Find the first digit.
    let digit_start = after_bang.find(|c: char| c.is_ascii_digit())?;
    let digits: String = after_bang[digit_start..]
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect();
    digits.parse::<usize>().ok()
}

/// Test that empty incoming keys are rejected with per-row errors.
#[tokio::test]
async fn test_empty_incoming_key_rejected() {
    let server = MockServer::start().await;
    stub_token_endpoint(&server, "tok").await;
    stub_values_get(&server, r#"{"values":[["id","name","value"]]}"#).await;
    stub_batch_update_200(&server).await;

    // Insert a row with an empty-string id (NOT null — DuckDB primary key
    // would reject null). We use a non-PK-constrained table so DuckDB
    // accepts the empty string.
    let tables = "CREATE TABLE test_table (\n    id VARCHAR,\n    name VARCHAR NOT NULL,\n    value INTEGER\n);";
    let insert = "INSERT INTO test_table VALUES ('', 'empty-key-row', 1);\n";
    let (_db_dir, db_path) = setup_test_db(tables, insert);
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest = build_dest(&server, "Sheet1", "id", 100).await;
    let dest_cfg = google_sheets_dest_config("test-spreadsheet-id", "Sheet1", "id", 100);
    let sync_config = create_google_sheets_sync_config(
        "gs_empty_key",
        "SELECT id, name, value FROM test_table",
        dest_cfg,
    );

    let result = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("sync should not hard-fail");
    assert_eq!(result.rows_synced, 0, "no rows should sync on empty key");
    // The empty-key error has no HTTP status code → pending (default Retry).
    assert!(
        count_journal_errors_containing(&engine, "gs_empty_key", "pending", "empty or null key")
            > 0
            || count_journal_errors_containing(
                &engine,
                "gs_empty_key",
                "dead",
                "empty or null key"
            ) > 0,
        "expected empty-key rejection errors in journal"
    );
}

/// Test that null incoming keys are rejected with per-row errors.
#[tokio::test]
async fn test_null_incoming_key_rejected() {
    let server = MockServer::start().await;
    stub_token_endpoint(&server, "tok").await;
    stub_values_get(&server, r#"{"values":[["id","name","value"]]}"#).await;
    stub_batch_update_200(&server).await;

    // Insert a row with a NULL id. Use a non-PK-constrained table.
    let tables = "CREATE TABLE test_table (\n    id VARCHAR,\n    name VARCHAR NOT NULL,\n    value INTEGER\n);";
    let insert = "INSERT INTO test_table VALUES (NULL, 'null-key-row', 1);\n";
    let (_db_dir, db_path) = setup_test_db(tables, insert);
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest = build_dest(&server, "Sheet1", "id", 100).await;
    let dest_cfg = google_sheets_dest_config("test-spreadsheet-id", "Sheet1", "id", 100);
    let sync_config = create_google_sheets_sync_config(
        "gs_null_key",
        "SELECT id, name, value FROM test_table",
        dest_cfg,
    );

    let result = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("sync should not hard-fail");
    assert_eq!(result.rows_synced, 0, "no rows should sync on null key");
    assert!(
        count_journal_errors_containing(&engine, "gs_null_key", "pending", "empty or null key") > 0
            || count_journal_errors_containing(&engine, "gs_null_key", "dead", "empty or null key")
                > 0,
        "expected null-key rejection errors in journal"
    );
}

/// Test that a failing-write response that echoes representative cell values
/// does not leak those values into RowError / journal / DLQ.
#[tokio::test]
async fn test_cell_values_not_leaked_in_errors() {
    let server = MockServer::start().await;
    stub_token_endpoint(&server, "tok").await;
    stub_values_get(&server, r#"{"values":[]}"#).await;

    // The server echoes the incoming cell values back in a 500 error body.
    // We use distinctive, easily-searched cell values.
    let sensitive_cell = "SUPER_SECRET_CELL_VALUE_42";
    let another_cell = "another-sensitive-row-name";
    Mock::given(method("POST"))
        .and(path_regex(r".*/values:batchUpdate$"))
        .respond_with(move |req: &wiremock::Request| {
            let body_str = String::from_utf8_lossy(&req.body).to_string();
            // Echo the full request body back in the error response.
            ResponseTemplate::new(500)
                .set_body_string(format!("error processing request: {body_str}"))
        })
        .mount(&server)
        .await;

    // Insert a row with distinctive cell values.
    let tables =
        "CREATE TABLE leak_test (\n    id VARCHAR,\n    name VARCHAR,\n    value INTEGER\n);";
    let insert = format!(
        "INSERT INTO leak_test VALUES ('pk-001', '{sensitive_cell}', 99);\nINSERT INTO leak_test VALUES ('pk-002', '{another_cell}', 100);\n"
    );
    let (_db_dir, db_path) = setup_test_db(tables, &insert);
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest = build_dest(&server, "Sheet1", "id", 100).await;
    let dest_cfg = google_sheets_dest_config("test-spreadsheet-id", "Sheet1", "id", 100);
    let sync_config = create_google_sheets_sync_config(
        "gs_cell_leak",
        "SELECT id, name, value FROM leak_test ORDER BY id",
        dest_cfg,
    );

    let _ = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("sync should not hard-fail");

    // Inspect the journal: neither cell value should appear in any
    // last_error column.
    let conn = engine.state().get_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT last_error FROM row_journal WHERE sync_name = ? AND last_error IS NOT NULL",
        )
        .unwrap();
    let errors: Vec<String> = stmt
        .query_map(duckdb::params!["gs_cell_leak"], |row| {
            row.get::<_, String>(0)
        })
        .unwrap()
        .filter_map(|r| r.ok())
        .filter(|s| !s.is_empty())
        .collect();
    assert!(
        !errors.is_empty(),
        "expected at least one error row in journal"
    );
    for e in &errors {
        assert!(
            !e.contains(sensitive_cell),
            "sensitive cell value leaked into journal error: {e}"
        );
        assert!(
            !e.contains(another_cell),
            "another sensitive cell value leaked into journal error: {e}"
        );
    }
}

/// Test that multiple Sheets API calls with a valid, unexpired token
/// perform only one OAuth2 token exchange. The token endpoint should be
/// hit exactly once; subsequent Sheets requests reuse the cached token.
#[tokio::test]
async fn test_token_cache_single_oauth_exchange() {
    let server = MockServer::start().await;

    // Count token endpoint calls.
    let token_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let token_calls_clone = token_calls.clone();
    Mock::given(method("POST"))
        .and(path_regex(r".*/token$"))
        .respond_with(move |_req: &wiremock::Request| {
            token_calls_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ResponseTemplate::new(200).set_body_string(
                r#"{"access_token":"cached-tok","token_type":"Bearer","expires_in":3600}"#,
            )
        })
        .mount(&server)
        .await;

    // values.get returns empty.
    stub_values_get(&server, r#"{"values":[]}"#).await;
    // batchUpdate returns 200.
    stub_batch_update_200(&server).await;

    // Run a sync that performs: 1 token exchange + 1 values.get + 1
    // batchUpdate. All three use the same token.
    let (_db_dir, db_path) = setup_test_db(create_test_table(), &insert_test_rows(2));
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest = build_dest(&server, "Sheet1", "id", 100).await;
    let dest_cfg = google_sheets_dest_config("test-spreadsheet-id", "Sheet1", "id", 100);
    let sync_config = create_google_sheets_sync_config(
        "gs_token_cache",
        "SELECT id, name, value FROM test_table ORDER BY id",
        dest_cfg,
    );

    let result = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("sync should succeed");
    assert_eq!(result.rows_synced, 2);

    // Exactly ONE token exchange should have occurred — the second Sheets
    // request (batchUpdate) must reuse the cached token.
    let exchanges = token_calls.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        exchanges, 1,
        "expected exactly 1 OAuth token exchange, got {exchanges} (token cache not working)"
    );
}

/// Test that an empty access token from the OAuth2 token endpoint yields a
/// sanitized auth error rather than an empty `Authorization: Bearer ` header.
#[tokio::test]
async fn test_empty_bearer_token_rejected() {
    let server = MockServer::start().await;
    // Token endpoint returns an empty `access_token`.
    Mock::given(method("POST"))
        .and(path_regex(r".*/token$"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"access_token":"","token_type":"Bearer","expires_in":3600}"#),
        )
        .mount(&server)
        .await;
    // values.get and batchUpdate should never be reached (the empty token
    // is rejected before any Sheets request).
    stub_values_get(&server, r#"{"values":[]}"#).await;
    stub_batch_update_200(&server).await;

    let (_db_dir, db_path) = setup_test_db(create_test_table(), &insert_test_rows(1));
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest = build_dest(&server, "Sheet1", "id", 100).await;
    let dest_cfg = google_sheets_dest_config("test-spreadsheet-id", "Sheet1", "id", 100);
    let sync_config = create_google_sheets_sync_config(
        "gs_empty_token",
        "SELECT id, name, value FROM test_table ORDER BY id",
        dest_cfg,
    );

    // The sync should not hard-fail (the error is a per-batch row error,
    // not a fatal Err). The row should be pending (retryable auth error,
    // no HTTP status code).
    let result = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("sync should not hard-fail on empty token");
    assert_eq!(result.rows_synced, 0, "no rows should sync on empty token");
    // The journal should have a pending row whose error mentions the
    // empty-token auth failure.
    assert!(
        count_journal_errors_containing(&engine, "gs_empty_token", "pending", "empty access token")
            > 0
            || count_journal_errors_containing(
                &engine,
                "gs_empty_token",
                "dead",
                "empty access token"
            ) > 0,
        "expected empty access token auth error in journal"
    );
}
