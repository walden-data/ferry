//! Integration tests for the production REST destination.
//!
//! These tests drive the full `Engine` + `DeliveryPipeline` against a
//! `wiremock` HTTP server, covering success, retryable/permanent error
//! classification, batch splitting, and secret non-disclosure in the state DB.

mod common;

use std::sync::Arc;
use std::time::Duration;

use ferry_core::config::{
    AuthConfig, DestinationConfig, RejectAction, RejectConfig, RejectMatch, RejectRule, SyncMode,
};
use ferry_core::engine::RunOptions;
use ferry_destinations::RestDestination;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

use common::*;

/// Build a `DestinationConfig::Rest` pointing at `server.uri()`. `allow_http`
/// is always true since wiremock serves over HTTP; production callers should
/// use https.
fn rest_dest_config(server_uri: String, max_batch_size: usize) -> DestinationConfig {
    DestinationConfig::Rest {
        url: server_uri,
        method: Some("POST".to_string()),
        headers: None,
        auth: None,
        body_template: None,
        timeout_secs: Some(5),
        connect_timeout_secs: Some(2),
        max_response_bytes: Some(1024 * 1024),
        allow_http: Some(true),
        max_batch_size: Some(max_batch_size),
    }
}

/// Count synced rows in the journal.
fn count_journal_synced(engine: &ferry_core::engine::Engine, sync_name: &str) -> usize {
    let conn = engine.state().get_conn().unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM row_journal WHERE sync_name = ? AND status = 'synced'",
            duckdb::params![sync_name],
            |row| row.get(0),
        )
        .unwrap_or(0);
    count as usize
}

/// Count rows in the journal with a given status whose `last_error` contains `needle`.
fn count_journal_errors_containing(
    engine: &ferry_core::engine::Engine,
    sync_name: &str,
    status: &str,
    needle: &str,
) -> usize {
    let conn = engine.state().get_conn().unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM row_journal WHERE sync_name = ? AND status = ? AND last_error LIKE ?",
            duckdb::params![sync_name, status, format!("%{needle}%")],
            |row| row.get(0),
        )
        .unwrap_or(0);
    count as usize
}

#[tokio::test]
async fn test_rest_success_syncs_all_rows() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let (_db_dir, db_path) = setup_test_db(create_test_table(), &insert_test_rows(10));
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest = RestDestination::new(&rest_dest_config(server.uri(), 100), "test_sync").unwrap();

    let sync_config = create_sync_config(
        "test_sync",
        SyncMode::Incremental,
        "SELECT id, name, value FROM test_table ORDER BY id",
    );

    let result = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("Sync should succeed");

    assert_eq!(result.rows_extracted, 10);
    assert_eq!(result.rows_synced, 10);
    assert_eq!(result.rows_pending, 0);
    assert_eq!(count_journal_synced(&engine, "test_sync"), 10);
}

#[tokio::test]
async fn test_rest_429_marks_rows_pending_with_retry_after() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "1"))
        .mount(&server)
        .await;

    let (_db_dir, db_path) = setup_test_db(create_test_table(), &insert_test_rows(5));
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest = RestDestination::new(&rest_dest_config(server.uri(), 100), "test_sync").unwrap();

    let sync_config = create_sync_config(
        "test_sync",
        SyncMode::Incremental,
        "SELECT id, name, value FROM test_table ORDER BY id",
    );

    let result = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("Sync should not hard-fail");
    // 429 is retryable → rows go pending.
    assert_eq!(result.rows_synced, 0);
    assert!(
        result.rows_pending > 0 || result.rows_failed > 0,
        "expected rows pending/failed, got synced={} pending={} failed={}",
        result.rows_synced,
        result.rows_pending,
        result.rows_failed
    );
    // Journal should have pending rows with HTTP 429 in last_error.
    assert!(count_journal_errors_containing(&engine, "test_sync", "pending", "HTTP 429") > 0);
}

#[tokio::test]
async fn test_rest_400_marks_rows_dead_letter() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(400).set_body_string("bad input"))
        .mount(&server)
        .await;

    let (_db_dir, db_path) = setup_test_db(create_test_table(), &insert_test_rows(3));
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest = RestDestination::new(&rest_dest_config(server.uri(), 100), "test_sync").unwrap();

    // Use a sync config with an on_reject rule that classifies 400 as DeadLetter.
    // The default (no on_reject) retries everything; the plan's 4xx→dead-letter
    // intent requires explicit classification.
    let mut sync_config = create_sync_config(
        "test_sync",
        SyncMode::Incremental,
        "SELECT id, name, value FROM test_table ORDER BY id",
    );
    if let Some(delivery) = sync_config.sync.delivery.as_mut() {
        delivery.on_reject = Some(RejectConfig {
            classify: vec![RejectRule {
                match_: RejectMatch {
                    status_code: Some(400),
                    body_contains: None,
                },
                action: RejectAction::DeadLetter,
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
    .expect("Sync should not hard-fail");
    // 400 is permanent → dead letter.
    assert_eq!(result.rows_synced, 0);
    assert!(count_journal_errors_containing(&engine, "test_sync", "dead", "HTTP 400") > 0);
}

#[tokio::test]
async fn test_rest_batch_splitting_sends_multiple_requests() {
    // 250 rows, max_batch_size=100 → 3 requests.
    let server = MockServer::start().await;
    let request_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let request_count_clone = request_count.clone();
    Mock::given(method("POST"))
        .respond_with(move |_req: &wiremock::Request| {
            request_count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ResponseTemplate::new(200)
        })
        .mount(&server)
        .await;

    let (_db_dir, db_path) = setup_test_db(create_test_table(), &insert_test_rows(250));
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest = RestDestination::new(&rest_dest_config(server.uri(), 100), "test_sync").unwrap();

    let sync_config = create_sync_config(
        "test_sync",
        SyncMode::Incremental,
        "SELECT id, name, value FROM test_table ORDER BY id",
    );

    let result = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("Sync should succeed");
    assert_eq!(result.rows_synced, 250);

    // The destination's max_batch_size is 100, so the pipeline splits into
    // ceil(250 / 100) = 3 batches.
    let n = request_count.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(n, 3, "expected 3 batched requests, got {n}");
}

#[tokio::test]
async fn test_rest_secret_not_in_journal_on_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_string("server error"))
        .mount(&server)
        .await;

    let (_db_dir, db_path) = setup_test_db(create_test_table(), &insert_test_rows(2));
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);

    // Configure with a bearer token; force HTTP for the wiremock endpoint.
    let mut cfg = rest_dest_config(server.uri(), 100);
    if let DestinationConfig::Rest { auth, .. } = &mut cfg {
        *auth = Some(AuthConfig::Bearer {
            token: "BEARERSECRET-donotleak".to_string(),
        });
    }
    let dest = RestDestination::new(&cfg, "test_sync").unwrap();

    let sync_config = create_sync_config(
        "test_sync",
        SyncMode::Incremental,
        "SELECT id, name, value FROM test_table ORDER BY id",
    );

    let _ = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("Sync should not hard-fail");

    // The bearer token must never appear in the journal's last_error column.
    let conn = engine.state().get_conn().unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM row_journal WHERE sync_name = ? AND last_error LIKE ?",
            duckdb::params!["test_sync", "%BEARERSECRET-donotleak%"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 0, "bearer token leaked into row_journal.last_error");
}

#[tokio::test]
async fn test_rest_timeout_does_not_deadletter() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(3)))
        .mount(&server)
        .await;

    let (_db_dir, db_path) = setup_test_db(create_test_table(), &insert_test_rows(2));
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);

    // 1s timeout; server sleeps 3s.
    let mut cfg = rest_dest_config(server.uri(), 100);
    if let DestinationConfig::Rest { timeout_secs, .. } = &mut cfg {
        *timeout_secs = Some(1);
    }
    let dest = RestDestination::new(&cfg, "test_sync").unwrap();

    let sync_config = create_sync_config(
        "test_sync",
        SyncMode::Incremental,
        "SELECT id, name, value FROM test_table ORDER BY id",
    );

    let result = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("Sync should not hard-fail");
    // Transport errors are retryable (default), not dead-letter.
    assert_eq!(result.rows_synced, 0);
    assert!(result.rows_pending > 0 || result.rows_failed > 0);
    // No rows should be in dead status.
    let conn = engine.state().get_conn().unwrap();
    let dead: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM row_journal WHERE sync_name = ? AND status = 'dead'",
            duckdb::params!["test_sync"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(dead, 0, "timeout should not dead-letter rows");
}

#[tokio::test]
async fn test_rest_real_pks_in_journal_on_error() {
    // Assert that real PK values (pk-0000, ...) — not row indices — appear in
    // the row_journal when the REST destination returns an error. This
    // validates the WriteConfig.pk_col change end-to-end.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let (_db_dir, db_path) = setup_test_db(create_test_table(), &insert_test_rows(3));
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest = RestDestination::new(&rest_dest_config(server.uri(), 100), "test_sync").unwrap();

    let sync_config = create_sync_config(
        "test_sync",
        SyncMode::Incremental,
        "SELECT id, name, value FROM test_table ORDER BY id",
    );

    let _ = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("Sync should not hard-fail");

    // The journal must contain the real PK values, not row-index strings.
    let conn = engine.state().get_conn().unwrap();
    let pk: String = conn
        .query_row(
            "SELECT primary_key FROM row_journal WHERE sync_name = ? ORDER BY primary_key LIMIT 1",
            duckdb::params!["test_sync"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(pk, "pk-0000", "expected real PK in journal, got: {pk}");

    // Ensure row-index strings ("0", "1", "2") are NOT present.
    let idx_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM row_journal WHERE sync_name = ? AND primary_key IN ('0','1','2')",
            duckdb::params!["test_sync"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(idx_count, 0, "row-index PKs leaked into journal");
}

#[tokio::test]
async fn test_rest_retry_after_injection_not_honored() {
    // A malicious server puts "retry_after: 99999999999" in its 500 body.
    // The body's retry_after must be stripped; the pipeline must not honor it.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_string("retry_after: 99999999999"))
        .mount(&server)
        .await;

    let (_db_dir, db_path) = setup_test_db(create_test_table(), &insert_test_rows(2));
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest = RestDestination::new(&rest_dest_config(server.uri(), 100), "test_sync").unwrap();

    let sync_config = create_sync_config(
        "test_sync",
        SyncMode::Incremental,
        "SELECT id, name, value FROM test_table ORDER BY id",
    );

    let _ = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("Sync should not hard-fail");

    // The journal's last_error must NOT contain the injected retry_after.
    let conn = engine.state().get_conn().unwrap();
    let leaked: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM row_journal WHERE sync_name = ? AND last_error LIKE '%retry_after: 99999999999%'",
            duckdb::params!["test_sync"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(leaked, 0, "body-injected retry_after leaked into journal");
}

#[tokio::test]
async fn test_rest_secret_echo_not_in_journal() {
    // A misconfigured server echoes the request's bearer token (a short,
    // hyphenated token) in its 500 error body. The exact token must be
    // redacted from the journal's last_error.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_string("echo: Bearer short-tok-xyz"))
        .mount(&server)
        .await;

    let (_db_dir, db_path) = setup_test_db(create_test_table(), &insert_test_rows(2));
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);

    let mut cfg = rest_dest_config(server.uri(), 100);
    if let DestinationConfig::Rest { auth, .. } = &mut cfg {
        *auth = Some(AuthConfig::Bearer {
            token: "short-tok-xyz".to_string(),
        });
    }
    let dest = RestDestination::new(&cfg, "test_sync").unwrap();

    let sync_config = create_sync_config(
        "test_sync",
        SyncMode::Incremental,
        "SELECT id, name, value FROM test_table ORDER BY id",
    );

    let _ = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("Sync should not hard-fail");

    let conn = engine.state().get_conn().unwrap();
    let leaked: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM row_journal WHERE sync_name = ? AND last_error LIKE '%short-tok-xyz%'",
            duckdb::params!["test_sync"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(leaked, 0, "echoed bearer token leaked into journal");
}

/// Two-run round-trip against the same engine/state + a real `RestDestination`
/// backed by a wiremock server: first response 429 (with `Retry-After`) leaves
/// the exact PK rows `pending` (not synced); second response 200 resends them
/// and marks them `synced`, with no dead rows and no duplicate/incorrect PK
/// journal state.
///
/// This closes the last coverage gap flagged by the FERRY-4 review: every other
/// REST integration test calls `run_sync` exactly once, so the
/// pending→resync→synced round-trip was only proven against `MockDestination`
/// in `engine.rs`. Here we exercise it end-to-end through the real
/// `RestDestination` + wiremock.
///
/// Wiremock sequencing uses a single atomic-responder closure (compatible with
/// the pinned wiremock 0.6.2): the first request gets 429 + `Retry-After: 0`,
/// every subsequent request gets 200. `Retry-After: 0` is chosen so the
/// delivery pipeline's `sleep(0)` is a no-op, keeping the test fast. (Re-
/// delivery in run 2 actually flows through the CDC diff path, not the
/// pending-rows reconciliation path — see the inline comments below.)
#[tokio::test]
async fn test_rest_429_then_200_resync_round_trip() {
    let server = MockServer::start().await;

    // Deterministic atomic responder: 429 on the first request, 200 afterwards.
    // Captures every received request body for post-hoc payload assertions.
    let request_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let received_bodies: Arc<tokio::sync::Mutex<Vec<Vec<u8>>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let rc_clone = request_count.clone();
    let bodies_clone = received_bodies.clone();
    Mock::given(method("POST"))
        .respond_with(move |req: &wiremock::Request| {
            let n = rc_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            // Capture the body for later payload assertions regardless of the
            // response path. Try-lock avoids deadlocking the wiremock runtime.
            if let Ok(mut guard) = bodies_clone.try_lock() {
                guard.push(req.body.clone());
            }
            if n == 0 {
                // First request: rate-limited, Retry-After: 0 (fast no-op sleep).
                ResponseTemplate::new(429).insert_header("Retry-After", "0")
            } else {
                ResponseTemplate::new(200)
            }
        })
        .mount(&server)
        .await;

    const NUM_ROWS: usize = 5;
    let (_db_dir, db_path) = setup_test_db(create_test_table(), &insert_test_rows(NUM_ROWS));
    let db_path_str = db_path.to_str().unwrap();
    let (engine, _state_dir) = create_test_engine(db_path_str);
    let source = create_source(db_path_str);
    let dest = RestDestination::new(&rest_dest_config(server.uri(), 100), "test_sync").unwrap();

    let sync_config = create_sync_config(
        "test_sync",
        SyncMode::Incremental,
        "SELECT id, name, value FROM test_table ORDER BY id",
    );

    // ── Run 1: 429 → all rows pending ───────────────────────────────────
    let result1 = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("First sync (429) should not hard-fail");

    assert_eq!(
        result1.rows_synced, 0,
        "run 1 must not sync any rows on 429"
    );
    assert_eq!(
        result1.rows_pending, NUM_ROWS,
        "run 1 must leave all rows pending"
    );
    assert_eq!(
        result1.rows_dead, 0,
        "run 1 must not dead-letter retryable 429 rows"
    );

    let conn = engine.state().get_conn().unwrap();

    // Exactly the original PKs are pending; none are synced or dead.
    let pending: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM row_journal WHERE sync_name = ? AND status = 'pending'",
            duckdb::params!["test_sync"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        pending as usize, NUM_ROWS,
        "run 1: expected {NUM_ROWS} pending rows"
    );

    let synced: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM row_journal WHERE sync_name = ? AND status = 'synced'",
            duckdb::params!["test_sync"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(synced, 0, "run 1: no rows should be synced");

    let dead: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM row_journal WHERE sync_name = ? AND status = 'dead'",
            duckdb::params!["test_sync"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(dead, 0, "run 1: no rows should be dead-lettered");

    // The 429 must be recorded in last_error for every pending row.
    let with_429: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM row_journal
             WHERE sync_name = ? AND status = 'pending' AND last_error LIKE '%HTTP 429%'",
            duckdb::params!["test_sync"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        with_429 as usize, NUM_ROWS,
        "run 1: every pending row must carry HTTP 429 in last_error"
    );

    // No duplicate PK journal rows: the (sync_name, primary_key) UNIQUE/PK
    // constraint enforces this, but assert it explicitly for clarity.
    let distinct_pks: i64 = conn
        .query_row(
            "SELECT COUNT(DISTINCT primary_key) FROM row_journal WHERE sync_name = ?",
            duckdb::params!["test_sync"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        distinct_pks as usize, NUM_ROWS,
        "run 1: no duplicate PK journal rows"
    );

    // ── Run 2: 200 → pending rows resent and marked synced ──────────────
    // The pending rows are re-delivered via the CDC diff path (not the
    // pending-rows reconciliation path): run 1 did not commit CDC hashes
    // (delivery_succeeded was false), so the run-2 diff sees all NUM_ROWS rows
    // as "added/changed" again. The delivery pipeline's exactly-once
    // `filter_undelivered` passes them through because their journal status is
    // `pending` (not `synced`), so they get re-POSTed and marked `synced`.
    let result2 = run_sync(
        &engine,
        &sync_config,
        &source,
        &dest,
        &RunOptions::default(),
    )
    .await
    .expect("Second sync (200) should succeed");

    assert_eq!(
        result2.rows_synced, NUM_ROWS,
        "run 2 must sync the previously-pending rows"
    );
    assert_eq!(result2.rows_pending, 0, "run 2 must clear all pending rows");
    assert_eq!(result2.rows_dead, 0, "run 2 must not dead-letter any rows");

    let conn = engine.state().get_conn().unwrap();

    // All rows now synced; none pending or dead.
    let synced: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM row_journal WHERE sync_name = ? AND status = 'synced'",
            duckdb::params!["test_sync"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(synced as usize, NUM_ROWS, "run 2: all rows must be synced");

    let pending: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM row_journal WHERE sync_name = ? AND status = 'pending'",
            duckdb::params!["test_sync"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(pending, 0, "run 2: no rows must remain pending");

    let dead: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM row_journal WHERE sync_name = ? AND status = 'dead'",
            duckdb::params!["test_sync"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(dead, 0, "run 2: no rows must be dead-lettered");

    // mark_synced nulls last_error — assert no stale 429 lingers on synced rows.
    let stale_error: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM row_journal
             WHERE sync_name = ? AND status = 'synced' AND last_error IS NOT NULL",
            duckdb::params!["test_sync"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        stale_error, 0,
        "run 2: synced rows must not retain a stale last_error"
    );

    // Still exactly NUM_ROWS distinct PKs — no duplicates were inserted.
    let distinct_pks: i64 = conn
        .query_row(
            "SELECT COUNT(DISTINCT primary_key) FROM row_journal WHERE sync_name = ?",
            duckdb::params!["test_sync"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        distinct_pks as usize, NUM_ROWS,
        "run 2: no duplicate PK journal rows after round-trip"
    );

    // The synced PKs must be exactly the original pk-0000..pk-0004 set —
    // no incorrect/row-index PKs leaked in.
    let synced_pks: Vec<String> = {
        let mut stmt = conn
            .prepare(
                "SELECT primary_key FROM row_journal
                 WHERE sync_name = ? AND status = 'synced'
                 ORDER BY primary_key",
            )
            .unwrap();
        let rows = stmt
            .query_map(duckdb::params!["test_sync"], |row| row.get::<_, String>(0))
            .unwrap();
        rows.map(|r| r.unwrap()).collect()
    };
    let expected: Vec<String> = (0..NUM_ROWS).map(|i| format!("pk-{:04}", i)).collect();
    assert_eq!(
        synced_pks, expected,
        "run 2: synced PKs must be exactly the original PK set"
    );

    // ── Request count + payload assertions ──────────────────────────────
    // Two sync runs → two POSTs (one batch each; NUM_ROWS < max_batch_size).
    let total_requests = request_count.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        total_requests, 2,
        "expected exactly 2 POST requests across both runs, got {total_requests}"
    );

    let bodies = received_bodies.lock().await.clone();
    assert_eq!(
        bodies.len(),
        2,
        "captured body count must match request count"
    );

    // Both bodies are JSON arrays of row objects. Run 2's body must carry the
    // exact PK set, proving the pending rows were resent (not a fresh/empty
    // extract).
    let body2: serde_json::Value =
        serde_json::from_slice(&bodies[1]).expect("run 2 body must be valid JSON");
    let arr2 = body2
        .as_array()
        .expect("run 2 body must be a JSON array of row objects");
    assert_eq!(
        arr2.len(),
        NUM_ROWS,
        "run 2 body must contain all {NUM_ROWS} rows"
    );
    let ids2: Vec<String> = arr2
        .iter()
        .map(|row| v_to_string(row.get("id").unwrap_or(&serde_json::Value::Null)))
        .collect();
    assert_eq!(
        ids2, expected,
        "run 2 body PKs must match the original PK set"
    );

    // Run 1's body must also carry the same PKs (the first, failed attempt).
    let body1: serde_json::Value =
        serde_json::from_slice(&bodies[0]).expect("run 1 body must be valid JSON");
    let arr1 = body1
        .as_array()
        .expect("run 1 body must be a JSON array of row objects");
    assert_eq!(
        arr1.len(),
        NUM_ROWS,
        "run 1 body must contain all {NUM_ROWS} rows"
    );
}

/// Best-effort conversion of a `serde_json::Value` to a string for the PK
/// assertion above. The `id` column is `VARCHAR` in the test schema, so the
/// normal path is `as_str()`; this helper covers the unexpected-numeric case
/// without panicking.
fn v_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        _ => v.to_string(),
    }
}
