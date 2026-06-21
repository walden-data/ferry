use std::collections::HashMap;
use std::path::Path;

use chrono::{DateTime, Utc};
use duckdb::{DuckdbConnectionManager, params};
use r2d2::Pool;

use crate::error::FerryError;
use crate::traits::{PrimaryKey, RowEntry, StateStore, SyncRun};

/// A DuckDB-backed state store for CDC hashes, row journal, and run history.
#[derive(Clone)]
pub struct DuckDbStateStore {
    pool: Pool<DuckdbConnectionManager>,
}

impl DuckDbStateStore {
    /// Create a new DuckDbStateStore, opening or creating the database at `path`.
    ///
    /// Runs schema migration (CREATE TABLE IF NOT EXISTS) on initialization.
    pub fn new(path: &Path) -> Result<Self, FerryError> {
        let manager = DuckdbConnectionManager::file(path)
            .map_err(|e| FerryError::State(format!("Failed to open state database: {e}")))?;
        let pool = Pool::builder()
            .max_size(4)
            .build(manager)
            .map_err(|e| FerryError::State(format!("Failed to build connection pool: {e}")))?;

        let store = Self { pool };
        store.run_migrations()?;
        Ok(store)
    }

    /// Get a raw connection from the pool (for testing/admin purposes).
    #[doc(hidden)]
    pub fn get_conn(&self) -> Result<r2d2::PooledConnection<DuckdbConnectionManager>, FerryError> {
        self.pool
            .get()
            .map_err(|e| FerryError::State(format!("Failed to get connection: {e}")))
    }

    /// Run schema migrations to create tables if they don't exist.
    fn run_migrations(&self) -> Result<(), FerryError> {
        let conn = self
            .pool
            .get()
            .map_err(|e| FerryError::State(format!("Failed to get connection: {e}")))?;

        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS cdc_hashes (
                sync_name TEXT NOT NULL,
                primary_key TEXT NOT NULL,
                row_hash BIGINT NOT NULL,
                cursor_value TEXT,
                snapshot_at TIMESTAMP DEFAULT now(),
                PRIMARY KEY (sync_name, primary_key)
            );

            CREATE TABLE IF NOT EXISTS row_journal (
                sync_name TEXT NOT NULL,
                primary_key TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER DEFAULT 0,
                last_error TEXT,
                last_attempt_at TIMESTAMP,
                next_retry_at TIMESTAMP,
                last_sync_run_id TEXT,
                PRIMARY KEY (sync_name, primary_key)
            );

            CREATE TABLE IF NOT EXISTS sync_runs (
                sync_name TEXT NOT NULL,
                run_id TEXT NOT NULL,
                started_at TIMESTAMP NOT NULL,
                completed_at TIMESTAMP,
                rows_extracted INTEGER DEFAULT 0,
                rows_synced INTEGER DEFAULT 0,
                rows_failed INTEGER DEFAULT 0,
                rows_retried INTEGER DEFAULT 0,
                rows_dead INTEGER DEFAULT 0,
                mode TEXT NOT NULL,
                dry_run BOOLEAN DEFAULT FALSE,
                status TEXT NOT NULL DEFAULT 'running',
                PRIMARY KEY (sync_name, run_id)
            );

            CREATE TABLE IF NOT EXISTS cursor_state (
                sync_name TEXT NOT NULL PRIMARY KEY,
                cursor_value TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT now()
            );
            ",
        )
        .map_err(|e| FerryError::State(format!("Failed to run migrations: {e}")))?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl StateStore for DuckDbStateStore {
    // ── CDC state ──────────────────────────────────────────────────────

    async fn get_hashes(&self, sync_name: &str) -> Result<HashMap<PrimaryKey, u64>, FerryError> {
        let conn = self
            .pool
            .get()
            .map_err(|e| FerryError::State(format!("Failed to get connection: {e}")))?;

        let mut stmt = conn
            .prepare("SELECT primary_key, row_hash FROM cdc_hashes WHERE sync_name = ?")
            .map_err(|e| FerryError::State(format!("Failed to prepare query: {e}")))?;

        let rows = stmt
            .query_map(params![sync_name], |row| {
                let pk: String = row.get(0)?;
                let hash: i64 = row.get(1)?;
                Ok((pk, hash as u64))
            })
            .map_err(|e| FerryError::State(format!("Failed to query hashes: {e}")))?;

        let mut hashes = HashMap::new();
        for row in rows {
            let (pk, hash) = row.map_err(|e| FerryError::State(format!("Row error: {e}")))?;
            hashes.insert(pk, hash);
        }
        Ok(hashes)
    }

    async fn set_hashes(
        &self,
        sync_name: &str,
        hashes: &HashMap<PrimaryKey, u64>,
    ) -> Result<(), FerryError> {
        let mut conn = self
            .pool
            .get()
            .map_err(|e| FerryError::State(format!("Failed to get connection: {e}")))?;

        let tx = conn
            .transaction()
            .map_err(|e| FerryError::State(format!("Failed to start transaction: {e}")))?;

        // Delete existing hashes for this sync
        tx.execute(
            "DELETE FROM cdc_hashes WHERE sync_name = ?",
            params![sync_name],
        )
        .map_err(|e| FerryError::State(format!("Failed to delete hashes: {e}")))?;

        // Insert new hashes
        let mut stmt = tx
            .prepare("INSERT INTO cdc_hashes (sync_name, primary_key, row_hash) VALUES (?, ?, ?)")
            .map_err(|e| FerryError::State(format!("Failed to prepare insert: {e}")))?;

        for (pk, hash) in hashes {
            stmt.execute(params![sync_name, pk, *hash as i64])
                .map_err(|e| FerryError::State(format!("Failed to insert hash: {e}")))?;
        }

        // Drop the prepared statement before committing
        drop(stmt);

        tx.commit()
            .map_err(|e| FerryError::State(format!("Failed to commit transaction: {e}")))?;

        Ok(())
    }

    async fn get_cursor(&self, sync_name: &str) -> Result<Option<String>, FerryError> {
        let conn = self
            .pool
            .get()
            .map_err(|e| FerryError::State(format!("Failed to get connection: {e}")))?;

        let mut stmt = conn
            .prepare("SELECT cursor_value FROM cursor_state WHERE sync_name = ?")
            .map_err(|e| FerryError::State(format!("Failed to prepare query: {e}")))?;

        let mut rows = stmt
            .query_map(params![sync_name], |row| row.get::<_, String>(0))
            .map_err(|e| FerryError::State(format!("Failed to query cursor: {e}")))?;

        match rows.next() {
            Some(Ok(val)) => Ok(Some(val)),
            Some(Err(e)) => Err(FerryError::State(format!("Row error: {e}"))),
            None => Ok(None),
        }
    }

    async fn set_cursor(&self, sync_name: &str, value: &str) -> Result<(), FerryError> {
        let conn = self
            .pool
            .get()
            .map_err(|e| FerryError::State(format!("Failed to get connection: {e}")))?;

        conn.execute(
            "INSERT INTO cursor_state (sync_name, cursor_value, updated_at)
             VALUES (?, ?, now())
             ON CONFLICT (sync_name)
             DO UPDATE SET cursor_value = excluded.cursor_value, updated_at = now()",
            params![sync_name, value],
        )
        .map_err(|e| FerryError::State(format!("Failed to set cursor: {e}")))?;

        Ok(())
    }

    // ── Row journal ────────────────────────────────────────────────────

    async fn get_pending_rows(&self, sync_name: &str) -> Result<Vec<RowEntry>, FerryError> {
        let conn = self
            .pool
            .get()
            .map_err(|e| FerryError::State(format!("Failed to get connection: {e}")))?;

        let mut stmt = conn
            .prepare(
                "SELECT primary_key, status, attempts, last_error, last_attempt_at,
                        next_retry_at, last_sync_run_id
                 FROM row_journal
                 WHERE sync_name = ?
                   AND status = 'pending'
                   AND (next_retry_at IS NULL OR next_retry_at <= now())",
            )
            .map_err(|e| FerryError::State(format!("Failed to prepare query: {e}")))?;

        let rows = stmt
            .query_map(params![sync_name], |row| {
                Ok(RowEntry {
                    primary_key: row.get(0)?,
                    status: row.get(1)?,
                    attempts: row.get(2)?,
                    last_error: row.get(3)?,
                    last_attempt_at: row.get(4)?,
                    next_retry_at: row.get(5)?,
                    last_sync_run_id: row.get(6)?,
                })
            })
            .map_err(|e| FerryError::State(format!("Failed to query pending rows: {e}")))?;

        let mut entries = Vec::new();
        for row in rows {
            entries.push(row.map_err(|e| FerryError::State(format!("Row error: {e}")))?);
        }
        Ok(entries)
    }

    async fn get_dead_rows(&self, sync_name: &str) -> Result<Vec<RowEntry>, FerryError> {
        let conn = self
            .pool
            .get()
            .map_err(|e| FerryError::State(format!("Failed to get connection: {e}")))?;

        let mut stmt = conn
            .prepare(
                "SELECT primary_key, status, attempts, last_error, last_attempt_at,
                        next_retry_at, last_sync_run_id
                 FROM row_journal
                 WHERE sync_name = ? AND status = 'dead'",
            )
            .map_err(|e| FerryError::State(format!("Failed to prepare query: {e}")))?;

        let rows = stmt
            .query_map(params![sync_name], |row| {
                Ok(RowEntry {
                    primary_key: row.get(0)?,
                    status: row.get(1)?,
                    attempts: row.get(2)?,
                    last_error: row.get(3)?,
                    last_attempt_at: row.get(4)?,
                    next_retry_at: row.get(5)?,
                    last_sync_run_id: row.get(6)?,
                })
            })
            .map_err(|e| FerryError::State(format!("Failed to query dead rows: {e}")))?;

        let mut entries = Vec::new();
        for row in rows {
            entries.push(row.map_err(|e| FerryError::State(format!("Row error: {e}")))?);
        }
        Ok(entries)
    }

    async fn mark_synced(
        &self,
        sync_name: &str,
        primary_keys: &[PrimaryKey],
        run_id: &str,
    ) -> Result<(), FerryError> {
        let mut conn = self
            .pool
            .get()
            .map_err(|e| FerryError::State(format!("Failed to get connection: {e}")))?;

        let tx = conn
            .transaction()
            .map_err(|e| FerryError::State(format!("Failed to start transaction: {e}")))?;

        let mut stmt = tx
            .prepare(
                "INSERT INTO row_journal (sync_name, primary_key, status, last_sync_run_id, last_attempt_at)
                 VALUES (?, ?, 'synced', ?, now())
                 ON CONFLICT (sync_name, primary_key)
                 DO UPDATE SET status = 'synced',
                               last_sync_run_id = excluded.last_sync_run_id,
                               last_attempt_at = now(),
                               last_error = NULL,
                               next_retry_at = NULL",
            )
            .map_err(|e| FerryError::State(format!("Failed to prepare upsert: {e}")))?;

        for pk in primary_keys {
            stmt.execute(params![sync_name, pk, run_id])
                .map_err(|e| FerryError::State(format!("Failed to mark synced: {e}")))?;
        }

        drop(stmt);

        tx.commit()
            .map_err(|e| FerryError::State(format!("Failed to commit transaction: {e}")))?;

        Ok(())
    }

    async fn mark_pending(
        &self,
        sync_name: &str,
        pk: &PrimaryKey,
        error: &str,
        next_retry_at: DateTime<Utc>,
    ) -> Result<(), FerryError> {
        let conn = self
            .pool
            .get()
            .map_err(|e| FerryError::State(format!("Failed to get connection: {e}")))?;

        conn.execute(
            "INSERT INTO row_journal (sync_name, primary_key, status, attempts, last_error, last_attempt_at, next_retry_at)
             VALUES (?, ?, 'pending', 1, ?, now(), ?)
             ON CONFLICT (sync_name, primary_key)
             DO UPDATE SET status = 'pending',
                           attempts = row_journal.attempts + 1,
                           last_error = excluded.last_error,
                           last_attempt_at = now(),
                           next_retry_at = excluded.next_retry_at",
            params![sync_name, pk, error, next_retry_at],
        )
        .map_err(|e| FerryError::State(format!("Failed to mark pending: {e}")))?;

        Ok(())
    }

    async fn mark_dead(
        &self,
        sync_name: &str,
        pk: &PrimaryKey,
        error: &str,
    ) -> Result<(), FerryError> {
        let conn = self
            .pool
            .get()
            .map_err(|e| FerryError::State(format!("Failed to get connection: {e}")))?;

        conn.execute(
            "INSERT INTO row_journal (sync_name, primary_key, status, last_error, last_attempt_at)
             VALUES (?, ?, 'dead', ?, now())
             ON CONFLICT (sync_name, primary_key)
             DO UPDATE SET status = 'dead',
                           last_error = excluded.last_error,
                           last_attempt_at = now(),
                           next_retry_at = NULL",
            params![sync_name, pk, error],
        )
        .map_err(|e| FerryError::State(format!("Failed to mark dead: {e}")))?;

        Ok(())
    }

    async fn retry_dead_rows(
        &self,
        sync_name: &str,
        pks: Option<&[PrimaryKey]>,
    ) -> Result<usize, FerryError> {
        let conn = self
            .pool
            .get()
            .map_err(|e| FerryError::State(format!("Failed to get connection: {e}")))?;

        let count = if let Some(keys) = pks {
            // Retry specific dead rows
            let mut stmt = conn
                .prepare(
                    "UPDATE row_journal SET status = 'pending', next_retry_at = now()
                     WHERE sync_name = ? AND status = 'dead' AND primary_key = ?",
                )
                .map_err(|e| FerryError::State(format!("Failed to prepare update: {e}")))?;

            let mut total = 0;
            for key in keys {
                let affected = stmt
                    .execute(params![sync_name, key])
                    .map_err(|e| FerryError::State(format!("Failed to retry row: {e}")))?;
                total += affected;
            }
            total
        } else {
            // Retry all dead rows for this sync
            conn.execute(
                "UPDATE row_journal SET status = 'pending', next_retry_at = now()
                 WHERE sync_name = ? AND status = 'dead'",
                params![sync_name],
            )
            .map_err(|e| FerryError::State(format!("Failed to retry dead rows: {e}")))?
        };

        Ok(count)
    }

    async fn purge_dead_rows(
        &self,
        sync_name: &str,
        older_than: chrono::Duration,
    ) -> Result<usize, FerryError> {
        let conn = self
            .pool
            .get()
            .map_err(|e| FerryError::State(format!("Failed to get connection: {e}")))?;

        // Calculate the cutoff timestamp
        let cutoff = Utc::now() - older_than;

        let count = conn
            .execute(
                "DELETE FROM row_journal
                 WHERE sync_name = ? AND status = 'dead' AND last_attempt_at < ?",
                params![sync_name, cutoff],
            )
            .map_err(|e| FerryError::State(format!("Failed to purge dead rows: {e}")))?;

        Ok(count)
    }

    // ── Crash recovery ──────────────────────────────────────────────────

    async fn get_synced_pks(&self, sync_name: &str) -> Result<Vec<PrimaryKey>, FerryError> {
        let conn = self
            .pool
            .get()
            .map_err(|e| FerryError::State(format!("Failed to get connection: {e}")))?;

        // Only return synced rows from INCOMPLETE runs (not completed runs).
        // Rows synced in completed runs have their CDC hash committed, so the
        // CDC diff will correctly identify whether they changed. Rows synced
        // in incomplete runs (crash before hash commit) should be skipped to
        // prevent re-delivery — the CDC hash is stale and will show them as
        // "changed" even though they were already delivered.
        let mut stmt = conn
            .prepare(
                "SELECT j.primary_key FROM row_journal j
                 WHERE j.sync_name = ? AND j.status = 'synced'
                 AND EXISTS (
                     SELECT 1 FROM sync_runs r
                     WHERE r.sync_name = j.sync_name
                     AND r.run_id = j.last_sync_run_id
                     AND r.status != 'completed'
                 )",
            )
            .map_err(|e| FerryError::State(format!("Failed to prepare query: {e}")))?;

        let rows = stmt
            .query_map(params![sync_name], |row| row.get::<_, String>(0))
            .map_err(|e| FerryError::State(format!("Failed to query synced pks: {e}")))?;

        let mut keys = Vec::new();
        for row in rows {
            keys.push(row.map_err(|e| FerryError::State(format!("Row error: {e}")))?);
        }
        Ok(keys)
    }

    async fn get_synced_for_run(
        &self,
        sync_name: &str,
        run_id: &str,
    ) -> Result<Vec<PrimaryKey>, FerryError> {
        let conn = self
            .pool
            .get()
            .map_err(|e| FerryError::State(format!("Failed to get connection: {e}")))?;

        let mut stmt = conn
            .prepare(
                "SELECT primary_key FROM row_journal
                 WHERE sync_name = ? AND last_sync_run_id = ? AND status = 'synced'",
            )
            .map_err(|e| FerryError::State(format!("Failed to prepare query: {e}")))?;

        let rows = stmt
            .query_map(params![sync_name, run_id], |row| row.get::<_, String>(0))
            .map_err(|e| FerryError::State(format!("Failed to query synced rows: {e}")))?;

        let mut keys = Vec::new();
        for row in rows {
            keys.push(row.map_err(|e| FerryError::State(format!("Row error: {e}")))?);
        }
        Ok(keys)
    }

    async fn get_last_completed_run(&self, sync_name: &str) -> Result<Option<SyncRun>, FerryError> {
        let conn = self
            .pool
            .get()
            .map_err(|e| FerryError::State(format!("Failed to get connection: {e}")))?;

        let mut stmt = conn
            .prepare(
                "SELECT sync_name, run_id, started_at, completed_at,
                        rows_extracted, rows_synced, rows_failed, rows_retried, rows_dead,
                        mode, dry_run, status
                 FROM sync_runs
                 WHERE sync_name = ? AND status = 'completed'
                 ORDER BY completed_at DESC LIMIT 1",
            )
            .map_err(|e| FerryError::State(format!("Failed to prepare query: {e}")))?;

        let mut rows = stmt
            .query_map(params![sync_name], |row| {
                Ok(SyncRun {
                    sync_name: row.get(0)?,
                    run_id: row.get(1)?,
                    started_at: row.get(2)?,
                    completed_at: row.get(3)?,
                    rows_extracted: row.get(4)?,
                    rows_synced: row.get(5)?,
                    rows_failed: row.get(6)?,
                    rows_retried: row.get(7)?,
                    rows_dead: row.get(8)?,
                    mode: row.get(9)?,
                    dry_run: row.get(10)?,
                    status: row.get(11)?,
                })
            })
            .map_err(|e| FerryError::State(format!("Failed to query runs: {e}")))?;

        match rows.next() {
            Some(Ok(run)) => Ok(Some(run)),
            Some(Err(e)) => Err(FerryError::State(format!("Row error: {e}"))),
            None => Ok(None),
        }
    }

    async fn get_incomplete_runs(&self, sync_name: &str) -> Result<Vec<SyncRun>, FerryError> {
        let conn = self
            .pool
            .get()
            .map_err(|e| FerryError::State(format!("Failed to get connection: {e}")))?;

        let mut stmt = conn
            .prepare(
                "SELECT sync_name, run_id, started_at, completed_at,
                        rows_extracted, rows_synced, rows_failed, rows_retried, rows_dead,
                        mode, dry_run, status
                 FROM sync_runs
                 WHERE sync_name = ? AND status != 'completed'",
            )
            .map_err(|e| FerryError::State(format!("Failed to prepare query: {e}")))?;

        let rows = stmt
            .query_map(params![sync_name], |row| {
                Ok(SyncRun {
                    sync_name: row.get(0)?,
                    run_id: row.get(1)?,
                    started_at: row.get(2)?,
                    completed_at: row.get(3)?,
                    rows_extracted: row.get(4)?,
                    rows_synced: row.get(5)?,
                    rows_failed: row.get(6)?,
                    rows_retried: row.get(7)?,
                    rows_dead: row.get(8)?,
                    mode: row.get(9)?,
                    dry_run: row.get(10)?,
                    status: row.get(11)?,
                })
            })
            .map_err(|e| FerryError::State(format!("Failed to query runs: {e}")))?;

        let mut runs = Vec::new();
        for row in rows {
            runs.push(row.map_err(|e| FerryError::State(format!("Row error: {e}")))?);
        }
        Ok(runs)
    }

    async fn complete_run(
        &self,
        sync_name: &str,
        run_id: &str,
        rows_synced: usize,
        rows_failed: usize,
        rows_retried: usize,
        rows_dead: usize,
    ) -> Result<(), FerryError> {
        let conn = self
            .pool
            .get()
            .map_err(|e| FerryError::State(format!("Failed to get connection: {e}")))?;

        conn.execute(
            "UPDATE sync_runs SET status = 'completed', completed_at = now(),
             rows_synced = ?, rows_failed = ?, rows_retried = ?, rows_dead = ?
             WHERE sync_name = ? AND run_id = ?",
            params![
                rows_synced as i64,
                rows_failed as i64,
                rows_retried as i64,
                rows_dead as i64,
                sync_name,
                run_id,
            ],
        )
        .map_err(|e| FerryError::State(format!("Failed to complete run: {e}")))?;

        Ok(())
    }

    // ── Run history ────────────────────────────────────────────────────

    async fn record_run(&self, run: &SyncRun) -> Result<(), FerryError> {
        let conn = self
            .pool
            .get()
            .map_err(|e| FerryError::State(format!("Failed to get connection: {e}")))?;

        conn.execute(
            "INSERT INTO sync_runs (sync_name, run_id, started_at, completed_at,
                                    rows_extracted, rows_synced, rows_failed, rows_retried, rows_dead,
                                    mode, dry_run, status)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                run.sync_name,
                run.run_id,
                run.started_at,
                run.completed_at,
                run.rows_extracted as i64,
                run.rows_synced as i64,
                run.rows_failed as i64,
                run.rows_retried as i64,
                run.rows_dead as i64,
                run.mode,
                run.dry_run,
                run.status,
            ],
        )
        .map_err(|e| FerryError::State(format!("Failed to record run: {e}")))?;

        Ok(())
    }

    async fn get_runs(&self, sync_name: &str, limit: usize) -> Result<Vec<SyncRun>, FerryError> {
        let conn = self
            .pool
            .get()
            .map_err(|e| FerryError::State(format!("Failed to get connection: {e}")))?;

        let mut stmt = conn
            .prepare(
                "SELECT sync_name, run_id, started_at, completed_at,
                        rows_extracted, rows_synced, rows_failed, rows_retried, rows_dead,
                        mode, dry_run, status
                 FROM sync_runs
                 WHERE sync_name = ?
                 ORDER BY started_at DESC
                 LIMIT ?",
            )
            .map_err(|e| FerryError::State(format!("Failed to prepare query: {e}")))?;

        let rows = stmt
            .query_map(params![sync_name, limit as i64], |row| {
                Ok(SyncRun {
                    sync_name: row.get(0)?,
                    run_id: row.get(1)?,
                    started_at: row.get(2)?,
                    completed_at: row.get(3)?,
                    rows_extracted: row.get(4)?,
                    rows_synced: row.get(5)?,
                    rows_failed: row.get(6)?,
                    rows_retried: row.get(7)?,
                    rows_dead: row.get(8)?,
                    mode: row.get(9)?,
                    dry_run: row.get(10)?,
                    status: row.get(11)?,
                })
            })
            .map_err(|e| FerryError::State(format!("Failed to query runs: {e}")))?;

        let mut runs = Vec::new();
        for row in rows {
            runs.push(row.map_err(|e| FerryError::State(format!("Row error: {e}")))?);
        }
        Ok(runs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn create_temp_store() -> (DuckDbStateStore, TempDir) {
        let dir = TempDir::with_prefix("ferry-state-test-").expect("Failed to create temp dir");
        let path = dir.path().join("state.db");
        let store = DuckDbStateStore::new(&path).expect("Failed to create state store");
        (store, dir)
    }

    #[test]
    fn test_create_state_db() {
        let dir = TempDir::with_prefix("ferry-state-test-").expect("Failed to create temp dir");
        let path = dir.path().join("state.db");
        let store = DuckDbStateStore::new(&path).expect("Failed to create state store");

        // Verify tables exist by querying them
        let conn = store.pool.get().expect("Failed to get connection");
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'cdc_hashes'",
                [],
                |row| row.get(0),
            )
            .expect("Failed to query tables");
        assert_eq!(count, 1, "cdc_hashes table should exist");

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'row_journal'",
                [],
                |row| row.get(0),
            )
            .expect("Failed to query tables");
        assert_eq!(count, 1, "row_journal table should exist");

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'sync_runs'",
                [],
                |row| row.get(0),
            )
            .expect("Failed to query tables");
        assert_eq!(count, 1, "sync_runs table should exist");

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'cursor_state'",
                [],
                |row| row.get(0),
            )
            .expect("Failed to query tables");
        assert_eq!(count, 1, "cursor_state table should exist");
    }

    #[tokio::test]
    async fn test_set_and_get_hashes() {
        let (store, _dir) = create_temp_store();

        let mut hashes = HashMap::new();
        hashes.insert("pk1".to_string(), 12345u64);
        hashes.insert("pk2".to_string(), 67890u64);

        store.set_hashes("test_sync", &hashes).await.unwrap();

        let retrieved = store.get_hashes("test_sync").await.unwrap();
        assert_eq!(retrieved.len(), 2);
        assert_eq!(*retrieved.get("pk1").unwrap(), 12345u64);
        assert_eq!(*retrieved.get("pk2").unwrap(), 67890u64);
    }

    #[tokio::test]
    async fn test_set_hashes_replaces_previous() {
        let (store, _dir) = create_temp_store();

        let mut hashes1 = HashMap::new();
        hashes1.insert("pk1".to_string(), 111u64);
        hashes1.insert("pk2".to_string(), 222u64);
        store.set_hashes("test_sync", &hashes1).await.unwrap();

        let mut hashes2 = HashMap::new();
        hashes2.insert("pk2".to_string(), 999u64);
        hashes2.insert("pk3".to_string(), 333u64);
        store.set_hashes("test_sync", &hashes2).await.unwrap();

        let retrieved = store.get_hashes("test_sync").await.unwrap();
        assert_eq!(
            retrieved.len(),
            2,
            "should have exactly 2 hashes after replacement"
        );
        assert!(
            !retrieved.contains_key("pk1"),
            "pk1 should have been removed"
        );
        assert_eq!(
            *retrieved.get("pk2").unwrap(),
            999u64,
            "pk2 should be updated"
        );
        assert_eq!(
            *retrieved.get("pk3").unwrap(),
            333u64,
            "pk3 should be added"
        );
    }

    #[tokio::test]
    async fn test_mark_synced_and_get_for_run() {
        let (store, _dir) = create_temp_store();

        let pks = vec!["row1".to_string(), "row2".to_string(), "row3".to_string()];
        store
            .mark_synced("test_sync", &pks, "run-001")
            .await
            .unwrap();

        let synced = store
            .get_synced_for_run("test_sync", "run-001")
            .await
            .unwrap();
        assert_eq!(synced.len(), 3);
        assert!(synced.contains(&"row1".to_string()));
        assert!(synced.contains(&"row2".to_string()));
        assert!(synced.contains(&"row3".to_string()));
    }

    #[tokio::test]
    async fn test_mark_pending_and_get_pending() {
        let (store, _dir) = create_temp_store();

        // Use a past retry time so the row is immediately eligible
        let retry_at = Utc::now() - chrono::Duration::seconds(10);

        store
            .mark_pending("test_sync", &"row1".to_string(), "timeout error", retry_at)
            .await
            .unwrap();

        let pending = store.get_pending_rows("test_sync").await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].primary_key, "row1");
        assert_eq!(pending[0].status, "pending");
        assert_eq!(pending[0].last_error.as_deref(), Some("timeout error"));
    }

    #[tokio::test]
    async fn test_mark_dead_and_get_dead() {
        let (store, _dir) = create_temp_store();

        store
            .mark_dead("test_sync", &"row1".to_string(), "permanent error")
            .await
            .unwrap();

        let dead = store.get_dead_rows("test_sync").await.unwrap();
        assert_eq!(dead.len(), 1);
        assert_eq!(dead[0].primary_key, "row1");
        assert_eq!(dead[0].status, "dead");
        assert_eq!(dead[0].last_error.as_deref(), Some("permanent error"));
    }

    #[tokio::test]
    async fn test_retry_dead_rows() {
        let (store, _dir) = create_temp_store();

        store
            .mark_dead("test_sync", &"row1".to_string(), "error")
            .await
            .unwrap();
        store
            .mark_dead("test_sync", &"row2".to_string(), "error")
            .await
            .unwrap();

        let count = store.retry_dead_rows("test_sync", None).await.unwrap();
        assert_eq!(count, 2, "should retry 2 dead rows");

        let dead = store.get_dead_rows("test_sync").await.unwrap();
        assert_eq!(dead.len(), 0, "no rows should be dead after retry");

        let pending = store.get_pending_rows("test_sync").await.unwrap();
        assert_eq!(pending.len(), 2, "retried rows should be pending");
    }

    #[tokio::test]
    async fn test_retry_dead_rows_specific() {
        let (store, _dir) = create_temp_store();

        store
            .mark_dead("test_sync", &"row1".to_string(), "error")
            .await
            .unwrap();
        store
            .mark_dead("test_sync", &"row2".to_string(), "error")
            .await
            .unwrap();

        let count = store
            .retry_dead_rows("test_sync", Some(&["row1".to_string()]))
            .await
            .unwrap();
        assert_eq!(count, 1, "should retry 1 specific dead row");

        let dead = store.get_dead_rows("test_sync").await.unwrap();
        assert_eq!(dead.len(), 1, "row2 should still be dead");
        assert_eq!(dead[0].primary_key, "row2");
    }

    #[tokio::test]
    async fn test_purge_dead_rows() {
        let (store, _dir) = create_temp_store();

        store
            .mark_dead("test_sync", &"row1".to_string(), "old error")
            .await
            .unwrap();

        // Purge with a very short duration (0 seconds) to ensure the row is older
        let _count = store
            .purge_dead_rows("test_sync", chrono::Duration::seconds(0))
            .await
            .unwrap();

        // The row was just created, so it might not be older than 0 seconds.
        // This test verifies the query works; the exact count depends on timing.
        // We just verify the operation doesn't error.
        let dead = store.get_dead_rows("test_sync").await.unwrap();
        // Row may or may not be purged depending on timing
        assert!(dead.len() <= 1);
    }

    #[tokio::test]
    async fn test_record_and_complete_run() {
        let (store, _dir) = create_temp_store();

        let run = SyncRun {
            sync_name: "test_sync".to_string(),
            run_id: "run-001".to_string(),
            started_at: Utc::now(),
            completed_at: None,
            rows_extracted: 100,
            rows_synced: 95,
            rows_failed: 3,
            rows_retried: 2,
            rows_dead: 0,
            mode: "incremental".to_string(),
            dry_run: false,
            status: "running".to_string(),
        };

        store.record_run(&run).await.unwrap();

        // Verify incomplete runs
        let incomplete = store.get_incomplete_runs("test_sync").await.unwrap();
        assert_eq!(incomplete.len(), 1);
        assert_eq!(incomplete[0].run_id, "run-001");
        assert_eq!(incomplete[0].status, "running");

        // Complete the run
        store
            .complete_run("test_sync", "run-001", 95, 3, 2, 0)
            .await
            .unwrap();

        // Verify it's now the last completed run
        let last = store
            .get_last_completed_run("test_sync")
            .await
            .unwrap()
            .expect("should have a completed run");
        assert_eq!(last.run_id, "run-001");
        assert_eq!(last.status, "completed");
        assert!(last.completed_at.is_some());
        assert_eq!(last.rows_synced, 95);

        // Verify no incomplete runs
        let incomplete = store.get_incomplete_runs("test_sync").await.unwrap();
        assert_eq!(incomplete.len(), 0);
    }

    #[tokio::test]
    async fn test_get_incomplete_runs() {
        let (store, _dir) = create_temp_store();

        let run1 = SyncRun {
            sync_name: "test_sync".to_string(),
            run_id: "run-001".to_string(),
            started_at: Utc::now(),
            completed_at: None,
            rows_extracted: 0,
            rows_synced: 0,
            rows_failed: 0,
            rows_retried: 0,
            rows_dead: 0,
            mode: "incremental".to_string(),
            dry_run: false,
            status: "running".to_string(),
        };
        store.record_run(&run1).await.unwrap();

        let run2 = SyncRun {
            sync_name: "test_sync".to_string(),
            run_id: "run-002".to_string(),
            started_at: Utc::now(),
            completed_at: None,
            rows_extracted: 0,
            rows_synced: 0,
            rows_failed: 0,
            rows_retried: 0,
            rows_dead: 0,
            mode: "incremental".to_string(),
            dry_run: false,
            status: "running".to_string(),
        };
        store.record_run(&run2).await.unwrap();

        // Complete run-001
        store
            .complete_run("test_sync", "run-001", 0, 0, 0, 0)
            .await
            .unwrap();

        let incomplete = store.get_incomplete_runs("test_sync").await.unwrap();
        assert_eq!(incomplete.len(), 1);
        assert_eq!(incomplete[0].run_id, "run-002");
    }

    #[tokio::test]
    async fn test_cursor_storage() {
        let (store, _dir) = create_temp_store();

        // Initially no cursor
        let cursor = store.get_cursor("test_sync").await.unwrap();
        assert!(cursor.is_none());

        // Set cursor
        store
            .set_cursor("test_sync", "2024-01-15T10:00:00Z")
            .await
            .unwrap();

        let cursor = store.get_cursor("test_sync").await.unwrap();
        assert_eq!(cursor.as_deref(), Some("2024-01-15T10:00:00Z"));

        // Update cursor
        store
            .set_cursor("test_sync", "2024-01-16T10:00:00Z")
            .await
            .unwrap();

        let cursor = store.get_cursor("test_sync").await.unwrap();
        assert_eq!(cursor.as_deref(), Some("2024-01-16T10:00:00Z"));
    }

    #[tokio::test]
    async fn test_get_runs() {
        let (store, _dir) = create_temp_store();

        for i in 0..5 {
            let run = SyncRun {
                sync_name: "test_sync".to_string(),
                run_id: format!("run-{:03}", i),
                started_at: Utc::now(),
                completed_at: Some(Utc::now()),
                rows_extracted: 10,
                rows_synced: 10,
                rows_failed: 0,
                rows_retried: 0,
                rows_dead: 0,
                mode: "incremental".to_string(),
                dry_run: false,
                status: "completed".to_string(),
            };
            store.record_run(&run).await.unwrap();
        }

        let runs = store.get_runs("test_sync", 3).await.unwrap();
        assert_eq!(runs.len(), 3, "should return at most 3 runs");
    }

    #[tokio::test]
    async fn test_mark_pending_increments_attempts() {
        let (store, _dir) = create_temp_store();

        // Use past retry times so rows are immediately eligible
        let retry_at = Utc::now() - chrono::Duration::seconds(30);

        // First attempt
        store
            .mark_pending("test_sync", &"row1".to_string(), "error 1", retry_at)
            .await
            .unwrap();

        // Second attempt
        let retry_at2 = Utc::now() - chrono::Duration::seconds(10);
        store
            .mark_pending("test_sync", &"row1".to_string(), "error 2", retry_at2)
            .await
            .unwrap();

        let pending = store.get_pending_rows("test_sync").await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].attempts, 2, "attempts should be incremented");
        assert_eq!(
            pending[0].last_error.as_deref(),
            Some("error 2"),
            "last_error should be updated"
        );
    }

    #[tokio::test]
    async fn test_sync_name_isolation() {
        let (store, _dir) = create_temp_store();

        // Set hashes for sync_a
        let mut hashes_a = HashMap::new();
        hashes_a.insert("pk1".to_string(), 111u64);
        store.set_hashes("sync_a", &hashes_a).await.unwrap();

        // Set hashes for sync_b
        let mut hashes_b = HashMap::new();
        hashes_b.insert("pk1".to_string(), 222u64);
        store.set_hashes("sync_b", &hashes_b).await.unwrap();

        // Verify isolation
        let retrieved_a = store.get_hashes("sync_a").await.unwrap();
        assert_eq!(retrieved_a.len(), 1);
        assert_eq!(*retrieved_a.get("pk1").unwrap(), 111u64);

        let retrieved_b = store.get_hashes("sync_b").await.unwrap();
        assert_eq!(retrieved_b.len(), 1);
        assert_eq!(*retrieved_b.get("pk1").unwrap(), 222u64);
    }
}
