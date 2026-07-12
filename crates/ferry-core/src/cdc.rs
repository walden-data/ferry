use std::collections::HashMap;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Float64Array, Int32Array, Int64Array,
    LargeStringArray, RecordBatch, StringArray,
};
use arrow_schema::DataType;
use xxhash_rust::xxh3::xxh3_64;

use crate::error::FerryError;
use crate::traits::{PrimaryKey, StateStore};

// ---------------------------------------------------------------------------
// CursorDiffResult
// ---------------------------------------------------------------------------

/// Result of a cursor-based CDC diff.
#[derive(Debug, Clone)]
pub struct CursorDiffResult {
    /// Row indices (global across all batches) that are new or updated.
    pub new_rows: Vec<usize>,
    /// The new cursor value (max value seen across all rows).
    pub new_cursor_value: String,
}

// ---------------------------------------------------------------------------
// Hash functions
// ---------------------------------------------------------------------------

/// Compute the xxh3_64 hash for a single row in a RecordBatch.
///
/// Hashes the specified columns in schema order. If `hash_columns` is empty,
/// all columns are hashed. Each column value is prefixed with a null-indicator
/// byte (`\x00` for null, `\x01` for non-null) to ensure that `NULL` and empty
/// string `""` produce different hashes.
pub fn hash_row(
    batch: &RecordBatch,
    row_idx: usize,
    hash_columns: &[String],
) -> Result<u64, FerryError> {
    let mut hasher_input = Vec::new();
    let schema = batch.schema();

    let columns_to_hash: Vec<usize> = if hash_columns.is_empty() {
        // Hash all columns in schema order
        (0..schema.fields().len()).collect()
    } else {
        hash_columns
            .iter()
            .map(|name| {
                schema
                    .index_of(name)
                    .map_err(|_| FerryError::Cdc(format!("Column '{}' not found in batch", name)))
            })
            .collect::<Result<Vec<_>, _>>()?
    };

    for &col_idx in &columns_to_hash {
        let col = batch.column(col_idx);

        if col.is_null(row_idx) {
            hasher_input.push(0x00); // null indicator
        } else {
            hasher_input.push(0x01); // non-null indicator
            append_column_bytes(&mut hasher_input, col, row_idx, col.data_type())?;
        }
    }

    Ok(xxh3_64(&hasher_input))
}

/// Append the bytes of a column value at a given row index to the buffer.
///
/// Type-dispatched extraction: each Arrow data type is converted to a byte
/// representation suitable for hashing.
fn append_column_bytes(
    buf: &mut Vec<u8>,
    col: &ArrayRef,
    row_idx: usize,
    dt: &DataType,
) -> Result<(), FerryError> {
    match dt {
        DataType::Int32 => {
            let arr = col
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| FerryError::Cdc("Failed to downcast Int32Array".into()))?;
            buf.extend_from_slice(&arr.value(row_idx).to_le_bytes());
        }
        DataType::Int64 => {
            let arr = col
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| FerryError::Cdc("Failed to downcast Int64Array".into()))?;
            buf.extend_from_slice(&arr.value(row_idx).to_le_bytes());
        }
        DataType::Float64 => {
            let arr = col
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| FerryError::Cdc("Failed to downcast Float64Array".into()))?;
            buf.extend_from_slice(&arr.value(row_idx).to_le_bytes());
        }
        DataType::Utf8 => {
            let arr = col
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| FerryError::Cdc("Failed to downcast StringArray".into()))?;
            buf.extend_from_slice(arr.value(row_idx).as_bytes());
        }
        DataType::LargeUtf8 => {
            let arr = col
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| FerryError::Cdc("Failed to downcast LargeStringArray".into()))?;
            buf.extend_from_slice(arr.value(row_idx).as_bytes());
        }
        DataType::Boolean => {
            let arr = col
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| FerryError::Cdc("Failed to downcast BooleanArray".into()))?;
            buf.push(if arr.value(row_idx) { 1u8 } else { 0u8 });
        }
        DataType::Date32 => {
            let arr = col
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| FerryError::Cdc("Failed to downcast Date32Array".into()))?;
            buf.extend_from_slice(&arr.value(row_idx).to_le_bytes());
        }
        DataType::Date64 => {
            let arr = col
                .as_any()
                .downcast_ref::<Date64Array>()
                .ok_or_else(|| FerryError::Cdc("Failed to downcast Date64Array".into()))?;
            buf.extend_from_slice(&arr.value(row_idx).to_le_bytes());
        }
        DataType::Timestamp(_, _) => {
            // All timestamp variants store i64 internally; access raw buffer
            let data = col.to_data();
            let buffer = data
                .buffers()
                .first()
                .ok_or_else(|| FerryError::Cdc("Timestamp array has no data buffer".into()))?;
            let offset = row_idx * 8;
            let slice = buffer
                .get(offset..offset + 8)
                .ok_or_else(|| FerryError::Cdc("Timestamp array row index out of bounds".into()))?;
            buf.extend_from_slice(slice);
        }
        _ => {
            // Fallback: convert to Debug string representation
            let display_val = format!("{:?}", col);
            buf.extend_from_slice(display_val.as_bytes());
        }
    }
    Ok(())
}

/// Extract the primary key value from a row as a string.
///
/// The primary key column must not be null. Supports Int32, Int64, Utf8,
/// and LargeUtf8 types with a Debug fallback for other types.
pub fn extract_primary_key(
    batch: &RecordBatch,
    row_idx: usize,
    pk_col: &str,
) -> Result<PrimaryKey, FerryError> {
    let schema = batch.schema();
    let col_idx = schema.index_of(pk_col).map_err(|_| {
        FerryError::Cdc(format!(
            "Primary key column '{}' not found in batch",
            pk_col
        ))
    })?;
    let col = batch.column(col_idx);

    if col.is_null(row_idx) {
        return Err(FerryError::Cdc(format!(
            "Primary key value is null at row {}",
            row_idx
        )));
    }

    match col.data_type() {
        DataType::Int32 => {
            let arr = col
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| FerryError::Cdc("Failed to downcast Int32Array".into()))?;
            Ok(arr.value(row_idx).to_string())
        }
        DataType::Int64 => {
            let arr = col
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| FerryError::Cdc("Failed to downcast Int64Array".into()))?;
            Ok(arr.value(row_idx).to_string())
        }
        DataType::Utf8 => {
            let arr = col
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| FerryError::Cdc("Failed to downcast StringArray".into()))?;
            Ok(arr.value(row_idx).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = col
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| FerryError::Cdc("Failed to downcast LargeStringArray".into()))?;
            Ok(arr.value(row_idx).to_string())
        }
        _ => {
            // Fallback: use Debug representation
            Ok(format!("{:?}", col))
        }
    }
}

/// Hash all rows across multiple RecordBatches, keyed by primary key.
///
/// For each row in each batch, extracts the primary key and computes the
/// xxh3_64 hash of the specified columns. Returns a map from primary key
/// to hash value.
pub fn hash_record_batches(
    batches: &[RecordBatch],
    pk_col: &str,
    hash_columns: &[String],
) -> Result<HashMap<PrimaryKey, u64>, FerryError> {
    let mut hashes = HashMap::new();

    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let pk = extract_primary_key(batch, row_idx, pk_col)?;
            let hash = hash_row(batch, row_idx, hash_columns)?;
            hashes.insert(pk, hash);
        }
    }

    Ok(hashes)
}

// ---------------------------------------------------------------------------
// HashCdc
// ---------------------------------------------------------------------------

/// Hash-based CDC engine.
///
/// Computes xxh3_64 hashes per row and diffs against stored state to
/// identify added, changed, and removed rows.
pub struct HashCdc<'a> {
    state: &'a dyn StateStore,
}

impl<'a> HashCdc<'a> {
    /// Create a new `HashCdc` engine backed by the given state store.
    pub fn new(state: &'a dyn StateStore) -> Self {
        Self { state }
    }

    /// Compute the diff between current data and stored state.
    ///
    /// Loads previous hashes from the state store, computes current hashes
    /// for all rows, and returns the set of added, changed, and removed
    /// primary keys along with the current hash map.
    pub async fn compute_diff(
        &self,
        sync_name: &str,
        current_batches: &[RecordBatch],
        pk_col: &str,
        hash_columns: &[String],
    ) -> Result<crate::traits::DiffResult, FerryError> {
        // Load previous hashes from state store
        let previous_hashes = self.state.get_hashes(sync_name).await?;

        // Compute current hashes
        let current_hashes = hash_record_batches(current_batches, pk_col, hash_columns)?;

        // Diff: added, changed, removed
        let mut added = Vec::new();
        let mut changed = Vec::new();
        let mut removed = Vec::new();

        for (pk, current_hash) in &current_hashes {
            match previous_hashes.get(pk) {
                None => added.push(pk.clone()),
                Some(prev_hash) if *prev_hash != *current_hash => changed.push(pk.clone()),
                _ => {} // unchanged
            }
        }

        for pk in previous_hashes.keys() {
            if !current_hashes.contains_key(pk) {
                removed.push(pk.clone());
            }
        }

        Ok(crate::traits::DiffResult {
            added,
            changed,
            removed,
            current_hashes,
        })
    }
}

// ---------------------------------------------------------------------------
// CursorCdc
// ---------------------------------------------------------------------------

/// Cursor-based CDC engine.
///
/// Filters rows where the cursor field value exceeds the last stored cursor
/// value, returning only new/updated rows and the new maximum cursor value.
pub struct CursorCdc<'a> {
    state: &'a dyn StateStore,
}

impl<'a> CursorCdc<'a> {
    /// Create a new `CursorCdc` engine backed by the given state store.
    pub fn new(state: &'a dyn StateStore) -> Self {
        Self { state }
    }

    /// Compute the diff using cursor-based filtering.
    ///
    /// Loads the last cursor value from the state store, filters rows where
    /// the cursor field value is greater than the last cursor, and returns
    /// the new row indices (global across all batches) and the new maximum
    /// cursor value.
    ///
    /// On the first run (no previous cursor), all rows are included.
    pub async fn compute_diff(
        &self,
        sync_name: &str,
        current_batches: &[RecordBatch],
        cursor_field: &str,
    ) -> Result<CursorDiffResult, FerryError> {
        let last_cursor = self.state.get_cursor(sync_name).await?;

        let mut new_rows = Vec::new();
        let mut max_cursor_value: Option<String> = None;
        let mut global_row_idx: usize = 0;

        for batch in current_batches {
            let schema = batch.schema();
            let col_idx = schema.index_of(cursor_field).map_err(|_| {
                FerryError::Cdc(format!(
                    "Cursor field '{}' not found in batch",
                    cursor_field
                ))
            })?;
            let col = batch.column(col_idx);

            for row_idx in 0..batch.num_rows() {
                let cursor_value = extract_cursor_value(col, row_idx)?;

                // Track the maximum cursor value seen (type-aware comparison)
                max_cursor_value = match &max_cursor_value {
                    None => Some(cursor_value.clone()),
                    Some(current_max) => {
                        if cursor_greater_than(&cursor_value, current_max, col.data_type()) {
                            Some(cursor_value.clone())
                        } else {
                            max_cursor_value
                        }
                    }
                };

                // Determine if this row is new (cursor > last_cursor)
                let is_new = match &last_cursor {
                    None => true, // First run: all rows are new
                    Some(last) => cursor_greater_than(&cursor_value, last, col.data_type()),
                };

                if is_new {
                    new_rows.push(global_row_idx);
                }

                global_row_idx += 1;
            }
        }

        let new_cursor_value = max_cursor_value.unwrap_or_default();

        Ok(CursorDiffResult {
            new_rows,
            new_cursor_value,
        })
    }
}

/// Compare two cursor values, using numeric comparison for numeric types
/// and string comparison for string types.
fn cursor_greater_than(a: &str, b: &str, dt: &DataType) -> bool {
    match dt {
        DataType::Int32 | DataType::Int64 => {
            let a_num: i64 = a.parse().unwrap_or(0);
            let b_num: i64 = b.parse().unwrap_or(0);
            a_num > b_num
        }
        DataType::Float64 => {
            let a_num: f64 = a.parse().unwrap_or(0.0);
            let b_num: f64 = b.parse().unwrap_or(0.0);
            a_num > b_num
        }
        _ => a > b, // String comparison for Utf8, LargeUtf8, etc.
    }
}

/// Extract the cursor value from a column at a given row as a string.
///
/// Supports Utf8, LargeUtf8, Int32, and Int64 types with a Debug fallback.
fn extract_cursor_value(col: &ArrayRef, row_idx: usize) -> Result<String, FerryError> {
    if col.is_null(row_idx) {
        return Err(FerryError::Cdc("Cursor value is null".into()));
    }

    match col.data_type() {
        DataType::Utf8 => {
            let arr = col
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| FerryError::Cdc("Failed to downcast StringArray".into()))?;
            Ok(arr.value(row_idx).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = col
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| FerryError::Cdc("Failed to downcast LargeStringArray".into()))?;
            Ok(arr.value(row_idx).to_string())
        }
        DataType::Int32 => {
            let arr = col
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| FerryError::Cdc("Failed to downcast Int32Array".into()))?;
            Ok(arr.value(row_idx).to_string())
        }
        DataType::Int64 => {
            let arr = col
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| FerryError::Cdc("Failed to downcast Int64Array".into()))?;
            Ok(arr.value(row_idx).to_string())
        }
        _ => {
            // Fallback: use Debug representation
            Ok(format!("{:?}", col))
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::builder::StringBuilder;
    use arrow_array::{Float64Array, Int32Array, Int64Array, StringArray};
    use arrow_schema::{Field, Schema};

    /// Helper: create a simple RecordBatch with id (Int32) and name (Utf8).
    fn make_test_batch(ids: Vec<i32>, names: Vec<&str>, null_name_rows: Vec<usize>) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let id_array = Int32Array::from(ids);
        let mut name_builder = StringBuilder::new();
        for (i, name) in names.iter().enumerate() {
            if null_name_rows.contains(&i) {
                name_builder.append_null();
            } else {
                name_builder.append_value(name);
            }
        }
        let name_array = name_builder.finish();

        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .expect("Failed to create test batch")
    }

    // ── hash_row tests ─────────────────────────────────────────────────

    #[test]
    fn test_hash_deterministic() {
        let batch = make_test_batch(vec![1], vec!["Alice"], vec![]);
        let hash1 = hash_row(&batch, 0, &[]).unwrap();
        let hash2 = hash_row(&batch, 0, &[]).unwrap();
        assert_eq!(hash1, hash2, "same row should produce same hash");
    }

    #[test]
    fn test_hash_different_rows() {
        let batch = make_test_batch(vec![1, 2], vec!["Alice", "Bob"], vec![]);
        let hash1 = hash_row(&batch, 0, &[]).unwrap();
        let hash2 = hash_row(&batch, 1, &[]).unwrap();
        assert_ne!(
            hash1, hash2,
            "different rows should produce different hashes"
        );
    }

    #[test]
    fn test_null_vs_empty_string() {
        // Row 0: name = "" (empty string)
        // Row 1: name = NULL
        let batch = make_test_batch(vec![1, 1], vec!["", "ignored"], vec![1]);

        let hash_empty = hash_row(&batch, 0, &["id".to_string(), "name".to_string()]).unwrap();
        let hash_null = hash_row(&batch, 1, &["id".to_string(), "name".to_string()]).unwrap();
        assert_ne!(
            hash_empty, hash_null,
            "null and empty string should produce different hashes"
        );
    }

    #[test]
    fn test_hash_int32() {
        let schema = Schema::new(vec![Field::new("val", DataType::Int32, false)]);
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Int32Array::from(vec![42]))])
                .unwrap();

        let hash = hash_row(&batch, 0, &["val".to_string()]).unwrap();
        // Just verify it produces a non-zero hash
        assert_ne!(hash, 0);
    }

    #[test]
    fn test_hash_int64() {
        let schema = Schema::new(vec![Field::new("val", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int64Array::from(vec![9999999999i64]))],
        )
        .unwrap();

        let hash = hash_row(&batch, 0, &["val".to_string()]).unwrap();
        assert_ne!(hash, 0);
    }

    #[test]
    fn test_hash_utf8() {
        let schema = Schema::new(vec![Field::new("val", DataType::Utf8, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(StringArray::from(vec!["hello"]))],
        )
        .unwrap();

        let hash = hash_row(&batch, 0, &["val".to_string()]).unwrap();
        assert_ne!(hash, 0);
    }

    #[test]
    fn test_hash_float64() {
        let schema = Schema::new(vec![Field::new("val", DataType::Float64, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Float64Array::from(vec![std::f64::consts::PI]))],
        )
        .unwrap();

        let hash = hash_row(&batch, 0, &["val".to_string()]).unwrap();
        assert_ne!(hash, 0);
    }

    #[test]
    fn test_hash_boolean() {
        use arrow_array::BooleanArray;

        let schema = Schema::new(vec![Field::new("val", DataType::Boolean, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(BooleanArray::from(vec![true]))],
        )
        .unwrap();

        let hash = hash_row(&batch, 0, &["val".to_string()]).unwrap();
        assert_ne!(hash, 0);
    }

    #[test]
    fn test_hash_all_columns() {
        // Empty hash_columns means hash all columns
        let batch = make_test_batch(vec![1], vec!["Alice"], vec![]);
        let hash = hash_row(&batch, 0, &[]).unwrap();
        assert_ne!(hash, 0);
    }

    #[test]
    fn test_hash_specific_columns() {
        let batch = make_test_batch(vec![1, 2], vec!["Alice", "Bob"], vec![]);

        // Hash only the "id" column
        let hash_id = hash_row(&batch, 0, &["id".to_string()]).unwrap();
        let hash_id2 = hash_row(&batch, 1, &["id".to_string()]).unwrap();
        assert_ne!(hash_id, hash_id2, "different ids should hash differently");

        // Hash only the "name" column
        let hash_name = hash_row(&batch, 0, &["name".to_string()]).unwrap();
        let hash_name2 = hash_row(&batch, 1, &["name".to_string()]).unwrap();
        assert_ne!(
            hash_name, hash_name2,
            "different names should hash differently"
        );
    }

    // ── hash_record_batches tests ──────────────────────────────────────

    #[test]
    fn test_hash_record_batches_single_batch() {
        let batch = make_test_batch(vec![1, 2, 3], vec!["Alice", "Bob", "Carol"], vec![]);
        let hashes = hash_record_batches(&[batch], "id", &[]).unwrap();
        assert_eq!(hashes.len(), 3);
        assert!(hashes.contains_key("1"));
        assert!(hashes.contains_key("2"));
        assert!(hashes.contains_key("3"));
    }

    #[test]
    fn test_hash_record_batches_multiple_batches() {
        let batch1 = make_test_batch(vec![1, 2], vec!["Alice", "Bob"], vec![]);
        let batch2 = make_test_batch(vec![3, 4], vec!["Carol", "Dave"], vec![]);
        let hashes = hash_record_batches(&[batch1, batch2], "id", &[]).unwrap();
        assert_eq!(hashes.len(), 4);
    }

    // ── HashCdc tests ───────────────────────────────────────────────────

    /// Create a temporary DuckDbStateStore for testing.
    async fn create_test_state() -> (crate::state::DuckDbStateStore, tempfile::TempDir) {
        let dir =
            tempfile::TempDir::with_prefix("ferry-cdc-test-").expect("Failed to create temp dir");
        let path = dir.path().join("state.db");
        let store =
            crate::state::DuckDbStateStore::new(&path).expect("Failed to create state store");
        (store, dir)
    }

    #[tokio::test]
    async fn test_compute_diff_added() {
        let (state, _dir) = create_test_state().await;
        let cdc = HashCdc::new(&state);

        // Previous state: hashes for pk1, pk2 (computed from actual data)
        let batch_prev = make_test_batch(vec![1, 2], vec!["Alice", "Bob"], vec![]);
        let prev_hashes = hash_record_batches(&[batch_prev], "id", &[]).unwrap();
        state.set_hashes("test_sync", &prev_hashes).await.unwrap();

        // Current data: pk1 (unchanged), pk2 (unchanged), pk3 (new)
        let batch_curr = make_test_batch(vec![1, 2, 3], vec!["Alice", "Bob", "Carol"], vec![]);
        let result = cdc
            .compute_diff("test_sync", &[batch_curr], "id", &[])
            .await
            .unwrap();

        assert_eq!(result.added, vec!["3".to_string()], "pk3 should be added");
        assert!(result.changed.is_empty(), "no rows should be changed");
        assert!(result.removed.is_empty(), "no rows should be removed");
    }

    #[tokio::test]
    async fn test_compute_diff_changed() {
        let (state, _dir) = create_test_state().await;
        let cdc = HashCdc::new(&state);

        // Previous state: hash for pk1 with name "Alice"
        let batch_prev = make_test_batch(vec![1], vec!["Alice"], vec![]);
        let prev_hashes = hash_record_batches(&[batch_prev], "id", &[]).unwrap();
        state.set_hashes("test_sync", &prev_hashes).await.unwrap();

        // Current data: pk1 with name "Bob" (changed)
        let batch_curr = make_test_batch(vec![1], vec!["Bob"], vec![]);
        let result = cdc
            .compute_diff("test_sync", &[batch_curr], "id", &[])
            .await
            .unwrap();

        assert!(result.added.is_empty(), "no rows should be added");
        assert_eq!(
            result.changed,
            vec!["1".to_string()],
            "pk1 should be changed"
        );
        assert!(result.removed.is_empty(), "no rows should be removed");
    }

    #[tokio::test]
    async fn test_compute_diff_removed() {
        let (state, _dir) = create_test_state().await;
        let cdc = HashCdc::new(&state);

        // Previous state: hashes for pk1, pk2
        let batch_prev = make_test_batch(vec![1, 2], vec!["Alice", "Bob"], vec![]);
        let prev_hashes = hash_record_batches(&[batch_prev], "id", &[]).unwrap();
        state.set_hashes("test_sync", &prev_hashes).await.unwrap();

        // Current data: only pk1 (pk2 removed)
        let batch_curr = make_test_batch(vec![1], vec!["Alice"], vec![]);
        let result = cdc
            .compute_diff("test_sync", &[batch_curr], "id", &[])
            .await
            .unwrap();

        assert!(result.added.is_empty(), "no rows should be added");
        assert!(result.changed.is_empty(), "no rows should be changed");
        assert_eq!(
            result.removed,
            vec!["2".to_string()],
            "pk2 should be removed"
        );
    }

    #[tokio::test]
    async fn test_compute_diff_no_changes() {
        let (state, _dir) = create_test_state().await;
        let cdc = HashCdc::new(&state);

        // Previous state: same as current
        let batch = make_test_batch(vec![1, 2], vec!["Alice", "Bob"], vec![]);
        let hashes = hash_record_batches(&[batch.clone()], "id", &[]).unwrap();
        state.set_hashes("test_sync", &hashes).await.unwrap();

        let result = cdc
            .compute_diff("test_sync", &[batch], "id", &[])
            .await
            .unwrap();

        assert!(result.added.is_empty(), "no rows should be added");
        assert!(result.changed.is_empty(), "no rows should be changed");
        assert!(result.removed.is_empty(), "no rows should be removed");
    }

    // ── CursorCdc tests ─────────────────────────────────────────────────

    #[tokio::test]
    async fn test_cursor_filter() {
        let (state, _dir) = create_test_state().await;

        // Set previous cursor
        state.set_cursor("test_sync", "2024-01-15").await.unwrap();

        let cdc = CursorCdc::new(&state);

        // Create batch with cursor field "updated_at"
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("updated_at", DataType::Utf8, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![
                    "2024-01-10", // before cursor → excluded
                    "2024-01-15", // equal to cursor → excluded
                    "2024-01-20", // after cursor → included
                ])),
            ],
        )
        .unwrap();

        let result = cdc
            .compute_diff("test_sync", &[batch], "updated_at")
            .await
            .unwrap();

        assert_eq!(result.new_rows.len(), 1, "only one row should be new");
        assert_eq!(result.new_rows[0], 2, "row index 2 should be new");
        assert_eq!(
            result.new_cursor_value, "2024-01-20",
            "cursor should advance to max value"
        );
    }

    #[tokio::test]
    async fn test_cursor_first_run() {
        let (state, _dir) = create_test_state().await;
        let cdc = CursorCdc::new(&state);

        // No previous cursor set → all rows should be included
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("updated_at", DataType::Utf8, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["2024-01-01", "2024-01-02"])),
            ],
        )
        .unwrap();

        let result = cdc
            .compute_diff("test_sync", &[batch], "updated_at")
            .await
            .unwrap();

        assert_eq!(
            result.new_rows.len(),
            2,
            "all rows should be new on first run"
        );
        assert_eq!(result.new_rows, vec![0, 1]);
        assert_eq!(result.new_cursor_value, "2024-01-02");
    }

    #[tokio::test]
    async fn test_cursor_multiple_batches() {
        let (state, _dir) = create_test_state().await;

        state.set_cursor("test_sync", "5").await.unwrap();

        let cdc = CursorCdc::new(&state);

        // Two batches with cursor values
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("seq", DataType::Int32, false),
        ]);

        let batch1 = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![3, 6])), // 3 < 5 (excluded), 6 > 5 (included)
            ],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(vec![3])),
                Arc::new(Int32Array::from(vec![10])), // 10 > 5 (included)
            ],
        )
        .unwrap();

        let result = cdc
            .compute_diff("test_sync", &[batch1, batch2], "seq")
            .await
            .unwrap();

        // Global row indices: batch1 has rows 0,1; batch2 has row 2
        assert_eq!(result.new_rows, vec![1, 2], "rows 1 and 2 should be new");
        assert_eq!(
            result.new_cursor_value, "10",
            "cursor should be the max value"
        );
    }

    // ── extract_primary_key tests ───────────────────────────────────────

    #[test]
    fn test_extract_primary_key_int32() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Int32Array::from(vec![42]))])
                .unwrap();

        let pk = extract_primary_key(&batch, 0, "id").unwrap();
        assert_eq!(pk, "42");
    }

    #[test]
    fn test_extract_primary_key_utf8() {
        let schema = Schema::new(vec![Field::new("id", DataType::Utf8, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(StringArray::from(vec!["abc"]))],
        )
        .unwrap();

        let pk = extract_primary_key(&batch, 0, "id").unwrap();
        assert_eq!(pk, "abc");
    }

    #[test]
    fn test_extract_primary_key_null_error() {
        let schema = Schema::new(vec![Field::new("id", DataType::Utf8, true)]);
        let mut builder = StringBuilder::new();
        builder.append_null();
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(builder.finish())]).unwrap();

        let result = extract_primary_key(&batch, 0, "id");
        assert!(result.is_err(), "null PK should produce an error");
    }

    #[test]
    fn test_extract_primary_key_missing_column() {
        let batch = make_test_batch(vec![1], vec!["Alice"], vec![]);
        let result = extract_primary_key(&batch, 0, "nonexistent");
        assert!(result.is_err(), "missing column should produce an error");
    }
}
