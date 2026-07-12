use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use arrow_array::cast::*;
use arrow_array::types::*;
use arrow_array::*;
use arrow_cast::display::array_value_to_string;
use arrow_schema::*;
use async_trait::async_trait;
use chrono::Utc;
use serde_json::Value;

use ferry_core::error::FerryError;
use ferry_core::traits::{
    Destination, IdempotencyCapability, RateLimit, RemoveCapability, RemoveResult, WriteConfig,
    WriteResult,
};

/// The file format to use when writing.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FileFormat {
    /// Comma-separated values format.
    Csv,
    /// JSON Lines format (one JSON object per line).
    Json,
}

/// A destination that writes RecordBatches to CSV or JSON files.
///
/// Files are written to `{output_dir}/{sync_name}_{timestamp}_{batch_index}.{ext}`.
pub struct FileDestination {
    output_dir: PathBuf,
    format: FileFormat,
    sync_name: String,
}

impl FileDestination {
    /// Create a new `FileDestination`.
    ///
    /// # Arguments
    ///
    /// * `output_dir` - The directory to write output files to.
    /// * `format` - The file format (CSV or JSON).
    /// * `sync_name` - The name of the sync, used in the output filename.
    pub fn new(output_dir: &Path, format: FileFormat, sync_name: &str) -> Self {
        Self {
            output_dir: output_dir.to_path_buf(),
            format,
            sync_name: sync_name.to_string(),
        }
    }

    /// Generate the output file path for a given batch index.
    fn output_path(&self, batch_index: usize) -> PathBuf {
        let timestamp = Utc::now().format("%Y%m%d_%H%M%S_%3f");
        let ext = match self.format {
            FileFormat::Csv => "csv",
            FileFormat::Json => "jsonl",
        };
        self.output_dir.join(format!(
            "{}_{}_{}.{}",
            self.sync_name, timestamp, batch_index, ext
        ))
    }

    /// Ensure the output directory exists, creating it if necessary.
    fn ensure_output_dir(&self) -> Result<(), FerryError> {
        fs::create_dir_all(&self.output_dir).map_err(|e| {
            FerryError::Destination(format!(
                "Failed to create output directory '{}': {}",
                self.output_dir.display(),
                e
            ))
        })
    }

    /// Write a RecordBatch to a CSV file.
    fn write_csv(&self, batch: &RecordBatch, path: &Path) -> Result<(), FerryError> {
        let file = fs::File::create(path).map_err(|e| {
            FerryError::Destination(format!(
                "Failed to create CSV file '{}': {}",
                path.display(),
                e
            ))
        })?;
        let mut writer = arrow::csv::Writer::new(BufWriter::new(file));
        writer.write(batch).map_err(|e| {
            FerryError::Destination(format!(
                "Failed to write CSV batch to '{}': {}",
                path.display(),
                e
            ))
        })?;
        Ok(())
    }

    /// Write a RecordBatch to a JSONL file.
    fn write_json(&self, batch: &RecordBatch, path: &Path) -> Result<(), FerryError> {
        let file = fs::File::create(path).map_err(|e| {
            FerryError::Destination(format!(
                "Failed to create JSON file '{}': {}",
                path.display(),
                e
            ))
        })?;
        let mut writer = BufWriter::new(file);

        let schema = batch.schema();
        let num_cols = batch.num_columns();
        let num_rows = batch.num_rows();

        for row_idx in 0..num_rows {
            let mut obj = serde_json::Map::new();
            for col_idx in 0..num_cols {
                let field = schema.field(col_idx);
                let column = batch.column(col_idx);
                let col_name = field.name().clone();
                let value = cell_to_json_value(column, row_idx);
                obj.insert(col_name, value);
            }
            let line = serde_json::to_string(&obj).map_err(|e| {
                FerryError::Destination(format!("Failed to serialize JSON row: {}", e))
            })?;
            writeln!(writer, "{}", line).map_err(|e| {
                FerryError::Destination(format!("Failed to write JSON line: {}", e))
            })?;
        }

        writer
            .flush()
            .map_err(|e| FerryError::Destination(format!("Failed to flush JSON writer: {}", e)))?;

        Ok(())
    }
}

/// Convert a single cell in an Arrow column to a `serde_json::Value`.
fn cell_to_json_value(column: &ArrayRef, row_idx: usize) -> Value {
    if column.is_null(row_idx) {
        return Value::Null;
    }

    // Use type-specific extraction for numeric and boolean types to get proper JSON types.
    // For all other types, fall back to string representation.
    match column.data_type() {
        DataType::Boolean => {
            let arr = as_boolean_array(column);
            Value::Bool(arr.value(row_idx))
        }
        DataType::Int8 => {
            let arr = as_primitive_array::<Int8Type>(column);
            Value::Number(serde_json::Number::from(arr.value(row_idx)))
        }
        DataType::Int16 => {
            let arr = as_primitive_array::<Int16Type>(column);
            Value::Number(serde_json::Number::from(arr.value(row_idx)))
        }
        DataType::Int32 => {
            let arr = as_primitive_array::<Int32Type>(column);
            Value::Number(serde_json::Number::from(arr.value(row_idx)))
        }
        DataType::Int64 => {
            let arr = as_primitive_array::<Int64Type>(column);
            Value::Number(serde_json::Number::from(arr.value(row_idx)))
        }
        DataType::UInt8 => {
            let arr = as_primitive_array::<UInt8Type>(column);
            Value::Number(serde_json::Number::from(arr.value(row_idx)))
        }
        DataType::UInt16 => {
            let arr = as_primitive_array::<UInt16Type>(column);
            Value::Number(serde_json::Number::from(arr.value(row_idx)))
        }
        DataType::UInt32 => {
            let arr = as_primitive_array::<UInt32Type>(column);
            Value::Number(serde_json::Number::from(arr.value(row_idx)))
        }
        DataType::UInt64 => {
            let arr = as_primitive_array::<UInt64Type>(column);
            Value::Number(serde_json::Number::from(arr.value(row_idx)))
        }
        DataType::Float32 => {
            let arr = as_primitive_array::<Float32Type>(column);
            let v = arr.value(row_idx) as f64;
            if let Some(n) = serde_json::Number::from_f64(v) {
                Value::Number(n)
            } else {
                Value::String(v.to_string())
            }
        }
        DataType::Float64 => {
            let arr = as_primitive_array::<Float64Type>(column);
            let v = arr.value(row_idx);
            if let Some(n) = serde_json::Number::from_f64(v) {
                Value::Number(n)
            } else {
                Value::String(v.to_string())
            }
        }
        // For all other types (strings, dates, timestamps, etc.), use string representation
        _ => match array_value_to_string(column.as_ref(), row_idx) {
            Ok(s) => Value::String(s),
            Err(_) => Value::String("<error>".to_string()),
        },
    }
}

#[async_trait]
impl Destination for FileDestination {
    fn name(&self) -> &str {
        &self.sync_name
    }

    async fn check_connection(&self) -> Result<(), FerryError> {
        if self.output_dir.exists() {
            if !self.output_dir.is_dir() {
                return Err(FerryError::Destination(format!(
                    "Output path '{}' exists but is not a directory",
                    self.output_dir.display()
                )));
            }
            // Check writable by creating a temp file
            let test_file = self.output_dir.join(".ferry_write_test");
            match fs::File::create(&test_file) {
                Ok(f) => {
                    drop(f);
                    let _ = fs::remove_file(&test_file);
                    Ok(())
                }
                Err(e) => Err(FerryError::Destination(format!(
                    "Output directory '{}' is not writable: {}",
                    self.output_dir.display(),
                    e
                ))),
            }
        } else {
            // Try to create the directory
            self.ensure_output_dir()
        }
    }

    async fn write(
        &self,
        batch: &RecordBatch,
        config: &WriteConfig,
    ) -> Result<WriteResult, FerryError> {
        self.ensure_output_dir()?;
        let path = self.output_path(config.batch_index);

        match self.format {
            FileFormat::Csv => self.write_csv(batch, &path)?,
            FileFormat::Json => self.write_json(batch, &path)?,
        }

        Ok(WriteResult {
            rows_written: batch.num_rows(),
            errors: vec![],
        })
    }

    fn max_batch_size(&self) -> usize {
        10000
    }

    fn rate_limit(&self) -> Option<RateLimit> {
        None
    }

    fn idempotency(&self) -> IdempotencyCapability {
        IdempotencyCapability::Idempotent
    }

    fn remove_capability(&self) -> RemoveCapability {
        RemoveCapability::RemoveAll
    }

    async fn remove(
        &self,
        _keys: &[Value],
        _config: &WriteConfig,
    ) -> Result<RemoveResult, FerryError> {
        Err(FerryError::Destination(
            "FileDestination does not support per-key removal; use replace_all instead".to_string(),
        ))
    }

    async fn replace_all(
        &self,
        batch: &RecordBatch,
        config: &WriteConfig,
    ) -> Result<WriteResult, FerryError> {
        // Overwrite: use batch_index from config for the single output file
        self.ensure_output_dir()?;
        let path = self.output_path(config.batch_index);

        match self.format {
            FileFormat::Csv => self.write_csv(batch, &path)?,
            FileFormat::Json => self.write_json(batch, &path)?,
        }

        Ok(WriteResult {
            rows_written: batch.num_rows(),
            errors: vec![],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Float64, true),
        ]));

        let ids = Int32Array::from(vec![1, 2, 3]);
        let names = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
        let scores = Float64Array::from(vec![Some(95.5), Some(87.3), None]);

        RecordBatch::try_new(
            schema,
            vec![Arc::new(ids), Arc::new(names), Arc::new(scores)],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_write_csv() {
        let dir = tempdir().unwrap();
        let dest = FileDestination::new(dir.path(), FileFormat::Csv, "test_sync");
        let batch = create_test_batch();
        let config = WriteConfig {
            sync_name: "test_sync".to_string(),
            batch_index: 0,
            total_batches: 1,
        };

        let result = dest.write(&batch, &config).await.unwrap();
        assert_eq!(result.rows_written, 3);

        // Find the CSV file
        let entries: Vec<_> = fs::read_dir(dir.path()).unwrap().collect();
        assert_eq!(entries.len(), 1);
        let path = entries.into_iter().next().unwrap().unwrap().path();
        assert!(path.to_string_lossy().ends_with(".csv"));

        // Read and verify contents
        let mut contents = String::new();
        fs::File::open(&path)
            .unwrap()
            .read_to_string(&mut contents)
            .unwrap();
        assert!(contents.contains("id,name,score"));
        assert!(contents.contains("1,Alice,95.5"));
        assert!(contents.contains("2,Bob,87.3"));
        assert!(contents.contains("3,Charlie,")); // null score
    }

    #[tokio::test]
    async fn test_write_json() {
        let dir = tempdir().unwrap();
        let dest = FileDestination::new(dir.path(), FileFormat::Json, "test_sync");
        let batch = create_test_batch();
        let config = WriteConfig {
            sync_name: "test_sync".to_string(),
            batch_index: 0,
            total_batches: 1,
        };

        let result = dest.write(&batch, &config).await.unwrap();
        assert_eq!(result.rows_written, 3);

        // Find the JSONL file
        let entries: Vec<_> = fs::read_dir(dir.path()).unwrap().collect();
        assert_eq!(entries.len(), 1);
        let path = entries.into_iter().next().unwrap().unwrap().path();
        assert!(path.to_string_lossy().ends_with(".jsonl"));

        // Read and verify contents
        let mut contents = String::new();
        fs::File::open(&path)
            .unwrap()
            .read_to_string(&mut contents)
            .unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 3);

        // Parse each line as JSON
        let row0: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(row0["id"], 1);
        assert_eq!(row0["name"], "Alice");
        assert_eq!(row0["score"], 95.5);

        let row1: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(row1["id"], 2);
        assert_eq!(row1["name"], "Bob");
        assert_eq!(row1["score"], 87.3);

        let row2: serde_json::Value = serde_json::from_str(lines[2]).unwrap();
        assert_eq!(row2["id"], 3);
        assert_eq!(row2["name"], "Charlie");
        assert_eq!(row2["score"], Value::Null);
    }

    #[tokio::test]
    async fn test_write_creates_output_dir() {
        let dir = tempdir().unwrap();
        let nested = dir.path().join("nested").join("output");
        let dest = FileDestination::new(&nested, FileFormat::Csv, "test_sync");
        let batch = create_test_batch();
        let config = WriteConfig {
            sync_name: "test_sync".to_string(),
            batch_index: 0,
            total_batches: 1,
        };

        let result = dest.write(&batch, &config).await.unwrap();
        assert_eq!(result.rows_written, 3);
        assert!(nested.exists());
        assert!(nested.is_dir());

        // Should have one CSV file
        let entries: Vec<_> = fs::read_dir(&nested).unwrap().collect();
        assert_eq!(entries.len(), 1);
    }

    #[tokio::test]
    async fn test_replace_all_overwrites() {
        let dir = tempdir().unwrap();
        let dest = FileDestination::new(dir.path(), FileFormat::Csv, "test_sync");

        // First write with batch_index 0
        let batch1 = create_test_batch();
        let config1 = WriteConfig {
            sync_name: "test_sync".to_string(),
            batch_index: 0,
            total_batches: 1,
        };
        dest.write(&batch1, &config1).await.unwrap();

        // replace_all with a different batch, using batch_index 1
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Float64, true),
        ]));
        let ids = Int32Array::from(vec![4, 5]);
        let names = StringArray::from(vec!["Dave", "Eve"]);
        let scores = Float64Array::from(vec![Some(100.0), None]);
        let batch2 = RecordBatch::try_new(
            schema,
            vec![Arc::new(ids), Arc::new(names), Arc::new(scores)],
        )
        .unwrap();

        let config2 = WriteConfig {
            sync_name: "test_sync".to_string(),
            batch_index: 1,
            total_batches: 1,
        };
        let result = dest.replace_all(&batch2, &config2).await.unwrap();
        assert_eq!(result.rows_written, 2);

        // Should have two files (write + replace_all create separate files)
        let entries: Vec<_> = fs::read_dir(dir.path()).unwrap().collect();
        assert_eq!(entries.len(), 2);

        // Find the file with batch_index 1 and verify it contains batch2 data
        let paths: Vec<_> = entries.into_iter().map(|e| e.unwrap().path()).collect();
        let replace_file = paths
            .iter()
            .find(|p| p.to_string_lossy().contains("_1.csv"))
            .expect("replace_all file with batch_index 1 should exist");

        let mut contents = String::new();
        fs::File::open(replace_file)
            .unwrap()
            .read_to_string(&mut contents)
            .unwrap();
        assert!(contents.contains("4,Dave,100"));
        assert!(contents.contains("5,Eve,"));
        assert!(!contents.contains("Alice"));
    }

    #[tokio::test]
    async fn test_check_connection_valid() {
        let dir = tempdir().unwrap();
        let dest = FileDestination::new(dir.path(), FileFormat::Csv, "test_sync");
        assert!(dest.check_connection().await.is_ok());
    }

    #[tokio::test]
    async fn test_check_connection_invalid() {
        // A path that exists but is a file, not a directory
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("not_a_dir");
        fs::write(&file_path, "this is a file").unwrap();

        let dest = FileDestination::new(&file_path, FileFormat::Csv, "test_sync");
        let result = dest.check_connection().await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("is not a directory")
        );
    }

    #[tokio::test]
    async fn test_remove_returns_error() {
        let dir = tempdir().unwrap();
        let dest = FileDestination::new(dir.path(), FileFormat::Csv, "test_sync");
        let config = WriteConfig {
            sync_name: "test_sync".to_string(),
            batch_index: 0,
            total_batches: 1,
        };
        let result = dest.remove(&[], &config).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("does not support per-key removal")
        );
    }

    #[tokio::test]
    async fn test_max_batch_size() {
        let dir = tempdir().unwrap();
        let dest = FileDestination::new(dir.path(), FileFormat::Csv, "test_sync");
        assert_eq!(dest.max_batch_size(), 10000);
    }

    #[tokio::test]
    async fn test_idempotency() {
        let dir = tempdir().unwrap();
        let dest = FileDestination::new(dir.path(), FileFormat::Csv, "test_sync");
        assert_eq!(dest.idempotency(), IdempotencyCapability::Idempotent);
    }

    #[tokio::test]
    async fn test_remove_capability() {
        let dir = tempdir().unwrap();
        let dest = FileDestination::new(dir.path(), FileFormat::Csv, "test_sync");
        assert_eq!(dest.remove_capability(), RemoveCapability::RemoveAll);
    }

    #[tokio::test]
    async fn test_rate_limit() {
        let dir = tempdir().unwrap();
        let dest = FileDestination::new(dir.path(), FileFormat::Csv, "test_sync");
        assert!(dest.rate_limit().is_none());
    }
}
