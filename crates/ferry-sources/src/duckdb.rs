use std::collections::HashMap;
use std::sync::Mutex;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use duckdb::Connection;

use ferry_core::error::FerryError;
use ferry_core::traits::{RecordBatchStream, Source, StreamSchema};

/// A DuckDB source connector that implements the `Source` trait.
///
/// Executes SQL queries against a DuckDB database and returns the results
/// as Arrow `RecordBatch` streams.
///
/// # Example
///
/// ```rust,no_run
/// use ferry_core::traits::Source;
/// use ferry_sources::duckdb::DuckDbSource;
///
/// let source = DuckDbSource::new("/path/to/database.duckdb").unwrap();
/// let stream = source.read("SELECT * FROM my_table");
/// ```
pub struct DuckDbSource {
    /// The DuckDB connection is wrapped in a `Mutex` because `duckdb::Connection`
    /// is not `Sync` (it uses `RefCell` internally for statement caching).
    conn: Mutex<Connection>,
    name: String,
}

impl DuckDbSource {
    /// Open a DuckDB database at the given path.
    ///
    /// Creates the file if it does not exist.
    pub fn new(path: &str) -> Result<Self, FerryError> {
        let conn = Connection::open(path)
            .map_err(|e| FerryError::Source(format!("Failed to open DuckDB at {}: {}", path, e)))?;
        Ok(Self {
            conn: Mutex::new(conn),
            name: "duckdb".to_string(),
        })
    }

    /// Wrap an existing DuckDB connection.
    pub fn from_conn(conn: Connection) -> Self {
        Self {
            conn: Mutex::new(conn),
            name: "duckdb".to_string(),
        }
    }

    /// Validate that all expected columns exist in the query result schema.
    ///
    /// This is used for schema drift detection — if a column referenced in the
    /// sync config's mapping is missing from the query result, an error is returned.
    pub fn validate_columns(
        &self,
        query: &str,
        expected_columns: &[&str],
    ) -> Result<(), FerryError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(query).map_err(|e| {
            FerryError::Source(format!("Failed to prepare query for validation: {}", e))
        })?;

        stmt.execute([]).map_err(|e| {
            FerryError::Source(format!("Failed to execute query for validation: {}", e))
        })?;

        let schema = stmt.schema();

        let result_columns: std::collections::HashSet<String> = schema
            .fields()
            .iter()
            .map(|f| f.name().to_lowercase())
            .collect();

        let mut missing: Vec<String> = Vec::new();
        for col in expected_columns {
            if !result_columns.contains(&col.to_lowercase()) {
                missing.push(col.to_string());
            }
        }

        if !missing.is_empty() {
            let mut available: Vec<&str> = result_columns.iter().map(|s| s.as_str()).collect();
            available.sort();
            return Err(FerryError::Source(format!(
                "Schema drift detected: columns {:?} not found in query result. \
                 Available columns: {:?}",
                missing, available,
            )));
        }

        Ok(())
    }

    /// Execute a query and collect all RecordBatches.
    ///
    /// # Note
    ///
    /// TODO: For Phase 1, we collect all batches into memory. In Phase 2, optimize
    /// to true streaming using `tokio::task::spawn_blocking` with a channel. The
    /// `ArrowStream` from duckdb-rs is `!Send` because it holds a reference to the
    /// `Statement`, so it cannot be sent across threads directly.
    fn execute_query(&self, query: &str) -> Result<Vec<RecordBatch>, FerryError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(query)
            .map_err(|e| FerryError::Source(format!("Failed to prepare query: {}", e)))?;

        // Must execute first to populate the schema
        stmt.execute([])
            .map_err(|e| FerryError::Source(format!("Failed to execute query: {}", e)))?;

        let schema = stmt.schema();

        let mut stream = stmt
            .stream_arrow([], schema)
            .map_err(|e| FerryError::Source(format!("Failed to create arrow stream: {}", e)))?;

        let mut batches = Vec::new();
        for batch in &mut stream {
            batches.push(batch);
        }

        Ok(batches)
    }
}

#[async_trait]
impl Source for DuckDbSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn check_connection(&self) -> Result<(), FerryError> {
        let conn = self.conn.lock().unwrap();
        conn.execute("SELECT 1", [])
            .map_err(|e| FerryError::Source(format!("Connection check failed: {}", e)))?;
        Ok(())
    }

    async fn discover(&self) -> Result<Vec<StreamSchema>, FerryError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT table_schema, table_name, column_name, data_type
                 FROM information_schema.columns
                 WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                 ORDER BY table_schema, table_name, ordinal_position",
            )
            .map_err(|e| FerryError::Source(format!("Failed to prepare discovery query: {}", e)))?;

        let rows = stmt
            .query_map([], |row| {
                let schema_name: String = row.get(0)?;
                let table_name: String = row.get(1)?;
                let column_name: String = row.get(2)?;
                let data_type: String = row.get(3)?;
                Ok((schema_name, table_name, column_name, data_type))
            })
            .map_err(|e| FerryError::Source(format!("Discovery query failed: {}", e)))?;

        let mut tables: HashMap<String, Vec<(String, DataType)>> = HashMap::new();

        for row in rows {
            let (schema_name, table_name, column_name, data_type_str) = row
                .map_err(|e| FerryError::Source(format!("Failed to read discovery row: {}", e)))?;
            let full_name = if schema_name == "main" {
                table_name
            } else {
                format!("{}.{}", schema_name, table_name)
            };
            let arrow_type = duckdb_type_to_arrow(&data_type_str);
            tables
                .entry(full_name)
                .or_default()
                .push((column_name, arrow_type));
        }

        let result: Vec<StreamSchema> = tables
            .into_iter()
            .map(|(name, columns)| {
                let fields: Vec<Field> = columns
                    .into_iter()
                    .map(|(col_name, data_type)| Field::new(&col_name, data_type, true))
                    .collect();
                StreamSchema {
                    name,
                    schema: Schema::new(fields),
                }
            })
            .collect();

        Ok(result)
    }

    fn read(&self, query: &str) -> RecordBatchStream {
        match self.execute_query(query) {
            Ok(batches) => {
                let stream = futures::stream::iter(batches.into_iter().map(Ok));
                Box::pin(stream)
            }
            Err(e) => Box::pin(futures::stream::once(async move { Err(e) })),
        }
    }
}

/// Map a DuckDB type string to an Arrow `DataType`.
///
/// This is used by `discover()` to build the Arrow schema for discovered tables.
/// Unknown types default to `Utf8` with a warning.
fn duckdb_type_to_arrow(duckdb_type: &str) -> DataType {
    match duckdb_type.to_uppercase().as_str() {
        "BOOLEAN" | "BOOL" | "LOGICAL" => DataType::Boolean,
        "TINYINT" | "INT1" => DataType::Int8,
        "SMALLINT" | "INT2" | "SHORT" => DataType::Int16,
        "INTEGER" | "INT" | "INT4" | "INT32" | "SIGNED" => DataType::Int32,
        "BIGINT" | "INT8" | "INT64" | "LONG" => DataType::Int64,
        "HUGEINT" => DataType::Int64,
        "FLOAT" | "REAL" | "FLOAT4" => DataType::Float32,
        "DOUBLE" | "FLOAT8" | "NUMERIC" | "DECIMAL" => DataType::Float64,
        "DATE" => DataType::Date64,
        "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" | "DATETIME" => {
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
        }
        "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => {
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("+00:00".into()))
        }
        "TIME" => DataType::Time64(arrow_schema::TimeUnit::Microsecond),
        "VARCHAR" | "TEXT" | "STRING" | "CHAR" | "CHARACTER" | "BPCHAR" => DataType::Utf8,
        "BLOB" | "BYTEA" | "BINARY" => DataType::Binary,
        "JSON" => DataType::Utf8,
        "UUID" => DataType::Utf8,
        "ENUM" => DataType::Utf8,
        "LIST" | "MAP" | "STRUCT" => DataType::Utf8,
        other => {
            tracing::warn!("Unknown DuckDB type '{}', defaulting to Utf8", other);
            DataType::Utf8
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::compute::concat;
    use arrow_array::{Int32Array, StringArray};
    use futures::StreamExt;
    use tempfile::TempDir;

    /// Create a temporary DuckDB database with a test table and data.
    fn create_test_db() -> (TempDir, DuckDbSource) {
        let dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = dir.path().join("test.duckdb");
        let source =
            DuckDbSource::new(db_path.to_str().unwrap()).expect("Failed to create DuckDbSource");

        {
            let conn = source.conn.lock().unwrap();
            conn.execute_batch(
                "CREATE TABLE test_table (
                     id INTEGER PRIMARY KEY,
                     name VARCHAR NOT NULL,
                     value INTEGER
                 );
                 INSERT INTO test_table VALUES (1, 'Alice', 100);
                 INSERT INTO test_table VALUES (2, 'Bob', 200);
                 INSERT INTO test_table VALUES (3, 'Carol', 300);",
            )
            .expect("Failed to create test data");
        }

        (dir, source)
    }

    #[tokio::test]
    async fn test_check_connection_valid() {
        let (_dir, source) = create_test_db();
        let result = source.check_connection().await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_connection_invalid() {
        // Path in a non-existent directory should fail
        let result = DuckDbSource::new("/nonexistent/path/test.duckdb");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_read_basic_query() {
        let (_dir, source) = create_test_db();
        let stream = source.read("SELECT * FROM test_table ORDER BY id");
        let batches: Vec<RecordBatch> = stream
            .collect::<Vec<Result<RecordBatch, FerryError>>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("Failed to collect batches");

        assert!(!batches.is_empty(), "Expected at least one batch");
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3, "Expected 3 total rows");
    }

    #[tokio::test]
    async fn test_read_returns_correct_schema() {
        let (_dir, source) = create_test_db();
        let stream = source.read("SELECT id, name, value FROM test_table ORDER BY id");
        let batches: Vec<RecordBatch> = stream
            .collect::<Vec<Result<RecordBatch, FerryError>>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("Failed to collect batches");

        assert!(!batches.is_empty());
        let batch = &batches[0];
        let schema = batch.schema();

        // Check field names
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(field_names, vec!["id", "name", "value"]);

        // Check field types
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(2).data_type(), &DataType::Int32);
    }

    #[tokio::test]
    async fn test_read_returns_correct_data() {
        let (_dir, source) = create_test_db();
        let stream = source.read("SELECT id, name, value FROM test_table ORDER BY id");
        let batches: Vec<RecordBatch> = stream
            .collect::<Vec<Result<RecordBatch, FerryError>>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("Failed to collect batches");

        // Flatten batches into a single batch for assertion
        let batch = if batches.len() == 1 {
            batches.into_iter().next().unwrap()
        } else {
            // Concatenate multiple batches
            let schema = batches[0].schema();
            let arrays: Vec<arrow_array::ArrayRef> = (0..schema.fields().len())
                .map(|i| {
                    let col_arrays: Vec<&dyn arrow_array::Array> =
                        batches.iter().map(|b| b.column(i).as_ref()).collect();
                    concat(&col_arrays).unwrap()
                })
                .collect();
            RecordBatch::try_new(schema, arrays).unwrap()
        };

        // Check id column
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id column should be Int32");
        assert_eq!(id_col.values(), &[1, 2, 3]);

        // Check name column
        let name_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name column should be Utf8");
        assert_eq!(name_col.value(0), "Alice");
        assert_eq!(name_col.value(1), "Bob");
        assert_eq!(name_col.value(2), "Carol");

        // Check value column
        let value_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("value column should be Int32");
        assert_eq!(value_col.values(), &[100, 200, 300]);
    }

    #[tokio::test]
    async fn test_read_multiple_batches() {
        let (_dir, source) = create_test_db();

        // Insert enough rows to potentially trigger multiple batches
        {
            let conn = source.conn.lock().unwrap();
            conn.execute_batch(
                "INSERT INTO test_table VALUES
                 (4, 'Dave', 400), (5, 'Eve', 500), (6, 'Frank', 600),
                 (7, 'Grace', 700), (8, 'Heidi', 800), (9, 'Ivan', 900),
                 (10, 'Judy', 1000), (11, 'Karl', 1100), (12, 'Leo', 1200);",
            )
            .expect("Failed to insert additional rows");
        }

        let stream = source.read("SELECT * FROM test_table ORDER BY id");
        let batches: Vec<RecordBatch> = stream
            .collect::<Vec<Result<RecordBatch, FerryError>>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("Failed to collect batches");

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 12, "Expected 12 total rows");
    }

    #[tokio::test]
    async fn test_discover_tables() {
        let (_dir, source) = create_test_db();

        // Create additional tables
        {
            let conn = source.conn.lock().unwrap();
            conn.execute_batch(
                "CREATE TABLE users (id INTEGER, email VARCHAR);
                 CREATE TABLE orders (id INTEGER, user_id INTEGER, amount DOUBLE);",
            )
            .expect("Failed to create additional tables");
        }

        let schemas = source.discover().await.expect("Failed to discover tables");

        let table_names: Vec<&str> = schemas.iter().map(|s| s.name.as_str()).collect();
        assert!(
            table_names.contains(&"test_table"),
            "Expected test_table in discovered tables, got {:?}",
            table_names
        );
        assert!(
            table_names.contains(&"users"),
            "Expected users in discovered tables, got {:?}",
            table_names
        );
        assert!(
            table_names.contains(&"orders"),
            "Expected orders in discovered tables, got {:?}",
            table_names
        );
    }

    #[tokio::test]
    async fn test_discover_returns_columns() {
        let (_dir, source) = create_test_db();
        let schemas = source.discover().await.expect("Failed to discover tables");

        let test_table = schemas
            .iter()
            .find(|s| s.name == "test_table")
            .expect("Expected test_table in discovered schemas");

        let col_names: Vec<&str> = test_table
            .schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(col_names, vec!["id", "name", "value"]);
    }

    #[test]
    fn test_validate_columns_ok() {
        let (_dir, source) = create_test_db();
        let result = source.validate_columns(
            "SELECT id, name, value FROM test_table",
            &["id", "name", "value"],
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_columns_missing() {
        let (_dir, source) = create_test_db();
        let result = source.validate_columns(
            "SELECT id, name FROM test_table",
            &["id", "name", "missing_column"],
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            FerryError::Source(msg) => {
                assert!(
                    msg.contains("missing_column"),
                    "Error should mention missing column: {}",
                    msg
                );
            }
            _ => panic!("Expected Source error, got {:?}", err),
        }
    }

    #[test]
    fn test_validate_columns_empty_query() {
        let (_dir, source) = create_test_db();
        let result = source.validate_columns("SELECT 1", &["id"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_name() {
        let (_dir, source) = create_test_db();
        assert_eq!(source.name(), "duckdb");
    }

    #[test]
    fn test_from_conn() {
        let dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = dir.path().join("test.duckdb");
        let conn = Connection::open(db_path).expect("Failed to open DuckDB");
        let source = DuckDbSource::from_conn(conn);
        assert_eq!(source.name(), "duckdb");
    }

    #[test]
    fn test_duckdb_type_to_arrow() {
        assert_eq!(duckdb_type_to_arrow("INTEGER"), DataType::Int32);
        assert_eq!(duckdb_type_to_arrow("BIGINT"), DataType::Int64);
        assert_eq!(duckdb_type_to_arrow("VARCHAR"), DataType::Utf8);
        assert_eq!(duckdb_type_to_arrow("BOOLEAN"), DataType::Boolean);
        assert_eq!(duckdb_type_to_arrow("DOUBLE"), DataType::Float64);
        assert_eq!(duckdb_type_to_arrow("FLOAT"), DataType::Float32);
        assert_eq!(duckdb_type_to_arrow("BLOB"), DataType::Binary);
        assert_eq!(duckdb_type_to_arrow("TEXT"), DataType::Utf8);
        assert_eq!(duckdb_type_to_arrow("UNKNOWN_TYPE"), DataType::Utf8);
    }
}
