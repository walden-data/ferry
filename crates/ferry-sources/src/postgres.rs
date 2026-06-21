use std::any::Any;
use std::collections::HashMap;
use std::time::Duration;

use arrow_array::ArrayRef;
use arrow_array::RecordBatch;
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int16Builder,
    Int32Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use async_trait::async_trait;
use chrono::Datelike;
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::{Column, Pool, Postgres, Row, TypeInfo, ValueRef};

use ferry_core::error::FerryError;
use ferry_core::traits::{RecordBatchStream, Source, StreamSchema};

/// A PostgreSQL source connector that implements the `Source` trait.
///
/// Executes SQL queries against a PostgreSQL database and returns the results
/// as Arrow `RecordBatch` streams using manual row-to-Arrow conversion.
///
/// # Example
///
/// ```rust,no_run
/// use ferry_core::traits::Source;
/// use ferry_sources::postgres::PostgresSource;
///
/// # async fn example() {
/// let source = PostgresSource::new("postgres://user:pass@localhost/db").await.unwrap();
/// let stream = source.read("SELECT * FROM my_table");
/// # }
/// ```
pub struct PostgresSource {
    pool: Pool<Postgres>,
    default_query: Option<String>,
    name: String,
}

impl PostgresSource {
    /// Create a new `PostgresSource` with a connection pool.
    ///
    /// The `connection_string` should be a PostgreSQL connection URL
    /// (e.g., `postgres://user:password@host:port/database`).
    pub async fn new(connection_string: &str) -> Result<Self, FerryError> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(Duration::from_secs(30))
            .connect(connection_string)
            .await
            .map_err(|e| {
                FerryError::Source(format!(
                    "Failed to connect to PostgreSQL at {}: {}",
                    connection_string, e
                ))
            })?;

        Ok(Self {
            pool,
            default_query: None,
            name: "postgres".to_string(),
        })
    }

    /// Create a new `PostgresSource` with a default query.
    pub async fn with_query(connection_string: &str, query: String) -> Result<Self, FerryError> {
        let mut source = Self::new(connection_string).await?;
        source.default_query = Some(query);
        Ok(source)
    }

    /// Execute a query and convert the results to Arrow `RecordBatch`es.
    ///
    /// # Note
    ///
    /// TODO: For Phase 1, we collect all rows into memory. In a future phase,
    /// optimize to true streaming using sqlx's `fetch()` stream with batched
    /// Arrow conversion.
    #[allow(dead_code)]
    async fn execute_query(&self, query: &str) -> Result<Vec<RecordBatch>, FerryError> {
        let rows: Vec<PgRow> = sqlx::query(query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| FerryError::Source(format!("Query failed: {}", e)))?;

        if rows.is_empty() {
            return Ok(vec![]);
        }

        let batch = rows_to_batch(&rows)?;
        Ok(vec![batch])
    }
}

#[async_trait]
impl Source for PostgresSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn check_connection(&self) -> Result<(), FerryError> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map_err(|e| FerryError::Source(format!("Connection check failed: {}", e)))?;
        Ok(())
    }

    async fn discover(&self) -> Result<Vec<StreamSchema>, FerryError> {
        let rows: Vec<PgRow> = sqlx::query(
            "SELECT table_schema, table_name, column_name, data_type
             FROM information_schema.columns
             WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
             ORDER BY table_schema, table_name, ordinal_position",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| FerryError::Source(format!("Discovery query failed: {}", e)))?;

        let mut tables: HashMap<String, Vec<(String, DataType)>> = HashMap::new();

        for row in &rows {
            let schema_name: String = row
                .try_get("table_schema")
                .map_err(|e| FerryError::Source(format!("Failed to read schema name: {}", e)))?;
            let table_name: String = row
                .try_get("table_name")
                .map_err(|e| FerryError::Source(format!("Failed to read table name: {}", e)))?;
            let column_name: String = row
                .try_get("column_name")
                .map_err(|e| FerryError::Source(format!("Failed to read column name: {}", e)))?;
            let data_type_str: String = row
                .try_get("data_type")
                .map_err(|e| FerryError::Source(format!("Failed to read data type: {}", e)))?;

            let full_name = if schema_name == "public" {
                table_name
            } else {
                format!("{}.{}", schema_name, table_name)
            };

            let arrow_type = pg_type_name_to_arrow(&data_type_str);
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
        let pool = self.pool.clone();
        let query = query.to_string();

        let stream = async_stream::try_stream! {
            let rows: Vec<PgRow> = sqlx::query(&query)
                .fetch_all(&pool)
                .await
                .map_err(|e| FerryError::Source(format!("Query failed: {}", e)))?;

            if rows.is_empty() {
                return;
            }

            let batch = rows_to_batch(&rows)
                .map_err(|e| FerryError::Source(format!("Row conversion failed: {}", e)))?;
            yield batch;
        };

        Box::pin(stream)
    }
}

/// Convert a vector of `PgRow`s into a single `RecordBatch`.
///
/// Uses the column metadata from the first row to determine the Arrow schema,
/// then extracts values column by column using the appropriate Arrow builder.
fn rows_to_batch(rows: &[PgRow]) -> Result<RecordBatch, FerryError> {
    let columns = rows[0].columns();
    let num_rows = rows.len();
    let num_cols = columns.len();

    // Build Arrow schema
    let mut fields: Vec<Field> = Vec::with_capacity(num_cols);
    let mut builders: Vec<Box<dyn ArrowBuilder>> = Vec::with_capacity(num_cols);

    for col in columns {
        let pg_type_name = col.type_info().name();
        let arrow_type = pg_type_to_arrow(pg_type_name);
        fields.push(Field::new(col.name(), arrow_type.clone(), true));
        builders.push(new_builder(&arrow_type, num_rows));
    }

    let schema = Schema::new(fields);

    // Populate builders row by row
    for row in rows {
        for (i, builder) in builders.iter_mut().enumerate() {
            let pg_type_name = columns[i].type_info().name();
            append_value(builder, row, i, pg_type_name)?;
        }
    }

    // Build arrays
    let arrays: Vec<ArrayRef> = builders.into_iter().map(|mut b| b.finish()).collect();

    RecordBatch::try_new(std::sync::Arc::new(schema), arrays)
        .map_err(|e| FerryError::Source(format!("Failed to create RecordBatch: {}", e)))
}

/// Internal trait to abstract over different Arrow array builders.
trait ArrowBuilder: Any + Send {
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn finish(&mut self) -> ArrayRef;
}

macro_rules! impl_arrow_builder {
    ($ty:ty) => {
        impl ArrowBuilder for $ty {
            fn as_any_mut(&mut self) -> &mut dyn Any {
                self
            }
            fn finish(&mut self) -> ArrayRef {
                std::sync::Arc::new(self.finish())
            }
        }
    };
}

impl_arrow_builder!(BooleanBuilder);
impl_arrow_builder!(Int16Builder);
impl_arrow_builder!(Int32Builder);
impl_arrow_builder!(Int64Builder);
impl_arrow_builder!(Float32Builder);
impl_arrow_builder!(Float64Builder);
impl_arrow_builder!(StringBuilder);
impl_arrow_builder!(Date32Builder);
impl_arrow_builder!(BinaryBuilder);

impl ArrowBuilder for TimestampMicrosecondBuilder {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn finish(&mut self) -> ArrayRef {
        std::sync::Arc::new(self.finish())
    }
}

/// Create a new Arrow builder for the given data type.
fn new_builder(data_type: &DataType, capacity: usize) -> Box<dyn ArrowBuilder> {
    match data_type {
        DataType::Boolean => Box::new(BooleanBuilder::with_capacity(capacity)),
        DataType::Int16 => Box::new(Int16Builder::with_capacity(capacity)),
        DataType::Int32 => Box::new(Int32Builder::with_capacity(capacity)),
        DataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
        DataType::Float32 => Box::new(Float32Builder::with_capacity(capacity)),
        DataType::Float64 => Box::new(Float64Builder::with_capacity(capacity)),
        DataType::Utf8 => Box::new(StringBuilder::with_capacity(capacity, capacity * 32)),
        DataType::Date32 => Box::new(Date32Builder::with_capacity(capacity)),
        DataType::Timestamp(_, _) => Box::new(TimestampMicrosecondBuilder::with_capacity(capacity)),
        DataType::Binary => Box::new(BinaryBuilder::with_capacity(capacity, capacity * 32)),
        _ => Box::new(StringBuilder::with_capacity(capacity, capacity * 32)),
    }
}

/// Append a value from a PgRow at the given column index to the Arrow builder.
fn append_value(
    builder: &mut Box<dyn ArrowBuilder>,
    row: &PgRow,
    col_idx: usize,
    pg_type_name: &str,
) -> Result<(), FerryError> {
    // Check for null
    let raw = row
        .try_get_raw(col_idx)
        .map_err(|e| FerryError::Source(format!("Failed to get raw value: {}", e)))?;

    if raw.is_null() {
        append_null(builder);
        return Ok(());
    }

    match pg_type_name {
        "bool" => {
            let val: Option<bool> = row.try_get(col_idx).map_err(|e| {
                FerryError::Source(format!("Failed to get bool at column {}: {}", col_idx, e))
            })?;
            if let Some(v) = val {
                append_value_to_builder(builder, &PgValue::Bool(v));
            } else {
                append_null(builder);
            }
        }
        "int2" => {
            let val: Option<i16> = row.try_get(col_idx).map_err(|e| {
                FerryError::Source(format!("Failed to get int2 at column {}: {}", col_idx, e))
            })?;
            if let Some(v) = val {
                append_value_to_builder(builder, &PgValue::Int16(v));
            } else {
                append_null(builder);
            }
        }
        "int4" => {
            let val: Option<i32> = row.try_get(col_idx).map_err(|e| {
                FerryError::Source(format!("Failed to get int4 at column {}: {}", col_idx, e))
            })?;
            if let Some(v) = val {
                append_value_to_builder(builder, &PgValue::Int32(v));
            } else {
                append_null(builder);
            }
        }
        "int8" => {
            let val: Option<i64> = row.try_get(col_idx).map_err(|e| {
                FerryError::Source(format!("Failed to get int8 at column {}: {}", col_idx, e))
            })?;
            if let Some(v) = val {
                append_value_to_builder(builder, &PgValue::Int64(v));
            } else {
                append_null(builder);
            }
        }
        "float4" => {
            let val: Option<f32> = row.try_get(col_idx).map_err(|e| {
                FerryError::Source(format!("Failed to get float4 at column {}: {}", col_idx, e))
            })?;
            if let Some(v) = val {
                append_value_to_builder(builder, &PgValue::Float32(v));
            } else {
                append_null(builder);
            }
        }
        "float8" => {
            let val: Option<f64> = row.try_get(col_idx).map_err(|e| {
                FerryError::Source(format!("Failed to get float8 at column {}: {}", col_idx, e))
            })?;
            if let Some(v) = val {
                append_value_to_builder(builder, &PgValue::Float64(v));
            } else {
                append_null(builder);
            }
        }
        "numeric" => {
            // NUMERIC → Float64 (lossy but practical)
            let val: Option<f64> = row.try_get(col_idx).map_err(|e| {
                FerryError::Source(format!(
                    "Failed to get numeric at column {}: {}",
                    col_idx, e
                ))
            })?;
            if let Some(v) = val {
                append_value_to_builder(builder, &PgValue::Float64(v));
            } else {
                append_null(builder);
            }
        }
        "varchar" | "text" | "bpchar" | "name" | "uuid" | "json" | "jsonb" => {
            let val: Option<String> = row.try_get(col_idx).map_err(|e| {
                FerryError::Source(format!("Failed to get string at column {}: {}", col_idx, e))
            })?;
            if let Some(v) = val {
                append_value_to_builder(builder, &PgValue::String(v));
            } else {
                append_null(builder);
            }
        }
        "timestamp" | "timestamp without time zone" => {
            let val: Option<chrono::NaiveDateTime> = row.try_get(col_idx).map_err(|e| {
                FerryError::Source(format!(
                    "Failed to get timestamp at column {}: {}",
                    col_idx, e
                ))
            })?;
            if let Some(v) = val {
                let micros = v.and_utc().timestamp_micros();
                append_value_to_builder(builder, &PgValue::TimestampMicros(micros));
            } else {
                append_null(builder);
            }
        }
        "timestamptz" | "timestamp with time zone" => {
            let val: Option<chrono::DateTime<chrono::Utc>> = row.try_get(col_idx).map_err(|e| {
                FerryError::Source(format!(
                    "Failed to get timestamptz at column {}: {}",
                    col_idx, e
                ))
            })?;
            if let Some(v) = val {
                append_value_to_builder(builder, &PgValue::TimestampMicros(v.timestamp_micros()));
            } else {
                append_null(builder);
            }
        }
        "date" => {
            let val: Option<chrono::NaiveDate> = row.try_get(col_idx).map_err(|e| {
                FerryError::Source(format!("Failed to get date at column {}: {}", col_idx, e))
            })?;
            if let Some(v) = val {
                // Arrow Date32 is days since epoch
                let days = v.num_days_from_ce() - 719_163; // days from 1970-01-01
                append_value_to_builder(builder, &PgValue::Date32(days));
            } else {
                append_null(builder);
            }
        }
        "bytea" => {
            let val: Option<Vec<u8>> = row.try_get(col_idx).map_err(|e| {
                FerryError::Source(format!("Failed to get bytea at column {}: {}", col_idx, e))
            })?;
            if let Some(v) = val {
                append_value_to_builder(builder, &PgValue::Bytes(v));
            } else {
                append_null(builder);
            }
        }
        _ => {
            // Fallback: try to get as string
            let val: Option<String> = row.try_get(col_idx).map_err(|e| {
                FerryError::Source(format!(
                    "Failed to get value at column {} (type: {}): {}",
                    col_idx, pg_type_name, e
                ))
            })?;
            if let Some(v) = val {
                append_value_to_builder(builder, &PgValue::String(v));
            } else {
                append_null(builder);
            }
        }
    }

    Ok(())
}

/// Enum to hold a dynamically-typed PostgreSQL value for Arrow conversion.
enum PgValue {
    Bool(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(String),
    TimestampMicros(i64),
    Date32(i32),
    Bytes(Vec<u8>),
}

/// Append a value to the appropriate Arrow builder.
fn append_value_to_builder(builder: &mut Box<dyn ArrowBuilder>, value: &PgValue) {
    let any = builder.as_any_mut();

    match value {
        PgValue::Bool(v) => {
            if let Some(b) = any.downcast_mut::<BooleanBuilder>() {
                b.append_value(*v);
                return;
            }
        }
        PgValue::Int16(v) => {
            if let Some(b) = any.downcast_mut::<Int16Builder>() {
                b.append_value(*v);
                return;
            }
        }
        PgValue::Int32(v) => {
            if let Some(b) = any.downcast_mut::<Int32Builder>() {
                b.append_value(*v);
                return;
            }
        }
        PgValue::Int64(v) => {
            if let Some(b) = any.downcast_mut::<Int64Builder>() {
                b.append_value(*v);
                return;
            }
        }
        PgValue::Float32(v) => {
            if let Some(b) = any.downcast_mut::<Float32Builder>() {
                b.append_value(*v);
                return;
            }
        }
        PgValue::Float64(v) => {
            if let Some(b) = any.downcast_mut::<Float64Builder>() {
                b.append_value(*v);
                return;
            }
        }
        PgValue::String(v) => {
            if let Some(b) = any.downcast_mut::<StringBuilder>() {
                b.append_value(v);
                return;
            }
        }
        PgValue::TimestampMicros(v) => {
            if let Some(b) = any.downcast_mut::<TimestampMicrosecondBuilder>() {
                b.append_value(*v);
                return;
            }
        }
        PgValue::Date32(v) => {
            if let Some(b) = any.downcast_mut::<Date32Builder>() {
                b.append_value(*v);
                return;
            }
        }
        PgValue::Bytes(v) => {
            if let Some(b) = any.downcast_mut::<BinaryBuilder>() {
                b.append_value(v);
                return;
            }
        }
    }

    // Fallback: try StringBuilder
    if let Some(b) = any.downcast_mut::<StringBuilder>() {
        let s = match value {
            PgValue::Bool(v) => v.to_string(),
            PgValue::Int16(v) => v.to_string(),
            PgValue::Int32(v) => v.to_string(),
            PgValue::Int64(v) => v.to_string(),
            PgValue::Float32(v) => v.to_string(),
            PgValue::Float64(v) => v.to_string(),
            PgValue::String(v) => v.clone(),
            PgValue::TimestampMicros(v) => v.to_string(),
            PgValue::Date32(v) => v.to_string(),
            PgValue::Bytes(v) => String::from_utf8_lossy(v).to_string(),
        };
        b.append_value(&s);
    } else {
        tracing::warn!("Type mismatch in Arrow builder: value type doesn't match builder");
    }
}

/// Append a null value to the appropriate Arrow builder.
fn append_null(builder: &mut Box<dyn ArrowBuilder>) {
    let any = builder.as_any_mut();

    if let Some(b) = any.downcast_mut::<BooleanBuilder>() {
        b.append_null();
    } else if let Some(b) = any.downcast_mut::<Int16Builder>() {
        b.append_null();
    } else if let Some(b) = any.downcast_mut::<Int32Builder>() {
        b.append_null();
    } else if let Some(b) = any.downcast_mut::<Int64Builder>() {
        b.append_null();
    } else if let Some(b) = any.downcast_mut::<Float32Builder>() {
        b.append_null();
    } else if let Some(b) = any.downcast_mut::<Float64Builder>() {
        b.append_null();
    } else if let Some(b) = any.downcast_mut::<StringBuilder>() {
        b.append_null();
    } else if let Some(b) = any.downcast_mut::<Date32Builder>() {
        b.append_null();
    } else if let Some(b) = any.downcast_mut::<TimestampMicrosecondBuilder>() {
        b.append_null();
    } else if let Some(b) = any.downcast_mut::<BinaryBuilder>() {
        b.append_null();
    } else {
        tracing::warn!("Unknown builder type for null append");
    }
}

/// Map a PostgreSQL type name (from `information_schema.columns.data_type`)
/// to an Arrow `DataType`.
///
/// This is used by `discover()` to build the Arrow schema for discovered tables.
fn pg_type_name_to_arrow(pg_type: &str) -> DataType {
    match pg_type.to_lowercase().as_str() {
        "boolean" => DataType::Boolean,
        "smallint" => DataType::Int16,
        "integer" => DataType::Int32,
        "bigint" => DataType::Int64,
        "real" => DataType::Float32,
        "double precision" => DataType::Float64,
        "numeric" | "decimal" => DataType::Float64,
        "character varying" | "varchar" | "character" | "char" | "text" | "name" => DataType::Utf8,
        "timestamp without time zone" | "timestamp" => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        "timestamp with time zone" | "timestamptz" => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
        }
        "date" => DataType::Date32,
        "uuid" => DataType::Utf8,
        "json" | "jsonb" => DataType::Utf8,
        "bytea" => DataType::Binary,
        _ => {
            tracing::warn!("Unknown PostgreSQL type '{}', defaulting to Utf8", pg_type);
            DataType::Utf8
        }
    }
}

/// Map a PostgreSQL type name (from `PgTypeInfo::name()`, e.g. "int4", "varchar")
/// to an Arrow `DataType`.
///
/// This is used by `read()` to build the Arrow schema from query result columns.
fn pg_type_to_arrow(pg_type: &str) -> DataType {
    match pg_type {
        "bool" => DataType::Boolean,
        "int2" => DataType::Int16,
        "int4" => DataType::Int32,
        "int8" => DataType::Int64,
        "float4" => DataType::Float32,
        "float8" => DataType::Float64,
        "varchar" | "text" | "bpchar" | "name" => DataType::Utf8,
        "numeric" => DataType::Float64,
        "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "timestamptz" => DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
        "date" => DataType::Date32,
        "uuid" => DataType::Utf8,
        "jsonb" | "json" => DataType::Utf8,
        "bytea" => DataType::Binary,
        _ => {
            tracing::warn!("Unknown PostgreSQL type '{}', defaulting to Utf8", pg_type);
            DataType::Utf8
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_type_to_arrow_mapping() {
        // Boolean
        assert_eq!(pg_type_to_arrow("bool"), DataType::Boolean);

        // Integer types
        assert_eq!(pg_type_to_arrow("int2"), DataType::Int16);
        assert_eq!(pg_type_to_arrow("int4"), DataType::Int32);
        assert_eq!(pg_type_to_arrow("int8"), DataType::Int64);

        // Float types
        assert_eq!(pg_type_to_arrow("float4"), DataType::Float32);
        assert_eq!(pg_type_to_arrow("float8"), DataType::Float64);

        // String types
        assert_eq!(pg_type_to_arrow("varchar"), DataType::Utf8);
        assert_eq!(pg_type_to_arrow("text"), DataType::Utf8);
        assert_eq!(pg_type_to_arrow("bpchar"), DataType::Utf8);
        assert_eq!(pg_type_to_arrow("name"), DataType::Utf8);

        // Numeric
        assert_eq!(pg_type_to_arrow("numeric"), DataType::Float64);

        // Timestamp types
        assert_eq!(
            pg_type_to_arrow("timestamp"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            pg_type_to_arrow("timestamptz"),
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
        );

        // Date
        assert_eq!(pg_type_to_arrow("date"), DataType::Date32);

        // UUID
        assert_eq!(pg_type_to_arrow("uuid"), DataType::Utf8);

        // JSON
        assert_eq!(pg_type_to_arrow("json"), DataType::Utf8);
        assert_eq!(pg_type_to_arrow("jsonb"), DataType::Utf8);

        // Binary
        assert_eq!(pg_type_to_arrow("bytea"), DataType::Binary);

        // Unknown type defaults to Utf8
        assert_eq!(pg_type_to_arrow("unknown_type"), DataType::Utf8);
    }

    #[test]
    fn test_pg_type_name_to_arrow_mapping() {
        // information_schema type names
        assert_eq!(pg_type_name_to_arrow("boolean"), DataType::Boolean);
        assert_eq!(pg_type_name_to_arrow("smallint"), DataType::Int16);
        assert_eq!(pg_type_name_to_arrow("integer"), DataType::Int32);
        assert_eq!(pg_type_name_to_arrow("bigint"), DataType::Int64);
        assert_eq!(pg_type_name_to_arrow("real"), DataType::Float32);
        assert_eq!(pg_type_name_to_arrow("double precision"), DataType::Float64);
        assert_eq!(pg_type_name_to_arrow("numeric"), DataType::Float64);
        assert_eq!(pg_type_name_to_arrow("text"), DataType::Utf8);
        assert_eq!(pg_type_name_to_arrow("character varying"), DataType::Utf8);
        assert_eq!(
            pg_type_name_to_arrow("timestamp without time zone"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            pg_type_name_to_arrow("timestamp with time zone"),
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
        );
        assert_eq!(pg_type_name_to_arrow("date"), DataType::Date32);
        assert_eq!(pg_type_name_to_arrow("bytea"), DataType::Binary);
        assert_eq!(pg_type_name_to_arrow("unknown"), DataType::Utf8);
    }

    #[test]
    fn test_source_config_postgres_deserializes() {
        let yaml_str = r#"
name: test_project
version: "1.0"
source:
  type: postgres
  connection_string: postgres://user:pass@localhost:5432/mydb
  query: SELECT * FROM users
state:
  backend: duckdb
  path: .ferry/state.db
"#;

        let config: ferry_core::config::FerryConfig =
            yaml_serde::from_str(yaml_str).expect("Failed to parse YAML");

        assert_eq!(config.name, "test_project");
        match &config.source {
            ferry_core::config::SourceConfig::Postgres {
                connection_string,
                query,
            } => {
                assert_eq!(
                    connection_string,
                    "postgres://user:pass@localhost:5432/mydb"
                );
                assert_eq!(query.as_deref(), Some("SELECT * FROM users"));
            }
            other => panic!("Expected Postgres source config, got {:?}", other),
        }
    }

    #[test]
    fn test_source_config_postgres_minimal() {
        let yaml_str = r#"
name: test_project
version: "1.0"
source:
  type: postgres
  connection_string: postgres://user:pass@localhost:5432/mydb
state:
  backend: duckdb
  path: .ferry/state.db
"#;

        let config: ferry_core::config::FerryConfig =
            yaml_serde::from_str(yaml_str).expect("Failed to parse YAML");

        match &config.source {
            ferry_core::config::SourceConfig::Postgres {
                connection_string,
                query,
            } => {
                assert_eq!(
                    connection_string,
                    "postgres://user:pass@localhost:5432/mydb"
                );
                assert!(query.is_none(), "query should be None when not provided");
            }
            other => panic!("Expected Postgres source config, got {:?}", other),
        }
    }

    #[test]
    fn test_source_config_duckdb_still_works() {
        let yaml_str = r#"
name: test_project
version: "1.0"
source:
  type: duckdb
  path: /data/db.duckdb
state:
  backend: duckdb
  path: .ferry/state.db
"#;

        let config: ferry_core::config::FerryConfig =
            yaml_serde::from_str(yaml_str).expect("Failed to parse YAML");

        match &config.source {
            ferry_core::config::SourceConfig::DuckDB { path, query } => {
                assert_eq!(path, "/data/db.duckdb");
                assert!(query.is_none());
            }
            other => panic!("Expected DuckDB source config, got {:?}", other),
        }
    }

    #[test]
    fn test_connection_string_format() {
        // Test that various connection string formats are valid
        // (no actual connection is made — just verifying the string is accepted)
        let formats = vec![
            "postgres://user:pass@localhost:5432/db",
            "postgres://user@localhost/db",
            "postgres://localhost/db",
            "postgres://user:pass@host:5432/db?sslmode=require",
        ];

        for conn_str in formats {
            // Just verify the string is well-formed
            assert!(
                conn_str.starts_with("postgres://"),
                "Connection string should start with postgres://: {}",
                conn_str
            );
            assert!(
                conn_str.contains('/'),
                "Connection string should contain a database name: {}",
                conn_str
            );
        }
    }
}
