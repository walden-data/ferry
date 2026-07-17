//! Shared utilities for destination connectors.
//!
//! The Arrow → `serde_json::Value` conversion here is extracted from
//! `file.rs` so the REST destination (and any future destination that emits
//! JSON) can reuse it without drift.

use arrow_array::cast::*;
use arrow_array::types::*;
use arrow_array::*;
use arrow_cast::display::array_value_to_string;
use arrow_schema::*;
use serde_json::{Map, Value};

/// Convert a single cell in an Arrow column to a `serde_json::Value`.
///
/// Null → `Value::Null`; boolean/integer/float → proper JSON number/bool;
/// all other types (strings, dates, timestamps, …) → string representation
/// via `array_value_to_string`.
pub fn cell_to_json_value(column: &ArrayRef, row_idx: usize) -> Value {
    if column.is_null(row_idx) {
        return Value::Null;
    }

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

/// Convert a single row of a `RecordBatch` to a JSON object (`Map<String, Value>`).
pub fn row_to_json_object(batch: &RecordBatch, row_idx: usize) -> Map<String, Value> {
    let schema = batch.schema();
    let num_cols = batch.num_columns();
    let mut obj = Map::new();
    for col_idx in 0..num_cols {
        let field = schema.field(col_idx);
        let column = batch.column(col_idx);
        let col_name = field.name().clone();
        let value = cell_to_json_value(column, row_idx);
        obj.insert(col_name, value);
    }
    obj
}

/// Convert all rows of a `RecordBatch` to a `Vec<serde_json::Value>` (one
/// object per row). Each row is a JSON object keyed by column name.
pub fn batch_to_json_rows(batch: &RecordBatch) -> Vec<Value> {
    (0..batch.num_rows())
        .map(|i| Value::Object(row_to_json_object(batch, i)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::{Float64Array, Int32Array, StringArray};

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

    #[test]
    fn test_batch_to_json_rows() {
        let batch = create_test_batch();
        let rows = batch_to_json_rows(&batch);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0]["id"], 1);
        assert_eq!(rows[0]["name"], "Alice");
        assert_eq!(rows[0]["score"], 95.5);
        assert_eq!(rows[2]["score"], Value::Null);
    }
}
