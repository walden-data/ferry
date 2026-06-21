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
        };

        let result = dest.write(&batch, &config).await.unwrap();
        assert_eq!(result.rows_written, 5);
        assert!(result.errors.is_empty());
    }
}
