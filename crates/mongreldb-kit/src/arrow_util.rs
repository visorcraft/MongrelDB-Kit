//! Arrow IPC <-> JSON row helpers, shared by the embedded SQL surface
//! ([`Database::sql`] / `sql_arrow` / `sql_rows`) and the optional `remote`
//! HTTP client. Kept here (rather than inside `remote.rs`) so the default
//! embedded build — which has no HTTP dependency — can still decode the Arrow
//! IPC bytes that `MongrelSession::run` produces.

use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    NullArray, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use serde_json::{json, Map, Value};

use crate::error::{boxed_query_metadata, KitError, QueryExecutionOutcome, Result};
use crate::SqlOutputLimits;

/// Decode Arrow IPC *file* bytes (the format `MongrelSession` and the daemon
/// both emit) into [`RecordBatch`]es. An empty input yields an empty vec.
pub fn read_arrow_ipc(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let cursor = std::io::Cursor::new(bytes);
    let reader = FileReader::try_new(cursor, None)
        .map_err(|e| KitError::Storage(format!("arrow ipc decode: {e}")))?;
    reader
        .into_iter()
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| KitError::Storage(format!("arrow ipc decode: {e}")))
}

/// Encode `RecordBatch`es as Arrow IPC *file* bytes — the wire format the
/// daemon and the NAPI addon both produce. Mirrors the node addon's
/// `native_cols_to_ipc_from_batches`.
pub fn batches_to_ipc(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    batches_to_ipc_with_checkpoint(batches, || Ok(()))
}

#[doc(hidden)]
pub fn batches_to_ipc_controlled(
    batches: &[RecordBatch],
    query: &mongreldb_query::RegisteredSqlQuery,
) -> Result<Vec<u8>> {
    batches_to_ipc_controlled_with_limits(batches, query, SqlOutputLimits::default())
}

#[doc(hidden)]
pub fn batches_to_ipc_controlled_with_limits(
    batches: &[RecordBatch],
    query: &mongreldb_query::RegisteredSqlQuery,
    limits: SqlOutputLimits,
) -> Result<Vec<u8>> {
    let schema = batches
        .first()
        .map(|batch| batch.schema())
        .unwrap_or_else(|| std::sync::Arc::new(arrow::datatypes::Schema::empty()));
    let exceeded = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut output = LimitedIpcOutput {
        bytes: Vec::new(),
        max_bytes: limits.max_bytes,
        exceeded: std::sync::Arc::clone(&exceeded),
    };
    let mut rows = 0usize;
    {
        let mut writer = match FileWriter::try_new(&mut output, &schema) {
            Ok(writer) => writer,
            Err(error) => {
                return serialization_or_limit_error(query, limits, &exceeded, error.to_string())
            }
        };
        for batch in batches {
            for offset in (0..batch.num_rows()).step_by(256) {
                query.checkpoint().map_err(KitError::from)?;
                let length = 256.min(batch.num_rows() - offset);
                rows = rows.saturating_add(length);
                if rows > limits.max_rows {
                    return result_limit_error(query, limits);
                }
                if let Err(error) = writer.write(&batch.slice(offset, length)) {
                    return serialization_or_limit_error(
                        query,
                        limits,
                        &exceeded,
                        error.to_string(),
                    );
                }
            }
        }
        if let Err(error) = writer.finish() {
            return serialization_or_limit_error(query, limits, &exceeded, error.to_string());
        }
    }
    Ok(output.bytes)
}

fn batches_to_ipc_with_checkpoint(
    batches: &[RecordBatch],
    mut checkpoint: impl FnMut() -> Result<()>,
) -> Result<Vec<u8>> {
    let schema = batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| std::sync::Arc::new(arrow::datatypes::Schema::empty()));
    let mut out = Vec::new();
    let mut writer =
        FileWriter::try_new(&mut out, &schema).map_err(|e| KitError::Storage(e.to_string()))?;
    for batch in batches {
        for offset in (0..batch.num_rows()).step_by(256) {
            checkpoint()?;
            let length = 256.min(batch.num_rows() - offset);
            writer
                .write(&batch.slice(offset, length))
                .map_err(|e| KitError::Storage(format!("arrow ipc encode: {e}")))?;
        }
    }
    writer
        .finish()
        .map_err(|e| KitError::Storage(format!("arrow ipc encode: {e}")))?;
    Ok(out)
}

/// Materialize one `RecordBatch` into JSON-row maps (column name → value).
pub fn batch_to_rows(b: &RecordBatch) -> Result<Vec<Map<String, Value>>> {
    let schema = b.schema();
    let mut rows = Vec::with_capacity(b.num_rows());
    for r in 0..b.num_rows() {
        let mut row = Map::new();
        for (c, field) in schema.fields().iter().enumerate() {
            let name = field.name();
            let arr = b.column(c);
            row.insert(name.clone(), cell_value(arr.as_ref(), r));
        }
        rows.push(row);
    }
    Ok(rows)
}

/// Flatten a slice of batches into a single list of JSON-row maps.
pub fn batches_to_rows(batches: &[RecordBatch]) -> Result<Vec<Map<String, Value>>> {
    batches_to_rows_with_checkpoint(batches, || Ok(()))
}

#[doc(hidden)]
pub fn batches_to_rows_controlled(
    batches: &[RecordBatch],
    query: &mongreldb_query::RegisteredSqlQuery,
) -> Result<Vec<Map<String, Value>>> {
    batches_to_rows_controlled_with_limits(batches, query, SqlOutputLimits::default())
}

#[doc(hidden)]
pub fn batches_to_rows_controlled_with_limits(
    batches: &[RecordBatch],
    query: &mongreldb_query::RegisteredSqlQuery,
    limits: SqlOutputLimits,
) -> Result<Vec<Map<String, Value>>> {
    let mut rows = Vec::new();
    let mut bytes = 2usize;
    for batch in batches {
        let schema = batch.schema();
        for row_index in 0..batch.num_rows() {
            if row_index % 256 == 0 {
                query.checkpoint().map_err(KitError::from)?;
            }
            if rows.len() >= limits.max_rows {
                return result_limit_error(query, limits);
            }
            let mut row = Map::new();
            for (column_index, field) in schema.fields().iter().enumerate() {
                row.insert(
                    field.name().clone(),
                    cell_value(batch.column(column_index).as_ref(), row_index),
                );
            }
            let row_bytes = serde_json::to_vec(&row)
                .map_err(|error| controlled_serialization_error(query, error.to_string()))?
                .len();
            bytes = bytes
                .saturating_add(row_bytes)
                .saturating_add(usize::from(!rows.is_empty()));
            if bytes > limits.max_bytes {
                return result_limit_error(query, limits);
            }
            rows.push(row);
        }
    }
    Ok(rows)
}

fn result_limit_error<T>(
    query: &mongreldb_query::RegisteredSqlQuery,
    limits: SqlOutputLimits,
) -> Result<T> {
    Err(controlled_result_limit_error(query, limits))
}

#[doc(hidden)]
pub fn controlled_result_limit_error(
    query: &mongreldb_query::RegisteredSqlQuery,
    limits: SqlOutputLimits,
) -> KitError {
    let status = query.status();
    KitError::ResultLimitExceeded {
        query_id: Some(query.id().to_string().into_boxed_str()),
        max_rows: Some(Box::new(limits.max_rows)),
        max_bytes: Some(Box::new(limits.max_bytes)),
        outcome: Box::new(QueryExecutionOutcome {
            committed: status.durable_outcome.committed,
            committed_statements: Some(status.durable_outcome.committed_statements),
            last_commit_epoch: status.durable_outcome.last_commit_epoch,
            first_commit_statement_index: status.durable_outcome.first_commit_statement_index,
            last_commit_statement_index: status.durable_outcome.last_commit_statement_index,
            completed_statements: status.completed_statements,
            statement_index: status.statement_index,
        }),
        message: format!(
            "SQL result exceeds {} rows or {} bytes",
            limits.max_rows, limits.max_bytes
        )
        .into_boxed_str(),
        metadata: boxed_query_metadata(None, None, Some(false), Some("serializing")),
    }
}

fn serialization_or_limit_error<T>(
    query: &mongreldb_query::RegisteredSqlQuery,
    limits: SqlOutputLimits,
    exceeded: &std::sync::atomic::AtomicBool,
    message: String,
) -> Result<T> {
    if exceeded.load(std::sync::atomic::Ordering::Acquire) {
        return result_limit_error(query, limits);
    }
    Err(controlled_serialization_error(query, message))
}

#[doc(hidden)]
pub fn controlled_serialization_error(
    query: &mongreldb_query::RegisteredSqlQuery,
    message: String,
) -> KitError {
    let status = query.status();
    KitError::SerializationFailed {
        query_id: Some(query.id().to_string()),
        outcome: Box::new(QueryExecutionOutcome {
            committed: status.durable_outcome.committed,
            committed_statements: Some(status.durable_outcome.committed_statements),
            last_commit_epoch: status.durable_outcome.last_commit_epoch,
            first_commit_statement_index: status.durable_outcome.first_commit_statement_index,
            last_commit_statement_index: status.durable_outcome.last_commit_statement_index,
            completed_statements: status.completed_statements,
            statement_index: status.statement_index,
        }),
        message: message.into_boxed_str(),
        metadata: boxed_query_metadata(None, None, Some(false), Some("serializing")),
    }
}

struct LimitedIpcOutput {
    bytes: Vec<u8>,
    max_bytes: usize,
    exceeded: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl std::io::Write for LimitedIpcOutput {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.bytes.len().saturating_add(bytes.len()) > self.max_bytes {
            self.exceeded
                .store(true, std::sync::atomic::Ordering::Release);
            return Err(std::io::Error::other("SQL result byte limit exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn batches_to_rows_with_checkpoint(
    batches: &[RecordBatch],
    mut checkpoint: impl FnMut() -> Result<()>,
) -> Result<Vec<Map<String, Value>>> {
    let mut rows = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        for row_index in 0..batch.num_rows() {
            if row_index % 256 == 0 {
                checkpoint()?;
            }
            let mut row = Map::new();
            for (column_index, field) in schema.fields().iter().enumerate() {
                row.insert(
                    field.name().clone(),
                    cell_value(batch.column(column_index).as_ref(), row_index),
                );
            }
            rows.push(row);
        }
    }
    Ok(rows)
}

fn cell_value(arr: &dyn Array, r: usize) -> Value {
    if arr.is_null(r) {
        return Value::Null;
    }
    // Signed integers → i64
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return json!(a.value(r));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int32Array>() {
        return json!(a.value(r));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int16Array>() {
        return json!(a.value(r));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int8Array>() {
        return json!(a.value(r));
    }
    // Unsigned integers → i64 (JSON has no unsigned; cast is lossless for normal values)
    if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
        return json!(a.value(r) as i64);
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt32Array>() {
        return json!(a.value(r) as i64);
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt16Array>() {
        return json!(a.value(r) as i64);
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt8Array>() {
        return json!(a.value(r) as i64);
    }
    // Floats
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return serde_json::Number::from_f64(a.value(r))
            .map(Value::Number)
            .unwrap_or(Value::Null);
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float32Array>() {
        return serde_json::Number::from_f64(a.value(r) as f64)
            .map(Value::Number)
            .unwrap_or(Value::Null);
    }
    // Boolean
    if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
        return Value::Bool(a.value(r));
    }
    // Strings
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return Value::String(a.value(r).to_string());
    }
    if arr.as_any().downcast_ref::<NullArray>().is_some() {
        return Value::Null;
    }
    // Fallback: stringify unknown types.
    Value::String(format!("{arr:?}"))
}

#[cfg(test)]
mod tests {
    use super::{batches_to_ipc_controlled_with_limits, batches_to_rows_controlled_with_limits};
    use crate::{KitError, SqlOutputLimits};
    use arrow::array::Int64Array;
    use arrow::record_batch::RecordBatch;
    use mongreldb_query::{SqlQueryOptions, SqlQueryRegistry};
    use std::sync::Arc;

    fn batch() -> RecordBatch {
        RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int64Array::from(vec![1, 2])) as arrow::array::ArrayRef,
        )])
        .unwrap()
    }

    #[test]
    fn controlled_converters_enforce_row_and_byte_limits() {
        for convert in ["arrow", "rows"] {
            for limits in [
                SqlOutputLimits {
                    max_rows: 1,
                    max_bytes: 1_024,
                },
                SqlOutputLimits {
                    max_rows: 10,
                    max_bytes: 1,
                },
            ] {
                let registry = Arc::new(SqlQueryRegistry::default());
                let query = registry.register(SqlQueryOptions::default()).unwrap();
                query.record_commit(2, 17);
                let batch = batch();
                let error = if convert == "arrow" {
                    batches_to_ipc_controlled_with_limits(
                        std::slice::from_ref(&batch),
                        &query,
                        limits,
                    )
                    .map(|_| ())
                    .unwrap_err()
                } else {
                    batches_to_rows_controlled_with_limits(
                        std::slice::from_ref(&batch),
                        &query,
                        limits,
                    )
                    .map(|_| ())
                    .unwrap_err()
                };
                assert!(matches!(
                    error,
                    KitError::ResultLimitExceeded {
                        outcome,
                        ..
                    } if outcome.committed
                        && outcome.committed_statements == Some(1)
                        && outcome.last_commit_epoch == Some(17)
                        && outcome.first_commit_statement_index == Some(2)
                        && outcome.last_commit_statement_index == Some(2)
                ));
                query.fail_result_limit();
                assert_eq!(
                    query.status().terminal_error.unwrap().code,
                    "RESULT_LIMIT_EXCEEDED"
                );
            }
        }
    }
}
