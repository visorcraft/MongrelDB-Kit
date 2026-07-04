//! Arrow IPC <-> JSON row helpers, shared by the embedded SQL surface
//! ([`Database::sql`] / `sql_arrow` / `sql_rows`) and the optional `remote`
//! HTTP client. Kept here (rather than inside `remote.rs`) so the default
//! embedded build — which has no HTTP dependency — can still decode the Arrow
//! IPC bytes that `MongrelSession::run` produces.

use arrow::array::{
    Array, BooleanArray, Float64Array, Int32Array, Int64Array, NullArray, StringArray,
};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use serde_json::{json, Map, Value};

use crate::error::{KitError, Result};

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
    let schema = batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| std::sync::Arc::new(arrow::datatypes::Schema::empty()));
    let mut out = Vec::new();
    let mut writer =
        FileWriter::try_new(&mut out, &schema).map_err(|e| KitError::Storage(e.to_string()))?;
    for batch in batches {
        writer
            .write(batch)
            .map_err(|e| KitError::Storage(format!("arrow ipc encode: {e}")))?;
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
    let mut rows = Vec::new();
    for batch in batches {
        rows.extend(batch_to_rows(batch)?);
    }
    Ok(rows)
}

fn cell_value(arr: &dyn Array, r: usize) -> Value {
    if arr.is_null(r) {
        return Value::Null;
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return json!(a.value(r));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int32Array>() {
        return json!(a.value(r));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return serde_json::Number::from_f64(a.value(r))
            .map(Value::Number)
            .unwrap_or(Value::Null);
    }
    if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
        return Value::Bool(a.value(r));
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return Value::String(a.value(r).to_string());
    }
    if arr.as_any().downcast_ref::<NullArray>().is_some() {
        return Value::Null;
    }
    // Fallback: stringify unknown types.
    Value::String(format!("{arr:?}"))
}
