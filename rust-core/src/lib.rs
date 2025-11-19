use wasm_bindgen::prelude::*;
use js_sys::Error as JsError;

use arrow_array::{RecordBatch, BooleanArray, Float64Array};
use arrow_schema::{ArrowError, SchemaRef};
use arrow_ipc::writer::StreamWriter;
use arrow_csv::reader::{ReaderBuilder, Format};

use arrow::compute::kernels::filter::filter_record_batch;

use once_cell::sync::Lazy;
use std::sync::{Mutex, Arc};
use std::io::Cursor;

/// Global schema + batches
static STORED_SCHEMA: Lazy<Mutex<Option<SchemaRef>>> =
    Lazy::new(|| Mutex::new(None));

static STORED_BATCHES: Lazy<Mutex<Option<Vec<RecordBatch>>>> =
    Lazy::new(|| Mutex::new(None));

/// A clean "string → JsValue" error builder
fn js_err(msg: &str) -> JsValue {
    JsError::new(msg).into()
}

/// ArrowError → JsValue mapper
fn js_err_arrow(e: ArrowError) -> JsValue {
    JsError::new(&format!("Arrow Error: {}", e)).into()
}


/// ------------------------------------------------------------------
///   CSV → Arrow IPC → store schema + batches
/// ------------------------------------------------------------------
#[wasm_bindgen]
pub fn csvtoarrow(bytes: &[u8]) -> Result<Vec<u8>, JsValue> {

    let mut cursor = Cursor::new(bytes);

    let fmt = Format::default().with_header(true);
    let (schema, _) = fmt.infer_schema(&mut cursor, None).map_err(js_err_arrow)?;
    let schema: SchemaRef = Arc::new(schema);

    cursor.set_position(0);


    let reader = ReaderBuilder::new(schema.clone())
        .with_header(true)
        .build(cursor)
        .map_err(js_err_arrow)?;

    let batches: Vec<RecordBatch> =
        reader.collect::<Result<Vec<_>, _>>().map_err(js_err_arrow)?;

    // store globally
    *STORED_SCHEMA.lock().unwrap() = Some(schema.clone());
    *STORED_BATCHES.lock().unwrap() = Some(batches.clone());

    // Encode to IPC
    let mut out = vec![];
    let mut writer =
        StreamWriter::try_new(&mut out, schema.as_ref()).map_err(js_err_arrow)?;

    for b in batches {
        writer.write(&b).map_err(js_err_arrow)?;
    }

    writer.finish().map_err(js_err_arrow)?;
    Ok(out)
}


/// ------------------------------------------------------------------
///   Filter stored Arrow batches
/// ------------------------------------------------------------------
#[wasm_bindgen]
pub fn aggregate(col: &str, op: &str, threshold: f64) -> Result<Vec<u8>, JsValue> {

    let schema = STORED_SCHEMA
        .lock()
        .unwrap()
        .clone()
        .ok_or_else(|| js_err("No schema loaded. Upload CSV first."))?;

    let batches = STORED_BATCHES
        .lock()
        .unwrap()
        .clone()
        .ok_or_else(|| js_err("No batches loaded. Upload CSV first."))?;

    let col_index = schema
        .fields()
        .iter()
        .position(|f| f.name() == col)
        .ok_or_else(|| js_err("Column not found"))?;

    let mut out_batches = vec![];

    for batch in batches {
        let col_arr = batch.column(col_index);

        let floats = col_arr
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| js_err("Only Float64 columns supported for filtering"))?;

        let mask: BooleanArray = match op {
            "greater" => floats.iter().map(|x| x.map(|v| v > threshold)).collect(),
            "less" => floats.iter().map(|x| x.map(|v| v < threshold)).collect(),
            "ge" => floats.iter().map(|x| x.map(|v| v >= threshold)).collect(),
            "le" => floats.iter().map(|x| x.map(|v| v <= threshold)).collect(),
            "eq" => floats.iter().map(|x| x.map(|v| v == threshold)).collect(),
            _ => return Err(js_err("Invalid operator")),
        };

        let filtered = filter_record_batch(&batch, &mask).map_err(js_err_arrow)?;
        out_batches.push(filtered);
    }

    let mut out = vec![];
    let mut writer =
        StreamWriter::try_new(&mut out, schema.as_ref()).map_err(js_err_arrow)?;

    for b in out_batches {
        writer.write(&b).map_err(js_err_arrow)?;
    }

    writer.finish().map_err(js_err_arrow)?;
    Ok(out)
}
