use arrow_array::{Array, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, SchemaRef};
use wasm_bindgen::JsValue;
use js_sys::{Object, Reflect, Uint8Array};
use std::sync::Arc;

use crate::error::{js_err_arrow, js_err};

/// Parse comma-separated column names
pub fn parse_cols(input: &str) -> Vec<String> {
    input
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Convert Arrow DataType to simple type string
pub fn to_simple_type(dt: &arrow_schema::DataType) -> &'static str {
    use arrow_schema::DataType::*;

    match dt {
        Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | Float16 | Float32
        | Float64 | Decimal128(_, _) | Decimal256(_, _) => "number",

        Utf8 | LargeUtf8 => "text",

        Boolean => "boolean",

        Date32 | Date64 => "date",

        Timestamp(_, _) => "datetime",

        List(_) | LargeList(_) | FixedSizeList(_, _) => "list",

        _ => "unknown",
    }
}

/// Encode RecordBatches into Arrow IPC format
pub fn encode_ipc(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<Vec<u8>, JsValue> {
    let mut out = Vec::new();

    // Create IPC writer
    let mut writer = StreamWriter::try_new(&mut out, schema.as_ref()).map_err(js_err_arrow)?;

    // Write each batch
    for batch in batches {
        writer.write(batch).map_err(js_err_arrow)?;
    }

    // Finish IPC stream
    writer.finish().map_err(js_err_arrow)?;

    Ok(out)
}

/// Combine multiple RecordBatches into a single batch for contiguous memory
pub fn combine_batches(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<RecordBatch, JsValue> {
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::clone(schema)));
    }
    
    if batches.len() == 1 {
        return Ok(batches[0].clone());
    }
    
    arrow::compute::concat_batches(schema, batches).map_err(js_err_arrow)
}

/// Extract raw Arrow column buffers for zero-copy transfer to JS
/// Returns a JS array of column buffer objects
pub fn extract_column_buffers(batch: &RecordBatch) -> Result<JsValue, JsValue> {
    let js_array = js_sys::Array::new();
    
    for (field, array) in batch.schema().fields().iter().zip(batch.columns().iter()) {
        let col_obj = extract_single_column(field.name(), array)?;
        js_array.push(&col_obj);
    }
    
    Ok(js_array.into())
}

/// Extract buffers from a single Arrow array
fn extract_single_column(name: &str, array: &Arc<dyn Array>) -> Result<JsValue, JsValue> {
    let obj = Object::new();
    let array_data = array.to_data();
    
    // Basic metadata
    Reflect::set(&obj, &"name".into(), &JsValue::from_str(name))?;
    Reflect::set(&obj, &"length".into(), &JsValue::from_f64(array.len() as f64))?;
    Reflect::set(&obj, &"nullCount".into(), &JsValue::from_f64(array.null_count() as f64))?;
    
    // Extract null bitmap if present
    if let Some(nulls) = array_data.nulls() {
        let null_buffer = nulls.buffer();
        let null_bytes = null_buffer.as_slice();
        let null_array = Uint8Array::from(null_bytes);
        Reflect::set(&obj, &"nullBitmap".into(), &null_array.buffer())?;
    } else {
        Reflect::set(&obj, &"nullBitmap".into(), &JsValue::NULL)?;
    }
    
    // Extract data buffers based on type
    match array.data_type() {
        DataType::Int8 | DataType::UInt8 => {
            Reflect::set(&obj, &"dataType".into(), &JsValue::from_str("Int8"))?;
            let buffer = &array_data.buffers()[0];
            let bytes = buffer.as_slice();
            let arr = Uint8Array::from(bytes);
            Reflect::set(&obj, &"values".into(), &arr.buffer())?;
            Reflect::set(&obj, &"offsets".into(), &JsValue::NULL)?;
        }
        
        DataType::Int16 | DataType::UInt16 => {
            Reflect::set(&obj, &"dataType".into(), &JsValue::from_str("Int16"))?;
            let buffer = &array_data.buffers()[0];
            let bytes = buffer.as_slice();
            let arr = Uint8Array::from(bytes);
            Reflect::set(&obj, &"values".into(), &arr.buffer())?;
            Reflect::set(&obj, &"offsets".into(), &JsValue::NULL)?;
        }
        
        DataType::Int32 | DataType::UInt32 => {
            Reflect::set(&obj, &"dataType".into(), &JsValue::from_str("Int32"))?;
            let buffer = &array_data.buffers()[0];
            let bytes = buffer.as_slice();
            let arr = Uint8Array::from(bytes);
            Reflect::set(&obj, &"values".into(), &arr.buffer())?;
            Reflect::set(&obj, &"offsets".into(), &JsValue::NULL)?;
        }
        
        DataType::Int64 | DataType::UInt64 => {
            Reflect::set(&obj, &"dataType".into(), &JsValue::from_str("Int64"))?;
            let buffer = &array_data.buffers()[0];
            let bytes = buffer.as_slice();
            let arr = Uint8Array::from(bytes);
            Reflect::set(&obj, &"values".into(), &arr.buffer())?;
            Reflect::set(&obj, &"offsets".into(), &JsValue::NULL)?;
        }
        
        DataType::Float32 => {
            Reflect::set(&obj, &"dataType".into(), &JsValue::from_str("Float32"))?;
            let buffer = &array_data.buffers()[0];
            let bytes = buffer.as_slice();
            let arr = Uint8Array::from(bytes);
            Reflect::set(&obj, &"values".into(), &arr.buffer())?;
            Reflect::set(&obj, &"offsets".into(), &JsValue::NULL)?;
        }
        
        DataType::Float64 => {
            Reflect::set(&obj, &"dataType".into(), &JsValue::from_str("Float64"))?;
            let buffer = &array_data.buffers()[0];
            let bytes = buffer.as_slice();
            let arr = Uint8Array::from(bytes);
            Reflect::set(&obj, &"values".into(), &arr.buffer())?;
            Reflect::set(&obj, &"offsets".into(), &JsValue::NULL)?;
        }
        
        DataType::Boolean => {
            Reflect::set(&obj, &"dataType".into(), &JsValue::from_str("Boolean"))?;
            let buffer = &array_data.buffers()[0];
            let bytes = buffer.as_slice();
            let arr = Uint8Array::from(bytes);
            Reflect::set(&obj, &"values".into(), &arr.buffer())?;
            Reflect::set(&obj, &"offsets".into(), &JsValue::NULL)?;
        }
        
        DataType::Utf8 => {
            Reflect::set(&obj, &"dataType".into(), &JsValue::from_str("Utf8"))?;
            
            // Buffer 0: offsets (i32), Buffer 1: values (u8)
            let offset_buffer = &array_data.buffers()[0];
            let value_buffer = &array_data.buffers()[1];
            
            let offset_bytes = offset_buffer.as_slice();
            let value_bytes = value_buffer.as_slice();
            
            let offset_array = Uint8Array::from(offset_bytes);
            let value_array = Uint8Array::from(value_bytes);
            
            Reflect::set(&obj, &"offsets".into(), &offset_array.buffer())?;
            Reflect::set(&obj, &"values".into(), &value_array.buffer())?;
        }
        
        DataType::LargeUtf8 => {
            Reflect::set(&obj, &"dataType".into(), &JsValue::from_str("LargeUtf8"))?;
            
            // Buffer 0: offsets (i64), Buffer 1: values (u8)
            let offset_buffer = &array_data.buffers()[0];
            let value_buffer = &array_data.buffers()[1];
            
            let offset_bytes = offset_buffer.as_slice();
            let value_bytes = value_buffer.as_slice();
            
            let offset_array = Uint8Array::from(offset_bytes);
            let value_array = Uint8Array::from(value_bytes);
            
            Reflect::set(&obj, &"offsets".into(), &offset_array.buffer())?;
            Reflect::set(&obj, &"values".into(), &value_array.buffer())?;
        }
        
        DataType::Date32 | DataType::Date64 => {
            Reflect::set(&obj, &"dataType".into(), &JsValue::from_str("Date"))?;
            let buffer = &array_data.buffers()[0];
            let bytes = buffer.as_slice();
            let arr = Uint8Array::from(bytes);
            Reflect::set(&obj, &"values".into(), &arr.buffer())?;
            Reflect::set(&obj, &"offsets".into(), &JsValue::NULL)?;
        }
        
        DataType::Timestamp(_, _) => {
            Reflect::set(&obj, &"dataType".into(), &JsValue::from_str("Timestamp"))?;
            let buffer = &array_data.buffers()[0];
            let bytes = buffer.as_slice();
            let arr = Uint8Array::from(bytes);
            Reflect::set(&obj, &"values".into(), &arr.buffer())?;
            Reflect::set(&obj, &"offsets".into(), &JsValue::NULL)?;
        }
        
        _ => {
            return Err(js_err(&format!("Unsupported data type for buffer extraction: {:?}", array.data_type())));
        }
    }
    
    Ok(obj.into())
}
