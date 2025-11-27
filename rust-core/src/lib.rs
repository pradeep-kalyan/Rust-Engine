use arrow_array::RecordBatch;
use arrow_csv::reader::{Format, ReaderBuilder};
use arrow_schema::{Schema, SchemaRef};
use std::io::Cursor;
use std::sync::Arc;
use wasm_bindgen::JsValue;

// Module declarations
mod api;
mod error;
mod filters;
mod helpers;
mod operations;
mod pivot;
mod query_types;
mod sorting;
mod storage;
mod timing;
mod types;

// Imports from modules
use error::{js_err, js_err_arrow};
use helpers::{to_simple_type, combine_batches, extract_column_buffers};
use query_types::DataQuery;
use storage::{STORED_BATCHES, STORED_SCHEMA};

// Re-export async WASM API
pub use api::{
    get_data_async,
    get_filter_options_async,
    get_meta_data_async,
    seed_async,
};

// Re-export streaming seed functions
use wasm_bindgen::prelude::*;

/// ------------------------------------------------------------------
///   STREAMING API: Initialize with CSV header to infer schema
/// ------------------------------------------------------------------
#[wasm_bindgen]
pub fn seed_start(header_bytes: &[u8]) -> Result<(), JsValue> {
    let mut cursor = Cursor::new(header_bytes);

    let fmt = Format::default().with_header(true);
    let (schema, _) = fmt.infer_schema(&mut cursor, None).map_err(js_err_arrow)?;
    let schema: SchemaRef = Arc::new(schema);

    // Initialize with schema and empty batches
    *STORED_SCHEMA.lock().unwrap() = Some(schema.clone());
    *STORED_BATCHES.lock().unwrap() = Some(Vec::new());

    Ok(())
}

/// ------------------------------------------------------------------
///   STREAMING API: Process and append a chunk of CSV data
/// ------------------------------------------------------------------
#[wasm_bindgen]
pub fn seed_chunk(chunk_bytes: &[u8], has_header: bool) -> Result<(), JsValue> {
    let schema_guard = STORED_SCHEMA.lock().unwrap();
    let schema = schema_guard
        .as_ref()
        .ok_or_else(|| js_err("Schema not initialized. Call seed_start first."))?;

    let cursor = Cursor::new(chunk_bytes);

    let reader = ReaderBuilder::new(schema.clone())
        .with_header(has_header)
        .build(cursor)
        .map_err(js_err_arrow)?;

    let new_batches: Vec<RecordBatch> = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(js_err_arrow)?;

    // Drop the schema guard before acquiring the batches lock
    drop(schema_guard);

    // Append to existing batches
    let mut batches_guard = STORED_BATCHES.lock().unwrap();
    if let Some(ref mut batches) = *batches_guard {
        batches.extend(new_batches);
    }

    Ok(())
}

/// ------------------------------------------------------------------
///   STREAMING API: Finalize streaming (returns total row count)
/// ------------------------------------------------------------------
#[wasm_bindgen]
pub fn seed_finalize() -> Result<usize, JsValue> {
    let batches_guard = STORED_BATCHES.lock().unwrap();
    let total_rows = batches_guard
        .as_ref()
        .map(|batches| batches.iter().map(|b| b.num_rows()).sum())
        .unwrap_or(0);
    Ok(total_rows)
}


/// ------------------------------------------------------------------
///   CSV → Arrow IPC → store schema + batches
///   Optimized: Avoid unnecessary clones, single pass
///   This is the non-streaming version for smaller files
/// ------------------------------------------------------------------
pub(crate) fn seed(bytes: &[u8]) -> Result<(), JsValue> {
    let mut cursor = Cursor::new(bytes);

    // Infer schema with optimized format
    let fmt = Format::default().with_header(true);
    let (schema, _) = fmt.infer_schema(&mut cursor, None).map_err(js_err_arrow)?;
    let schema: SchemaRef = Arc::new(schema);

    // Reset cursor for reading
    cursor.set_position(0);

    // Build reader with optimized settings
    let reader = ReaderBuilder::new(Arc::clone(&schema))
        .with_header(true)
        .build(cursor)
        .map_err(js_err_arrow)?;

    // Collect batches with pre-allocated capacity hint
    let batches: Vec<RecordBatch> = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(js_err_arrow)?;

    // Store globally (single lock acquisition)
    {
        let mut schema_lock = STORED_SCHEMA.lock().unwrap();
        let mut batches_lock = STORED_BATCHES.lock().unwrap();
        *schema_lock = Some(schema);
        *batches_lock = Some(batches);
    }

    Ok(())
}

pub(crate) fn get_meta_data() -> Result<JsValue, JsValue> {
    // Lock once and clone only the Arc (cheap)
    let schema = STORED_SCHEMA
        .lock()
        .unwrap()
        .as_ref()
        .ok_or_else(|| js_err("No schema stored. Call seed() first."))?  
        .clone();

    // Pre-allocate with exact capacity
    let field_count = schema.fields().len();
    let mut cols = Vec::with_capacity(field_count);

    // Iterate efficiently
    for field in schema.fields() {
        let simple_type = to_simple_type(field.data_type());

        cols.push(serde_json::json!({
            "name": field.name(),
            "type": simple_type,
            "nullable": field.is_nullable()
        }));
    }

    let meta = serde_json::json!({
        "columns": cols,
        "column_count": field_count,
    });

    // Serialize directly to string
    let json_string = serde_json::to_string(&meta)
        .map_err(|e| js_err(&format!("Serialization error: {}", e)))?;
    
    Ok(JsValue::from_str(&json_string))
}


/// Advanced get_data with filters, sorting, and pivot support
/// Returns a JS object with { columns: [...], rowCount: number }
pub(crate) fn get_data(query_json: &str) -> Result<JsValue, JsValue> {
	// Parse query JSON
	let query: DataQuery = serde_json::from_str(query_json)
			.map_err(|e| js_err(&format!("Invalid query JSON: {}", e)))?;

    // Note: Fast path cache removed since we're returning column buffers
    // Caching would need to cache the JS objects, which is not efficient

    // Load stored data (single lock acquisition per store)
    let schema = STORED_SCHEMA
        .lock()
        .unwrap()
        .as_ref()
        .ok_or_else(|| js_err("No schema stored. Call seed() first."))?  
        .clone();

    let batches_ref = STORED_BATCHES
        .lock()
        .unwrap();
    let stored_batches = batches_ref
        .as_ref()
        .ok_or_else(|| js_err("No batches stored."))?;
    
    // Fast path: Only limit/offset (no filters, sorting, pivot, or column projection)
    let only_limit_offset = query.filters.is_none() 
        && query.sort.is_none() 
        && query.pivot.is_none()
        && query.columns.is_none()
        && (query.limit.is_some() || query.offset.is_some());
    
    if only_limit_offset {
        let offset = query.offset.unwrap_or(0);
        let limit = query.limit;
        let result = apply_limit_offset_borrow(stored_batches, offset, limit)?;
        drop(batches_ref);
        
        // Combine and extract buffers
        let combined_batch = combine_batches(&schema, &result)?;
        let columns = extract_column_buffers(&combined_batch)?;
        let row_count = combined_batch.num_rows();
        
        let response = js_sys::Object::new();
        js_sys::Reflect::set(&response, &"columns".into(), &columns)?;
        js_sys::Reflect::set(&response, &"rowCount".into(), &JsValue::from_f64(row_count as f64))?;
        
        return Ok(response.into());
    }
    
    let mut batches = stored_batches.clone();
    drop(batches_ref);

	// Apply filters
	if let Some(ref filter_conditions) = query.filters {
		batches = filters::apply_filters(batches, filter_conditions)?;
	}

	// Apply pivot (grouping)
	if let Some(ref pivot_spec) = query.pivot {
		batches = pivot::apply_pivot(batches, pivot_spec)?;
	}

	// Apply sorting
	if let Some(ref sort_specs) = query.sort {
		batches = sorting::apply_sorting(batches, sort_specs)?;
	}

    // Apply column projection (optimized)
    if let Some(ref cols) = query.columns {
        if !cols.is_empty() {
            let current_schema = if batches.is_empty() {
                Arc::clone(&schema)
            } else {
                batches[0].schema()
            };

            // Pre-allocate indices vector
            let mut indices = Vec::with_capacity(cols.len());
            for name in cols {
                match current_schema.index_of(name) {
                    Ok(i) => indices.push(i),
                    Err(_) => return Err(js_err(&format!("Column not found: {}", name))),
                }
            }

            // Build projected schema once
            let projected_fields: Vec<_> = indices
                .iter()
                .map(|&i| current_schema.field(i).clone())
                .collect();
            let projected_schema: SchemaRef = Arc::new(Schema::new(projected_fields));

            // Project batches with pre-allocated capacity
            let mut projected_batches = Vec::with_capacity(batches.len());
            for batch in batches {
                let cols: Vec<_> = indices
                    .iter()
                    .map(|&i| Arc::clone(batch.column(i)))
                    .collect();

                let projected = RecordBatch::try_new(Arc::clone(&projected_schema), cols)
                    .map_err(js_err_arrow)?;

                projected_batches.push(projected);
            }

            batches = projected_batches;
        }
    }

	// Apply limit and offset
	if query.limit.is_some() || query.offset.is_some() {
		batches = apply_limit_offset(batches, query.limit, query.offset)?;
	}

	// NEW: Combine batches and extract column buffers (zero-copy)
	let final_schema = if batches.is_empty() {
		schema
	} else {
		batches[0].schema()
	};
	
	let combined_batch = combine_batches(&final_schema, &batches)?;
	
	// Create response object with columns and metadata
	let columns = extract_column_buffers(&combined_batch)?;
	let row_count = combined_batch.num_rows();
	
	let response = js_sys::Object::new();
	js_sys::Reflect::set(&response, &"columns".into(), &columns)?;
	js_sys::Reflect::set(&response, &"rowCount".into(), &JsValue::from_f64(row_count as f64))?;
	
	Ok(response.into())
}

/// Apply limit and offset by borrowing slices without cloning batches (fast path)
fn apply_limit_offset_borrow(
    batches: &[RecordBatch],
    offset: usize,
    limit: Option<usize>,
) -> Result<Vec<RecordBatch>, JsValue> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }

    let mut total_rows = 0;
    for batch in batches {
        total_rows += batch.num_rows();
    }

    if offset >= total_rows {
        return Ok(Vec::new());
    }

    let mut result = Vec::new();
    let mut rows_skipped = 0;
    let mut rows_taken = 0;
    let max_rows = limit.unwrap_or(total_rows - offset);

    for batch in batches {
        let batch_rows = batch.num_rows();
        
        // Skip batches before offset
        if rows_skipped + batch_rows <= offset {
            rows_skipped += batch_rows;
            continue;
        }

        // Check if we've taken enough rows
        if rows_taken >= max_rows {
            break;
        }

        // Calculate slice range for this batch
        let start = if rows_skipped < offset {
            offset - rows_skipped
        } else {
            0
        };

        let remaining = max_rows - rows_taken;
        let end = (start + remaining).min(batch_rows);

        if start < end {
            let sliced = batch.slice(start, end - start);
            result.push(sliced);
            rows_taken += end - start;
        }

        rows_skipped += batch_rows;
    }

    Ok(result)
}

/// Apply limit and offset to batches (for owned batches after other operations)
fn apply_limit_offset(
    batches: Vec<RecordBatch>,
    limit: Option<usize>,
    offset: Option<usize>,
) -> Result<Vec<RecordBatch>, JsValue> {
    if batches.is_empty() {
        return Ok(batches);
    }

    let mut remaining_offset = offset.unwrap_or(0);
    let mut remaining_limit = limit.unwrap_or(usize::MAX);

    // If nothing to skip and no limit, just return as-is (zero work)
    if remaining_offset == 0 && remaining_limit == usize::MAX {
        return Ok(batches);
    }

    // Save schema before moving batches
    let schema = batches[0].schema();
    let mut result = Vec::with_capacity(batches.len());

    for batch in batches {
        let rows = batch.num_rows();

        // Skip whole batches while offset is larger than current batch
        if remaining_offset >= rows {
            remaining_offset -= rows;
            continue;
        }

        // We are inside this batch now
        let start_in_batch = remaining_offset;
        let available_here = rows - start_in_batch;
        let take = remaining_limit.min(available_here);

        if take == 0 {
            break;
        }

        let sliced = batch.slice(start_in_batch, take);
        result.push(sliced);

        remaining_limit -= take;
        remaining_offset = 0;

        if remaining_limit == 0 {
            break;
        }
    }

    // If nothing left (offset beyond end), return empty batch with same schema
    if result.is_empty() {
        let empty = RecordBatch::new_empty(schema);
        return Ok(vec![empty]);
    }

    Ok(result)
}

pub(crate) fn get_filter_options(col_name: &str) -> Result<JsValue, JsValue> {
	operations::get_filter_options(col_name)
}
