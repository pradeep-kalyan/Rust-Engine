use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

use js_sys::{Object, Reflect};

use crate::{get_data, get_filter_options, get_meta_data, seed};
use crate::timing::measure;

/// Build a standard JS response object matching the IResponse<T> shape on the
/// TypeScript side: { success, message, data, timeTaken }
fn build_response(data: JsValue, success: bool, message: &str, time_taken_ms: f64) -> JsValue {
    let obj = Object::new();

    // These unwraps are safe here because we're setting properties on a fresh
    // JS object and not depending on external state.
    Reflect::set(&obj, &JsValue::from_str("success"), &JsValue::from_bool(success)).unwrap();
    Reflect::set(&obj, &JsValue::from_str("message"), &JsValue::from_str(message)).unwrap();
    Reflect::set(&obj, &JsValue::from_str("data"), &data).unwrap();
    Reflect::set(
        &obj,
        &JsValue::from_str("timeTaken"),
        &JsValue::from_f64(time_taken_ms),
    )
    .unwrap();

    JsValue::from(obj)
}

/// Async WASM export: seed data and return a structured response with timing
#[wasm_bindgen]
pub fn seed_async(bytes: Vec<u8>) -> js_sys::Promise {
    future_to_promise(async move {
        let (result, duration) = measure(|| seed(&bytes));

        match result {
            Ok(()) => Ok(build_response(JsValue::NULL, true, "", duration)),
            Err(_) => Ok(build_response(JsValue::NULL, false, "seed failed", duration)),
        }
    })
}

/// Async WASM export: get meta data with a structured response
#[wasm_bindgen]
pub fn get_meta_data_async() -> js_sys::Promise {
    future_to_promise(async move {
        let (result, duration) = measure(|| get_meta_data());

        match result {
            Ok(meta) => Ok(build_response(meta, true, "", duration)),
            Err(_) => Ok(build_response(JsValue::NULL, false, "get_meta_data failed", duration)),
        }
    })
}

/// Async version of get_data with a structured response envelope
/// Returns { columns: [...], rowCount: number } instead of IPC bytes
#[wasm_bindgen]
pub fn get_data_async(query_json: String) -> js_sys::Promise {
    future_to_promise(async move {
        let (result, duration) = measure(|| get_data(&query_json));

        match result {
            Ok(data_obj) => {
                // data_obj is already a JS object with { columns, rowCount }
                Ok(build_response(data_obj, true, "", duration))
            }
            Err(_) => Ok(build_response(JsValue::NULL, false, "get_data failed", duration)),
        }
    })
}

/// Async version of get_filter_options with a structured response envelope
#[wasm_bindgen]
pub fn get_filter_options_async(col_name: String) -> js_sys::Promise {
    future_to_promise(async move {
        let (result, duration) = measure(|| get_filter_options(&col_name));

        match result {
            Ok(options) => Ok(build_response(options, true, "", duration)),
            Err(_) => Ok(build_response(JsValue::NULL, false, "get_filter_options failed", duration)),
        }
    })
}
