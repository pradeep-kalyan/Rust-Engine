/* tslint:disable */
/* eslint-disable */
/**
 * ------------------------------------------------------------------
 *   STREAMING API: Finalize streaming (returns total row count)
 * ------------------------------------------------------------------
 */
export function seed_finalize(): number;
/**
 * ------------------------------------------------------------------
 *   STREAMING API: Initialize with CSV header to infer schema
 * ------------------------------------------------------------------
 */
export function seed_start(header_bytes: Uint8Array): void;
/**
 * ------------------------------------------------------------------
 *   STREAMING API: Process and append a chunk of CSV data
 * ------------------------------------------------------------------
 */
export function seed_chunk(chunk_bytes: Uint8Array, has_header: boolean): void;
/**
 * Clear the timing log
 */
export function clear_timing_log(): void;
/**
 * Get all timing log entries
 */
export function get_timing_log(): any;
/**
 * Async version of get_data with a structured response envelope
 * Returns { columns: [...], rowCount: number } instead of IPC bytes
 */
export function get_data_async(query_json: string): Promise<any>;
/**
 * Async WASM export: get meta data with a structured response
 */
export function get_meta_data_async(): Promise<any>;
/**
 * Async version of get_filter_options with a structured response envelope
 */
export function get_filter_options_async(col_name: string): Promise<any>;
/**
 * Async WASM export: seed data and return a structured response with timing
 */
export function seed_async(bytes: Uint8Array): Promise<any>;

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
  readonly memory: WebAssembly.Memory;
  readonly seed_chunk: (a: number, b: number, c: number) => [number, number];
  readonly seed_finalize: () => [number, number, number];
  readonly seed_start: (a: number, b: number) => [number, number];
  readonly clear_timing_log: () => void;
  readonly get_timing_log: () => any;
  readonly get_data_async: (a: number, b: number) => any;
  readonly get_filter_options_async: (a: number, b: number) => any;
  readonly get_meta_data_async: () => any;
  readonly seed_async: (a: number, b: number) => any;
  readonly wasm_bindgen__convert__closures_____invoke__h31f9d501116eaee8: (a: number, b: number, c: any) => void;
  readonly wasm_bindgen__closure__destroy__h56051a08f764ac79: (a: number, b: number) => void;
  readonly wasm_bindgen__convert__closures_____invoke__h3b0bb0f0824ea72c: (a: number, b: number, c: any, d: any) => void;
  readonly __wbindgen_malloc: (a: number, b: number) => number;
  readonly __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
  readonly __wbindgen_exn_store: (a: number) => void;
  readonly __externref_table_alloc: () => number;
  readonly __wbindgen_externrefs: WebAssembly.Table;
  readonly __externref_table_dealloc: (a: number) => void;
  readonly __wbindgen_start: () => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;
/**
* Instantiates the given `module`, which can either be bytes or
* a precompiled `WebAssembly.Module`.
*
* @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
*
* @returns {InitOutput}
*/
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
* If `module_or_path` is {RequestInfo} or {URL}, makes a request and
* for everything else, calls `WebAssembly.instantiate` directly.
*
* @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
*
* @returns {Promise<InitOutput>}
*/
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
