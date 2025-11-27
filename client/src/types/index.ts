export type Row = Record<string, string | number | boolean | null>;

export interface TableData {
    rows: Row[];
    columns: string[];
}

// Arrow column buffer structure from WASM
export interface ArrowColumnBuffer {
    name: string;
    dataType: 'Int8' | 'Int16' | 'Int32' | 'Int64' | 'Float32' | 'Float64' | 'Boolean' | 'Utf8' | 'LargeUtf8' | 'Date' | 'Timestamp';
    length: number;
    nullCount: number;
    nullBitmap: ArrayBuffer | null;
    offsets: ArrayBuffer | null; // for Utf8/LargeUtf8
    values: ArrayBuffer; // main data buffer
}

// Response from Rust with column buffers
export interface ColumnBufferResponse {
    columns: ArrowColumnBuffer[];
    rowCount: number;
}

export interface MetaData {
    rowCount: number;
    columnCount: number;
    columns: string[];
}

export interface FilterOption {
    column: string;
    values: string[];
}

export interface PivotConfig {
    rows: string[];
    columns: string[];
    values: string[];
    aggregation: 'sum' | 'count' | 'avg' | 'min' | 'max';
}

export interface FilterConfig {
    column: string;
    operator: 'equals' | 'contains' | 'greater' | 'less';
    value: string | number;
}

export type ProcessingStatus = 'idle' | 'loading' | 'processing' | 'success' | 'error';

export interface AppState {
    tableData: TableData;
    metadata: MetaData | null;
    filterOptions: Map<string, string[]>;
    selectedFile: File | null;
    processingStatus: ProcessingStatus;
    error: string | null;
}
