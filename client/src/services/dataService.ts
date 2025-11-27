import {
    Vector,
    makeData,
    makeVector,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Bool,
    Utf8,
    DateMillisecond,
    TimestampMillisecond,
} from 'apache-arrow';
import type { Row, TableData, ArrowColumnBuffer, ColumnBufferResponse } from '../types';
import type { IGetMetaDataResponse, IColumnMeta } from '../types/metadata';
import { rustTypeToDataType } from '../types/metadata';
import type { IFieldsKeeperItem } from 'react-fields-keeper';
import type { IColumnField, TimingLog } from '../store/fieldsStore';
import { useStore } from '../store/fieldsStore';
import { getWorkerClient } from '../worker/WorkerClient';

// Query types matching Rust implementation
export interface FilterCondition {
    column: string;
    operator:
        | 'equals'
        | 'notequals'
        | 'greaterthan'
        | 'lessthan'
        | 'greaterthanorequal'
        | 'lessthanorequal'
        | 'contains'
        | 'notcontains'
        | 'in'
        | 'notin'
        | 'between';
    value: string | number | boolean | string[] | { min: number; max: number };
}

export interface SortSpec {
    column: string;
    direction: 'asc' | 'desc';
}

export interface PivotValue {
    column: string;
    aggregation: 'sum' | 'average' | 'count' | 'min' | 'max';
}

export interface PivotSpec {
    rows: string[];
    columns?: string[];
    values: PivotValue[];
}

export interface DataQuery {
    columns?: string[];
    filters?: FilterCondition[];
    sort?: SortSpec[];
    pivot?: PivotSpec;
    limit?: number;
    offset?: number;
}

/**
 * Reconstruct Arrow Vector from raw column buffers (zero-copy)
 */
function reconstructArrowColumn(buffer: ArrowColumnBuffer): Vector {
    const { dataType, length, nullCount, nullBitmap, offsets, values } = buffer;

    const nullBitmapArray = nullBitmap ? new Uint8Array(nullBitmap) : null;

    switch (dataType) {
        case 'Int8':
            return makeVector(
                makeData({
                    type: new Int8(),
                    length,
                    nullCount,
                    nullBitmap: nullBitmapArray,
                    data: new Int8Array(values),
                }),
            );

        case 'Int16':
            return makeVector(
                makeData({
                    type: new Int16(),
                    length,
                    nullCount,
                    nullBitmap: nullBitmapArray,
                    data: new Int16Array(values),
                }),
            );

        case 'Int32':
            return makeVector(
                makeData({
                    type: new Int32(),
                    length,
                    nullCount,
                    nullBitmap: nullBitmapArray,
                    data: new Int32Array(values),
                }),
            );

        case 'Int64':
            return makeVector(
                makeData({
                    type: new Int64(),
                    length,
                    nullCount,
                    nullBitmap: nullBitmapArray,
                    data: new BigInt64Array(values),
                }),
            );

        case 'Float32':
            return makeVector(
                makeData({
                    type: new Float32(),
                    length,
                    nullCount,
                    nullBitmap: nullBitmapArray,
                    data: new Float32Array(values),
                }),
            );

        case 'Float64':
            return makeVector(
                makeData({
                    type: new Float64(),
                    length,
                    nullCount,
                    nullBitmap: nullBitmapArray,
                    data: new Float64Array(values),
                }),
            );

        case 'Boolean':
            return makeVector(
                makeData({
                    type: new Bool(),
                    length,
                    nullCount,
                    nullBitmap: nullBitmapArray,
                    data: new Uint8Array(values),
                }),
            );

        case 'Utf8':
            if (!offsets) throw new Error('Utf8 column requires offsets buffer');
            return makeVector(
                makeData({
                    type: new Utf8(),
                    length,
                    nullCount,
                    nullBitmap: nullBitmapArray,
                    valueOffsets: new Int32Array(offsets),
                    data: new Uint8Array(values),
                }),
            );

        case 'Date':
            return makeVector(
                makeData({
                    type: new DateMillisecond(),
                    length,
                    nullCount,
                    nullBitmap: nullBitmapArray,
                    data: new Int32Array(values),
                }),
            );

        case 'Timestamp':
            return makeVector(
                makeData({
                    type: new TimestampMillisecond(),
                    length,
                    nullCount,
                    nullBitmap: nullBitmapArray,
                    data: new BigInt64Array(values),
                }),
            );

        default:
            throw new Error(`Unsupported data type: ${dataType}`);
    }
}

/**
 * DataService - Professional singleton service for data operations
 *
 * Responsibilities:
 * 1. WASM Worker communication (all Rust operations run in Web Worker)
 * 2. File seeding (data stored in Rust Worker, not here)
 * 3. Metadata management (fetched from Rust Worker)
 * 4. Data retrieval with pivot/filter support
 * 5. Store integration for status updates
 *
 * NEW: Stores Arrow Vectors (columnar) instead of JS row objects
 * All heavy operations run off the main thread to prevent UI freezes
 */
class DataService {
    private static instance: DataService;
    private isInitialized = false;
    private workerClient = getWorkerClient();

    // Store metadata from Rust (not data!)
    private metadata: IGetMetaDataResponse | null = null;

    // NEW: Columnar storage with Arrow Vectors
    private resultColumns: Map<string, Vector> = new Map();
    private resultSchema: { name: string; type: string }[] = [];
    private rowCount: number = 0;

    private constructor() {}

    static getInstance(): DataService {
        if (!DataService.instance) {
            DataService.instance = new DataService();
        }
        return DataService.instance;
    }

    /**
     * Initialize WASM module in worker
     */
    private async initialize(): Promise<void> {
        if (this.isInitialized) return;
        await this.workerClient.init();
        this.isInitialized = true;
        console.log('[DataService] WASM Worker initialized');
    }

    /**
     * Process file: Seed data to Rust and fetch metadata
     * This is the main entry point after file upload
     * Uses streaming for large files (>5MB) with progress updates
     */
    async processFile(file: File): Promise<IFieldsKeeperItem<IColumnField>[]> {
        const store = useStore.getState();

        try {
            store.setProcessingStatus('loading');
            store.setError(null);
            store.setLatestTiming(null); // Clear old timing

            // 1. Initialize WASM
            await this.initialize();

            store.setProcessingStatus('processing');

            const useStreaming = file.size > 5 * 1024 * 1024; // > 5MB

            if (useStreaming) {
                // 2a. Use streaming API with progress updates
                console.log(`[DataService] Using streaming mode for file: ${file.name} (${(file.size / 1024 / 1024).toFixed(2)} MB)`);

                const result = await this.workerClient.processFile(file, (progress) => {
                    console.log(`[DataService] Progress: ${progress.percent}%`);
                    // You can emit progress to UI here if needed
                    // store.setLoadingProgress?.(progress.percent);
                });

                if (!result.success) {
                    throw new Error(result.message || 'Failed to process file');
                }

                // Extract metadata from streaming result
                this.metadata = JSON.parse(result.data.metadataJson) as IGetMetaDataResponse;

                // Log timing info from streaming
                if (result.data.timing && result.data.timing.length > 0) {
                    console.log('[DataService] Timing logs:', result.data.timing);
                }

                const seedTiming: TimingLog = {
                    operation: 'Loading Data (Streaming)',
                    duration_ms: result.timeTaken || 0,
                };
                store.setLatestTiming(seedTiming);
            } else {
                // 2b. Small file: Use traditional seed method (faster for small files)
                console.log(`[DataService] Using direct mode for file: ${file.name} (${(file.size / 1024).toFixed(2)} KB)`);

                const bytes = new Uint8Array(await file.arrayBuffer());
                const seedResponse = await this.workerClient.seed(bytes);
                if (!seedResponse.success) {
                    throw new Error(seedResponse.message || 'Failed to seed data');
                }

                const seedTiming: TimingLog = {
                    operation: 'Loading Data',
                    duration_ms: seedResponse.timeTaken,
                };
                store.setLatestTiming(seedTiming);

                // 3. Get metadata from Rust Worker
                const metadataResponse = await this.workerClient.getMetaData();
                if (!metadataResponse.success) {
                    throw new Error(metadataResponse.message || 'Failed to get metadata');
                }

                const metaTiming: TimingLog = {
                    operation: 'Analyzing File',
                    duration_ms: metadataResponse.timeTaken,
                };
                store.setLatestTiming(metaTiming);

                this.metadata = JSON.parse(metadataResponse.data) as IGetMetaDataResponse;
            }

            console.log('Metadata received:', this.metadata);

            // 4. Convert metadata to FieldsKeeper items
            const allItems = this.createFieldItems();

            store.setProcessingStatus('success');

            return allItems;
        } catch (err) {
            const errorMessage = err instanceof Error ? err.message : 'Failed to process file';
            console.error('Error processing file:', err);
            store.setError(errorMessage);
            store.setProcessingStatus('error');
            throw err;
        }
    }

    /**
     * Get data from Rust based on current pivot/filter configuration
     * Called when pivot/filter changes or "Apply" is clicked
     */

    /**
     * Advanced query with filters, sorting, and pivot
     * Runs in Web Worker to prevent UI freezes
     * NEW: Uses columnar buffers instead of IPC for zero-copy performance
     */
    getData(query: DataQuery): void {
        const store = useStore.getState();

        store.setProcessingStatus('processing');

        // Call Rust Worker with query JSON
        const queryJson = JSON.stringify(query);
        const dataResponse = this.workerClient.getData(queryJson);

        const mainThreadStart = performance.now();

        dataResponse
            .then((response) => {
                if (!response.success) throw new Error(response.message || 'Failed to get data');

                // NEW: response.data is { columns: ArrowColumnBuffer[], rowCount: number }
                const bufferResponse = response.data as unknown as ColumnBufferResponse;

                const reconstructStart = performance.now();

                // Reconstruct Arrow Vectors (zero-copy, instant)
                this.resultColumns.clear();
                this.resultSchema = [];
                this.rowCount = bufferResponse.rowCount;

                for (const colBuffer of bufferResponse.columns) {
                    const vector = reconstructArrowColumn(colBuffer);
                    this.resultColumns.set(colBuffer.name, vector);
                    this.resultSchema.push({
                        name: colBuffer.name,
                        type: colBuffer.dataType,
                    });
                }

                const reconstructTime = performance.now() - reconstructStart;
                const totalMainThreadTime = performance.now() - mainThreadStart;

                // Log timing breakdown
                console.log(`[DataService] ✓ Query complete`);
                console.log(`  ├─ Worker processing: ${response.timeTaken.toFixed(2)}ms`);
                console.log(`  ├─ JS reconstruction: ${reconstructTime.toFixed(2)}ms`);
                console.log(`  └─ Total (main thread): ${totalMainThreadTime.toFixed(2)}ms`);
                console.log(`[DataService] Rows: ${this.rowCount.toLocaleString()}, Columns: ${this.resultSchema.length}`);

                const timing: TimingLog = {
                    operation: 'Processing Query',
                    duration_ms: totalMainThreadTime,
                };
                store.setLatestTiming(timing);
                store.setProcessingStatus('success');
                store.incrementTableRenderCounter();
            })
            .catch((err) => {
                const errorMessage = err instanceof Error ? err.message : 'Failed to get data';
                console.error('[DataService] Error getting data:', err);
                store.setError(errorMessage);
                store.setProcessingStatus('error');
                throw err;
            });
    }

    /**
     * Get filter options for a column
     * Runs in Web Worker to prevent UI freezes
     */
    async getFilterOptions(column: string): Promise<unknown> {
        try {
            const response = await this.workerClient.getFilterOptions(column);
            if (!response.success) {
                throw new Error(response.message || 'Failed to get filter options');
            }

            const store = useStore.getState();
            const timing: TimingLog = {
                operation: 'Loading Filters',
                duration_ms: response.timeTaken,
            };
            store.setLatestTiming(timing);

            return JSON.parse(response.data);
        } catch (err) {
            console.error('[DataService] Error getting filter options:', err);
            throw err;
        }
    }

    /**
     * Get current result data (cached after last getData call)
     * DEPRECATED: Use getRowCount(), getColumnNames(), and getCell() instead
     */
    getCurrentData(): TableData {
        // For backward compatibility, materialize a subset of rows
        // But this defeats the purpose of columnar storage!
        console.warn('[DataService] getCurrentData() is deprecated. Use columnar access methods.');

        const columns = this.resultSchema.map((c) => c.name);
        const rows: Row[] = [];

        // Only materialize first 1000 rows to avoid memory issues
        const maxRows = Math.min(this.rowCount, 1000);

        for (let i = 0; i < maxRows; i++) {
            const row: Row = {};
            for (const col of columns) {
                row[col] = this.getCell(i, col);
            }
            rows.push(row);
        }

        return { rows, columns };
    }

    /**
     * Get cell value by row and column (zero-copy columnar access)
     */
    getCell(rowIndex: number, columnName: string): any {
        const vector = this.resultColumns.get(columnName);
        if (!vector) return null;

        if (rowIndex < 0 || rowIndex >= this.rowCount) return null;

        return vector.get(rowIndex);
    }

    /**
     * Get number of rows in result set
     */
    getRowCount(): number {
        return this.rowCount;
    }

    /**
     * Get column names in result set
     */
    getColumnNames(): string[] {
        return this.resultSchema.map((c) => c.name);
    }

    /**
     * Get column schema
     */
    getColumnSchema(): { name: string; type: string }[] {
        return [...this.resultSchema];
    }

    /**
     * Get metadata
     */
    getMetadata(): IGetMetaDataResponse | null {
        return this.metadata;
    }

    /**
     * Get column metadata by name
     */
    getColumnMeta(columnName: string): IColumnMeta | undefined {
        return this.metadata?.columns.find((col) => col.name === columnName);
    }

    /**
     * Get column type from metadata
     */
    getColumnType(columnName: string): 'string' | 'number' | 'boolean' {
        const colMeta = this.getColumnMeta(columnName);
        if (!colMeta) return 'string';
        return rustTypeToDataType(colMeta.type);
    }

    /**
     * Create FieldsKeeper items from metadata
     */
    private createFieldItems(): IFieldsKeeperItem<IColumnField>[] {
        if (!this.metadata) return [];

        return this.metadata.columns.map((col) => ({
            id: col.name,
            label: col.name,
            value: {
                id: col.name,
                name: col.name,
                dataType: rustTypeToDataType(col.type),
            },
            prefixNode: rustTypeToDataType(col.type) === 'number' ? 'measure-icon' : undefined,
        }));
    }

    /**
     * Get all field items (for FieldsKeeper)
     */
    getAllFieldItems(): IFieldsKeeperItem<IColumnField>[] {
        return this.createFieldItems();
    }

    /**
     * Use All Data - Move all columns to columns bucket
     */
    async useAllData(): Promise<void> {
        const store = useStore.getState();
        const allItems = this.getAllFieldItems();

        // Set all columns in columns bucket
        store.setPivotBuckets([
            { id: 'columns', items: allItems },
            { id: 'values', items: [] },
        ]);

        // Clear filters
        store.setFilterBuckets([{ id: 'filters', items: [] }]);

        // Fetch all data
        this.getData({});
    }

    /**
     * Clear all data - Reset pivot and filters
     */
    clearAllData(): void {
        const store = useStore.getState();
        store.setPivotBuckets([
            { id: 'columns', items: [] },
            { id: 'values', items: [] },
        ]);
        this.resultColumns.clear();
        this.resultSchema = [];
        this.rowCount = 0;
        store.incrementTableRenderCounter();
    }

    /**
     * Reset service (clear metadata and result data)
     */
    reset(): void {
        this.metadata = null;
        this.resultColumns.clear();
        this.resultSchema = [];
        this.rowCount = 0;
        const store = useStore.getState();
        store.resetStore();
    }
}

// Export singleton instance
export const dataService = DataService.getInstance();
