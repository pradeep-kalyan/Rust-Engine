import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { dataService } from '../services/dataService';
import { useStore, selectProcessingStatus } from '../store/fieldsStore';
import { useMemo, useRef } from 'react';
import { useVirtualizer } from '@tanstack/react-virtual';

export const TableView = () => {
    const processingStatus = useStore(selectProcessingStatus);
    const tableRenderCounter = useStore((state) => state.tableRenderCounter);

    const parentRef = useRef<HTMLDivElement | null>(null);
    const headerRowRef = useRef<HTMLDivElement | null>(null);

    const tableMetadata = useMemo(() => {
        return {
            rowCount: dataService.getRowCount(),
            columns: dataService.getColumnNames(),
        };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [tableRenderCounter]);

    const rowVirtualizer = useVirtualizer({
        count: tableMetadata.rowCount,
        getScrollElement: () => parentRef.current,
        estimateSize: () => 40,
        overscan: 40,
    });

    const virtualItems = rowVirtualizer.getVirtualItems();
    console.log(`[TableView] Rendering ${virtualItems.length} rows out of ${tableMetadata.rowCount} total rows`);

    if (processingStatus === 'idle') {
        return (
            <div className="flex-1 flex items-center justify-center p-8">
                <div className="text-center text-muted-foreground">
                    <p className="text-lg font-semibold mb-2">No Data</p>
                    <p className="text-sm">Upload a CSV file to view data</p>
                </div>
            </div>
        );
    }

    return (
        <div className="flex-1 overflow-auto p-4 h-screen">
            <Card className="h-full flex flex-col">
                <CardHeader className="pb-3 border-b">
                    <CardTitle>Data Preview</CardTitle>
                    <CardDescription>
                        {tableMetadata.rowCount.toLocaleString()} rows × {tableMetadata.columns.length} columns
                    </CardDescription>
                </CardHeader>

                <CardContent className="flex-1 p-0 flex flex-col overflow-hidden">
                    {/* Single scroll container for both vertical & horizontal */}
                    <div ref={parentRef} className="flex-1 overflow-auto">
                        <div className="min-w-full">
                            {/* Sticky header */}
                            <div
                                className="flex sticky top-0 z-20 border-b"
                                ref={headerRowRef}
                                style={{
                                    backgroundColor: 'var(--color-muted)',
                                    position: 'sticky',
                                    top: 0,
                                }}
                            >
                                {tableMetadata.columns.map((col) => (
                                    <div
                                        key={col}
                                        className="px-4 py-3 text-left font-semibold text-foreground flex-none"
                                        style={{
                                            minWidth: '150px',
                                            maxWidth: '150px',
                                            width: '150px',
                                            backgroundColor: 'var(--color-muted)',
                                        }}
                                    >
                                        <span className="truncate block">{col}</span>
                                    </div>
                                ))}
                            </div>

                            {/* Virtualized rows */}
                            <div
                                style={{
                                    height: `${rowVirtualizer.getTotalSize()}px`,
                                    position: 'relative',
                                    width: '100%',
                                }}
                            >
                                {virtualItems.map((virtualRow) => {
                                    const index = virtualRow.index;
                                    const isEven = index % 2 === 0;

                                    return (
                                        <div
                                            key={virtualRow.key}
                                            ref={rowVirtualizer.measureElement}
                                            data-index={virtualRow.index}
                                            className="absolute top-0 left-0 right-0 border-b"
                                            style={{
                                                transform: `translateY(${virtualRow.start}px)`,
                                                height: `${virtualRow.size}px`,
                                            }}
                                        >
                                            <div className="flex h-full group">
                                                {tableMetadata.columns.map((col) => {
                                                    const value = dataService.getCell(index, col);
                                                    return (
                                                        <div
                                                            key={col}
                                                            className="px-4 py-2 text-sm text-foreground flex-none w-[150px] flex items-center group-hover:bg-accent transition-colors"
                                                            style={{
                                                                minWidth: '150px',
                                                                maxWidth: '150px',
                                                                backgroundColor: isEven ? 'var(--color-background)' : 'var(--color-muted)',
                                                            }}
                                                        >
                                                            <span className="truncate w-full">
                                                                {value === null || value === undefined ? (
                                                                    <span className="text-muted-foreground italic">null</span>
                                                                ) : (
                                                                    String(value)
                                                                )}
                                                            </span>
                                                        </div>
                                                    );
                                                })}
                                            </div>
                                        </div>
                                    );
                                })}
                            </div>
                        </div>
                    </div>
                </CardContent>
            </Card>
        </div>
    );
};
