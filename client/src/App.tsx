import { useState } from "react";
import init, { csvtoarrow, aggregate } from "./wasm/package/rust_core";
import { tableFromIPC, Table } from "apache-arrow";

type Row = Record<string, string | number | boolean | null>;

const App: React.FC = () => {
  const [rows, setRows] = useState<Row[]>([]);
  const [columns, setColumns] = useState<string[]>([]);

  const handleFileUpload = async (
    e: React.ChangeEvent<HTMLInputElement>
  ): Promise<void> => {
    const file = e.target.files?.[0];
    if (!file) return;

    await init();

    const bytes = new Uint8Array(await file.arrayBuffer());

    const arrowBuffer: Uint8Array | number[] = csvtoarrow(bytes);

    console.log(arrowBuffer);

    const arr =
      arrowBuffer instanceof Uint8Array
        ? arrowBuffer
        : new Uint8Array(arrowBuffer);

    const table: Table = tableFromIPC(arr);

    console.log(table.schema.fields.length);

    const colNames = table.schema.fields.map((f) => f.name);
    setColumns(colNames);

    const parsedRows: Row[] = [];

    for (let i = 0; i < table.numRows; i++) {
      const row: Row = {};
      for (const col of colNames) {
        const colVector = table.getChild(col);
        row[col] = colVector?.get(i) ?? null;
      }
      parsedRows.push(row);
    }

    setRows(parsedRows);
  };

  const aggregate_fxn = async () => {
    await init();
    const result = aggregate("votes", "greater", 1000);

    console.log("Aggregate Result:", result);
  };

  return (
    <div className="p-6 max-w-6xl mx-auto">
      <h1 className="text-2xl font-bold mb-4 text-gray-800">
        CSV → Arrow → Table Viewer
      </h1>

      {/* Upload Box */}
      <div className="mb-6">
        <label className="flex flex-col items-center justify-center w-full h-32 border-2 border-dashed border-gray-300 rounded-xl cursor-pointer bg-gray-50 hover:bg-gray-100 transition">
          <div className="flex flex-col items-center pt-4 pb-3">
            <svg
              aria-hidden="true"
              className="w-10 h-10 mb-2 text-gray-400"
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth="2"
                d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6H16a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12"
              ></path>
            </svg>
            <p className="text-sm text-gray-600">
              <span className="font-semibold">Click to upload CSV</span>
            </p>
          </div>
          <input
            type="file"
            accept=".csv"
            onChange={handleFileUpload}
            className="hidden"
          />
        </label>
      </div>

      {/* Table Container */}
      {rows.length > 0 && (
        <div className="overflow-auto border border-gray-300 rounded-lg shadow-md max-h-[500px]">
          <table className="min-w-full text-sm text-left">
            <thead className="bg-gray-200 sticky top-0 z-10">
              <tr>
                {columns.map((c) => (
                  <th
                    key={c}
                    className="px-4 py-2 font-semibold text-gray-700 border-b border-gray-300"
                  >
                    {c}
                  </th>
                ))}
              </tr>
            </thead>

            <tbody>
              {rows.map((row, idx) => (
                <tr
                  key={idx}
                  className={`${
                    idx % 2 === 0 ? "bg-white" : "bg-gray-500"
                  } hover:bg-gray-100 transition`}
                >
                  {columns.map((c) => (
                    <td
                      key={c}
                      className="px-4 py-2 border-b border-gray-200 text-gray-800"
                    >
                      {row[c] === null ? "" : row[c]?.toString()}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {rows.length === 0 && (
        <p className="text-gray-500 text-center mt-6">
          Upload a CSV file to preview its data.
        </p>
      )}
      <button onClick={aggregate_fxn}>aggregate</button>
    </div>
  );
};

export default App;
