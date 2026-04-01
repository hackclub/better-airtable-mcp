import { formatFieldValue } from "../formatters";
import type { OperationPreview } from "../types";

interface RecordTableProps {
  operation: OperationPreview;
}

function collectColumns(operation: OperationPreview): string[] {
  return Array.from(
    new Set(
      operation.records.flatMap((record) => Object.keys(record.fields ?? {})),
    ),
  ).sort();
}

export function RecordTable({ operation }: RecordTableProps) {
  const columns = collectColumns(operation);

  return (
    <section className="preview-card">
      <div className="preview-header">
        <div>
          <h2>Create in {operation.original_table_name ?? operation.table}</h2>
          <p className="preview-subtitle">
            {operation.records.length} record(s) will be created.
          </p>
        </div>
      </div>
      <div className="table-shell">
        <table>
          <thead>
            <tr>
              <th>Record</th>
              {columns.map((column) => (
                <th key={column}>{column}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {operation.records.map((record, index) => (
              <tr key={`${record.id ?? "new"}-${index}`}>
                <th>New record {index + 1}</th>
                {columns.map((column) => (
                  <td key={column}>{formatFieldValue(record.fields?.[column])}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
