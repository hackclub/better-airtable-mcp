import { formatFieldValue } from "../formatters";
import type { OperationPreview } from "../types";

interface DeletePreviewProps {
  operation: OperationPreview;
}

export function DeletePreview({ operation }: DeletePreviewProps) {
  return (
    <section className="preview-card">
      <div className="preview-header">
        <div>
          <h2>Delete from {operation.original_table_name ?? operation.table}</h2>
          <p className="preview-subtitle">
            Full current data is shown so the user can see what will be removed.
          </p>
        </div>
      </div>
      <div className="record-grid">
        {operation.records.map((record, index) => (
          <div className="record-card" key={`${record.id ?? "delete"}-${index}`}>
            <div className="record-title">{record.id ?? "Unknown record"}</div>
            <table>
              <thead>
                <tr>
                  <th>Field</th>
                  <th>Current value</th>
                </tr>
              </thead>
              <tbody>
                {Object.keys(record.current_fields ?? {})
                  .sort()
                  .map((fieldName) => (
                    <tr key={fieldName}>
                      <th>{fieldName}</th>
                      <td>{formatFieldValue(record.current_fields?.[fieldName])}</td>
                    </tr>
                  ))}
              </tbody>
            </table>
          </div>
        ))}
      </div>
    </section>
  );
}
