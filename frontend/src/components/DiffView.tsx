import { collectFieldNames, formatFieldValue } from "../formatters";
import type { OperationPreview, OperationPreviewRecord } from "../types";

interface DiffViewProps {
  operation: OperationPreview;
}

function valuesEqual(left: unknown, right: unknown): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}

function DiffRow({ record }: { record: OperationPreviewRecord }) {
  const currentFields = record.current_fields ?? {};
  const nextFields = record.fields ?? {};
  const fieldNames = collectFieldNames(currentFields, nextFields);

  return (
    <div className="record-card">
      <div className="record-title">{record.id ?? "Unknown record"}</div>
      <table>
        <thead>
          <tr>
            <th>Field</th>
            <th>Current</th>
            <th>Requested</th>
          </tr>
        </thead>
        <tbody>
          {fieldNames.map((fieldName) => {
            const currentValue = currentFields[fieldName];
            const nextValue = nextFields[fieldName];
            const changed = !valuesEqual(currentValue, nextValue);

            return (
              <tr
                key={fieldName}
                className={changed ? "field-changed" : "field-unchanged"}
              >
                <th>{fieldName}</th>
                <td>{formatFieldValue(currentValue)}</td>
                <td>{formatFieldValue(nextValue)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

export function DiffView({ operation }: DiffViewProps) {
  return (
    <section className="preview-card">
      <div className="preview-header">
        <div>
          <h2>Update in {operation.original_table_name ?? operation.table}</h2>
          <p className="preview-subtitle">
            Changed rows are highlighted. Unchanged fields remain visible for
            context.
          </p>
        </div>
      </div>
      <div className="record-grid">
        {operation.records.map((record, index) => (
          <DiffRow key={`${record.id ?? "record"}-${index}`} record={record} />
        ))}
      </div>
    </section>
  );
}
