import { formatFieldValue } from "../formatters";
import type { OperationPreview, OperationPreviewRecord } from "../types";

interface DiffViewProps {
  operation: OperationPreview;
}

function DiffRow({ record }: { record: OperationPreviewRecord }) {
  const currentFields = record.current_fields ?? {};
  const nextFields = record.fields ?? {};

  // Only show fields that are part of the mutation request
  const changedFieldNames = Object.keys(nextFields).sort();

  // Unchanged fields: in current snapshot but not in the mutation request
  const unchangedFieldNames = Object.keys(currentFields)
    .filter((name) => !(name in nextFields))
    .sort();

  const truncatedNames = unchangedFieldNames.slice(0, 12);
  const tooltipText =
    truncatedNames.join(", ") +
    (unchangedFieldNames.length > 12
      ? `, and ${unchangedFieldNames.length - 12} more`
      : "");

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
          {changedFieldNames.map((fieldName) => (
            <tr key={fieldName} className="field-changed">
              <th>{fieldName}</th>
              <td>{formatFieldValue(currentFields[fieldName])}</td>
              <td>{formatFieldValue(nextFields[fieldName])}</td>
            </tr>
          ))}
          {unchangedFieldNames.length > 0 && (
            <tr className="field-unchanged-summary">
              <td colSpan={3}>
                <span className="unchanged-hint" title={tooltipText}>
                  {unchangedFieldNames.length} other field
                  {unchangedFieldNames.length === 1 ? "" : "s"}
                </span>{" "}
                will remain unchanged
              </td>
            </tr>
          )}
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
            Only modified fields are shown. Other fields remain untouched.
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
