import { useState } from "react";
import type { ReactNode } from "react";
import type { FieldMeta, LinkedRecordRef, OperationStatus } from "./types";

// Per-render context for value formatting: lets linked-record cells resolve a
// rec id to its primary-field name + a link to the record.
export interface FieldContext {
  baseId?: string;
  linked?: Record<string, LinkedRecordRef>;
}

function isDateLikeString(value: string): boolean {
  if (!value.includes("T") && !/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return false;
  }
  return !Number.isNaN(Date.parse(value));
}

const URL_RE = /^https?:\/\/\S+$/i;
const EMAIL_RE = /^[^@\s]+@[^@\s]+\.[^@\s]+$/;
const RECORD_ID_RE = /^rec[A-Za-z0-9]{14,}$/;

export type FieldType =
  | "text"
  | "multilineText"
  | "number"
  | "currency"
  | "percent"
  | "rating"
  | "formula"
  | "checkbox"
  | "date"
  | "email"
  | "url"
  | "phone"
  | "singleSelect"
  | "multiSelect"
  | "linkedRecord"
  | "attachment";

// Best-effort field type from a value (the approval payload carries no schema).
export function inferFieldType(value: unknown): FieldType {
  if (typeof value === "boolean") return "checkbox";
  if (typeof value === "number") return "number";
  if (typeof value === "string") {
    if (URL_RE.test(value)) return "url";
    if (EMAIL_RE.test(value)) return "email";
    if (isDateLikeString(value)) return "date";
    return "text";
  }
  if (Array.isArray(value)) {
    if (value.every((v) => v !== null && typeof v === "object")) return "attachment";
    if (value.some((v) => typeof v === "string" && RECORD_ID_RE.test(v))) {
      return "linkedRecord";
    }
    return "multiSelect";
  }
  return "text";
}

// Type from the first non-empty value across a set of records, for column headers.
export function inferColumnType(values: unknown[]): FieldType {
  for (const v of values) {
    if (v !== null && v !== undefined && v !== "" && !(Array.isArray(v) && v.length === 0)) {
      return inferFieldType(v);
    }
  }
  return "text";
}

// Map an Airtable field type (from the payload schema) to an icon type.
const AT_TYPE_TO_ICON: Record<string, FieldType> = {
  singleLineText: "text",
  multilineText: "multilineText",
  richText: "multilineText",
  formula: "formula",
  autoNumber: "number",
  number: "number",
  currency: "currency",
  percent: "percent",
  count: "number",
  rollup: "number",
  duration: "number",
  rating: "rating",
  singleSelect: "singleSelect",
  multipleSelects: "multiSelect",
  date: "date",
  dateTime: "date",
  createdTime: "date",
  lastModifiedTime: "date",
  checkbox: "checkbox",
  email: "email",
  url: "url",
  phoneNumber: "phone",
  multipleAttachments: "attachment",
  multipleRecordLinks: "linkedRecord",
  foreignKey: "linkedRecord",
  multipleCollaborators: "linkedRecord",
  singleCollaborator: "linkedRecord",
  createdBy: "linkedRecord",
  lastModifiedBy: "linkedRecord",
};

export function fieldTypeIcon(atType: string): FieldType {
  return AT_TYPE_TO_ICON[atType] ?? "text";
}

// Human-readable field-type names, shown on hover of a column/field header (the
// same labels Airtable uses in its field menu). Precise names come from the raw
// Airtable type; the FieldType map is the fallback when there's no schema.
const AT_TYPE_LABELS: Record<string, string> = {
  singleLineText: "Single line text",
  multilineText: "Long text",
  richText: "Rich text",
  formula: "Formula",
  autoNumber: "Autonumber",
  number: "Number",
  currency: "Currency",
  percent: "Percent",
  count: "Count",
  rollup: "Rollup",
  duration: "Duration",
  rating: "Rating",
  singleSelect: "Single select",
  multipleSelects: "Multiple select",
  date: "Date",
  dateTime: "Date",
  createdTime: "Created time",
  lastModifiedTime: "Last modified time",
  checkbox: "Checkbox",
  email: "Email",
  url: "URL",
  phoneNumber: "Phone number",
  multipleAttachments: "Attachment",
  multipleRecordLinks: "Linked records",
  foreignKey: "Linked records",
  multipleCollaborators: "Collaborators",
  singleCollaborator: "Collaborator",
  createdBy: "Created by",
  lastModifiedBy: "Last modified by",
};

const FIELD_TYPE_LABELS: Record<FieldType, string> = {
  text: "Single line text",
  multilineText: "Long text",
  number: "Number",
  currency: "Currency",
  percent: "Percent",
  rating: "Rating",
  formula: "Formula",
  checkbox: "Checkbox",
  date: "Date",
  email: "Email",
  url: "URL",
  phone: "Phone number",
  singleSelect: "Single select",
  multiSelect: "Multiple select",
  linkedRecord: "Linked records",
  attachment: "Attachment",
};

export function fieldTypeLabel(atType: string | undefined, fieldType: FieldType): string {
  if (atType && AT_TYPE_LABELS[atType]) return AT_TYPE_LABELS[atType];
  return FIELD_TYPE_LABELS[fieldType] ?? "Field";
}

// Airtable's default select-choice palette (the "Light2" token backgrounds,
// pulled from the live CSS bundle). When a new select field is created without
// explicit colors, Airtable auto-assigns them by cycling this sequence, so we
// mirror that order to preview what the choices will look like.
const CHOICE_COLORS = [
  "#d1e2ff", // blue
  "#c4ecff", // cyan
  "#c1f5f0", // teal
  "#cff5d1", // green
  "#ffeab6", // yellow
  "#ffe0cc", // orange
  "#ffd4e0", // red
  "#fad2fc", // pink
  "#e0dafd", // purple
  "#e5e9f0", // gray
];

export function choiceColor(index: number): string {
  return CHOICE_COLORS[((index % CHOICE_COLORS.length) + CHOICE_COLORS.length) % CHOICE_COLORS.length];
}

export interface DisplayColumn {
  label: string;
  type: FieldType;
  atType?: string;
  get: (fields?: Record<string, unknown>) => unknown;
}

// Ordered columns for a create table: primary field first (always shown), then
// other schema fields that any record sets, then any unknown keys — Airtable order.
export function buildColumns(
  fields: FieldMeta[] | undefined,
  records: { fields?: Record<string, unknown> }[],
): DisplayColumn[] {
  const maps = records.map((r) => r.fields ?? {});
  const valuesFor = (col: DisplayColumn) => maps.map((m) => col.get(m));

  if (fields && fields.length > 0) {
    const cols: DisplayColumn[] = [];
    const used = new Set<string>();
    const present = (f: FieldMeta) =>
      maps.some((m) => f.name in m || (f.key !== undefined && f.key in m));

    fields.forEach((field, index) => {
      if (index !== 0 && !present(field)) return; // primary always shown
      const get = (m?: Record<string, unknown>) =>
        m?.[field.name] ?? (field.key !== undefined ? m?.[field.key] : undefined);
      cols.push({ label: field.name, type: fieldTypeIcon(field.type), atType: field.type, get });
      used.add(field.name);
      if (field.key) used.add(field.key);
    });

    const extras = new Set<string>();
    maps.forEach((m) => Object.keys(m).forEach((k) => !used.has(k) && extras.add(k)));
    [...extras].sort().forEach((k) => {
      const col = { label: k, type: "text" as FieldType, get: (m?: Record<string, unknown>) => m?.[k] };
      col.type = inferColumnType(valuesFor(col));
      cols.push(col);
    });
    return cols;
  }

  // Fallback: union of record keys, type inferred from values.
  const keys = Array.from(new Set(maps.flatMap((m) => Object.keys(m)))).sort();
  return keys.map((k) => {
    const col = { label: k, type: "text" as FieldType, get: (m?: Record<string, unknown>) => m?.[k] };
    col.type = inferColumnType(maps.map((m) => m[k]));
    return col;
  });
}

// Order a set of field keys (from record maps) by Airtable schema order, with the
// nicer display label + type from the schema when available.
export function orderFieldKeys(
  keys: string[],
  fields: FieldMeta[] | undefined,
  valueForType?: (key: string) => unknown,
): { key: string; label: string; type: FieldType; atType?: string }[] {
  if (fields && fields.length > 0) {
    const index = new Map<string, number>();
    const metaByKey = new Map<string, FieldMeta>();
    fields.forEach((f, i) => {
      metaByKey.set(f.name, f);
      index.set(f.name, i);
      if (f.key) {
        metaByKey.set(f.key, f);
        index.set(f.key, i);
      }
    });
    return keys
      .slice()
      .sort((a, b) => (index.get(a) ?? 1e9) - (index.get(b) ?? 1e9) || a.localeCompare(b))
      .map((key) => {
        const meta = metaByKey.get(key);
        return {
          key,
          label: meta?.name ?? key,
          type: meta ? fieldTypeIcon(meta.type) : inferFieldType(valueForType?.(key)),
          atType: meta?.type,
        };
      });
  }
  return keys
    .slice()
    .sort()
    .map((key) => ({ key, label: key, type: inferFieldType(valueForType?.(key)) }));
}

export function recordCountLabel(count: number): string {
  return `${count} record${count === 1 ? "" : "s"}`;
}

export function baseUrl(baseId?: string): string | null {
  return baseId ? `https://airtable.com/${baseId}` : null;
}

export function tableUrl(baseId?: string, tableId?: string): string | null {
  return baseId && tableId ? `https://airtable.com/${baseId}/${tableId}` : null;
}

export function recordUrl(baseId?: string, tableId?: string, recordId?: string): string | null {
  return baseId && tableId && recordId
    ? `https://airtable.com/${baseId}/${tableId}/${recordId}`
    : null;
}

export function ExternalLinkIcon() {
  return (
    <svg
      className="ext-icon"
      width="11"
      height="11"
      viewBox="0 0 16 16"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.4"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d="M9 3h4v4M13 3l-6 6M11 9.5V12.5a1 1 0 0 1-1 1H3.5a1 1 0 0 1-1-1V6a1 1 0 0 1 1-1H6.5" />
    </svg>
  );
}

function CopyIcon() {
  return (
    <svg
      width="11"
      height="11"
      viewBox="0 0 16 16"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.4"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <rect x="5.5" y="5.5" width="7.5" height="7.5" rx="1.3" />
      <path d="M10.5 5.5V4.2a1 1 0 0 0-1-1H4.2a1 1 0 0 0-1 1v5.3a1 1 0 0 0 1 1H5.5" />
    </svg>
  );
}

function CheckIcon() {
  return (
    <svg
      width="11"
      height="11"
      viewBox="0 0 16 16"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.6"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d="M3.5 8.5l3 3 6-6.5" />
    </svg>
  );
}

// The record id is not a link — it's a copy-to-clipboard control (icon swaps to
// a check briefly on copy). Linking to the record is the job of the record name.
export function CopyId({ id }: { id: string }) {
  const [copied, setCopied] = useState(false);
  const onCopy = async () => {
    try {
      await navigator.clipboard.writeText(id);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1200);
    } catch {
      /* clipboard unavailable (e.g. insecure context) — no-op */
    }
  };
  return (
    <button
      type="button"
      className={`copy-id${copied ? " is-copied" : ""}`}
      onClick={onCopy}
      data-tooltip={copied ? "Copied" : "Copy record id"}
    >
      <span className="copy-id-text">{id}</span>
      {copied ? <CheckIcon /> : <CopyIcon />}
    </button>
  );
}

// The record's name (primary-field value) is the link to the record in Airtable,
// with the open-in-new-tab icon on hover. Empty primary → "Untitled record".
export function RecordNameLink({
  baseId,
  tableId,
  recordId,
  primary,
}: {
  baseId?: string;
  tableId?: string;
  recordId?: string;
  primary: string | null;
}) {
  const href = recordUrl(baseId, tableId, recordId);
  const untitled = primary === null;
  const text = primary ?? "Untitled record";
  const cls = `record-name${untitled ? " is-untitled" : ""}`;
  if (!href) {
    return <span className={cls}>{text}</span>;
  }
  return (
    <a
      className={`${cls} record-name-link`}
      href={href}
      target="_blank"
      rel="noreferrer noopener"
      data-tooltip="Open record in Airtable"
    >
      {text}
      <ExternalLinkIcon />
    </a>
  );
}

// Table name → opens the table in Airtable, with the open-in-new-tab icon
// revealed on hover (matches RecordLink). Falls back to plain text when we
// can't build a URL (no base/table id).
export function TableLink({ href, name }: { href: string | null; name: string }) {
  if (!href) {
    return <>{name}</>;
  }
  return (
    <a className="table-link" href={href} target="_blank" rel="noreferrer noopener" data-tooltip="Open table in Airtable">
      <span className="link-text">{name}</span>
      <ExternalLinkIcon />
    </a>
  );
}

// Base name linking to the base in Airtable (open-in-new-tab on hover). No icon
// tile: the public Meta API doesn't expose a base's real color/icon, and a
// derived color would just be a guess.
export function BaseLink({ baseId, name }: { baseId: string; name: string }) {
  const href = baseUrl(baseId);
  if (!href) {
    return <span className="base-ref">{name}</span>;
  }
  return (
    <a className="base-ref base-link" href={href} target="_blank" rel="noreferrer noopener" data-tooltip="Open base in Airtable">
      <span className="base-name">{name}</span>
      <ExternalLinkIcon />
    </a>
  );
}

// The record's primary-field value (Airtable shows this as a record's name in
// expanded views). fields[0] is the primary field; merge current + next so we
// show the most relevant title for updates and deletes alike.
export function primaryFieldDisplay(
  record: { fields?: Record<string, unknown>; current_fields?: Record<string, unknown> },
  fields?: FieldMeta[],
): string | null {
  if (!fields || fields.length === 0) return null;
  const primary = fields[0];
  const source = { ...(record.current_fields ?? {}), ...(record.fields ?? {}) };
  const value = source[primary.name] ?? (primary.key !== undefined ? source[primary.key] : undefined);
  if (value === null || value === undefined || value === "") return null;
  const text = valueToText(value).trim();
  return text === "" || text === "Empty" ? null : text;
}

export type OperationKind = "create" | "update" | "delete";

// Leading glyph per operation type (stroke style, matches ExternalLinkIcon):
// plus for create, pencil for update, trash for delete.
const OP_ICON_D: Record<OperationKind, string> = {
  create: "M8 3.25v9.5M3.25 8h9.5",
  update: "M10.4 3.4 12.6 5.6 6 12.2l-2.6.4.4-2.6 6.6-6.6Z",
  delete:
    "M3.5 4.5h9M6.25 4.5V3.4a.9.9 0 0 1 .9-.9h1.7a.9.9 0 0 1 .9.9v1.1M5.4 4.5l.5 7.6a.9.9 0 0 0 .9.84h2.4a.9.9 0 0 0 .9-.84l.5-7.6",
};

// Small tinted square that gives each operation card an at-a-glance semantic
// color (Airtable's green/blue/red tints): create=add, update=change, delete=remove.
export function OperationBadge({ kind }: { kind: OperationKind }) {
  return (
    <span className={`op-badge is-${kind}`} aria-hidden="true">
      <svg
        width="15"
        height="15"
        viewBox="0 0 16 16"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.4"
        strokeLinecap="round"
        strokeLinejoin="round"
      >
        <path d={OP_ICON_D[kind]} />
      </svg>
    </span>
  );
}

// Real Airtable field-type icons (Phosphor set, extracted from the live app
// bundle). Each is a single filled path at viewBox 0 0 16 16; see
// claude-workspace/airtable-ref/field-icons.json.
const ICON_D: Record<FieldType, string> = {
  text: "M8.44187 3.26606C8.35522 3.10237 8.18518 3 7.99998 3C7.81477 3 7.64474 3.10237 7.55808 3.26606L3.05808 11.7661C2.92888 12.0101 3.02198 12.3127 3.26603 12.4419C3.51009 12.5711 3.81267 12.478 3.94187 12.2339L5.12455 10H10.8754L12.0581 12.2339C12.1873 12.478 12.4899 12.5711 12.7339 12.4419C12.978 12.3127 13.0711 12.0101 12.9419 11.7661L8.44187 3.26606ZM10.346 9L7.99998 4.56863L5.65396 9H10.346Z",
  multilineText: "M4.24999 3C4.43937 3 4.6125 3.107 4.6972 3.27639L6.4472 6.77639C6.5707 7.02338 6.47058 7.32372 6.22359 7.44721C5.9766 7.57071 5.67627 7.4706 5.55277 7.22361L5.17327 6.4646H3.3267L2.9472 7.22361C2.82371 7.4706 2.52337 7.57071 2.27638 7.44721C2.02939 7.32372 1.92928 7.02338 2.05277 6.77639L3.80277 3.27639C3.88747 3.107 4.0606 3 4.24999 3ZM3.8267 5.4646H4.67327L4.24999 4.61803L3.8267 5.4646Z M7.5 3.75C7.22386 3.75 7 3.97386 7 4.25C7 4.52614 7.22386 4.75 7.5 4.75H13.5C13.7761 4.75 14 4.52614 14 4.25C14 3.97386 13.7761 3.75 13.5 3.75H7.5Z M8 6.75C8 6.47386 8.22386 6.25 8.5 6.25H11.5C11.7761 6.25 12 6.47386 12 6.75C12 7.02614 11.7761 7.25 11.5 7.25H8.5C8.22386 7.25 8 7.02614 8 6.75Z M2 9.25C2 8.97386 2.22386 8.75 2.5 8.75H13.5C13.7761 8.75 14 8.97386 14 9.25C14 9.52614 13.7761 9.75 13.5 9.75H2.5C2.22386 9.75 2 9.52614 2 9.25Z M2 11.75C2 11.4739 2.22386 11.25 2.5 11.25H11.5C11.7761 11.25 12 11.4739 12 11.75C12 12.0261 11.7761 12.25 11.5 12.25H2.5C2.22386 12.25 2 12.0261 2 11.75Z",
  number: "M6 2C5.86739 2 5.74021 2.05268 5.64645 2.14645C5.55268 2.24021 5.5 2.36739 5.5 2.5V5.5H2.5C2.36739 5.5 2.24021 5.55268 2.14645 5.64645C2.05268 5.74021 2 5.86739 2 6C2 6.13261 2.05268 6.25979 2.14645 6.35355C2.24021 6.44732 2.36739 6.5 2.5 6.5H5.5V9.5H2.5C2.36739 9.5 2.24021 9.55268 2.14645 9.64645C2.05268 9.74021 2 9.86739 2 10C2 10.1326 2.05268 10.2598 2.14645 10.3536C2.24021 10.4473 2.36739 10.5 2.5 10.5H5.5V13.5C5.5 13.6326 5.55268 13.7598 5.64645 13.8536C5.74021 13.9473 5.86739 14 6 14C6.13261 14 6.25979 13.9473 6.35355 13.8536C6.44732 13.7598 6.5 13.6326 6.5 13.5V10.5H9.5V13.5C9.5 13.6326 9.55268 13.7598 9.64645 13.8536C9.74021 13.9473 9.86739 14 10 14C10.1326 14 10.2598 13.9473 10.3536 13.8536C10.4473 13.7598 10.5 13.6326 10.5 13.5V10.5H13.5C13.6326 10.5 13.7598 10.4473 13.8536 10.3536C13.9473 10.2598 14 10.1326 14 10C14 9.86739 13.9473 9.74021 13.8536 9.64645C13.7598 9.55268 13.6326 9.5 13.5 9.5H10.5V6.5H13.5C13.6326 6.5 13.7598 6.44732 13.8536 6.35355C13.9473 6.25979 14 6.13261 14 6C14 5.86739 13.9473 5.74021 13.8536 5.64645C13.7598 5.55268 13.6326 5.5 13.5 5.5H10.5V2.5C10.5 2.36739 10.4473 2.24021 10.3536 2.14645C10.2598 2.05268 10.1326 2 10 2C9.86739 2 9.74021 2.05268 9.64645 2.14645C9.55268 2.24021 9.5 2.36739 9.5 2.5V5.5H6.5V2.5C6.5 2.36739 6.44732 2.24021 6.35355 2.14645C6.25979 2.05268 6.13261 2 6 2ZM6.5 6.5H9.5V9.5H6.5V6.5Z",
  currency: "M8 1C7.86739 1 7.74021 1.05268 7.64645 1.14645C7.55268 1.24021 7.5 1.36739 7.5 1.5V2.5H6.75C5.09907 2.5 3.75 3.84907 3.75 5.5C3.75 7.15093 5.09907 8.5 6.75 8.5H9.5C10.0306 8.49999 10.539 8.7106 10.9142 9.08582C11.6995 9.87098 11.6995 11.129 10.9142 11.9142C10.539 12.2894 10.0306 12.5 9.5 12.5H6.5C5.3895 12.5001 4.49994 11.6105 4.5 10.5C4.5 10.3674 4.44732 10.2402 4.35355 10.1464C4.25979 10.0527 4.13261 10 4 10C3.86739 10 3.74021 10.0527 3.64645 10.1464C3.55268 10.2402 3.5 10.3674 3.5 10.5C3.49992 12.151 4.84902 13.5001 6.5 13.5H7.5V14.5C7.5 14.6326 7.55268 14.7598 7.64645 14.8536C7.74021 14.9473 7.86739 15 8 15C8.13261 15 8.25979 14.9473 8.35355 14.8536C8.44732 14.7598 8.5 14.6326 8.5 14.5V13.5H9.5C10.2954 13.5 11.0588 13.1838 11.6212 12.6213C12.7888 11.454 12.7888 9.54599 11.6212 8.37866C11.0588 7.81619 10.2954 7.49998 9.5 7.5H6.75C5.63951 7.5 4.75 6.61049 4.75 5.5C4.75 4.38951 5.63951 3.5 6.75 3.5H9C10.1105 3.50008 10.9999 4.38953 11 5.5C11 5.63261 11.0527 5.75979 11.1464 5.85355C11.2402 5.94732 11.3674 6 11.5 6C11.6326 6 11.7598 5.94732 11.8536 5.85355C11.9473 5.75979 12 5.63261 12 5.5C11.9999 3.84914 10.6509 2.50012 9 2.5H8.5V1.5C8.5 1.36739 8.44732 1.24021 8.35355 1.14645C8.25979 1.05268 8.13261 1 8 1Z",
  percent: "M4.75 2.5C3.51328 2.5 2.5 3.51328 2.5 4.75C2.5 5.98672 3.51328 7 4.75 7C5.98672 7 7 5.98672 7 4.75C7 3.51328 5.98672 2.5 4.75 2.5ZM4.75 3.5C5.44628 3.5 6 4.05372 6 4.75C6 5.44628 5.44628 6 4.75 6C4.05372 6 3.5 5.44628 3.5 4.75C3.5 4.05372 4.05372 3.5 4.75 3.5Z M11.25 9C10.0133 9 9 10.0133 9 11.25C9 12.4867 10.0133 13.5 11.25 13.5C12.4867 13.5 13.5 12.4867 13.5 11.25C13.5 10.0133 12.4867 9 11.25 9ZM11.25 10C11.9463 10 12.5 10.5537 12.5 11.25C12.5 11.9463 11.9463 12.5 11.25 12.5C10.5537 12.5 10 11.9463 10 11.25C10 10.5537 10.5537 10 11.25 10Z M12.5 3C12.3674 3.00002 12.2402 3.05271 12.1465 3.14648L3.14648 12.1465C3.05274 12.2402 3.00008 12.3674 3.00008 12.5C3.00008 12.6326 3.05274 12.7598 3.14648 12.8535C3.24025 12.9473 3.36741 12.9999 3.5 12.9999C3.63259 12.9999 3.75975 12.9473 3.85352 12.8535L12.8535 3.85352C12.9473 3.75975 12.9999 3.63259 12.9999 3.5C12.9999 3.36741 12.9473 3.24025 12.8535 3.14648C12.7598 3.05271 12.6326 3.00002 12.5 3Z",
  rating: "M7.99975 0.996094C7.57318 0.996128 7.18836 1.26571 7.04259 1.66663L7.04784 1.65295L5.67284 5.11548C5.67142 5.119 5.67004 5.12254 5.66869 5.1261C5.66565 5.13431 5.66096 5.1378 5.65221 5.13831C5.6512 5.13838 5.65018 5.13846 5.64916 5.13855L1.96166 5.37598C1.96179 5.37594 1.96154 5.37602 1.96166 5.37598C1.51229 5.40516 1.16169 5.73277 1.04735 6.10218C0.933105 6.47128 1.03046 6.92969 1.37353 7.21521L4.19848 9.57142C4.19901 9.57187 4.19954 9.57232 4.20007 9.57276C4.21392 9.58422 4.21899 9.59964 4.21471 9.61707C4.21472 9.61701 4.2147 9.61712 4.21471 9.61707L3.37182 12.9387C3.2506 13.4131 3.44889 13.8848 3.78808 14.1426C4.12727 14.4004 4.64722 14.4608 5.06213 14.1968L7.99255 12.3412C7.9923 12.3414 7.99279 12.341 7.99255 12.3412C7.99764 12.338 8.00161 12.3375 8.00671 12.3407C8.00662 12.3407 8.00679 12.3407 8.00671 12.3407L11.157 14.3408C11.1574 14.3411 11.1578 14.3414 11.1582 14.3417C11.5342 14.5789 12.0093 14.5257 12.3175 14.2924C12.6257 14.059 12.8055 13.6299 12.6971 13.2023C12.6972 13.2024 12.6971 13.2021 12.6971 13.2023L11.7853 9.61706C11.781 9.59967 11.786 9.58428 11.7998 9.57287C11.8004 9.57238 11.8009 9.5719 11.8015 9.57141L14.6261 7.21545C14.9694 6.92994 15.0669 6.4714 14.9526 6.10217C14.8383 5.73276 14.4881 5.40527 14.0387 5.37609C14.0388 5.37613 14.0386 5.37605 14.0387 5.37609L10.3508 5.13854C10.3498 5.13846 10.3487 5.13838 10.3476 5.1383C10.3389 5.13779 10.3343 5.13439 10.3313 5.12621C10.3299 5.12262 10.3286 5.11904 10.3271 5.11547L8.95213 1.65295L8.95738 1.66674C8.81167 1.26574 8.4264 0.996056 7.99975 0.996094ZM7.99987 1.99609C7.99974 1.99609 8 1.99609 7.99987 1.99609C8.00935 1.99609 8.01434 1.99939 8.01758 2.0083C8.01926 2.01292 8.02101 2.01752 8.02283 2.02209L9.39783 5.4845L9.39368 5.47375C9.53379 5.85173 9.88715 6.11327 10.2896 6.13672L13.9741 6.37402C14.006 6.37609 13.9898 6.37346 13.9973 6.39782C14.0048 6.42217 14.0118 6.42588 13.9868 6.44665C13.9865 6.44686 13.9862 6.44707 13.986 6.44728L11.1627 8.80214C10.8543 9.05717 10.7183 9.46962 10.8147 9.85805C10.8149 9.85894 10.8152 9.85984 10.8154 9.86073L11.7278 13.4478C11.7382 13.4889 11.7274 13.4848 11.7137 13.4951C11.7001 13.5055 11.722 13.5149 11.6918 13.4959L8.54296 11.4967C8.21256 11.2868 7.78728 11.2867 7.4569 11.4967C7.45696 11.4967 7.45684 11.4968 7.4569 11.4967L4.52623 13.3525C4.52591 13.3527 4.52558 13.353 4.52526 13.3532C4.45892 13.3954 4.43836 13.3808 4.39318 13.3465C4.34799 13.3121 4.31816 13.2744 4.34068 13.1863C4.3406 13.1866 4.34077 13.186 4.34068 13.1863L5.18468 9.86049C5.18489 9.85959 5.18509 9.8587 5.18529 9.8578C5.28156 9.46947 5.14573 9.05742 4.83752 8.80237L2.01403 6.44727C2.01375 6.44706 2.01346 6.44685 2.01318 6.44664C1.98816 6.42587 1.99514 6.42216 2.00268 6.39781C2.01021 6.37347 1.99424 6.37596 2.02612 6.37389L5.71337 6.13646L5.71032 6.13659C6.11276 6.11317 6.46615 5.85184 6.60632 5.47387L7.97717 2.02209C7.97898 2.01751 7.98073 2.01292 7.98242 2.00829C7.98567 1.99933 7.99034 1.99609 7.99987 1.99609Z",
  formula: "M9.66763 2H11.5C11.7761 2 12 2.22386 12 2.5C12 2.77614 11.7761 3 11.5 3H9.66935C9.31784 3.00091 8.97771 3.12472 8.70787 3.35002C8.43803 3.57531 8.25549 3.88789 8.19186 4.23359L7.59893 7.5H10.5C10.7761 7.5 11 7.72386 11 8C11 8.27614 10.7761 8.5 10.5 8.5H7.41741L6.79196 11.9456L6.79177 11.9466C6.68604 12.5221 6.38224 13.0426 5.93303 13.4176C5.48383 13.7927 4.91755 13.9987 4.33237 14H2.5C2.22386 14 2 13.7761 2 13.5C2 13.2239 2.22386 13 2.5 13H4.33013C4.68169 12.9991 5.02226 12.8753 5.29213 12.65C5.56198 12.4247 5.74452 12.1121 5.80814 11.7664L6.40107 8.5H3.5C3.22386 8.5 3 8.27614 3 8C3 7.72386 3.22386 7.5 3.5 7.5H6.58259L7.20823 4.05341C7.31396 3.47785 7.61776 2.95744 8.06697 2.58239C8.51617 2.20735 9.08245 2.00131 9.66763 2Z M13.3536 10.8536C13.5488 10.6583 13.5488 10.3417 13.3536 10.1464C13.1583 9.95118 12.8417 9.95118 12.6464 10.1464L11.5 11.2929L10.3536 10.1464C10.1583 9.95118 9.84171 9.95118 9.64645 10.1464C9.45118 10.3417 9.45118 10.6583 9.64645 10.8536L10.7929 12L9.64645 13.1464C9.45118 13.3417 9.45118 13.6583 9.64645 13.8536C9.84171 14.0488 10.1583 14.0488 10.3536 13.8536L11.5 12.7071L12.6464 13.8536C12.8417 14.0488 13.1583 14.0488 13.3536 13.8536C13.5488 13.6583 13.5488 13.3417 13.3536 13.1464L12.2071 12L13.3536 10.8536Z",
  checkbox: "M10.7617 6.00012C10.6292 5.99702 10.5008 6.04668 10.4049 6.13818L7.08154 9.30872L5.59546 7.88855C5.49959 7.79693 5.37126 7.74715 5.23869 7.75014C5.10612 7.75314 4.98017 7.80868 4.88855 7.90454C4.79693 8.00041 4.74715 8.12874 4.75014 8.26131C4.75314 8.39388 4.80868 8.51983 4.90454 8.61145L6.73584 10.3615C6.82881 10.4503 6.95244 10.4999 7.08104 10.5C7.20964 10.5 7.33332 10.4506 7.42639 10.3618L11.0951 6.86182C11.1426 6.81651 11.1807 6.76227 11.2073 6.70222C11.2338 6.64217 11.2483 6.57748 11.2499 6.51183C11.2514 6.44619 11.24 6.38089 11.2163 6.31965C11.1927 6.25841 11.1571 6.20243 11.1118 6.15492C11.0665 6.10739 11.0122 6.06925 10.9522 6.04269C10.8921 6.01614 10.8274 6.00166 10.7617 6.00012Z M3 2C2.45364 2 2 2.45364 2 3V13C2 13.5464 2.45364 14 3 14H13C13.5464 14 14 13.5464 14 13V3C14 2.45364 13.5464 2 13 2H3ZM3 3H13V13H3V3Z",
  date: "M4.5 8C4.22386 8 4 8.22386 4 8.5C4 8.77614 4.22386 9 4.5 9H11C11.2761 9 11.5 8.77614 11.5 8.5C11.5 8.22386 11.2761 8 11 8H4.5Z M4 10.5C4 10.2239 4.22386 10 4.5 10H7.5C7.77614 10 8 10.2239 8 10.5C8 10.7761 7.77614 11 7.5 11H4.5C4.22386 11 4 10.7761 4 10.5Z M11.5 0.5C11.5 0.223858 11.2761 0 11 0C10.7239 0 10.5 0.223858 10.5 0.5V2H5.5V0.5C5.5 0.223858 5.27614 0 5 0C4.72386 0 4.5 0.223858 4.5 0.5V2H2.5C1.67157 2 1 2.67157 1 3.5V12.5C1 13.3284 1.67157 14 2.5 14H13.5C14.3284 14 15 13.3284 15 12.5V3.5C15 2.67157 14.3284 2 13.5 2H11.5V0.5ZM5 3.00082C4.99029 3.00082 4.98064 3.00055 4.97107 3H2.5C2.22386 3 2 3.22386 2 3.5V5H14V3.5C14 3.22386 13.7761 3 13.5 3H11.0289C11.0194 3.00055 11.0097 3.00082 11 3.00082C10.9903 3.00082 10.9806 3.00055 10.9711 3H5.02893C5.01936 3.00055 5.00971 3.00082 5 3.00082ZM2 12.5V6H14V12.5C14 12.7761 13.7761 13 13.5 13H2.5C2.22386 13 2 12.7761 2 12.5Z",
  email: "M2.5 4H13.5V12H2.50012L2.5 4Z M2 3C1.8674 3.00001 1.74023 3.0527 1.64646 3.14646C1.5527 3.24023 1.50001 3.3674 1.5 3.5V12C1.50007 12.5463 1.95357 12.9999 2.49988 13C2.49984 13 2.49992 13 2.49988 13H13.5C14.0464 13 14.5 12.5464 14.5 12V3.5C14.5 3.3674 14.4473 3.24023 14.3535 3.14646C14.2598 3.0527 14.1326 3.00001 14 3H2ZM1.97827 3.00049C1.84581 3.00625 1.72107 3.06439 1.63147 3.16211C1.54186 3.25985 1.49475 3.38919 1.50049 3.52167C1.50624 3.65414 1.56437 3.77891 1.66211 3.86853L7.66211 9.36853C7.75433 9.45307 7.87489 9.49996 8 9.49996C8.12511 9.49996 8.24567 9.45307 8.33789 9.36853L14.3379 3.86853C14.4356 3.77891 14.4938 3.65414 14.4995 3.52167C14.5053 3.38919 14.4581 3.25985 14.3685 3.16211C14.2789 3.06437 14.1541 3.00624 14.0217 3.00049C13.8892 2.99475 13.7599 3.04186 13.6621 3.13147L8 8.32166L2.33789 3.13147C2.28949 3.08709 2.23281 3.05268 2.17111 3.03021C2.10941 3.00773 2.04388 2.99764 1.97827 3.00049Z",
  url: "M6.93749 5.81154C6.07524 5.8115 5.24801 6.15511 4.63952 6.76601L2.87206 8.52712C2.86639 8.53281 2.86085 8.53863 2.85546 8.54458C2.29141 9.16344 1.98729 9.97626 2.0067 10.8134C2.03651 12.0876 2.80961 13.23 3.98144 13.7313C5.15327 14.2327 6.51333 14.003 7.45556 13.1447C7.4613 13.1395 7.46691 13.1341 7.4724 13.1287L8.70983 11.8911C8.75627 11.8447 8.7931 11.7896 8.81823 11.7289C8.84336 11.6683 8.8563 11.6032 8.8563 11.5376C8.8563 11.4719 8.84336 11.4069 8.81823 11.3462C8.7931 11.2855 8.75627 11.2304 8.70983 11.184C8.61606 11.0902 8.4889 11.0376 8.35631 11.0376C8.22372 11.0376 8.09656 11.0902 8.0028 11.184L6.77868 12.4081C6.12438 13.0019 5.18732 13.1596 4.37475 12.8119C3.56087 12.4637 3.02721 11.6752 3.00646 10.7902C2.99307 10.2112 3.20288 9.64994 3.59215 9.22133L5.34667 7.47304C5.34712 7.47259 5.34757 7.47215 5.34801 7.4717C5.76912 7.04892 6.34077 6.81151 6.93749 6.81155C7.53422 6.81155 8.10583 7.04894 8.52697 7.4717C8.57331 7.51823 8.62836 7.55517 8.68898 7.58041C8.7496 7.60566 8.8146 7.61872 8.88027 7.61884C8.94593 7.61897 9.01098 7.60616 9.0717 7.58114C9.13241 7.55612 9.1876 7.51939 9.23412 7.47305C9.32804 7.37946 9.38095 7.2524 9.3812 7.11981C9.38145 6.98722 9.32903 6.85996 9.23547 6.76601C8.62695 6.15516 7.79971 5.81155 6.93749 5.81154Z M10.8133 2.00905C10.0028 1.99035 9.18543 2.27105 8.54442 2.85549C8.53873 2.86069 8.53315 2.86602 8.5277 2.87148L7.29015 4.10891C7.24372 4.15534 7.20688 4.21046 7.18175 4.27113C7.15662 4.33179 7.14368 4.39682 7.14368 4.46248C7.14368 4.52815 7.15662 4.59317 7.18175 4.65384C7.20688 4.71451 7.24372 4.76963 7.29015 4.81606C7.33658 4.8625 7.3917 4.89933 7.45237 4.92446C7.51304 4.9496 7.57806 4.96253 7.64373 4.96253C7.70939 4.96253 7.77442 4.9496 7.83508 4.92446C7.89575 4.89933 7.95087 4.8625 7.9973 4.81606L9.22155 3.59194C10.1163 2.77921 11.4786 2.81129 12.3337 3.6664C13.1889 4.5216 13.2208 5.88409 12.4078 6.77883L10.6533 8.52712C10.6529 8.52757 10.6524 8.52802 10.652 8.52846C10.0089 9.17407 9.04352 9.36659 8.20202 9.01699C8.20206 9.01699 8.20197 9.01699 8.20202 9.01699C7.92924 8.90373 7.68135 8.7377 7.47289 8.52846C7.3793 8.43454 7.25224 8.38164 7.11965 8.38138C6.98706 8.38113 6.8598 8.43355 6.76586 8.52712C6.71933 8.57346 6.68239 8.62851 6.65714 8.68913C6.6319 8.74975 6.61884 8.81475 6.61871 8.88042C6.61859 8.94609 6.6314 9.01113 6.65642 9.07185C6.68143 9.13256 6.71816 9.18776 6.76451 9.23427C7.06587 9.53674 7.42403 9.77671 7.81835 9.94045C9.03169 10.4445 10.4332 10.165 11.3605 9.23415L13.1279 7.47304C13.1336 7.4674 13.1391 7.46162 13.1445 7.45571C14.3134 6.17372 14.2675 4.18601 13.0408 2.95925C12.4274 2.34587 11.6239 2.02776 10.8133 2.00905Z",
  phone: "M5.05505 1.529C3.31253 1.75232 2.0004 3.24304 2 4.99983C2 4.99979 2 4.99987 2 4.99983C2 9.96446 6.03536 13.9999 11 13.9999C12.7569 13.9997 14.2478 12.6874 14.471 10.9448C14.5276 10.503 14.2784 10.0721 13.8672 9.90082L10.9504 8.64814C10.9505 8.64815 10.9504 8.64812 10.9504 8.64814C10.6386 8.51176 10.2765 8.54499 9.99475 8.73591C9.99494 8.73578 9.99463 8.73603 9.99475 8.73591L8.4348 9.77791C8.43415 9.77831 8.4335 9.77872 8.43285 9.77913C7.4727 9.31646 6.69738 8.54299 6.23241 7.58393L7.27404 5.99958C7.27519 5.99788 7.27633 5.99617 7.27746 5.99446C7.45646 5.71466 7.48468 5.36245 7.35265 5.05769L6.09912 2.13276C5.92786 1.72149 5.49693 1.4723 5.05505 1.529ZM5.17798 2.52192L6.4342 5.4532C6.43449 5.45389 6.43477 5.45458 6.43506 5.45527C6.43544 5.45472 6.43531 5.45587 6.43506 5.45527L5.39465 7.03779C5.394 7.0388 5.39335 7.03982 5.3927 7.04084C5.2041 7.33226 5.18035 7.7027 5.3302 8.01582C5.33052 8.01651 5.33085 8.0172 5.33118 8.01789C5.89488 9.182 6.8364 10.121 8.00195 10.6817C8.00285 10.6822 8.00374 10.6826 8.00464 10.6831C8.3233 10.834 8.70011 10.8053 8.99231 10.608L10.5527 9.56574C10.5526 9.56578 10.5528 9.5657 10.5527 9.56574L13.4777 10.8218C13.4776 10.8217 13.4778 10.8219 13.4777 10.8218C13.3156 12.0709 12.26 12.9997 11 12.9999C6.5758 12.9999 3 9.42414 3 4.99995C3.00034 3.73992 3.92891 2.68417 5.17798 2.52192Z",
  singleSelect: "M5.77625 6.75073C5.64385 6.74375 5.5141 6.78963 5.41553 6.8783C5.36671 6.92222 5.32702 6.97532 5.29873 7.03458C5.27044 7.09384 5.2541 7.1581 5.25064 7.22367C5.24719 7.28925 5.25668 7.35486 5.27858 7.41677C5.30048 7.47868 5.33437 7.53566 5.3783 7.58447L7.6283 10.0845C7.67519 10.1366 7.73251 10.1782 7.79655 10.2068C7.86058 10.2353 7.9299 10.25 8 10.25C8.0701 10.25 8.13942 10.2353 8.20345 10.2068C8.26749 10.1782 8.32481 10.1366 8.3717 10.0845L10.6217 7.58447C10.6656 7.53566 10.6995 7.47868 10.7214 7.41677C10.7433 7.35486 10.7528 7.28925 10.7494 7.22367C10.7459 7.1581 10.7296 7.09384 10.7013 7.03458C10.673 6.97532 10.6333 6.92222 10.5845 6.8783C10.5357 6.83437 10.4787 6.80048 10.4168 6.77858C10.3549 6.75668 10.2892 6.74719 10.2237 6.75064C10.1581 6.7541 10.0938 6.77044 10.0346 6.79873C9.97532 6.82702 9.92222 6.86671 9.8783 6.91553L8 9.00256L6.1217 6.91553C6.07777 6.86672 6.02464 6.82704 5.96537 6.79877C5.90609 6.77049 5.84183 6.75417 5.77625 6.75073Z M8 1.5C4.41604 1.5 1.5 4.41604 1.5 8C1.5 11.5839 4.41603 14.5 8 14.5C11.5839 14.5 14.5 11.5839 14.5 8C14.5 4.41603 11.5839 1.5 8 1.5ZM8 2.5C11.0435 2.5 13.5 4.95647 13.5 8C13.5 11.0435 11.0435 13.5 8 13.5C4.95647 13.5 2.5 11.0435 2.5 8C2.5 4.95647 4.95647 2.5 8 2.5Z",
  multiSelect: "M10.6096 3.18767C10.7821 2.97204 11.0967 2.93708 11.3124 3.10958L12.25 3.85971L13.1877 3.10958C13.4033 2.93708 13.7179 2.97204 13.8905 3.18767C14.063 3.4033 14.028 3.71795 13.8124 3.89045L12.5624 4.89045C12.3798 5.03654 12.1203 5.03654 11.9377 4.89045L10.6877 3.89045C10.472 3.71795 10.4371 3.4033 10.6096 3.18767Z M2 4C2 3.72386 2.22386 3.5 2.5 3.5H8C8.27614 3.5 8.5 3.72386 8.5 4C8.5 4.27614 8.27614 4.5 8 4.5H2.5C2.22386 4.5 2 4.27614 2 4Z M2.5 7.5C2.22386 7.5 2 7.72386 2 8C2 8.27614 2.22386 8.5 2.5 8.5H8C8.27614 8.5 8.5 8.27614 8.5 8C8.5 7.72386 8.27614 7.5 8 7.5H2.5Z M2.5 11.5C2.22386 11.5 2 11.7239 2 12C2 12.2761 2.22386 12.5 2.5 12.5H8C8.27614 12.5 8.5 12.2761 8.5 12C8.5 11.7239 8.27614 11.5 8 11.5H2.5Z M10.6096 7.18767C10.7821 6.97204 11.0967 6.93708 11.3124 7.10958L12.25 7.85971L13.1877 7.10958C13.4033 6.93708 13.7179 6.97204 13.8905 7.18767C14.063 7.4033 14.028 7.71795 13.8124 7.89045L12.5624 8.89045C12.3798 9.03654 12.1203 9.03654 11.9377 8.89045L10.6877 7.89045C10.472 7.71795 10.4371 7.4033 10.6096 7.18767Z M10.6096 11.1877C10.7821 10.972 11.0967 10.9371 11.3124 11.1096L12.25 11.8597L13.1877 11.1096C13.4033 10.9371 13.7179 10.972 13.8905 11.1877C14.063 11.4033 14.028 11.7179 13.8124 11.8905L12.5624 12.8905C12.3798 13.0365 12.1203 13.0365 11.9377 12.8905L10.6877 11.8905C10.472 11.7179 10.4371 11.4033 10.6096 11.1877Z",
  linkedRecord: "M3.5 3.5C3.22386 3.5 3 3.72386 3 4C3 4.27614 3.22386 4.5 3.5 4.5H13.5C13.7761 4.5 14 4.27614 14 4C14 3.72386 13.7761 3.5 13.5 3.5H3.5Z M3.5 11.5C3.22386 11.5 3 11.7239 3 12C3 12.2761 3.22386 12.5 3.5 12.5H13.5C13.7761 12.5 14 12.2761 14 12C14 11.7239 13.7761 11.5 13.5 11.5H3.5Z M9.25 7.5C8.97386 7.5 8.75 7.72386 8.75 8C8.75 8.27614 8.97386 8.5 9.25 8.5H13.5C13.7761 8.5 14 8.27614 14 8C14 7.72386 13.7761 7.5 13.5 7.5H9.25Z M5.85355 6.14645C5.65829 5.95118 5.34171 5.95118 5.14645 6.14645C4.95118 6.34171 4.95118 6.65829 5.14645 6.85355L5.79289 7.5H1.5C1.22386 7.5 1 7.72386 1 8C1 8.27614 1.22386 8.5 1.5 8.5H5.79289L5.14645 9.14645C4.95118 9.34171 4.95118 9.65829 5.14645 9.85355C5.34171 10.0488 5.65829 10.0488 5.85355 9.85355L7.35355 8.35355C7.40149 8.30562 7.43766 8.25036 7.46206 8.19139C7.48615 8.13331 7.4996 8.0697 7.49999 8.003L7.5 8L7.49999 7.997C7.4996 7.9303 7.48615 7.86669 7.46206 7.80861C7.43766 7.74964 7.40149 7.69439 7.35355 7.64645L5.85355 6.14645Z",
  attachment: "M9.5 1.5C9.36739 1.5 9.24021 1.55268 9.14645 1.64645C9.05268 1.74021 9 1.86739 9 2V5.5C9.00001 5.6326 9.0527 5.75977 9.14646 5.85354C9.24023 5.9473 9.3674 5.99999 9.5 6H13C13.1326 6 13.2598 5.94732 13.3536 5.85355C13.4473 5.75979 13.5 5.63261 13.5 5.5C13.5 5.36739 13.4473 5.24021 13.3536 5.14645C13.2598 5.05268 13.1326 5 13 5H10V2C10 1.86739 9.94732 1.74021 9.85355 1.64645C9.75979 1.55268 9.63261 1.5 9.5 1.5Z M3.5 1.5C2.95364 1.5 2.5 1.95364 2.5 2.5V13.5C2.50007 14.0463 2.95357 14.4999 3.49988 14.5C3.49984 14.5 3.49992 14.5 3.49988 14.5H12.5C13.0464 14.5 13.5 14.0464 13.5 13.5V5.5C13.5 5.36739 13.4473 5.24021 13.3536 5.14645L9.85355 1.64645C9.75979 1.55268 9.63261 1.5 9.5 1.5H3.5ZM3.5 2.5H9.29285L12.5 5.70715V13.5H3.50012L3.5 2.5Z",
};

export function FieldIcon({ type }: { type: FieldType }) {
  return (
    <svg
      className="field-icon"
      width="16"
      height="16"
      viewBox="0 0 16 16"
      fill="currentColor"
      aria-hidden="true"
    >
      <path fillRule="evenodd" clipRule="evenodd" d={ICON_D[type]} />
    </svg>
  );
}

function CheckboxGlyph({ checked }: { checked: boolean }) {
  return (
    <span className={`at-bool ${checked ? "is-on" : "is-off"}`}>
      <svg className="at-check" width="14" height="14" viewBox="0 0 14 14" aria-hidden="true">
        <rect
          x="0.5"
          y="0.5"
          width="13"
          height="13"
          rx="2.5"
          fill={checked ? "currentColor" : "none"}
          stroke="currentColor"
        />
        {checked ? (
          <path
            d="M3.5 7.2l2.2 2.2 4.6-4.6"
            fill="none"
            stroke="#fff"
            strokeWidth="1.6"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        ) : null}
      </svg>
      {checked ? "Checked" : "Unchecked"}
    </span>
  );
}

function formatDate(value: string): string {
  // A bare date parses as UTC midnight, which toLocaleDateString renders as
  // the previous day in timezones west of UTC; parse it as local time instead.
  return /^\d{4}-\d{2}-\d{2}$/.test(value)
    ? new Date(`${value}T00:00:00`).toLocaleDateString()
    : new Date(value).toLocaleString();
}

export function formatFieldValue(value: unknown, ctx?: FieldContext): ReactNode {
  if (value === null || value === undefined || value === "") {
    return <span className="field-empty">Empty</span>;
  }

  if (typeof value === "boolean") {
    return <CheckboxGlyph checked={value} />;
  }

  if (typeof value === "number") {
    return value.toLocaleString();
  }

  if (typeof value === "string") {
    if (isDateLikeString(value)) return formatDate(value);
    if (URL_RE.test(value)) {
      return (
        <a className="cell-link" href={value} target="_blank" rel="noreferrer noopener">
          {value}
        </a>
      );
    }
    if (EMAIL_RE.test(value)) {
      return (
        <a className="cell-link" href={`mailto:${value}`}>
          {value}
        </a>
      );
    }
    return value;
  }

  if (Array.isArray(value)) {
    if (value.length === 0) {
      return <span className="field-empty">Empty</span>;
    }
    return (
      <div className="field-stack">
        {value.map((entry, index) => {
          if (entry !== null && typeof entry === "object") {
            const obj = entry as Record<string, unknown>;
            const filename = typeof obj.filename === "string" ? obj.filename : undefined;
            const url = typeof obj.url === "string" ? obj.url : undefined;
            const mime = typeof obj.type === "string" ? obj.type : "";
            type Thumb = { url?: string; width?: number; height?: number };
            const thumbs = obj.thumbnails as
              | { small?: Thumb; large?: Thumb; full?: Thumb }
              | undefined;
            // Prefer the large thumbnail so a 64px-tall preview stays crisp.
            const thumb = thumbs?.large ?? thumbs?.full ?? thumbs?.small;
            const thumbUrl = thumb?.url ?? url;
            const width = typeof obj.width === "number" ? obj.width : thumb?.width;
            const height = typeof obj.height === "number" ? obj.height : thumb?.height;
            // Reserve the cell's width from the known aspect ratio so loading the
            // image doesn't reflow the column.
            const aspectStyle =
              width && height ? { aspectRatio: `${width} / ${height}` } : undefined;
            const isImage =
              mime.startsWith("image/") ||
              (filename ? /\.(png|jpe?g|gif|webp|bmp|svg)$/i.test(filename) : false);
            if (url && isImage && thumbUrl) {
              // Image attachments preview as thumbnails, the way Airtable cells do.
              return (
                <a
                  className="at-thumb-link"
                  key={index}
                  href={url}
                  target="_blank"
                  rel="noreferrer noopener"
                  title={filename}
                >
                  <img
                    className="at-thumb"
                    src={thumbUrl}
                    alt={filename ?? "attachment"}
                    loading="lazy"
                    style={aspectStyle}
                  />
                </a>
              );
            }
            if (filename && url) {
              return (
                <a className="cell-link" key={index} href={url} target="_blank" rel="noreferrer noopener">
                  {filename}
                </a>
              );
            }
            return <span className="at-token" key={index}>{filename ?? "Attachment"}</span>;
          }
          const text = String(entry);
          if (RECORD_ID_RE.test(text)) {
            // Linked record: show its primary-field name (resolved server-side)
            // and link to the record in a new tab when we know its table.
            const ref = ctx?.linked?.[text];
            const label = ref?.name && ref.name.trim() !== "" ? ref.name : text;
            const href = recordUrl(ctx?.baseId, ref?.table_id, text);
            if (href) {
              return (
                <a
                  className="at-link"
                  key={index}
                  href={href}
                  target="_blank"
                  rel="noreferrer noopener"
                  data-tooltip="Open linked record in Airtable"
                >
                  {label}
                </a>
              );
            }
            return (
              <span className="at-link" key={index}>
                {label}
              </span>
            );
          }
          return (
            <span className="at-token" key={index}>
              {text}
            </span>
          );
        })}
      </div>
    );
  }

  return <span className="field-empty">Object</span>;
}

// Plain-text version of a value, for cell title (hover) tooltips on truncated cells.
export function valueToText(value: unknown, ctx?: FieldContext): string {
  if (value === null || value === undefined || value === "") return "Empty";
  if (typeof value === "boolean") return value ? "Checked" : "Unchecked";
  if (typeof value === "number") return value.toLocaleString();
  if (typeof value === "string") return isDateLikeString(value) ? formatDate(value) : value;
  if (Array.isArray(value)) {
    return value
      .map((v) => {
        if (v !== null && typeof v === "object") {
          return String((v as Record<string, unknown>).filename ?? "Attachment");
        }
        const text = String(v);
        const name = ctx?.linked?.[text]?.name;
        return name && name.trim() !== "" ? name : text;
      })
      .join(", ");
  }
  return JSON.stringify(value);
}

export function getOperationIDFromPath(pathname: string): string {
  const parts = pathname.split("/").filter(Boolean);
  if (parts.length < 2 || parts[0] !== "approve") {
    return "";
  }
  return parts[1];
}

export function countdownLabel(expiresAt: string, now = new Date()): string {
  const remaining = new Date(expiresAt).getTime() - now.getTime();
  if (remaining <= 0) {
    return "expired";
  }
  const totalSeconds = Math.floor(remaining / 1000);
  const minutes = Math.floor(totalSeconds / 60);
  if (minutes >= 60) {
    return `${Math.floor(minutes / 60)}h ${minutes % 60}m`;
  }
  const seconds = totalSeconds % 60;
  return `${minutes}m ${String(seconds).padStart(2, "0")}s`;
}

export function relativeTime(iso: string, now = new Date()): string {
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) {
    return "unknown";
  }
  const diffSeconds = Math.round((now.getTime() - then) / 1000);
  const absSeconds = Math.abs(diffSeconds);
  if (absSeconds < 45) {
    return "just now";
  }
  const minutes = Math.round(absSeconds / 60);
  if (minutes < 60) {
    return diffSeconds >= 0 ? `${minutes} min ago` : `in ${minutes} min`;
  }
  const hours = Math.round(minutes / 60);
  if (hours < 24) {
    return diffSeconds >= 0 ? `${hours} hr ago` : `in ${hours} hr`;
  }
  return new Date(iso).toLocaleDateString();
}

// Full timestamp for hover tooltips on relative-time labels (e.g. "Jun 26, 2026, 2:34 PM").
export function absoluteTime(iso: string): string {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) {
    return "";
  }
  return date.toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" });
}

const STATUS_LABELS: Record<OperationStatus, string> = {
  pending_approval: "Waiting for your approval",
  approved: "Approved",
  rejected: "Rejected",
  expired: "Request expired",
  executing: "Applying changes",
  completed: "Changes applied",
  partially_completed: "Partly applied",
  failed: "Couldn't apply changes",
};

export type StatusTone = "info" | "success" | "warning" | "danger" | "neutral";

const STATUS_TONES: Record<OperationStatus, StatusTone> = {
  pending_approval: "info",
  approved: "info",
  executing: "info",
  completed: "success",
  partially_completed: "warning",
  failed: "danger",
  expired: "neutral",
  rejected: "danger",
};

export function statusLabel(status: OperationStatus): string {
  return STATUS_LABELS[status] ?? status;
}

export function statusTone(status: OperationStatus): StatusTone {
  return STATUS_TONES[status] ?? "neutral";
}

export function collectFieldNames(
  currentFields?: Record<string, unknown>,
  nextFields?: Record<string, unknown>,
): string[] {
  return Array.from(
    new Set([...Object.keys(currentFields ?? {}), ...Object.keys(nextFields ?? {})]),
  ).sort();
}
