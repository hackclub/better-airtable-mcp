import type { ReactNode } from "react";

function isDateLikeString(value: string): boolean {
  if (!value.includes("T") && !/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return false;
  }
  return !Number.isNaN(Date.parse(value));
}

export function formatFieldValue(value: unknown): ReactNode {
  if (value === null || value === undefined) {
    return <span className="field-empty">empty</span>;
  }

  if (typeof value === "boolean") {
    return <span>{value ? "[x] checked" : "[ ] unchecked"}</span>;
  }

  if (typeof value === "number") {
    return value.toLocaleString();
  }

  if (typeof value === "string") {
    if (isDateLikeString(value)) {
      return new Date(value).toLocaleString();
    }
    return value;
  }

  if (Array.isArray(value)) {
    if (value.length === 0) {
      return <span className="field-empty">empty</span>;
    }

    return (
      <div className="field-stack">
        {value.map((entry, index) => (
          <div key={index}>{formatFieldValue(entry)}</div>
        ))}
      </div>
    );
  }

  return <pre className="field-pre">{JSON.stringify(value, null, 2)}</pre>;
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
  const seconds = totalSeconds % 60;
  return `${minutes}m ${String(seconds).padStart(2, "0")}s`;
}

export function collectFieldNames(
  currentFields?: Record<string, unknown>,
  nextFields?: Record<string, unknown>,
): string[] {
  return Array.from(
    new Set([
      ...Object.keys(currentFields ?? {}),
      ...Object.keys(nextFields ?? {}),
    ]),
  ).sort();
}
