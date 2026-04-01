import { useEffect, useMemo, useState } from "react";
import { DeletePreview } from "./components/DeletePreview";
import { DiffView } from "./components/DiffView";
import { RecordTable } from "./components/RecordTable";
import {
  countdownLabel,
  getOperationIDFromPath,
} from "./formatters";
import type { OperationPreview, OperationView } from "./types";
import "./styles.css";

interface AppProps {
  pathname?: string;
  fetchImpl?: typeof fetch;
}

function operationClientLabel(operation: OperationView): string {
  if (operation.mcp_client_name && operation.mcp_client_id) {
    return `${operation.mcp_client_name} (${operation.mcp_client_id})`;
  }
  return operation.mcp_client_name ?? operation.mcp_client_id ?? "unknown";
}

function StatusPanel({
  operation,
  onAction,
  busy,
}: {
  operation: OperationView;
  onAction: (action: "approve" | "reject") => Promise<void>;
  busy: boolean;
}) {
  const [now, setNow] = useState(() => new Date());

  useEffect(() => {
    const timer = window.setInterval(() => setNow(new Date()), 1000);
    return () => window.clearInterval(timer);
  }, []);

  return (
    <aside className="detail-card">
      <h2>Request Details</h2>
      <dl className="detail-list">
        <div>
          <dt>Operation ID</dt>
          <dd>{operation.operation_id}</dd>
        </div>
        <div>
          <dt>MCP Session</dt>
          <dd>{operation.mcp_session_id ?? "unknown"}</dd>
        </div>
        <div>
          <dt>MCP Client</dt>
          <dd>{operationClientLabel(operation)}</dd>
        </div>
        <div>
          <dt>Created</dt>
          <dd>{new Date(operation.created_at).toLocaleString()}</dd>
        </div>
        <div>
          <dt>Expires</dt>
          <dd>{new Date(operation.expires_at).toLocaleString()}</dd>
        </div>
        <div>
          <dt>Time Remaining</dt>
          <dd>{countdownLabel(operation.expires_at, now)}</dd>
        </div>
      </dl>

      <div className="action-row">
        <button
          className="action-button approve"
          disabled={!operation.can_approve || busy}
          onClick={() => void onAction("approve")}
        >
          Approve
        </button>
        <button
          className="action-button reject"
          disabled={!operation.can_reject || busy}
          onClick={() => void onAction("reject")}
        >
          Reject
        </button>
      </div>
    </aside>
  );
}

function OperationSection({ operation }: { operation: OperationPreview }) {
  if (operation.type === "create_records") {
    return <RecordTable operation={operation} />;
  }
  if (operation.type === "delete_records") {
    return <DeletePreview operation={operation} />;
  }
  return <DiffView operation={operation} />;
}

export default function App({
  pathname = window.location.pathname,
  fetchImpl = window.fetch.bind(window),
}: AppProps) {
  const operationID = useMemo(() => getOperationIDFromPath(pathname), [pathname]);
  const [operation, setOperation] = useState<OperationView | null>(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    if (!operationID) {
      setError("Approval URL is missing an operation ID.");
      return;
    }

    let cancelled = false;
    async function load() {
      const response = await fetchImpl(`/api/operations/${operationID}`, {
        headers: { Accept: "application/json" },
      });
      const payload = (await response.json()) as OperationView & { error?: string };
      if (cancelled) {
        return;
      }
      if (!response.ok) {
        setError(payload.error ?? "Failed to load approval request.");
        return;
      }
      setOperation(payload);
      setError("");
    }

    void load().catch((cause) => {
      if (!cancelled) {
        setError(String(cause));
      }
    });

    return () => {
      cancelled = true;
    };
  }, [fetchImpl, operationID]);

  async function handleAction(action: "approve" | "reject") {
    if (!operationID) {
      return;
    }
    setBusy(true);
    try {
      const response = await fetchImpl(`/api/operations/${operationID}/${action}`, {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({}),
      });
      const payload = (await response.json()) as OperationView & { error?: string };
      if (!response.ok) {
        setError(payload.error ?? `Failed to ${action} operation.`);
        return;
      }
      setOperation(payload);
      setError("");
    } catch (cause) {
      setError(String(cause));
    } finally {
      setBusy(false);
    }
  }

  if (error) {
    return (
      <main className="page-shell">
        <section className="hero-card">
          <h1>Approval Request</h1>
          <p className="error-text">{error}</p>
        </section>
      </main>
    );
  }

  if (!operation) {
    return (
      <main className="page-shell">
        <section className="hero-card">
          <h1>Approval Request</h1>
          <p className="meta-text">Loading approval request...</p>
        </section>
      </main>
    );
  }

  return (
    <main className="page-shell">
      <section className="hero-grid">
        <div className="hero-card">
          <div className="pill-row">
            <span className="pill">Status: {operation.status}</span>
            <span className="pill">
              Preview cache: {new Date(operation.last_synced_at).toLocaleString()}
            </span>
          </div>
          <h1>{operation.summary}</h1>
          <p className="meta-text">
            Base: {operation.base_name} ({operation.base_id})
          </p>
          <div className="notice-stack">
            <p className="notice">
              Anyone with this approval URL can approve or reject the request
              until it expires.
            </p>
            <p className="notice">
              This preview comes from the latest synced DuckDB snapshot, not a
              live Airtable reread, so Airtable data may have changed since this
              preview was generated.
            </p>
          </div>
          {operation.error ? (
            <p className="error-text">{operation.error}</p>
          ) : null}
        </div>
        <StatusPanel operation={operation} onAction={handleAction} busy={busy} />
      </section>

      <section className="preview-stack">
        {operation.operations.map((item, index) => (
          <OperationSection key={`${item.type}-${item.table}-${index}`} operation={item} />
        ))}
      </section>

      {operation.result ? (
        <section className="preview-card">
          <h2>Execution Result</h2>
          <pre className="field-pre">{JSON.stringify(operation.result, null, 2)}</pre>
        </section>
      ) : null}
    </main>
  );
}
