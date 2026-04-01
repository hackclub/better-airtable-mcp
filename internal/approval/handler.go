package approval

import (
	"html/template"
	"net/http"
	"strings"

	"github.com/hackclub/better-airtable-mcp/internal/httpx"
)

type Handler struct {
	service *Service
	page    *template.Template
}

type approvalPageData struct {
	OperationID string
}

func NewHandler(service *Service) *Handler {
	return &Handler{
		service: service,
		page:    template.Must(template.New("approval").Parse(approvalPageTemplate)),
	}
}

func (h *Handler) ServeApprovalPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpx.MethodNotAllowed(w, http.MethodGet)
		return
	}
	if h.service == nil {
		httpx.WriteError(w, http.StatusNotImplemented, "approval service is not configured")
		return
	}

	operationID := strings.TrimPrefix(r.URL.Path, "/approve/")
	if operationID == "" || operationID == r.URL.Path {
		http.NotFound(w, r)
		return
	}
	if _, err := h.service.GetOperation(r.Context(), operationID); err != nil {
		httpx.WriteError(w, http.StatusNotFound, "operation was not found")
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.page.Execute(w, approvalPageData{OperationID: operationID}); err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, "failed to render approval page")
	}
}

func (h *Handler) ServeOperationAPI(w http.ResponseWriter, r *http.Request) {
	if h.service == nil {
		httpx.WriteError(w, http.StatusNotImplemented, "approval service is not configured")
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/operations/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}

	operationID := parts[0]
	switch {
	case len(parts) == 1 && r.Method == http.MethodGet:
		operation, err := h.service.GetOperation(r.Context(), operationID)
		if err != nil {
			httpx.WriteError(w, http.StatusNotFound, "operation was not found")
			return
		}
		httpx.WriteJSON(w, http.StatusOK, operation)
	case len(parts) == 2 && r.Method == http.MethodPost && parts[1] == "approve":
		operation, err := h.service.Approve(r.Context(), operationID)
		if err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		httpx.WriteJSON(w, http.StatusOK, operation)
	case len(parts) == 2 && r.Method == http.MethodPost && parts[1] == "reject":
		operation, err := h.service.Reject(r.Context(), operationID)
		if err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		httpx.WriteJSON(w, http.StatusOK, operation)
	default:
		switch len(parts) {
		case 1:
			httpx.MethodNotAllowed(w, http.MethodGet)
		case 2:
			httpx.MethodNotAllowed(w, http.MethodPost)
		default:
			http.NotFound(w, r)
		}
	}
}

const approvalPageTemplate = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Approval Request</title>
  <style>
    :root {
      --bg: #f4f1ea;
      --panel: #fffdf7;
      --line: #ded8cb;
      --text: #211d18;
      --muted: #72685d;
      --approve: #0e8c61;
      --reject: #b64334;
      --accent: #d6cab5;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Iowan Old Style, Palatino Linotype, Book Antiqua, serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, #efe3d0 0, transparent 32rem),
        linear-gradient(180deg, #f6f1e8 0, #efe8db 100%);
    }
    main {
      max-width: 1100px;
      margin: 0 auto;
      padding: 32px 20px 48px;
    }
    .card {
      background: rgba(255, 253, 247, 0.92);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 20px;
      box-shadow: 0 18px 50px rgba(58, 48, 33, 0.08);
      backdrop-filter: blur(14px);
    }
    h1, h2, h3 {
      margin: 0 0 10px;
      font-weight: 600;
    }
    h1 {
      font-size: clamp(2rem, 4vw, 3.4rem);
      line-height: 1;
      margin-bottom: 16px;
    }
    h2 { font-size: 1.25rem; margin-top: 24px; }
    .meta, .notice {
      color: var(--muted);
      line-height: 1.5;
    }
    .hero {
      display: grid;
      gap: 18px;
      grid-template-columns: 2fr 1fr;
      margin-bottom: 20px;
    }
    .pill {
      display: inline-block;
      border-radius: 999px;
      padding: 6px 12px;
      background: var(--accent);
      margin-right: 8px;
      margin-bottom: 8px;
      font-size: 0.95rem;
    }
    button {
      border: 0;
      border-radius: 999px;
      padding: 12px 18px;
      font: inherit;
      cursor: pointer;
      color: white;
      margin-right: 10px;
    }
    button[disabled] { opacity: 0.45; cursor: default; }
    .approve { background: var(--approve); }
    .reject { background: var(--reject); }
    .grid {
      display: grid;
      gap: 16px;
    }
    .record {
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 16px;
      background: #fff;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 10px;
      font-size: 0.96rem;
    }
    th, td {
      text-align: left;
      padding: 8px 10px;
      border-top: 1px solid var(--line);
      vertical-align: top;
    }
    th { color: var(--muted); font-weight: 600; }
    pre {
      margin: 0;
      white-space: pre-wrap;
      word-break: break-word;
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size: 0.88rem;
    }
    .muted { color: var(--muted); }
    .error { color: var(--reject); }
    .result { margin-top: 16px; }
    #countdown { font-variant-numeric: tabular-nums; }
    @media (max-width: 800px) {
      .hero { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <main>
    <div id="app" class="card" data-operation-id="{{ .OperationID }}">Loading approval request...</div>
  </main>
  <script>
    const operationId = document.getElementById("app").dataset.operationId;

    const escapeHtml = (value) => String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");

    const formatValue = (value) => {
      if (value === null || value === undefined) return '<span class="muted">null</span>';
      if (typeof value === "string") return escapeHtml(value);
      return '<pre>' + escapeHtml(JSON.stringify(value, null, 2)) + '</pre>';
    };

    const renderRecord = (operation, record) => {
      const nextFields = record.fields || {};
      const currentFields = record.current_fields || {};
      const fieldNames = Array.from(new Set([...Object.keys(currentFields), ...Object.keys(nextFields)])).sort();
      const rows = fieldNames.map((fieldName) => {
        const currentValue = fieldName in currentFields ? formatValue(currentFields[fieldName]) : '<span class="muted">empty</span>';
        const nextValue = fieldName in nextFields ? formatValue(nextFields[fieldName]) : '<span class="muted">empty</span>';
        return '<tr><th>' + escapeHtml(fieldName) + '</th><td>' + currentValue + '</td><td>' + nextValue + '</td></tr>';
      }).join("");
      const body = rows || '<tr><td colspan="3" class="muted">No field data.</td></tr>';
      return '<div class="record"><div><strong>' + escapeHtml(record.id || "New record") + '</strong></div><table><thead><tr><th>Field</th><th>Current</th><th>Requested</th></tr></thead><tbody>' + body + '</tbody></table></div>';
    };

    const render = (operation) => {
      if (!operation || operation.error && !operation.operations) {
        document.getElementById("app").innerHTML =
          '<div class="card"><h1>Approval Request</h1><p class="error">' + escapeHtml(operation && operation.error ? operation.error : 'Operation payload was incomplete.') + '</p></div>';
        return;
      }

      const statusPill = '<span class="pill">Status: ' + escapeHtml(operation.status) + '</span>';
      const expiryPill = '<span class="pill">Expires: ' + escapeHtml(new Date(operation.expires_at).toLocaleString()) + '</span>';
      const syncPill = '<span class="pill">Preview cache: ' + escapeHtml(new Date(operation.last_synced_at).toLocaleString()) + '</span>';
      const clientLabel = operation.mcp_client_name ? operation.mcp_client_name + ' (' + (operation.mcp_client_id || 'unknown') + ')' : (operation.mcp_client_id || 'unknown');
      const actionButtons =
        '<button class="approve" ' + (operation.can_approve ? '' : 'disabled') + ' onclick="act(\'approve\')">Approve</button>' +
        '<button class="reject" ' + (operation.can_reject ? '' : 'disabled') + ' onclick="act(\'reject\')">Reject</button>';
      const operations = Array.isArray(operation.operations) ? operation.operations : [];
      const preview = operations.map((item) => {
        const records = Array.isArray(item.records) ? item.records : [];
        return (
        '<section class="card">' +
          '<h2>' + escapeHtml(item.type.replaceAll("_", " ")) + ' in ' + escapeHtml(item.original_table_name || item.table) + '</h2>' +
          '<div class="grid">' + records.map((record) => renderRecord(item, record)).join("") + '</div>' +
        '</section>'
        );
      }).join("");
      const result = operation.result ?
        '<section class="card result">' +
          '<h2>Execution Result</h2>' +
          '<pre>' + escapeHtml(JSON.stringify(operation.result, null, 2)) + '</pre>' +
        '</section>' : "";
      const error = operation.error ? '<p class="error">' + escapeHtml(operation.error) + '</p>' : "";

      document.getElementById("app").innerHTML =
        '<section class="hero">' +
          '<div class="card">' +
            '<h1>' + escapeHtml(operation.summary) + '</h1>' +
            '<p class="meta">Base: ' + escapeHtml(operation.base_name) + ' (' + escapeHtml(operation.base_id) + ')</p>' +
            '<p class="notice">Anyone with this approval URL can approve or reject the request until it expires. The preview below comes from the latest synced DuckDB snapshot, not a live Airtable reread.</p>' +
            '<div>' + statusPill + expiryPill + syncPill + '</div>' +
            '<div style="margin-top: 18px">' + actionButtons + '</div>' +
            error +
          '</div>' +
          '<div class="card">' +
            '<h2>Request Details</h2>' +
            '<p class="meta">Operation ID: ' + escapeHtml(operation.operation_id) + '</p>' +
            '<p class="meta">MCP Session: ' + escapeHtml(operation.mcp_session_id || 'unknown') + '</p>' +
            '<p class="meta">MCP Client: ' + escapeHtml(clientLabel) + '</p>' +
            '<p class="meta">Created: ' + escapeHtml(new Date(operation.created_at).toLocaleString()) + '</p>' +
            '<p class="meta">Time Remaining: <span id="countdown">' + escapeHtml(formatCountdown(operation.expires_at)) + '</span></p>' +
            '<p class="meta">Approval URL is the credential: ' + (operation.approval_url_is_credential ? 'yes' : 'no') + '</p>' +
          '</div>' +
        '</section>' +
        preview +
        result;

      startCountdown(operation.expires_at);
    };

    const formatCountdown = (expiresAt) => {
      const msRemaining = new Date(expiresAt).getTime() - Date.now();
      if (msRemaining <= 0) return 'expired';
      const totalSeconds = Math.floor(msRemaining / 1000);
      const minutes = Math.floor(totalSeconds / 60);
      const seconds = totalSeconds % 60;
      return minutes + 'm ' + String(seconds).padStart(2, '0') + 's';
    };

    let countdownTimer = null;
    const startCountdown = (expiresAt) => {
      if (countdownTimer) window.clearInterval(countdownTimer);
      const node = document.getElementById('countdown');
      if (!node) return;
      const renderCountdown = () => {
        const next = document.getElementById('countdown');
        if (!next) {
          window.clearInterval(countdownTimer);
          countdownTimer = null;
          return;
        }
        next.textContent = formatCountdown(expiresAt);
      };
      renderCountdown();
      countdownTimer = window.setInterval(renderCountdown, 1000);
    };

    async function load() {
      const response = await fetch('/api/operations/' + operationId, { headers: { 'Accept': 'application/json' } });
      const payload = await response.json();
      if (!response.ok) {
        render(payload);
        return;
      }
      render(payload);
    }

    async function act(action) {
      const response = await fetch('/api/operations/' + operationId + '/' + action, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
        body: JSON.stringify({})
      });
      const payload = await response.json();
      if (!response.ok) {
        render(payload);
        return;
      }
      render(payload);
    }

    load().catch(async (error) => {
      document.getElementById('app').textContent = 'Failed to load approval request: ' + error;
    });
  </script>
</body>
</html>`
