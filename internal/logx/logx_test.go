package logx

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRedactStringRemovesSecretsAndApprovalCredentials(t *testing.T) {
	input := `Authorization: Bearer secret-token access_token=abc refresh_token=def code=ghi state=jkl code_verifier=mno https://example.test/approve/op_sensitive`
	redacted := RedactString(input)

	for _, forbidden := range []string{"secret-token", "abc", "def", "ghi", "jkl", "mno", "op_sensitive"} {
		if strings.Contains(redacted, forbidden) {
			t.Fatalf("expected %q to be redacted from %q", forbidden, redacted)
		}
	}
	if !strings.Contains(redacted, "[REDACTED_APPROVAL_URL]") {
		t.Fatalf("expected approval URL placeholder in %q", redacted)
	}
}

func TestSanitizeSQLPreviewReplacesSensitiveLiterals(t *testing.T) {
	preview := SanitizeSQLPreview(`SELECT * FROM projects WHERE name = 'super secret' AND amount = 42 AND id = '550e8400-e29b-41d4-a716-446655440000'`)
	if strings.Contains(preview, "super secret") || strings.Contains(preview, "42") || strings.Contains(preview, "550e8400") {
		t.Fatalf("expected SQL literals to be redacted, got %q", preview)
	}
	expected := "SELECT * FROM projects WHERE name = ? AND amount = ? AND id = ?"
	if preview != expected {
		t.Fatalf("expected %q, got %q", expected, preview)
	}
}

func TestSummarizeToolArgumentsMutateOmitsFieldValues(t *testing.T) {
	summary := SummarizeToolArguments("mutate", json.RawMessage(`{
		"base": "Project Tracker",
		"operations": [{
			"type": "update_records",
			"table": "projects",
			"records": [{
				"id": "rec123",
				"fields": {
					"status": "ULTRA SECRET STATUS",
					"notes": "do not log me"
				}
			}]
		}]
	}`))

	encoded, err := json.Marshal(summary)
	if err != nil {
		t.Fatalf("json.Marshal() returned error: %v", err)
	}
	logText := string(encoded)
	for _, forbidden := range []string{"ULTRA SECRET STATUS", "do not log me", "Project Tracker"} {
		if strings.Contains(logText, forbidden) {
			t.Fatalf("expected summary to exclude %q, got %s", forbidden, logText)
		}
	}
	if got := summary["operation_count"]; got != 1 {
		t.Fatalf("expected operation_count=1, got %#v", got)
	}
	if got := summary["record_count"]; got != 1 {
		t.Fatalf("expected record_count=1, got %#v", got)
	}
}

func TestHTTPMiddlewareLogsRouteTemplateAndRequestID(t *testing.T) {
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(NewLogger(&output))
	t.Cleanup(func() {
		slog.SetDefault(previous)
	})

	handler := HTTPMiddleware(Route("/approve/:operation", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})))

	request := httptest.NewRequest(http.MethodGet, "/approve/op_sensitive?code=abc", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("expected HTTP 204, got %d", recorder.Code)
	}

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected one log line, got %d: %q", len(lines), output.String())
	}

	var entry map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatalf("json.Unmarshal() returned error: %v", err)
	}
	if entry["event"] != "http.request.completed" {
		t.Fatalf("expected http.request.completed event, got %#v", entry)
	}
	if entry["route"] != "/approve/:operation" {
		t.Fatalf("expected route template, got %#v", entry["route"])
	}
	if requestID, _ := entry["request_id"].(string); requestID == "" {
		t.Fatalf("expected request_id in %#v", entry)
	}
	if strings.Contains(output.String(), "op_sensitive") || strings.Contains(output.String(), "code=abc") {
		t.Fatalf("expected raw path/query to stay out of logs, got %q", output.String())
	}
}
