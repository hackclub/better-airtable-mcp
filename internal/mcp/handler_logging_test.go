package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/hackclub/better-airtable-mcp/internal/logx"
)

func TestHandlerLogsSanitizedToolArguments(t *testing.T) {
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(logx.NewLogger(&output))
	t.Cleanup(func() {
		slog.SetDefault(previous)
	})

	handler := logx.HTTPMiddleware(logx.Route("/mcp", NewHandler("better-airtable-mcp", "0.1.0", []Tool{queryLogTestTool{}})))
	sessionID := initializeSession(t, handler)

	performRPCRequest(t, handler, sessionID, map[string]any{
		"jsonrpc": "2.0",
		"id":      2,
		"method":  "tools/call",
		"params": map[string]any{
			"name": "query",
			"arguments": map[string]any{
				"base": "Project Tracker",
				"sql":  []string{`SELECT * FROM projects WHERE status = 'ULTRA SECRET STATUS'`},
			},
		},
	})

	logText := output.String()
	for _, forbidden := range []string{"ULTRA SECRET STATUS", "Project Tracker"} {
		if strings.Contains(logText, forbidden) {
			t.Fatalf("expected logs to redact %q, got %s", forbidden, logText)
		}
	}

	entry := findLogEntry(t, logText, "mcp.rpc.completed", func(item map[string]any) bool {
		return item["tool_name"] == "query"
	})
	toolArgs := entry["tool_args"].(map[string]any)
	if toolArgs["query_count"] != float64(1) {
		t.Fatalf("expected query_count=1, got %#v", toolArgs)
	}
	baseRef := toolArgs["base_ref"].(map[string]any)
	if baseRef["kind"] != "hash" {
		t.Fatalf("expected hashed base reference, got %#v", baseRef)
	}
}

type queryLogTestTool struct{}

func (queryLogTestTool) Definition() ToolDefinition {
	return ToolDefinition{
		Name:        "query",
		Description: "Log test query tool.",
		InputSchema: map[string]any{"type": "object"},
	}
}

func (queryLogTestTool) Call(context.Context, json.RawMessage) (ToolCallResult, error) {
	return TextResult("ok", map[string]any{"ok": true}), nil
}

func findLogEntry(t *testing.T, logText, event string, predicate func(map[string]any) bool) map[string]any {
	t.Helper()

	for _, line := range strings.Split(strings.TrimSpace(logText), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("json.Unmarshal(%q) returned error: %v", line, err)
		}
		if entry["event"] != event {
			continue
		}
		if predicate == nil || predicate(entry) {
			return entry
		}
	}

	t.Fatalf("did not find event %q in logs: %s", event, logText)
	return nil
}
