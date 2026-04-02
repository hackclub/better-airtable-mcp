package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHandlerInitialize(t *testing.T) {
	handler := NewHandler("better-airtable-mcp", "0.1.0", []Tool{testTool{}})

	recorder, response := performRPCRequest(t, handler, "", map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "initialize",
	})

	result, ok := response["result"].(map[string]any)
	if !ok {
		t.Fatalf("expected initialize result, got %#v", response)
	}

	if result["protocolVersion"] != "2025-11-25" {
		t.Fatalf("unexpected protocol version %#v", result["protocolVersion"])
	}
	if sessionID := recorder.Header().Get(SessionHeader); sessionID == "" {
		t.Fatal("expected initialize to issue an MCP session id")
	}
}

func TestHandlerToolsList(t *testing.T) {
	handler := NewHandler("better-airtable-mcp", "0.1.0", []Tool{testTool{}})
	sessionID := initializeSession(t, handler)

	_, response := performRPCRequest(t, handler, sessionID, map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tools/list",
	})

	result := response["result"].(map[string]any)
	toolList := result["tools"].([]any)
	if len(toolList) != 1 {
		t.Fatalf("expected 1 tool, got %d", len(toolList))
	}
}

func TestHandlerToolsCallReturnsToolErrorResult(t *testing.T) {
	handler := NewHandler("better-airtable-mcp", "0.1.0", []Tool{testTool{}})
	sessionID := initializeSession(t, handler)

	_, response := performRPCRequest(t, handler, sessionID, map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tools/call",
		"params": map[string]any{
			"name": "test_tool",
			"arguments": map[string]any{
				"mode": "error",
			},
		},
	})

	result := response["result"].(map[string]any)
	if result["isError"] != true {
		t.Fatalf("expected tool error result, got %#v", result)
	}

	content := result["content"].([]any)
	firstEntry := content[0].(map[string]any)
	text := firstEntry["text"].(string)
	if !strings.Contains(text, "boom") {
		t.Fatalf("expected validation error message, got %q", text)
	}
}

func TestHandlerGetMCPReturnsSSEStream(t *testing.T) {
	handler := NewHandler("better-airtable-mcp", "0.1.0", nil)
	handler.heartbeat = 10 * time.Millisecond
	sessionID := initializeSession(t, handler)
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Millisecond)
	defer cancel()
	request := httptest.NewRequest(http.MethodGet, "/mcp", nil).WithContext(ctx)
	request.Header.Set(SessionHeader, sessionID)
	recorder := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(recorder, request)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected GET /mcp handler to exit after context cancellation")
	}

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected GET /mcp to return 200, got %d", recorder.Code)
	}
	if got := recorder.Header().Get("Content-Type"); !strings.Contains(got, "text/event-stream") {
		t.Fatalf("expected SSE content type, got %q", got)
	}
	if body := recorder.Body.String(); !strings.Contains(body, "event: ready") {
		t.Fatalf("expected SSE ready event, got %q", body)
	}
	if body := recorder.Body.String(); !strings.Contains(body, "event: ping") {
		t.Fatalf("expected SSE heartbeat ping event, got %q", body)
	}
}

func TestHandlerRejectsMissingSessionOnNonInitializeRequest(t *testing.T) {
	handler := NewHandler("better-airtable-mcp", "0.1.0", []Tool{testTool{}})

	payload, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tools/list",
	})
	if err != nil {
		t.Fatalf("json.Marshal() returned error: %v", err)
	}

	request := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected HTTP 400 for missing session, got %d", recorder.Code)
	}
}

func TestHandlerDeleteMCPDeletesSession(t *testing.T) {
	handler := NewHandler("better-airtable-mcp", "0.1.0", []Tool{testTool{}})
	sessionID := initializeSession(t, handler)

	deleteRequest := httptest.NewRequest(http.MethodDelete, "/mcp", nil)
	deleteRequest.Header.Set(SessionHeader, sessionID)
	deleteRecorder := httptest.NewRecorder()
	handler.ServeHTTP(deleteRecorder, deleteRequest)
	if deleteRecorder.Code != http.StatusNoContent {
		t.Fatalf("expected HTTP 204 from DELETE /mcp, got %d", deleteRecorder.Code)
	}

	payload, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tools/list",
	})
	if err != nil {
		t.Fatalf("json.Marshal() returned error: %v", err)
	}

	request := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(SessionHeader, sessionID)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected deleted session to return 404, got %d", recorder.Code)
	}
}

func TestHandlerPassesSessionIDToToolContext(t *testing.T) {
	handler := NewHandler("better-airtable-mcp", "0.1.0", []Tool{testTool{}})
	sessionID := initializeSession(t, handler)

	_, response := performRPCRequest(t, handler, sessionID, map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tools/call",
		"params": map[string]any{
			"name":      "test_tool",
			"arguments": map[string]any{},
		},
	})

	result := response["result"].(map[string]any)
	structured := result["structuredContent"].(map[string]any)
	if structured["session_id"] != sessionID {
		t.Fatalf("expected tool context to receive session id %q, got %#v", sessionID, structured)
	}
}

func TestSessionManagerPruneExpiredRemovesIdleSessions(t *testing.T) {
	now := time.Date(2026, 4, 1, 18, 0, 0, 0, time.UTC)
	manager := NewSessionManager()
	manager.now = func() time.Time { return now }
	manager.idleTTL = 30 * time.Minute
	manager.sessions["stale"] = Session{
		ID:         "stale",
		CreatedAt:  now.Add(-time.Hour),
		LastSeenAt: now.Add(-31 * time.Minute),
	}
	manager.sessions["fresh"] = Session{
		ID:         "fresh",
		CreatedAt:  now.Add(-time.Hour),
		LastSeenAt: now.Add(-29 * time.Minute),
	}

	removed := manager.PruneExpired()
	if removed != 1 {
		t.Fatalf("expected 1 expired session to be removed, got %d", removed)
	}
	if _, ok := manager.sessions["stale"]; ok {
		t.Fatal("expected stale session to be removed")
	}
	if _, ok := manager.sessions["fresh"]; !ok {
		t.Fatal("expected fresh session to remain")
	}
}

func TestHandlerGetMCPHeartbeatTouchesSession(t *testing.T) {
	handler := NewHandler("better-airtable-mcp", "0.1.0", nil)
	handler.heartbeat = 10 * time.Millisecond
	sessionID := initializeSession(t, handler)

	sessionBefore, ok := handler.sessions.sessions[sessionID]
	if !ok {
		t.Fatalf("expected session %q to exist", sessionID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Millisecond)
	defer cancel()
	request := httptest.NewRequest(http.MethodGet, "/mcp", nil).WithContext(ctx)
	request.Header.Set(SessionHeader, sessionID)
	recorder := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(recorder, request)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected GET /mcp handler to exit after context cancellation")
	}

	sessionAfter, ok := handler.sessions.sessions[sessionID]
	if !ok {
		t.Fatalf("expected session %q to still exist", sessionID)
	}
	if !sessionAfter.LastSeenAt.After(sessionBefore.LastSeenAt) {
		t.Fatalf("expected heartbeat to refresh session last-seen time, before=%s after=%s", sessionBefore.LastSeenAt, sessionAfter.LastSeenAt)
	}
}

func performRPCRequest(t *testing.T, handler http.Handler, sessionID string, body map[string]any) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()

	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("json.Marshal() returned error: %v", err)
	}

	request := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/json")
	if sessionID != "" {
		request.Header.Set(SessionHeader, sessionID)
	}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d with body %s", recorder.Code, recorder.Body.String())
	}

	var response map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("json.Unmarshal() returned error: %v", err)
	}

	return recorder, response
}

func initializeSession(t *testing.T, handler http.Handler) string {
	t.Helper()

	recorder, _ := performRPCRequest(t, handler, "", map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "initialize",
	})
	sessionID := recorder.Header().Get(SessionHeader)
	if sessionID == "" {
		t.Fatal("expected initialize response to include session header")
	}
	return sessionID
}

type testTool struct{}

func (testTool) Definition() ToolDefinition {
	return ToolDefinition{
		Name:        "test_tool",
		Description: "Test helper tool.",
		InputSchema: map[string]any{
			"type": "object",
		},
	}
}

func (testTool) Call(ctx context.Context, raw json.RawMessage) (ToolCallResult, error) {
	var input struct {
		Mode string `json:"mode"`
	}
	if err := json.Unmarshal(raw, &input); err != nil {
		return ToolCallResult{}, err
	}
	if input.Mode == "error" {
		return ToolCallResult{}, errors.New("boom")
	}

	sessionID, _ := SessionIDFromContext(ctx)
	return TextResult("ok", map[string]any{"mode": input.Mode, "session_id": sessionID}), nil
}
