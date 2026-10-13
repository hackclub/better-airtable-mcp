package mcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

type blockingTool struct{}

func (blockingTool) Definition() ToolDefinition {
	return ToolDefinition{Name: "blocking_tool", Description: "Blocks until its context ends.", InputSchema: map[string]any{"type": "object"}}
}

func (blockingTool) Call(ctx context.Context, _ json.RawMessage) (ToolCallResult, error) {
	<-ctx.Done()
	return ToolCallResult{}, ctx.Err()
}

func TestHandlerToolsCallReturnsReadableErrorWhenToolExceedsDeadline(t *testing.T) {
	handler := NewHandler("better-airtable-mcp", "0.1.0", []Tool{blockingTool{}})
	handler.toolTimeout = 50 * time.Millisecond
	sessionID := initializeSession(t, handler)

	startedAt := time.Now()
	_, response := performRPCRequest(t, handler, sessionID, map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tools/call",
		"params":  map[string]any{"name": "blocking_tool", "arguments": map[string]any{}},
	})
	if elapsed := time.Since(startedAt); elapsed > 5*time.Second {
		t.Fatalf("tool call should end at the deadline, took %s", elapsed)
	}

	result := response["result"].(map[string]any)
	if result["isError"] != true {
		t.Fatalf("expected tool error result, got %#v", result)
	}
	text := result["content"].([]any)[0].(map[string]any)["text"].(string)
	for _, want := range []string{"blocking_tool did not finish within 50ms", "Retry in a minute", "check_operation"} {
		if !strings.Contains(text, want) {
			t.Fatalf("expected timeout message to contain %q, got %q", want, text)
		}
	}
}

func TestDefaultToolCallTimeoutStaysUnderIngressTimeout(t *testing.T) {
	handler := NewHandler("better-airtable-mcp", "0.1.0", nil)
	if handler.toolTimeout <= 0 || handler.toolTimeout >= 50*time.Second {
		t.Fatalf("tool timeout %s must be positive and below the ~50s ingress timeout", handler.toolTimeout)
	}
}
