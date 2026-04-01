package tools

import (
	"strings"
	"testing"
)

func TestFormatListBasesCSV(t *testing.T) {
	text := formatListBasesCSV([]map[string]any{
		{"id": "app2", "name": "Beta", "permission_level": "edit"},
		{"id": "app1", "name": "Alpha", "permission_level": "create"},
	})

	if !strings.HasPrefix(text, "id,name,permission_level\n") {
		t.Fatalf("expected CSV header, got %q", text)
	}
	if !strings.Contains(text, "app1,Alpha,create\napp2,Beta,edit\n") {
		t.Fatalf("expected sorted CSV rows, got %q", text)
	}
}

func TestFormatSingleRowCSV(t *testing.T) {
	text := formatSingleRowCSV([]string{"operation_id", "status", "summary"}, map[string]any{
		"operation_id": "op_123",
		"status":       "pending_approval",
		"summary":      "Create 1 record",
	})

	expected := "operation_id,status,summary\nop_123,pending_approval,Create 1 record\n"
	if text != expected {
		t.Fatalf("expected %q, got %q", expected, text)
	}
}
