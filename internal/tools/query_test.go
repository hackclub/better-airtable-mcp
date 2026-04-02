package tools

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/hackclub/better-airtable-mcp/internal/duckdb"
)

func TestNormalizeQueryBatchAcceptsArray(t *testing.T) {
	queries, err := normalizeQueryBatch([]string{"SELECT * FROM projects", "SELECT * FROM tasks"})
	if err != nil {
		t.Fatalf("normalizeQueryBatch() returned error: %v", err)
	}
	if len(queries) != 2 {
		t.Fatalf("expected 2 normalized queries, got %#v", queries)
	}
	if queries[0] != "SELECT * FROM projects" || queries[1] != "SELECT * FROM tasks" {
		t.Fatalf("unexpected normalized queries %#v", queries)
	}
}

func TestQueryToolDefinitionRequiresSQLArray(t *testing.T) {
	definition := NewQueryTool(100, 1000, nil).Definition()

	if definition.Name != "query" {
		t.Fatalf("expected query tool name, got %q", definition.Name)
	}

	properties := definition.InputSchema["properties"].(map[string]any)
	sqlProperty := properties["sql"].(map[string]any)
	if sqlType, _ := sqlProperty["type"].(string); sqlType != "array" {
		t.Fatalf("expected sql type array, got %#v", sqlProperty["type"])
	}
	if minItems, _ := sqlProperty["minItems"].(int); minItems != 1 {
		t.Fatalf("expected sql minItems 1, got %#v", sqlProperty["minItems"])
	}

	items := sqlProperty["items"].(map[string]any)
	if itemType, _ := items["type"].(string); itemType != "string" {
		t.Fatalf("expected sql items type string, got %#v", items["type"])
	}

	required := definition.InputSchema["required"].([]string)
	if len(required) != 2 || required[0] != "base" || required[1] != "sql" {
		t.Fatalf("unexpected required fields %#v", required)
	}
}

func TestFormatBatchQueryCSVIncludesIndexedSectionsForMultipleQueries(t *testing.T) {
	text := formatBatchQueryCSV([]formattedQueryResult{
		{
			SQL:          "SELECT id FROM projects LIMIT 2",
			Columns:      []string{"id"},
			Rows:         [][]any{{"rec1"}, {"rec2"}},
			RowCount:     2,
			Truncated:    false,
			LastSyncedAt: "2026-04-01T12:00:00Z",
			NextSyncAt:   "2026-04-01T12:01:00Z",
		},
		{
			SQL:          "SELECT id FROM tasks LIMIT 1",
			Columns:      []string{"id"},
			Rows:         [][]any{{"recTask1"}},
			RowCount:     1,
			Truncated:    false,
			LastSyncedAt: "2026-04-01T12:00:00Z",
			NextSyncAt:   "2026-04-01T12:01:00Z",
		},
	})

	if !strings.Contains(text, "query_1_metadata\n") {
		t.Fatalf("expected first query metadata section, got %q", text)
	}
	if !strings.Contains(text, "query_2_rows\n") {
		t.Fatalf("expected second query rows section, got %q", text)
	}
	if !strings.Contains(text, "SELECT id FROM tasks LIMIT 1") {
		t.Fatalf("expected metadata to include normalized SQL, got %q", text)
	}
}

func TestNormalizeQueryBatchRejectsEmptyEntries(t *testing.T) {
	_, err := normalizeQueryBatch([]string{"SELECT * FROM projects", "  "})
	if err == nil {
		t.Fatal("expected normalizeQueryBatch() to reject empty SQL entries")
	}
	if err.Error() != "sql[1] is required" {
		t.Fatalf("unexpected error %q", err)
	}
}

func TestQueryInputRejectsSingleStringSQL(t *testing.T) {
	var input QueryInput
	err := decodeArgs(json.RawMessage(`{"base":"Project Tracker","sql":"SELECT * FROM projects"}`), &input)
	if err == nil {
		t.Fatal("expected decodeArgs() to reject a single sql string")
	}
	if !strings.Contains(err.Error(), "cannot unmarshal string into Go struct field QueryInput.sql of type []string") {
		t.Fatalf("unexpected error %q", err)
	}
}

func TestApplyQueryResultLimitMarksTruncationWhenServerAddedLimit(t *testing.T) {
	result := duckdb.QueryResult{
		Columns:          []string{"id"},
		Rows:             [][]any{{"rec1"}, {"rec2"}, {"rec3"}},
		RowCount:         3,
		LastSyncedAt:     time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC),
		LastSyncDuration: 5 * time.Second,
	}

	trimmed, truncated := applyQueryResultLimit(result, NormalizedQuery{
		EffectiveLimit:     2,
		ServerAppliedLimit: true,
	})

	if !truncated {
		t.Fatal("expected query result to be marked truncated")
	}
	if trimmed.RowCount != 2 {
		t.Fatalf("expected truncated row count 2, got %d", trimmed.RowCount)
	}
	if len(trimmed.Rows) != 2 {
		t.Fatalf("expected 2 rows after truncation, got %d", len(trimmed.Rows))
	}
}

func TestApplyQueryResultLimitLeavesExplicitlyLimitedResultsUntouched(t *testing.T) {
	result := duckdb.QueryResult{
		Rows:     [][]any{{"rec1"}, {"rec2"}},
		RowCount: 2,
	}

	trimmed, truncated := applyQueryResultLimit(result, NormalizedQuery{
		EffectiveLimit:     1,
		ServerAppliedLimit: false,
	})

	if truncated {
		t.Fatal("expected explicit SQL limit to avoid truncation flag")
	}
	if trimmed.RowCount != 2 || len(trimmed.Rows) != 2 {
		t.Fatalf("expected rows to remain untouched, got %#v", trimmed)
	}
}
