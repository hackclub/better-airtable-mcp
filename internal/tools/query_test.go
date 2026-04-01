package tools

import (
	"testing"
	"time"

	"github.com/hackclub/better-airtable-mcp/internal/duckdb"
)

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
