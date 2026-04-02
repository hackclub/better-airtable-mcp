package tools

import (
	"strings"
	"testing"
)

func TestFormatSchemaCSVUsesSyncBaseAndPerTableSampleSections(t *testing.T) {
	text := formatSchemaCSV("appProjects", "Project Tracker", []map[string]any{
		{
			"duckdb_table_name": "projects",
			"fields": []map[string]any{
				{"duckdb_column_name": "name"},
				{"duckdb_column_name": "status"},
			},
			"sample_rows": []map[string]any{
				{
					"id":           "recProject1",
					"created_time": "2026-04-01T12:00:00Z",
					"name":         "Website Redesign",
					"status":       "In Progress",
				},
			},
		},
		{
			"duckdb_table_name": "tasks",
			"fields": []map[string]any{
				{"duckdb_column_name": "name"},
			},
			"sample_rows": []map[string]any{
				{
					"id":           "recTask1",
					"created_time": "2026-04-01T13:00:00Z",
					"name":         "Design homepage",
				},
			},
		},
	}, &formattedSyncStatus{
		OperationID:          "sync_appProjects",
		Status:               "syncing",
		ReadSnapshot:         "partial",
		SyncStartedAt:        "2026-04-02T17:13:20Z",
		LastSyncedAt:         "",
		TablesTotal:          2,
		TablesStarted:        2,
		TablesCompleted:      0,
		PagesFetched:         2,
		RecordsVisible:       2,
		RecordsSyncedThisRun: 2,
		Error:                "",
	})

	if !strings.Contains(text, "sync_status\n") {
		t.Fatalf("expected sync_status section, got %q", text)
	}
	if !strings.Contains(text, "base\n\nbase_id,base_name\nappProjects,Project Tracker\n") {
		t.Fatalf("expected base section without last_synced_at, got %q", text)
	}
	if !strings.Contains(text, "tables\n\n# projects\n") {
		t.Fatalf("expected tables heading before table sample sections, got %q", text)
	}
	if !strings.Contains(text, "# projects\n\nid,created_time,name,status\nrecProject1,2026-04-01T12:00:00Z,Website Redesign,In Progress\n") {
		t.Fatalf("expected projects sample section, got %q", text)
	}
	if !strings.Contains(text, "# tasks\n\nid,created_time,name\nrecTask1,2026-04-01T13:00:00Z,Design homepage\n") {
		t.Fatalf("expected tasks sample section, got %q", text)
	}
	if strings.Contains(text, "fields\n") || strings.Contains(text, "sample_rows\n") {
		t.Fatalf("expected per-table sample sections instead of legacy schema sections, got %q", text)
	}
}
