package duckdb

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWriteSnapshotReplacesOldRowsAndReadTableRowsByIDs(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "base.db")
	ctx := context.Background()

	firstSnapshot := BaseSnapshot{
		BaseID:   "app123",
		BaseName: "Test Base",
		SyncedAt: time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC),
		Tables: []TableSnapshot{
			{
				AirtableTableID: "tblProjects",
				OriginalName:    "Projects",
				DuckDBTableName: "projects",
				Fields: []FieldSnapshot{
					{
						AirtableFieldID:   "fldName",
						OriginalFieldName: "Name",
						DuckDBColumnName:  "name",
						AirtableFieldType: "singleLineText",
						DuckDBType:        "VARCHAR",
					},
				},
				Records: []RecordSnapshot{
					{ID: "rec1", CreatedTime: time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC), Fields: map[string]any{"Name": "First"}},
					{ID: "rec2", CreatedTime: time.Date(2026, 4, 1, 12, 1, 0, 0, time.UTC), Fields: map[string]any{"Name": "Second"}},
				},
			},
		},
	}
	if err := WriteSnapshot(ctx, dbPath, firstSnapshot); err != nil {
		t.Fatalf("WriteSnapshot(first) returned error: %v", err)
	}

	rows, err := ReadTableRowsByIDs(ctx, dbPath, "projects", []string{"rec2", "rec1"})
	if err != nil {
		t.Fatalf("ReadTableRowsByIDs() returned error: %v", err)
	}
	if len(rows) != 2 || rows[0]["id"] != "rec2" || rows[1]["name"] != "First" {
		t.Fatalf("unexpected row lookup result %#v", rows)
	}

	secondSnapshot := BaseSnapshot{
		BaseID:   "app123",
		BaseName: "Test Base",
		SyncedAt: time.Date(2026, 4, 1, 12, 5, 0, 0, time.UTC),
		Tables: []TableSnapshot{
			{
				AirtableTableID: "tblProjects",
				OriginalName:    "Projects",
				DuckDBTableName: "projects",
				Fields: []FieldSnapshot{
					{
						AirtableFieldID:   "fldName",
						OriginalFieldName: "Name",
						DuckDBColumnName:  "name",
						AirtableFieldType: "singleLineText",
						DuckDBType:        "VARCHAR",
					},
				},
				Records: []RecordSnapshot{
					{ID: "rec2", CreatedTime: time.Date(2026, 4, 1, 12, 1, 0, 0, time.UTC), Fields: map[string]any{"Name": "Second Updated"}},
					{ID: "rec3", CreatedTime: time.Date(2026, 4, 1, 12, 6, 0, 0, time.UTC), Fields: map[string]any{"Name": "Third"}},
				},
			},
		},
	}
	if err := WriteSnapshot(ctx, dbPath, secondSnapshot); err != nil {
		t.Fatalf("WriteSnapshot(second) returned error: %v", err)
	}

	result, err := Query(ctx, dbPath, `SELECT id, name FROM projects ORDER BY id`)
	if err != nil {
		t.Fatalf("Query() returned error: %v", err)
	}
	if result.RowCount != 2 {
		t.Fatalf("expected 2 rows after replacement, got %d", result.RowCount)
	}
	if result.Rows[0][0] != "rec2" || result.Rows[0][1] != "Second Updated" {
		t.Fatalf("unexpected first row after replacement %#v", result.Rows[0])
	}
	if result.Rows[1][0] != "rec3" {
		t.Fatalf("expected stale row rec1 to be removed, got %#v", result.Rows)
	}
	if result.LastSyncDuration != 0 {
		t.Fatalf("expected zero sync duration for second snapshot, got %s", result.LastSyncDuration)
	}
}

func TestQueryReturnsSyncMetadata(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "base.db")
	ctx := context.Background()
	syncedAt := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)

	if err := WriteSnapshot(ctx, dbPath, BaseSnapshot{
		BaseID:       "app123",
		BaseName:     "Test Base",
		SyncedAt:     syncedAt,
		SyncDuration: 15 * time.Second,
		Tables: []TableSnapshot{
			{
				AirtableTableID: "tblProjects",
				OriginalName:    "Projects",
				DuckDBTableName: "projects",
				Fields: []FieldSnapshot{
					{
						AirtableFieldID:   "fldName",
						OriginalFieldName: "Name",
						DuckDBColumnName:  "name",
						AirtableFieldType: "singleLineText",
						DuckDBType:        "VARCHAR",
					},
				},
				Records: []RecordSnapshot{
					{ID: "rec1", CreatedTime: syncedAt, Fields: map[string]any{"Name": "One"}},
				},
			},
		},
	}); err != nil {
		t.Fatalf("WriteSnapshot() returned error: %v", err)
	}

	result, err := Query(ctx, dbPath, `SELECT name FROM projects`)
	if err != nil {
		t.Fatalf("Query() returned error: %v", err)
	}
	if !result.LastSyncedAt.Equal(syncedAt) {
		t.Fatalf("unexpected LastSyncedAt %s", result.LastSyncedAt)
	}
	if result.LastSyncDuration != 15*time.Second {
		t.Fatalf("expected 15s sync duration, got %s", result.LastSyncDuration)
	}
}

func TestOpenDatabaseDisablesExternalAccessAndExtensionAutoload(t *testing.T) {
	db, err := openDatabase(":memory:", "READ_WRITE")
	if err != nil {
		t.Fatalf("openDatabase() returned error: %v", err)
	}
	defer db.Close()

	assertSettingValue(t, db, "enable_external_access", "false")
	assertSettingValue(t, db, "autoload_known_extensions", "false")
	assertSettingValue(t, db, "autoinstall_known_extensions", "false")
}

func TestBuildDSNIncludesSecurityOptions(t *testing.T) {
	dsn := buildDSN("/tmp/base.db", "READ_ONLY")
	requiredFragments := []string{
		"/tmp/base.db?",
		"access_mode=READ_ONLY",
		"enable_external_access=false",
		"autoload_known_extensions=false",
		"autoinstall_known_extensions=false",
	}
	for _, fragment := range requiredFragments {
		if !strings.Contains(dsn, fragment) {
			t.Fatalf("expected DSN %q to contain %q", dsn, fragment)
		}
	}
}

func assertSettingValue(t *testing.T, db *sql.DB, settingName, expected string) {
	t.Helper()

	var value string
	if err := db.QueryRow(`SELECT value FROM duckdb_settings() WHERE name = ?`, settingName).Scan(&value); err != nil {
		t.Fatalf("read DuckDB setting %q: %v", settingName, err)
	}
	if value != expected {
		t.Fatalf("expected DuckDB setting %q=%q, got %q", settingName, expected, value)
	}
}
