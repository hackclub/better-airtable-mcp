package duckdb

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// everyTypeTable covers every DuckDB column type the type mapping produces,
// with values shaped the way Airtable's JSON decodes them.
func everyTypeTable() TableSnapshot {
	field := func(name, airtableType, duckDBType string) FieldSnapshot {
		return FieldSnapshot{
			AirtableFieldID:   "fld" + name,
			OriginalFieldName: name,
			DuckDBColumnName:  strings.ToLower(name),
			AirtableFieldType: airtableType,
			DuckDBType:        duckDBType,
		}
	}
	created := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	return TableSnapshot{
		AirtableTableID: "tblAll",
		OriginalName:    "All",
		DuckDBTableName: "all_types",
		Fields: []FieldSnapshot{
			field("Text", "singleLineText", "VARCHAR"),
			field("Num", "number", "DOUBLE"),
			field("Auto", "autoNumber", "BIGINT"),
			field("Done", "checkbox", "BOOLEAN"),
			field("When", "dateTime", "TIMESTAMP"),
			field("Day", "date", "TIMESTAMP"),
			field("Tags", "multipleSelects", "VARCHAR[]"),
			field("Links", "multipleRecordLinks", "VARCHAR[]"),
			field("Look", "lookup", "JSON"),
			field("Files", "multipleAttachments", "JSON"),
			field("Calc", "formula", "VARCHAR"),
			field("Who", "createdBy", "VARCHAR"),
			field("Stars", "rating", "BIGINT"),
		},
		Records: []RecordSnapshot{
			{ID: "rec1", CreatedTime: created, Fields: map[string]any{
				"Text":  "hello, \"world\"",
				"Num":   12.5,
				"Auto":  float64(7),
				"Done":  true,
				"When":  "2026-10-13T15:04:05.123Z",
				"Day":   "2026-10-13",
				"Tags":  []any{"a", "b"},
				"Links": []any{"recX"},
				"Look":  []any{"x", 2.0, map[string]any{"k": "v"}},
				"Files": []any{map[string]any{"url": "https://example.com/a.png", "size": 10.0}},
				"Calc":  3.0,
				"Who":   map[string]any{"id": "usr1", "name": "Augie"},
				"Stars": float64(4),
			}},
			// Every field missing: all NULL.
			{ID: "rec2", CreatedTime: created.Add(time.Minute), Fields: map[string]any{}},
			{ID: "rec3", CreatedTime: created.Add(2 * time.Minute), Fields: map[string]any{
				"Text": "",
				"Num":  -0.25,
				"Done": false,
				"Tags": []any{},
				"Calc": "#ERROR!",
			}},
		},
	}
}

func dumpRows(t *testing.T, path, table string) [][]any {
	t.Helper()
	db, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT * FROM ` + quoteIdent(table) + ` ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, _ := rows.Columns()
	var out [][]any
	for rows.Next() {
		values, err := scanRow(rows, len(columns))
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, values)
	}
	return out
}

func initAppendTarget(t *testing.T, table TableSnapshot) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "append.db")
	if err := InitializeSnapshot(context.Background(), path, SnapshotInit{
		BaseID: "app1", BaseName: "Base", Tables: []TableSnapshot{table}, SyncStartedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestAppenderWritesWhatInsertWrites(t *testing.T) {
	ctx := context.Background()
	table := everyTypeTable()

	insertPath := filepath.Join(t.TempDir(), "insert.db")
	if err := WriteSnapshot(ctx, insertPath, BaseSnapshot{BaseID: "app1", BaseName: "Base", Tables: []TableSnapshot{table}, SyncedAt: time.Now()}); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}

	if _, ok := appenderRows(table); !ok {
		t.Fatal("test records should all take the appender path")
	}
	appendPath := initAppendTarget(t, table)
	page := TablePage{Table: table, Records: table.Records, State: TableSyncState{TableName: table.DuckDBTableName, SyncStatus: "completed"}}
	if err := ApplyTablePages(ctx, appendPath, []TablePage{page}, SyncInfo{Status: "syncing"}); err != nil {
		t.Fatalf("ApplyTablePages: %v", err)
	}

	want := dumpRows(t, insertPath, table.DuckDBTableName)
	got := dumpRows(t, appendPath, table.DuckDBTableName)
	if len(want) != len(table.Records) {
		t.Fatalf("insert path wrote %d rows, want %d", len(want), len(table.Records))
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("appender rows differ from insert rows\n got: %#v\nwant: %#v", got, want)
	}
}

func TestAppenderFallsBackToInsertForInexactValues(t *testing.T) {
	ctx := context.Background()
	table := everyTypeTable()
	// Not RFC 3339, so parseTimestamp leaves it a string; DuckDB's cast on
	// the INSERT path still reads it.
	table.Records = []RecordSnapshot{{ID: "rec1", CreatedTime: time.Now().UTC(), Fields: map[string]any{"When": "2026-10-13 15:04:05"}}}
	if _, ok := appenderRows(table); ok {
		t.Fatal("expected the inexact timestamp to force the insert path")
	}

	path := initAppendTarget(t, table)
	page := TablePage{Table: table, Records: table.Records, State: TableSyncState{TableName: table.DuckDBTableName}}
	if err := ApplyTablePages(ctx, path, []TablePage{page}, SyncInfo{Status: "syncing"}); err != nil {
		t.Fatalf("ApplyTablePages: %v", err)
	}
	rows := dumpRows(t, path, table.DuckDBTableName)
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
}

func TestApplyTablePagesRollsBackAppendedRowsOnFailure(t *testing.T) {
	ctx := context.Background()
	table := everyTypeTable()
	path := initAppendTarget(t, table)

	good := TablePage{Table: table, Records: table.Records, State: TableSyncState{TableName: table.DuckDBTableName}}
	missing := table
	missing.DuckDBTableName = "no_such_table"
	bad := TablePage{Table: missing, Records: table.Records, State: TableSyncState{TableName: "no_such_table"}}

	if err := ApplyTablePages(ctx, path, []TablePage{good, bad}, SyncInfo{Status: "syncing"}); err == nil {
		t.Fatal("expected an error appending into a missing table")
	}
	if rows := dumpRows(t, path, table.DuckDBTableName); len(rows) != 0 {
		t.Fatalf("failed batch left %d appended rows behind", len(rows))
	}
}
