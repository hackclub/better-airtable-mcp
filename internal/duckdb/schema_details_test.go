package duckdb

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func detailTestSnapshot() BaseSnapshot {
	created := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	return BaseSnapshot{
		BaseID:   "app123",
		BaseName: "Test Base",
		SyncedAt: created,
		Tables: []TableSnapshot{{
			AirtableTableID: "tblProjects",
			OriginalName:    "Projects",
			DuckDBTableName: "projects",
			Description:     "Every shipped project",
			Fields: []FieldSnapshot{
				{AirtableFieldID: "fldName", OriginalFieldName: "Name", DuckDBColumnName: "name", AirtableFieldType: "singleLineText", DuckDBType: "VARCHAR", IsPrimary: true},
				{AirtableFieldID: "fldStatus", OriginalFieldName: "Status", DuckDBColumnName: "status", AirtableFieldType: "singleSelect", DuckDBType: "VARCHAR", Description: "Review state", Options: `{"choices":[{"id":"sel1","name":"Approved"},{"id":"sel2","name":"Rejected"}]}`},
				{AirtableFieldID: "fldHours", OriginalFieldName: "Hours", DuckDBColumnName: "hours", AirtableFieldType: "number", DuckDBType: "DOUBLE"},
				{AirtableFieldID: "fldNotes", OriginalFieldName: "Notes", DuckDBColumnName: "notes", AirtableFieldType: "multilineText", DuckDBType: "VARCHAR"},
			},
			Records: []RecordSnapshot{
				{ID: "rec1", CreatedTime: created, Fields: map[string]any{"Name": "First", "Status": "Approved", "Hours": 4.5}},
				{ID: "rec2", CreatedTime: created, Fields: map[string]any{"Name": "Second", "Notes": ""}},
				{ID: "rec3", CreatedTime: created, Fields: map[string]any{"Name": "Third", "Status": "Rejected"}},
			},
		}},
	}
}

func TestReadSchemaReturnsFieldDetails(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "base.db")
	if err := WriteSnapshot(ctx, path, detailTestSnapshot()); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}

	schema, err := ReadSchema(ctx, path, "app123", "Test Base")
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	if !schema.DetailsLoaded {
		t.Fatal("expected a freshly written file to carry field details")
	}
	table := schema.Tables[0]
	if table.Description != "Every shipped project" || table.TotalRecordCount != 3 {
		t.Fatalf("unexpected table %#v", table)
	}
	if !table.Fields[0].IsPrimary || table.Fields[1].IsPrimary {
		t.Fatalf("expected only name to be primary, got %#v", table.Fields)
	}
	status := table.Fields[1]
	if status.Description != "Review state" || status.Options != detailTestSnapshot().Tables[0].Fields[1].Options {
		t.Fatalf("unexpected status details %#v", status)
	}
	if table.Fields[2].Options != "" || table.Fields[2].Description != "" {
		t.Fatalf("expected blank details for hours, got %#v", table.Fields[2])
	}
}

func TestReadColumnStatsCountsFilledRowsAndPicksAnExample(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "base.db")
	if err := WriteSnapshot(ctx, path, detailTestSnapshot()); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}
	schema, err := ReadSchema(ctx, path, "app123", "Test Base")
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}

	stats, err := ReadColumnStats(ctx, path, schema.Tables)
	if err != nil {
		t.Fatalf("ReadColumnStats: %v", err)
	}
	projects := stats["projects"]
	if projects.Rows != 3 {
		t.Fatalf("expected 3 rows, got %d", projects.Rows)
	}
	if got := projects.Columns["name"]; got.Filled != 3 || got.Example == "" {
		t.Fatalf("unexpected name stats %#v", got)
	}
	if got := projects.Columns["status"]; got.Filled != 2 || (got.Example != "Approved" && got.Example != "Rejected") {
		t.Fatalf("unexpected status stats %#v", got)
	}
	if got := projects.Columns["hours"]; got.Filled != 1 || got.Example != "4.5" {
		t.Fatalf("unexpected hours stats %#v", got)
	}
	// A blank string is never offered as the example.
	if got := projects.Columns["notes"]; got.Example != "" {
		t.Fatalf("expected no example for a column holding only a blank string, got %#v", got)
	}
}

func TestWriteFieldDetailsUpgradesAFileWrittenBeforeDetailsWereStored(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "base.db")
	snapshot := detailTestSnapshot()
	if err := WriteSnapshot(ctx, path, snapshot); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}

	// Put the file back into the layout older builds wrote.
	db, err := openDatabase(path, "READ_WRITE")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for _, column := range []string{"field_description", "field_options", "is_primary_field", "table_description"} {
		if _, err := db.ExecContext(ctx, `ALTER TABLE _metadata DROP COLUMN `+column); err != nil {
			t.Fatalf("drop %s: %v", column, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	schema, err := ReadSchema(ctx, path, "app123", "Test Base")
	if err != nil {
		t.Fatalf("ReadSchema on an old file: %v", err)
	}
	if schema.DetailsLoaded || schema.Tables[0].Fields[1].Options != "" || len(schema.Tables[0].Fields) != 4 {
		t.Fatalf("expected an old file to read with blank details, got %#v", schema)
	}
	if current, err := FieldDetailsCurrent(ctx, path, snapshot.Tables); err != nil || current {
		t.Fatalf("expected an old file to need details (current=%v, err=%v)", current, err)
	}

	if err := WriteFieldDetails(ctx, path, snapshot.Tables); err != nil {
		t.Fatalf("WriteFieldDetails: %v", err)
	}
	schema, err = ReadSchema(ctx, path, "app123", "Test Base")
	if err != nil {
		t.Fatalf("ReadSchema after upgrade: %v", err)
	}
	if !schema.DetailsLoaded || schema.Tables[0].Description != "Every shipped project" || !schema.Tables[0].Fields[0].IsPrimary {
		t.Fatalf("expected details after upgrade, got %#v", schema.Tables[0])
	}
	if schema.Tables[0].TotalRecordCount != 3 {
		t.Fatalf("upgrade must leave rows alone, got %d rows", schema.Tables[0].TotalRecordCount)
	}
	if current, err := FieldDetailsCurrent(ctx, path, snapshot.Tables); err != nil || !current {
		t.Fatalf("expected details to be current after writing them (current=%v, err=%v)", current, err)
	}

	// A new select choice is a details change, not a layout change.
	snapshot.Tables[0].Fields[1].Options = `{"choices":[{"id":"sel1","name":"Approved"},{"id":"sel2","name":"Rejected"},{"id":"sel3","name":"Pending"}]}`
	if current, err := FieldDetailsCurrent(ctx, path, snapshot.Tables); err != nil || current {
		t.Fatalf("expected a new choice to make details stale (current=%v, err=%v)", current, err)
	}
}
