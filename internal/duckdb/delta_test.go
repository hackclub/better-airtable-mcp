package duckdb

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func deltaTestTable(records ...RecordSnapshot) TableSnapshot {
	return TableSnapshot{
		AirtableTableID: "tblPeople",
		OriginalName:    "People",
		DuckDBTableName: "people",
		Fields: []FieldSnapshot{
			{AirtableFieldID: "fldName", OriginalFieldName: "Name", DuckDBColumnName: "name", AirtableFieldType: "singleLineText", DuckDBType: "VARCHAR"},
			{AirtableFieldID: "fldScore", OriginalFieldName: "Score", DuckDBColumnName: "score", AirtableFieldType: "number", DuckDBType: "DOUBLE"},
			{AirtableFieldID: "fldFriends", OriginalFieldName: "Friends", DuckDBColumnName: "friends", AirtableFieldType: "multipleRecordLinks", DuckDBType: "VARCHAR[]"},
		},
		Records: records,
	}
}

func deltaTestRecord(id, name string, score float64, friends ...string) RecordSnapshot {
	fields := map[string]any{"Name": name, "Score": score}
	if len(friends) > 0 {
		links := make([]any, 0, len(friends))
		for _, friend := range friends {
			links = append(links, friend)
		}
		fields["Friends"] = links
	}
	return RecordSnapshot{ID: id, CreatedTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Fields: fields}
}

func seedDeltaTestFile(t *testing.T) (string, time.Time) {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "appDelta.db")
	startedAt := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	table := deltaTestTable()
	if err := InitializeSnapshot(ctx, path, SnapshotInit{BaseID: "appDelta", BaseName: "Delta", Tables: []TableSnapshot{table}, SyncStartedAt: startedAt}); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	records := []RecordSnapshot{
		deltaTestRecord("recA", "Ada", 1, "recB", "recC"),
		deltaTestRecord("recB", "Bo", 2),
		deltaTestRecord("recC", "Cy", 3),
	}
	info := SyncInfo{SyncStartedAt: startedAt, LastSyncedAt: startedAt, TotalRecords: 3, TotalTables: 1, Status: "completed", TablesStarted: 1, TablesCompleted: 1}
	if err := ApplyTablePage(ctx, path, table, records, TableSyncState{TableName: "people", SyncStatus: "completed", VisibleRecordCount: 3, PagesFetched: 1}, info); err != nil {
		t.Fatalf("apply page: %v", err)
	}
	if err := FinalizeSnapshot(ctx, path, info); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if err := MarkReconciled(ctx, path, ReconcileMeta{SchemaHash: "hash-1", Watermark: startedAt, ReconciledAt: startedAt.Add(time.Minute)}); err != nil {
		t.Fatalf("mark reconciled: %v", err)
	}
	return path, startedAt
}

func TestApplyDeltaUpsertsDeletesAndReportsChanges(t *testing.T) {
	ctx := context.Background()
	path, startedAt := seedDeltaTestFile(t)
	checkedAt := startedAt.Add(time.Hour)

	results, err := ApplyDelta(ctx, path, []DeltaTable{{
		Table: deltaTestTable(
			deltaTestRecord("recA", "Ada", 9, "recB"), // score changed, link to recC dropped
			deltaTestRecord("recB", "Bo", 2),          // refetched but identical
			deltaTestRecord("recD", "Di", 4),          // new
		),
		DeleteIDs:        []string{"recC", "recMissing"},
		WatchColumns:     []string{"name", "score"},
		WatchAny:         true,
		LinkColumns:      []string{"friends"},
		AdvanceWatermark: true,
		BasedOnWatermark: startedAt,
		NewWatermark:     checkedAt,
		CheckedAt:        checkedAt,
	}}, checkedAt)
	if err != nil {
		t.Fatalf("apply delta: %v", err)
	}
	got := results[0]
	if got.Upserted != 3 || got.Deleted != 1 || got.RecordCount != 3 {
		t.Fatalf("counts = upserted %d deleted %d rows %d, want 3 1 3", got.Upserted, got.Deleted, got.RecordCount)
	}
	if want := map[string][]string{"score": {"recA"}}; !reflect.DeepEqual(got.ChangedByColumn, want) {
		t.Fatalf("ChangedByColumn = %v, want %v", got.ChangedByColumn, want)
	}
	if want := []string{"recA"}; !reflect.DeepEqual(got.ChangedAny, want) {
		t.Fatalf("ChangedAny = %v, want %v", got.ChangedAny, want)
	}
	if want := map[string][]string{"friends": {"recC"}}; !reflect.DeepEqual(got.DroppedLinks, want) {
		t.Fatalf("DroppedLinks = %v, want %v", got.DroppedLinks, want)
	}
	if !got.WatermarkAdvanced {
		t.Fatalf("watermark was not advanced")
	}

	result, err := Query(ctx, path, `SELECT id, name, score FROM people ORDER BY id`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	want := [][]any{{"recA", "Ada", 9.0}, {"recB", "Bo", 2.0}, {"recD", "Di", 4.0}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %v, want %v", result.Rows, want)
	}

	state, err := ReadCacheState(ctx, path)
	if err != nil {
		t.Fatalf("read cache state: %v", err)
	}
	if state.SchemaHash != "hash-1" || state.CacheFormat != CacheFormat {
		t.Fatalf("schema hash/format = %q/%d", state.SchemaHash, state.CacheFormat)
	}
	table := state.Tables[0]
	if !table.Watermark.Equal(checkedAt) || !table.CheckedAt.Equal(checkedAt) || table.RecordCount != 3 {
		t.Fatalf("table state = %+v", table)
	}
	if !table.ReconciledAt.Equal(startedAt.Add(time.Minute)) {
		t.Fatalf("reconciled_at changed to %v", table.ReconciledAt)
	}
	if !state.Info.LastSyncedAt.Equal(checkedAt) {
		t.Fatalf("last_synced_at = %v, want %v", state.Info.LastSyncedAt, checkedAt)
	}
}

func TestApplyDeltaKeepsWatermarkWhenFileWasSwappedUnderneath(t *testing.T) {
	ctx := context.Background()
	path, startedAt := seedDeltaTestFile(t)

	results, err := ApplyDelta(ctx, path, []DeltaTable{{
		Table:            deltaTestTable(deltaTestRecord("recB", "Bo", 5)),
		AdvanceWatermark: true,
		BasedOnWatermark: startedAt.Add(-time.Hour), // planned against an older file
		NewWatermark:     startedAt.Add(time.Hour),
		CheckedAt:        startedAt.Add(time.Hour),
	}}, time.Time{})
	if err != nil {
		t.Fatalf("apply delta: %v", err)
	}
	if results[0].WatermarkAdvanced {
		t.Fatalf("watermark advanced past a rewind")
	}
	state, err := ReadCacheState(ctx, path)
	if err != nil {
		t.Fatalf("read cache state: %v", err)
	}
	if !state.Tables[0].Watermark.Equal(startedAt) {
		t.Fatalf("watermark = %v, want %v", state.Tables[0].Watermark, startedAt)
	}
}

func TestSyncInfoWritesKeepSchemaHashAndSingleRow(t *testing.T) {
	ctx := context.Background()
	path, startedAt := seedDeltaTestFile(t)

	for i := 0; i < 3; i++ {
		if err := UpdateSyncFailure(ctx, path, SyncInfo{SyncStartedAt: startedAt, Status: "failed", Error: "boom"}); err != nil {
			t.Fatalf("update sync info: %v", err)
		}
	}
	state, err := ReadCacheState(ctx, path)
	if err != nil {
		t.Fatalf("read cache state: %v", err)
	}
	if state.SchemaHash != "hash-1" || state.Info.Status != "failed" {
		t.Fatalf("state = %+v", state)
	}
	result, err := Query(ctx, path, `SELECT count(*) FROM _sync_info`)
	if err != nil {
		t.Fatalf("count sync info rows: %v", err)
	}
	if got := result.Rows[0][0]; got != int64(1) {
		t.Fatalf("_sync_info rows = %v, want 1", got)
	}
}

func TestFindRowsLinkingAndExistingRecordIDs(t *testing.T) {
	ctx := context.Background()
	path, _ := seedDeltaTestFile(t)

	ids, err := FindRowsLinking(ctx, path, "people", "friends", []string{"recC", "recZ"})
	if err != nil {
		t.Fatalf("find rows linking: %v", err)
	}
	if want := []string{"recA"}; !reflect.DeepEqual(ids, want) {
		t.Fatalf("linking rows = %v, want %v", ids, want)
	}
	found, err := ExistingRecordIDs(ctx, path, "people", []string{"recA", "recZ", "recC"})
	if err != nil {
		t.Fatalf("existing ids: %v", err)
	}
	if want := []string{"recA", "recC"}; !reflect.DeepEqual(found, want) {
		t.Fatalf("existing = %v, want %v", found, want)
	}
	if _, err := FindRowsLinking(ctx, path, "people", "friends", []string{"rec'; DROP TABLE people; --"}); err == nil {
		t.Fatalf("expected an invalid record id to be refused")
	}
}

func TestLookupAndCountFieldsAreCached(t *testing.T) {
	for airtableType, want := range map[string]string{"multipleLookupValues": "JSON", "count": "BIGINT"} {
		mapping, ok := AirtableTypeToDuckDBType(airtableType)
		if !ok || mapping.Omitted || mapping.DuckDBType != want {
			t.Fatalf("%s maps to %+v (known %v), want %s", airtableType, mapping, ok, want)
		}
	}

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "appTypes.db")
	table := TableSnapshot{
		AirtableTableID: "tblT", OriginalName: "T", DuckDBTableName: "t",
		Fields: []FieldSnapshot{
			{AirtableFieldID: "fldL", OriginalFieldName: "Names", DuckDBColumnName: "names", AirtableFieldType: "multipleLookupValues", DuckDBType: "JSON"},
			{AirtableFieldID: "fldC", OriginalFieldName: "Count", DuckDBColumnName: "count", AirtableFieldType: "count", DuckDBType: "BIGINT"},
		},
	}
	now := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	if err := InitializeSnapshot(ctx, path, SnapshotInit{BaseID: "appTypes", BaseName: "Types", Tables: []TableSnapshot{table}, SyncStartedAt: now}); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	records := []RecordSnapshot{{ID: "recA", CreatedTime: now, Fields: map[string]any{"Names": []any{"Ada", "Bo"}, "Count": float64(2)}}}
	info := SyncInfo{SyncStartedAt: now, LastSyncedAt: now, Status: "completed", TotalTables: 1}
	if err := ApplyTablePage(ctx, path, table, records, TableSyncState{TableName: "t", SyncStatus: "completed", VisibleRecordCount: 1}, info); err != nil {
		t.Fatalf("apply page: %v", err)
	}
	result, err := Query(ctx, path, `SELECT count, json_array_length(names), names->>0 FROM t`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if got := fmt.Sprint(result.Rows[0]); got != "[2 2 Ada]" {
		t.Fatalf("row = %v, want count 2 and a two-element lookup starting with Ada", result.Rows[0])
	}
}
