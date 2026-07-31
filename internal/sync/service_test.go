package syncer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestBuildSyncPlansDisambiguatesDuplicateFieldNames(t *testing.T) {
	tables := []Table{{
		ID:   "tbl1",
		Name: "Tasks",
		Fields: []Field{
			{ID: "fld1", Name: "Status", Type: "singleLineText"},
			{ID: "fld2", Name: "Status", Type: "singleLineText"},
		},
	}}

	plans, err := buildSyncPlans(tables)
	if err != nil {
		t.Fatalf("buildSyncPlans() returned error: %v", err)
	}
	if len(plans) != 1 {
		t.Fatalf("expected 1 plan, got %d", len(plans))
	}

	fields := plans[0].Table.Fields
	if len(fields) != 2 {
		t.Fatalf("expected 2 fields, got %d (%#v)", len(fields), fields)
	}

	if fields[0].AirtableFieldID != "fld1" || fields[1].AirtableFieldID != "fld2" {
		t.Fatalf("field order not preserved: got %q, %q", fields[0].AirtableFieldID, fields[1].AirtableFieldID)
	}
	if fields[0].DuckDBColumnName == fields[1].DuckDBColumnName {
		t.Fatalf("duplicate field names collapsed to the same column %q", fields[0].DuckDBColumnName)
	}
	if got := fields[0].DuckDBColumnName; got != "status" {
		t.Fatalf("field fld1: want column %q, got %q", "status", got)
	}
	if got := fields[1].DuckDBColumnName; got != "status_2" {
		t.Fatalf("field fld2: want column %q, got %q", "status_2", got)
	}
}

func TestBuildSyncPlansKeepsSanitizedIndexAlignedAcrossOmittedFields(t *testing.T) {
	// "button" maps to an omitted DuckDB type, so it is dropped from the synced
	// fields. The sanitized-name index must skip it without drifting, otherwise
	// the fields straddling the omission get the wrong columns.
	tables := []Table{{
		ID:   "tbl1",
		Name: "Tasks",
		Fields: []Field{
			{ID: "fld1", Name: "Status", Type: "singleLineText"},
			{ID: "fldBtn", Name: "Open", Type: "button"},
			{ID: "fld2", Name: "Status", Type: "singleLineText"},
		},
	}}

	plans, err := buildSyncPlans(tables)
	if err != nil {
		t.Fatalf("buildSyncPlans() returned error: %v", err)
	}

	fields := plans[0].Table.Fields
	if len(fields) != 2 {
		t.Fatalf("expected the omitted button field to be dropped, got %d fields (%#v)", len(fields), fields)
	}

	want := map[string]string{
		"fld1": "status",
		"fld2": "status_2",
	}
	for _, field := range fields {
		if got := field.DuckDBColumnName; got != want[field.AirtableFieldID] {
			t.Fatalf("field %s: want column %q, got %q", field.AirtableFieldID, want[field.AirtableFieldID], got)
		}
	}
}

type countingBasesClient struct {
	listBasesCalls atomic.Int32
	bases          []Base
}

func (c *countingBasesClient) ListBases(ctx context.Context, accessToken string) ([]Base, error) {
	c.listBasesCalls.Add(1)
	return c.bases, nil
}

func (c *countingBasesClient) GetBaseSchema(ctx context.Context, accessToken, baseID string) ([]Table, error) {
	return nil, nil
}

func (c *countingBasesClient) ListRecordsPage(ctx context.Context, accessToken, baseID, tableID string, options ListRecordsPageOptions) (ListRecordsPageResult, error) {
	return ListRecordsPageResult{}, nil
}

func (c *countingBasesClient) ListRecords(ctx context.Context, accessToken, baseID, tableID string) ([]Record, error) {
	return nil, nil
}

func TestResolveBaseCachesBaseListPerToken(t *testing.T) {
	client := &countingBasesClient{bases: []Base{
		{ID: "appProjects", Name: "Project Tracker", PermissionLevel: "create"},
		{ID: "appOther", Name: "Other Base", PermissionLevel: "read"},
	}}
	service := NewService(client, t.TempDir())

	current := time.Now()
	service.now = func() time.Time { return current }

	for _, ref := range []string{"appProjects", "Project Tracker", "project"} {
		base, err := service.resolveBase(context.Background(), "token-a", ref)
		if err != nil {
			t.Fatalf("resolveBase(%q) returned error: %v", ref, err)
		}
		if base.ID != "appProjects" {
			t.Fatalf("resolveBase(%q) = %#v, want appProjects", ref, base)
		}
	}
	if got := client.listBasesCalls.Load(); got != 1 {
		t.Fatalf("expected one ListBases call within the cache TTL, got %d", got)
	}

	// A different token must not share the cache entry.
	if _, err := service.resolveBase(context.Background(), "token-b", "appProjects"); err != nil {
		t.Fatalf("resolveBase() with second token returned error: %v", err)
	}
	if got := client.listBasesCalls.Load(); got != 2 {
		t.Fatalf("expected a fresh ListBases call for a new token, got %d", got)
	}

	// Past the TTL the list is re-fetched.
	current = current.Add(baseListCacheTTL + time.Second)
	if _, err := service.resolveBase(context.Background(), "token-a", "appProjects"); err != nil {
		t.Fatalf("resolveBase() after TTL returned error: %v", err)
	}
	if got := client.listBasesCalls.Load(); got != 3 {
		t.Fatalf("expected a refreshed ListBases call after the TTL, got %d", got)
	}
}
