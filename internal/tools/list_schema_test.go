package tools

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/hackclub/better-airtable-mcp/internal/duckdb"
	syncer "github.com/hackclub/better-airtable-mcp/internal/sync"
)

func callListSchema(t *testing.T, fakeSync *fakeSyncer, fakeManager *fakeSyncManager, args map[string]any) (string, error) {
	t.Helper()
	raw, err := json.Marshal(args)
	if err != nil {
		t.Fatalf("marshal args: %v", err)
	}
	result, err := NewListSchemaTool(newFakeRuntime(fakeSync, fakeManager)).Call(authedContext(), raw)
	if err != nil {
		return "", err
	}
	if result.StructuredContent != nil {
		t.Fatalf("list_schema must return text only, got structured content %#v", result.StructuredContent)
	}
	if len(result.Content) != 1 {
		t.Fatalf("expected one text block, got %#v", result.Content)
	}
	return result.Content[0].Text, nil
}

// largeSchemaTestBase is too wide to list every column unasked.
func largeSchemaTestBase() duckdb.BaseSchema {
	schema := schemaTestBase()
	for tableIndex := 0; tableIndex < 8; tableIndex++ {
		table := duckdb.TableSchema{
			AirtableTableID: fmt.Sprintf("tblBig%d", tableIndex),
			DuckDBTableName: fmt.Sprintf("big_%d", tableIndex),
			OriginalName:    fmt.Sprintf("Big %d", tableIndex),
			TableComplete:   true,
		}
		for fieldIndex := 0; fieldIndex < 60; fieldIndex++ {
			table.Fields = append(table.Fields, duckdb.FieldSchema{
				AirtableFieldID:  fmt.Sprintf("fld%d_%d", tableIndex, fieldIndex),
				DuckDBColumnName: fmt.Sprintf("measure_%d", fieldIndex),
				Type:             "DOUBLE",
				AirtableType:     "number",
			})
		}
		schema.Tables = append(schema.Tables, table)
	}
	return schema
}

func TestListSchemaReturnsEveryColumnOfASmallBase(t *testing.T) {
	fakeSync := &fakeSyncer{schema: schemaTestBase(), stats: schemaTestStats()}
	fakeManager := &fakeSyncManager{freshness: syncer.Freshness{State: syncer.FreshnessFresh}}

	text, err := callListSchema(t, fakeSync, fakeManager, map[string]any{"base": "appProjects"})
	if err != nil {
		t.Fatalf("Call() returned error: %v", err)
	}
	for _, want := range []string{
		"freshness\n",
		"# projects: 200 rows, 7 columns",
		"status\tVARCHAR\tsingleSelect: Approved | Rejected\t1%\tApproved\t",
		"# tasks: 40 rows so far (still downloading), 2 columns",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("expected %q in:\n%s", want, text)
		}
	}
	if strings.Contains(text, "not stored for this base yet") {
		t.Fatalf("unexpected missing-details note:\n%s", text)
	}
}

func TestListSchemaReturnsAnOverviewOfALargeBase(t *testing.T) {
	fakeSync := &fakeSyncer{schema: largeSchemaTestBase(), stats: schemaTestStats()}
	fakeManager := &fakeSyncManager{}

	text, err := callListSchema(t, fakeSync, fakeManager, map[string]any{"base": "appProjects"})
	if err != nil {
		t.Fatalf("Call() returned error: %v", err)
	}
	if !strings.Contains(text, "table\trows\tcolumns\tprimary\tlinks_to") || !strings.Contains(text, "big_7\t0\t60\t") {
		t.Fatalf("expected a table overview, got:\n%s", text)
	}
	if strings.Contains(text, "measure_0") {
		t.Fatalf("the overview must not list columns, got:\n%s", text)
	}
	if len(fakeSync.statsTables) != 0 {
		t.Fatalf("a base too wide for the every-column view must not have its stats computed, got %v", fakeSync.statsTables)
	}
	if len(text) > 2000 {
		t.Fatalf("expected a small overview, got %d characters", len(text))
	}

	// Asking for tables returns their columns, with stats for those only.
	// Tables resolve by DuckDB name, Airtable name or Airtable ID.
	text, err = callListSchema(t, fakeSync, fakeManager, map[string]any{"base": "appProjects", "tables": []string{"Tasks", "tblProjects", "tasks"}})
	if err != nil {
		t.Fatalf("Call(tables) returned error: %v", err)
	}
	if strings.Join(fakeSync.statsTables, " ") != "tasks projects" {
		t.Fatalf("expected stats for the requested tables in order, got %v", fakeSync.statsTables)
	}
	if !strings.Contains(text, "hours_spent\tDOUBLE\tnumber\t25%\t2.5") || !strings.Contains(text, "# projects: 200 rows") || strings.Contains(text, "# big_0") {
		t.Fatalf("expected only the requested tables, got:\n%s", text)
	}
	if strings.Index(text, "# tasks") > strings.Index(text, "# projects") {
		t.Fatalf("expected tables in the order asked for, got:\n%s", text)
	}

	// Search runs across every table and computes stats only where it hit.
	text, err = callListSchema(t, fakeSync, fakeManager, map[string]any{"base": "appProjects", "search": "hours"})
	if err != nil {
		t.Fatalf("Call(search) returned error: %v", err)
	}
	if strings.Join(fakeSync.statsTables, " ") != "projects tasks" {
		t.Fatalf("expected stats for the matched tables only, got %v", fakeSync.statsTables)
	}
	if !strings.Contains(text, "2 columns match any of: hours.") || !strings.Contains(text, "tasks\thours_spent\tDOUBLE\tnumber\t25%\t2.5") {
		t.Fatalf("unexpected search result:\n%s", text)
	}

	// Search can be narrowed to some tables.
	text, err = callListSchema(t, fakeSync, fakeManager, map[string]any{"base": "appProjects", "search": "hours", "tables": []string{"tasks"}})
	if err != nil {
		t.Fatalf("Call(search, tables) returned error: %v", err)
	}
	if !strings.Contains(text, "1 column matches any of: hours.") || strings.Contains(text, "task_hours") {
		t.Fatalf("unexpected narrowed search result:\n%s", text)
	}
}

func TestListSchemaNamesTheTablesWhenOneIsUnknown(t *testing.T) {
	fakeSync := &fakeSyncer{schema: schemaTestBase()}
	_, err := callListSchema(t, fakeSync, &fakeSyncManager{}, map[string]any{"base": "appProjects", "tables": []string{"nope"}})
	if err == nil || err.Error() != `unknown table "nope"; tables in this base: projects, tasks` {
		t.Fatalf("unexpected error %v", err)
	}
}

func TestListSchemaSaysWhenFieldDetailsAreNotStoredYet(t *testing.T) {
	schema := schemaTestBase()
	schema.DetailsLoaded = false
	text, err := callListSchema(t, &fakeSyncer{schema: schema}, &fakeSyncManager{}, map[string]any{"base": "appProjects"})
	if err != nil {
		t.Fatalf("Call() returned error: %v", err)
	}
	if !strings.Contains(text, "not stored for this base yet") {
		t.Fatalf("expected the missing-details note, got:\n%s", text)
	}
}
