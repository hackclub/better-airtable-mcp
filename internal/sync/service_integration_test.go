package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/hackclub/better-airtable-mcp/internal/duckdb"
)

func TestServiceSyncListSchemaAndQuery(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v0/meta/bases":
			writeJSON(t, w, map[string]any{
				"bases": []map[string]any{
					{"id": "appProjects", "name": "Project Tracker", "permissionLevel": "create"},
					{"id": "appOther", "name": "Other Base", "permissionLevel": "read"},
				},
			})
		case "/v0/meta/bases/appProjects/tables":
			writeJSON(t, w, map[string]any{
				"tables": []map[string]any{
					{
						"id":   "tblProjects",
						"name": "Projects",
						"fields": []map[string]any{
							{"id": "fldProjectName", "name": "Name", "type": "singleLineText"},
							{"id": "fldProjectStatus", "name": "Status", "type": "singleSelect"},
							{"id": "fldLinkedTasks", "name": "Linked Tasks", "type": "multipleRecordLinks"},
						},
					},
					{
						"id":   "tblTasks",
						"name": "Tasks",
						"fields": []map[string]any{
							{"id": "fldTaskName", "name": "Name", "type": "singleLineText"},
							{"id": "fldTaskDone", "name": "Done", "type": "checkbox"},
						},
					},
				},
			})
		case "/v0/appProjects/tblProjects":
			writeJSON(t, w, map[string]any{
				"records": []map[string]any{
					{
						"id":          "recProject1",
						"createdTime": "2026-04-01T12:00:00Z",
						"fields": map[string]any{
							"Name":         "Website Redesign",
							"Status":       "In Progress",
							"Linked Tasks": []string{"recTask1", "recTask2"},
						},
					},
					{
						"id":          "recProject2",
						"createdTime": "2026-04-02T12:00:00Z",
						"fields": map[string]any{
							"Name":         "API Migration",
							"Status":       "Done",
							"Linked Tasks": []string{"recTask3"},
						},
					},
				},
			})
		case "/v0/appProjects/tblTasks":
			switch r.URL.Query().Get("offset") {
			case "":
				writeJSON(t, w, map[string]any{
					"records": []map[string]any{
						{
							"id":          "recTask1",
							"createdTime": "2026-04-01T13:00:00Z",
							"fields": map[string]any{
								"Name": "Design new homepage",
								"Done": false,
							},
						},
						{
							"id":          "recTask2",
							"createdTime": "2026-04-01T14:00:00Z",
							"fields": map[string]any{
								"Name": "QA landing flow",
								"Done": false,
							},
						},
					},
					"offset": "page2",
				})
			case "page2":
				writeJSON(t, w, map[string]any{
					"records": []map[string]any{
						{
							"id":          "recTask3",
							"createdTime": "2026-04-02T14:00:00Z",
							"fields": map[string]any{
								"Name": "Cut over API clients",
								"Done": true,
							},
						},
					},
				})
			default:
				t.Fatalf("unexpected offset %q", r.URL.RawQuery)
			}
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	defer server.Close()

	service := NewService(NewHTTPClient(server.URL, server.Client()), t.TempDir())
	ctx := context.Background()

	bases, err := service.SearchBases(ctx, "test-token", "project")
	if err != nil {
		t.Fatalf("SearchBases() returned error: %v", err)
	}
	if len(bases) != 1 || bases[0].ID != "appProjects" {
		t.Fatalf("unexpected base search result: %#v", bases)
	}

	syncResult, err := service.SyncBase(ctx, "test-token", "Project Tracker")
	if err != nil {
		t.Fatalf("SyncBase() returned error: %v", err)
	}
	if syncResult.TablesSynced != 2 {
		t.Fatalf("expected 2 tables synced, got %d", syncResult.TablesSynced)
	}
	if syncResult.RecordsSynced != 5 {
		t.Fatalf("expected 5 records synced, got %d", syncResult.RecordsSynced)
	}

	schema, err := service.ListSchema(ctx, "test-token", "appProjects")
	if err != nil {
		t.Fatalf("ListSchema() returned error: %v", err)
	}
	if len(schema.Tables) != 2 {
		t.Fatalf("expected 2 tables in schema, got %d", len(schema.Tables))
	}

	projectTable := findTable(t, schema, "projects")
	if projectTable.TotalRecordCount != 2 {
		t.Fatalf("expected 2 project records, got %d", projectTable.TotalRecordCount)
	}
	if len(projectTable.SampleRows) == 0 {
		t.Fatal("expected project sample rows to be populated")
	}
	if projectTable.SampleRows[0]["linked_tasks"] == nil {
		t.Fatalf("expected linked_tasks sample row data, got %#v", projectTable.SampleRows[0])
	}

	queryResult, err := service.QueryBase(ctx, "test-token", "Project Tracker", `
		SELECT p.name, t.name AS task_name
		FROM projects p, UNNEST(p.linked_tasks) AS u(task_id)
		JOIN tasks t ON t.id = u.task_id
		ORDER BY p.name, t.name
	`)
	if err != nil {
		t.Fatalf("QueryBase() returned error: %v", err)
	}
	if queryResult.RowCount != 3 {
		t.Fatalf("expected 3 query rows, got %d", queryResult.RowCount)
	}
	if queryResult.Rows[0][0] != "API Migration" {
		t.Fatalf("unexpected first query row %#v", queryResult.Rows[0])
	}
	if queryResult.Rows[2][1] != "QA landing flow" {
		t.Fatalf("unexpected final query row %#v", queryResult.Rows[2])
	}
}

func TestServiceQueryAutoSyncsUnsyncedBase(t *testing.T) {
	var recordsCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v0/meta/bases":
			writeJSON(t, w, map[string]any{
				"bases": []map[string]any{
					{"id": "appProjects", "name": "Project Tracker", "permissionLevel": "create"},
				},
			})
		case "/v0/meta/bases/appProjects/tables":
			writeJSON(t, w, map[string]any{
				"tables": []map[string]any{
					{
						"id":   "tblProjects",
						"name": "Projects",
						"fields": []map[string]any{
							{"id": "fldProjectName", "name": "Name", "type": "singleLineText"},
						},
					},
				},
			})
		case "/v0/appProjects/tblProjects":
			recordsCalls.Add(1)
			writeJSON(t, w, map[string]any{
				"records": []map[string]any{
					{"id": "rec1", "createdTime": "2026-04-01T12:00:00Z", "fields": map[string]any{"Name": "Auto Synced"}},
				},
			})
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	defer server.Close()

	service := NewService(NewHTTPClient(server.URL, server.Client()), t.TempDir())
	ctx := context.Background()

	result, err := service.QueryBase(ctx, "test-token", "Project Tracker", `SELECT name FROM projects`)
	if err != nil {
		t.Fatalf("QueryBase() returned error: %v", err)
	}
	if recordsCalls.Load() != 1 {
		t.Fatalf("expected first query to trigger an implicit sync, got %d record fetches", recordsCalls.Load())
	}
	if result.RowCount != 1 || result.Rows[0][0] != "Auto Synced" {
		t.Fatalf("unexpected query result %#v", result.Rows)
	}
}

func TestServiceSyncBaseFullRefreshReplacesPriorSnapshot(t *testing.T) {
	var generation atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v0/meta/bases":
			writeJSON(t, w, map[string]any{
				"bases": []map[string]any{
					{"id": "appProjects", "name": "Project Tracker", "permissionLevel": "create"},
				},
			})
		case "/v0/meta/bases/appProjects/tables":
			writeJSON(t, w, map[string]any{
				"tables": []map[string]any{
					{
						"id":   "tblProjects",
						"name": "Projects",
						"fields": []map[string]any{
							{"id": "fldProjectName", "name": "Name", "type": "singleLineText"},
							{"id": "fldProjectStatus", "name": "Status", "type": "singleSelect"},
						},
					},
				},
			})
		case "/v0/appProjects/tblProjects":
			if generation.Load() == 0 {
				writeJSON(t, w, map[string]any{
					"records": []map[string]any{
						{"id": "rec1", "createdTime": "2026-04-01T12:00:00Z", "fields": map[string]any{"Name": "Old A", "Status": "Todo"}},
						{"id": "rec2", "createdTime": "2026-04-01T12:00:00Z", "fields": map[string]any{"Name": "Old B", "Status": "Done"}},
					},
				})
				return
			}
			writeJSON(t, w, map[string]any{
				"records": []map[string]any{
					{"id": "rec2", "createdTime": "2026-04-01T12:00:00Z", "fields": map[string]any{"Name": "Old B Updated", "Status": "Doing"}},
					{"id": "rec3", "createdTime": "2026-04-02T12:00:00Z", "fields": map[string]any{"Name": "New C", "Status": "Todo"}},
				},
			})
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	defer server.Close()

	service := NewService(NewHTTPClient(server.URL, server.Client()), t.TempDir())
	ctx := context.Background()

	if _, err := service.SyncBase(ctx, "test-token", "Project Tracker"); err != nil {
		t.Fatalf("first SyncBase() returned error: %v", err)
	}
	generation.Store(1)
	if _, err := service.SyncBase(ctx, "test-token", "Project Tracker"); err != nil {
		t.Fatalf("second SyncBase() returned error: %v", err)
	}

	queryResult, err := service.QueryBase(ctx, "test-token", "appProjects", `SELECT id, name, status FROM projects ORDER BY id`)
	if err != nil {
		t.Fatalf("QueryBase() returned error: %v", err)
	}
	if queryResult.RowCount != 2 {
		t.Fatalf("expected 2 rows after full refresh replacement, got %d", queryResult.RowCount)
	}
	if queryResult.Rows[0][0] != "rec2" || queryResult.Rows[0][1] != "Old B Updated" {
		t.Fatalf("unexpected first row after replacement %#v", queryResult.Rows[0])
	}
	if queryResult.Rows[1][0] != "rec3" {
		t.Fatalf("expected stale row rec1 to be removed, got %#v", queryResult.Rows)
	}
}

func TestServiceFailedSyncRetainsPreviousSnapshot(t *testing.T) {
	var failSchema atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v0/meta/bases":
			writeJSON(t, w, map[string]any{
				"bases": []map[string]any{
					{"id": "appProjects", "name": "Project Tracker", "permissionLevel": "create"},
				},
			})
		case "/v0/meta/bases/appProjects/tables":
			if failSchema.Load() {
				http.Error(w, `{"error":"boom"}`, http.StatusInternalServerError)
				return
			}
			writeJSON(t, w, map[string]any{
				"tables": []map[string]any{
					{
						"id":   "tblProjects",
						"name": "Projects",
						"fields": []map[string]any{
							{"id": "fldProjectName", "name": "Name", "type": "singleLineText"},
						},
					},
				},
			})
		case "/v0/appProjects/tblProjects":
			writeJSON(t, w, map[string]any{
				"records": []map[string]any{
					{"id": "rec1", "createdTime": "2026-04-01T12:00:00Z", "fields": map[string]any{"Name": "Stable Snapshot"}},
				},
			})
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	defer server.Close()

	service := NewService(NewHTTPClient(server.URL, server.Client()), t.TempDir())
	ctx := context.Background()

	if _, err := service.SyncBase(ctx, "test-token", "Project Tracker"); err != nil {
		t.Fatalf("initial SyncBase() returned error: %v", err)
	}

	failSchema.Store(true)
	if _, err := service.SyncBase(ctx, "test-token", "Project Tracker"); err == nil {
		t.Fatal("expected failed SyncBase() call to return an error")
	}

	queryResult, err := service.QueryBase(ctx, "test-token", "Project Tracker", `SELECT id, name FROM projects`)
	if err != nil {
		t.Fatalf("QueryBase() after failed sync returned error: %v", err)
	}
	if queryResult.RowCount != 1 || queryResult.Rows[0][1] != "Stable Snapshot" {
		t.Fatalf("expected previous snapshot to remain queryable, got %#v", queryResult.Rows)
	}
}

func TestServiceSyncBaseHandlesLargePaginatedTable(t *testing.T) {
	const totalRecords = 237
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v0/meta/bases":
			writeJSON(t, w, map[string]any{
				"bases": []map[string]any{
					{"id": "appLarge", "name": "Large Base", "permissionLevel": "create"},
				},
			})
		case "/v0/meta/bases/appLarge/tables":
			writeJSON(t, w, map[string]any{
				"tables": []map[string]any{
					{
						"id":   "tblItems",
						"name": "Items",
						"fields": []map[string]any{
							{"id": "fldName", "name": "Name", "type": "singleLineText"},
						},
					},
				},
			})
		case "/v0/appLarge/tblItems":
			offset := r.URL.Query().Get("offset")
			start := 0
			switch offset {
			case "":
				start = 0
			case "page2":
				start = 100
			case "page3":
				start = 200
			default:
				t.Fatalf("unexpected offset %q", offset)
			}

			records := make([]map[string]any, 0, 100)
			for i := start; i < totalRecords && i < start+100; i++ {
				records = append(records, map[string]any{
					"id":          fmt.Sprintf("rec%03d", i),
					"createdTime": "2026-04-01T12:00:00Z",
					"fields":      map[string]any{"Name": fmt.Sprintf("Item %03d", i)},
				})
			}

			payload := map[string]any{"records": records}
			if start == 0 {
				payload["offset"] = "page2"
			}
			if start == 100 {
				payload["offset"] = "page3"
			}
			writeJSON(t, w, payload)
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	defer server.Close()

	service := NewService(NewHTTPClient(server.URL, server.Client()), t.TempDir())
	ctx := context.Background()

	result, err := service.SyncBase(ctx, "test-token", "Large Base")
	if err != nil {
		t.Fatalf("SyncBase() returned error: %v", err)
	}
	if result.RecordsSynced != totalRecords {
		t.Fatalf("expected %d synced records, got %d", totalRecords, result.RecordsSynced)
	}

	queryResult, err := service.QueryBase(ctx, "test-token", "appLarge", `SELECT COUNT(*) AS total FROM items`)
	if err != nil {
		t.Fatalf("QueryBase() returned error: %v", err)
	}
	if queryResult.Rows[0][0] != int64(totalRecords) {
		t.Fatalf("expected COUNT(*) = %d, got %#v", totalRecords, queryResult.Rows)
	}
}

func TestServiceSyncBaseRefreshesSchemaMetadataOnDrift(t *testing.T) {
	var generation atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v0/meta/bases":
			writeJSON(t, w, map[string]any{
				"bases": []map[string]any{
					{"id": "appProjects", "name": "Project Tracker", "permissionLevel": "create"},
				},
			})
		case "/v0/meta/bases/appProjects/tables":
			if generation.Load() == 0 {
				writeJSON(t, w, map[string]any{
					"tables": []map[string]any{
						{
							"id":   "tblProjects",
							"name": "Projects",
							"fields": []map[string]any{
								{"id": "fldName", "name": "Name", "type": "singleLineText"},
								{"id": "fldStatus", "name": "Status", "type": "singleSelect"},
							},
						},
					},
				})
				return
			}
			writeJSON(t, w, map[string]any{
				"tables": []map[string]any{
					{
						"id":   "tblProjects",
						"name": "Projects",
						"fields": []map[string]any{
							{"id": "fldProjectName", "name": "Project Name", "type": "singleLineText"},
							{"id": "fldPriority", "name": "Priority (%)", "type": "number"},
						},
					},
				},
			})
		case "/v0/appProjects/tblProjects":
			if generation.Load() == 0 {
				writeJSON(t, w, map[string]any{
					"records": []map[string]any{
						{"id": "rec1", "createdTime": "2026-04-01T12:00:00Z", "fields": map[string]any{"Name": "Old Field Set", "Status": "Todo"}},
					},
				})
				return
			}
			writeJSON(t, w, map[string]any{
				"records": []map[string]any{
					{"id": "rec1", "createdTime": "2026-04-01T12:00:00Z", "fields": map[string]any{"Project Name": "New Field Set", "Priority (%)": 75}},
				},
			})
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	defer server.Close()

	service := NewService(NewHTTPClient(server.URL, server.Client()), t.TempDir())
	ctx := context.Background()

	if _, err := service.SyncBase(ctx, "test-token", "Project Tracker"); err != nil {
		t.Fatalf("first SyncBase() returned error: %v", err)
	}

	generation.Store(1)
	if _, err := service.SyncBase(ctx, "test-token", "Project Tracker"); err != nil {
		t.Fatalf("second SyncBase() returned error: %v", err)
	}

	schema, err := service.ListSchema(ctx, "test-token", "appProjects")
	if err != nil {
		t.Fatalf("ListSchema() returned error: %v", err)
	}
	projectTable := findTable(t, schema, "projects")
	if len(projectTable.Fields) != 2 {
		t.Fatalf("expected exactly 2 fields after schema drift refresh, got %#v", projectTable.Fields)
	}
	if projectTable.Fields[0].DuckDBColumnName != "project_name" || projectTable.Fields[1].DuckDBColumnName != "priority" {
		t.Fatalf("unexpected drifted schema fields %#v", projectTable.Fields)
	}

	queryResult, err := service.QueryBase(ctx, "test-token", "appProjects", `SELECT project_name, priority FROM projects`)
	if err != nil {
		t.Fatalf("QueryBase() returned error: %v", err)
	}
	if queryResult.RowCount != 1 || queryResult.Rows[0][0] != "New Field Set" || queryResult.Rows[0][1] != float64(75) {
		t.Fatalf("unexpected rows after schema drift refresh %#v", queryResult.Rows)
	}
}

func writeJSON(t *testing.T, w http.ResponseWriter, payload any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		t.Fatalf("json.NewEncoder().Encode() returned error: %v", err)
	}
}

func findTable(t *testing.T, schema duckdb.BaseSchema, name string) duckdb.TableSchema {
	t.Helper()

	for _, table := range schema.Tables {
		if table.DuckDBTableName == name {
			return table
		}
	}

	t.Fatalf("table %q not found in schema %#v", name, schema.Tables)
	return duckdb.TableSchema{}
}
