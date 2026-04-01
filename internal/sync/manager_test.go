package syncer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hackclub/better-airtable-mcp/internal/duckdb"
)

type staticTokenSource struct{}

func (staticTokenSource) AirtableAccessToken(context.Context, string) (string, error) {
	return "token", nil
}

func TestManagerCoalescesManualSyncRequestsDuringInFlightSync(t *testing.T) {
	blockRecords := make(chan struct{})
	releaseRecords := make(chan struct{})
	var schemaCalls atomic.Int32
	var recordCalls atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v0/meta/bases":
			writeManagerJSON(t, w, map[string]any{
				"bases": []map[string]any{{"id": "appProjects", "name": "Project Tracker", "permissionLevel": "create"}},
			})
		case "/v0/meta/bases/appProjects/tables":
			schemaCalls.Add(1)
			writeManagerJSON(t, w, map[string]any{
				"tables": []map[string]any{
					{
						"id":   "tblProjects",
						"name": "Projects",
						"fields": []map[string]any{
							{"id": "fldName", "name": "Name", "type": "singleLineText"},
						},
					},
				},
			})
		case "/v0/appProjects/tblProjects":
			recordCalls.Add(1)
			select {
			case blockRecords <- struct{}{}:
			default:
			}
			<-releaseRecords
			writeManagerJSON(t, w, map[string]any{
				"records": []map[string]any{
					{"id": "rec1", "createdTime": "2026-04-01T12:00:00Z", "fields": map[string]any{"Name": "A"}},
				},
			})
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	defer server.Close()

	service := NewService(NewHTTPClient(server.URL, server.Client()), t.TempDir())
	manager := NewManager(service, nil, staticTokenSource{}, time.Minute, time.Minute)

	status, err := manager.RequestSync(context.Background(), "user_1", "Project Tracker")
	if err != nil {
		t.Fatalf("RequestSync() returned error: %v", err)
	}
	if status.Status != "syncing" {
		t.Fatalf("expected syncing status, got %#v", status)
	}

	select {
	case <-blockRecords:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first sync to start record fetch")
	}

	status, err = manager.RequestSync(context.Background(), "user_1", "Project Tracker")
	if err != nil {
		t.Fatalf("second RequestSync() returned error: %v", err)
	}
	if status.OperationID != "sync_appProjects" {
		t.Fatalf("unexpected operation id %#v", status)
	}

	close(releaseRecords)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		current, found, err := manager.CheckOperation(context.Background(), "sync_appProjects")
		if err != nil {
			t.Fatalf("CheckOperation() returned error: %v", err)
		}
		if found && current.Status == "completed" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if got := recordCalls.Load(); got != 1 {
		t.Fatalf("expected one record fetch despite repeated manual sync requests, got %d", got)
	}
	if got := schemaCalls.Load(); got != 1 {
		t.Fatalf("expected one schema fetch, got %d", got)
	}
}

func TestManagerRemovesDuckDBFileAfterTTLExpiry(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v0/meta/bases":
			writeManagerJSON(t, w, map[string]any{
				"bases": []map[string]any{{"id": "appProjects", "name": "Project Tracker", "permissionLevel": "create"}},
			})
		case "/v0/meta/bases/appProjects/tables":
			writeManagerJSON(t, w, map[string]any{
				"tables": []map[string]any{
					{
						"id":   "tblProjects",
						"name": "Projects",
						"fields": []map[string]any{
							{"id": "fldName", "name": "Name", "type": "singleLineText"},
						},
					},
				},
			})
		case "/v0/appProjects/tblProjects":
			writeManagerJSON(t, w, map[string]any{
				"records": []map[string]any{
					{"id": "rec1", "createdTime": "2026-04-01T12:00:00Z", "fields": map[string]any{"Name": "A"}},
				},
			})
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	defer server.Close()

	service := NewService(NewHTTPClient(server.URL, server.Client()), t.TempDir())
	manager := NewManager(service, nil, staticTokenSource{}, time.Hour, 400*time.Millisecond)

	base, err := manager.EnsureBaseReady(context.Background(), "user_1", "Project Tracker")
	if err != nil {
		t.Fatalf("EnsureBaseReady() returned error: %v", err)
	}

	dbPath := service.DatabasePath(base.ID)
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("expected DuckDB file to exist: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(dbPath); os.IsNotExist(err) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("expected DuckDB file %s to be removed after TTL expiry", dbPath)
}

func TestManagerEnsureBaseReadyRefreshesExistingSnapshotBeforeReturning(t *testing.T) {
	releaseRecords := make(chan struct{})
	var recordCalls atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v0/meta/bases":
			writeManagerJSON(t, w, map[string]any{
				"bases": []map[string]any{{"id": "appProjects", "name": "Project Tracker", "permissionLevel": "create"}},
			})
		case "/v0/meta/bases/appProjects/tables":
			writeManagerJSON(t, w, map[string]any{
				"tables": []map[string]any{
					{
						"id":   "tblProjects",
						"name": "Projects",
						"fields": []map[string]any{
							{"id": "fldName", "name": "Name", "type": "singleLineText"},
						},
					},
				},
			})
		case "/v0/appProjects/tblProjects":
			recordCalls.Add(1)
			<-releaseRecords
			writeManagerJSON(t, w, map[string]any{
				"records": []map[string]any{
					{"id": "rec1", "createdTime": "2026-04-01T12:00:00Z", "fields": map[string]any{"Name": "Fresh Snapshot"}},
				},
			})
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	defer server.Close()

	service := NewService(NewHTTPClient(server.URL, server.Client()), t.TempDir())
	if err := duckdb.WriteSnapshot(context.Background(), service.DatabasePath("appProjects"), duckdb.BaseSnapshot{
		BaseID:   "appProjects",
		BaseName: "Project Tracker",
		SyncedAt: time.Date(2026, 4, 1, 11, 0, 0, 0, time.UTC),
		Tables: []duckdb.TableSnapshot{
			{
				AirtableTableID: "tblProjects",
				OriginalName:    "Projects",
				DuckDBTableName: "projects",
				Fields: []duckdb.FieldSnapshot{
					{
						AirtableFieldID:   "fldName",
						OriginalFieldName: "Name",
						DuckDBColumnName:  "name",
						AirtableFieldType: "singleLineText",
						DuckDBType:        "VARCHAR",
					},
				},
				Records: []duckdb.RecordSnapshot{
					{ID: "rec1", CreatedTime: time.Date(2026, 4, 1, 11, 0, 0, 0, time.UTC), Fields: map[string]any{"Name": "Stale Snapshot"}},
				},
			},
		},
	}); err != nil {
		t.Fatalf("WriteSnapshot() returned error: %v", err)
	}

	manager := NewManager(service, nil, staticTokenSource{}, time.Hour, time.Minute)
	go func() {
		time.Sleep(150 * time.Millisecond)
		close(releaseRecords)
	}()

	started := time.Now()
	if _, err := manager.EnsureBaseReady(context.Background(), "user_1", "Project Tracker"); err != nil {
		t.Fatalf("EnsureBaseReady() returned error: %v", err)
	}
	if recordCalls.Load() != 1 {
		t.Fatalf("expected initial refresh call, got %d record fetches", recordCalls.Load())
	}
	if time.Since(started) < 100*time.Millisecond {
		t.Fatal("expected EnsureBaseReady() to wait for the initial refresh of an existing snapshot")
	}

	result, err := service.QueryBase(context.Background(), "token", "appProjects", `SELECT name FROM projects`)
	if err != nil {
		t.Fatalf("QueryBase() returned error: %v", err)
	}
	if result.RowCount != 1 || result.Rows[0][0] != "Fresh Snapshot" {
		t.Fatalf("expected refreshed snapshot after EnsureBaseReady(), got %#v", result.Rows)
	}
}

func TestManagerEnsureBaseReadyFallsBackToExistingSnapshotWhenRefreshFails(t *testing.T) {
	var schemaCalls atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v0/meta/bases":
			writeManagerJSON(t, w, map[string]any{
				"bases": []map[string]any{{"id": "appProjects", "name": "Project Tracker", "permissionLevel": "create"}},
			})
		case "/v0/meta/bases/appProjects/tables":
			schemaCalls.Add(1)
			http.Error(w, `{"error":"schema unavailable"}`, http.StatusInternalServerError)
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	defer server.Close()

	service := NewService(NewHTTPClient(server.URL, server.Client()), t.TempDir())
	if err := duckdb.WriteSnapshot(context.Background(), service.DatabasePath("appProjects"), duckdb.BaseSnapshot{
		BaseID:   "appProjects",
		BaseName: "Project Tracker",
		SyncedAt: time.Date(2026, 4, 1, 11, 0, 0, 0, time.UTC),
		Tables: []duckdb.TableSnapshot{
			{
				AirtableTableID: "tblProjects",
				OriginalName:    "Projects",
				DuckDBTableName: "projects",
				Fields: []duckdb.FieldSnapshot{
					{
						AirtableFieldID:   "fldName",
						OriginalFieldName: "Name",
						DuckDBColumnName:  "name",
						AirtableFieldType: "singleLineText",
						DuckDBType:        "VARCHAR",
					},
				},
				Records: []duckdb.RecordSnapshot{
					{ID: "rec1", CreatedTime: time.Date(2026, 4, 1, 11, 0, 0, 0, time.UTC), Fields: map[string]any{"Name": "Stale But Available"}},
				},
			},
		},
	}); err != nil {
		t.Fatalf("WriteSnapshot() returned error: %v", err)
	}

	manager := NewManager(service, nil, staticTokenSource{}, time.Hour, time.Minute)
	if _, err := manager.EnsureBaseReady(context.Background(), "user_1", "Project Tracker"); err != nil {
		t.Fatalf("expected existing snapshot fallback on failed refresh, got error: %v", err)
	}
	if schemaCalls.Load() != 1 {
		t.Fatalf("expected one attempted refresh before fallback, got %d schema calls", schemaCalls.Load())
	}

	result, err := service.QueryBase(context.Background(), "token", "appProjects", `SELECT name FROM projects`)
	if err != nil {
		t.Fatalf("QueryBase() returned error: %v", err)
	}
	if result.RowCount != 1 || result.Rows[0][0] != "Stale But Available" {
		t.Fatalf("expected existing snapshot to remain queryable, got %#v", result.Rows)
	}
}

func TestManagerContinuouslyResyncsWhileActive(t *testing.T) {
	var generation atomic.Int32
	var recordCalls atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v0/meta/bases":
			writeManagerJSON(t, w, map[string]any{
				"bases": []map[string]any{{"id": "appProjects", "name": "Project Tracker", "permissionLevel": "create"}},
			})
		case "/v0/meta/bases/appProjects/tables":
			writeManagerJSON(t, w, map[string]any{
				"tables": []map[string]any{
					{
						"id":   "tblProjects",
						"name": "Projects",
						"fields": []map[string]any{
							{"id": "fldName", "name": "Name", "type": "singleLineText"},
						},
					},
				},
			})
		case "/v0/appProjects/tblProjects":
			recordCalls.Add(1)
			name := "Initial Snapshot"
			if generation.Load() > 0 {
				name = "Updated Snapshot"
			}
			writeManagerJSON(t, w, map[string]any{
				"records": []map[string]any{
					{"id": "rec1", "createdTime": "2026-04-01T12:00:00Z", "fields": map[string]any{"Name": name}},
				},
			})
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	defer server.Close()

	service := NewService(NewHTTPClient(server.URL, server.Client()), t.TempDir())
	manager := NewManager(service, nil, staticTokenSource{}, 120*time.Millisecond, 1500*time.Millisecond)

	if _, err := manager.EnsureBaseReady(context.Background(), "user_1", "Project Tracker"); err != nil {
		t.Fatalf("EnsureBaseReady() returned error: %v", err)
	}

	generation.Store(1)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		result, err := service.QueryBase(context.Background(), "token", "appProjects", `SELECT name FROM projects`)
		if err != nil {
			t.Fatalf("QueryBase() returned error: %v", err)
		}
		if result.RowCount == 1 && result.Rows[0][0] == "Updated Snapshot" {
			if recordCalls.Load() < 2 {
				t.Fatalf("expected at least two record fetches for continuous resync, got %d", recordCalls.Load())
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("expected continuous resync to refresh snapshot while active; record calls=%d", recordCalls.Load())
}

func writeManagerJSON(t *testing.T, w http.ResponseWriter, payload any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		t.Fatalf("json.NewEncoder().Encode() returned error: %v", err)
	}
}
