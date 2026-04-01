package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"

	"github.com/hackclub/better-airtable-mcp/internal/db"
	"github.com/hackclub/better-airtable-mcp/internal/duckdb"
)

func TestManagerRestoreActiveWorkersResumesBackgroundSync(t *testing.T) {
	port := syncManagerFreePort(t)
	postgres := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Port(uint32(port)).
			Database("better_airtable_sync_restore_test").
			Username("postgres").
			Password("postgres").
			BinariesPath(filepath.Join(t.TempDir(), "postgres-binaries")).
			DataPath(filepath.Join(t.TempDir(), "postgres-data")).
			RuntimePath(filepath.Join(t.TempDir(), "postgres-runtime")),
	)
	if err := postgres.Start(); err != nil {
		t.Fatalf("embedded postgres start failed: %v", err)
	}
	defer postgres.Stop()

	store, err := db.Open(context.Background(), fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/better_airtable_sync_restore_test?sslmode=disable", port))
	if err != nil {
		t.Fatalf("db.Open() returned error: %v", err)
	}
	defer store.Close()
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("store.Migrate() returned error: %v", err)
	}
	if err := store.UpsertUser(context.Background(), db.User{ID: "user_1"}); err != nil {
		t.Fatalf("store.UpsertUser() returned error: %v", err)
	}

	var generation atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v0/meta/bases":
			writeRestoreJSON(t, w, map[string]any{
				"bases": []map[string]any{{"id": "appProjects", "name": "Project Tracker", "permissionLevel": "create"}},
			})
		case "/v0/meta/bases/appProjects/tables":
			writeRestoreJSON(t, w, map[string]any{
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
			name := "Before Restart"
			if generation.Load() > 0 {
				name = "After Restore"
			}
			writeRestoreJSON(t, w, map[string]any{
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
	lastSyncedAt := time.Now().Add(-time.Second).UTC()
	syncDurationMS := int64(50)
	totalRecords := int64(1)
	totalTables := 1
	activeUntil := time.Now().Add(2 * time.Minute).UTC()
	if err := duckdb.WriteSnapshot(context.Background(), service.DatabasePath("appProjects"), duckdb.BaseSnapshot{
		BaseID:       "appProjects",
		BaseName:     "Project Tracker",
		SyncedAt:     lastSyncedAt,
		SyncDuration: 50 * time.Millisecond,
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
					{ID: "rec1", CreatedTime: lastSyncedAt, Fields: map[string]any{"Name": "Before Restart"}},
				},
			},
		},
	}); err != nil {
		t.Fatalf("WriteSnapshot() returned error: %v", err)
	}
	if err := store.PutSyncState(context.Background(), db.SyncState{
		BaseID:             "appProjects",
		LastSyncedAt:       &lastSyncedAt,
		LastSyncDurationMS: &syncDurationMS,
		TotalRecords:       &totalRecords,
		TotalTables:        &totalTables,
		ActiveUntil:        &activeUntil,
		SyncTokenUserID:    ptrString("user_1"),
	}); err != nil {
		t.Fatalf("PutSyncState() returned error: %v", err)
	}

	generation.Store(1)
	manager := NewManager(service, store, staticTokenSource{}, 150*time.Millisecond, 2*time.Minute)
	if err := manager.RestoreActiveWorkers(context.Background()); err != nil {
		t.Fatalf("RestoreActiveWorkers() returned error: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		result, err := service.QueryBase(context.Background(), "token", "appProjects", `SELECT name FROM projects`)
		if err != nil {
			t.Fatalf("QueryBase() returned error: %v", err)
		}
		if result.RowCount == 1 && result.Rows[0][0] == "After Restore" {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}

	t.Fatal("expected restored worker to refresh the snapshot in the background")
}

func TestManagerSweepStaleDuckDBFilesRemovesExpiredAndOrphanedFiles(t *testing.T) {
	port := syncManagerFreePort(t)
	postgres := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Port(uint32(port)).
			Database("better_airtable_sync_sweep_test").
			Username("postgres").
			Password("postgres").
			BinariesPath(filepath.Join(t.TempDir(), "postgres-binaries")).
			DataPath(filepath.Join(t.TempDir(), "postgres-data")).
			RuntimePath(filepath.Join(t.TempDir(), "postgres-runtime")),
	)
	if err := postgres.Start(); err != nil {
		t.Fatalf("embedded postgres start failed: %v", err)
	}
	defer postgres.Stop()

	store, err := db.Open(context.Background(), fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/better_airtable_sync_sweep_test?sslmode=disable", port))
	if err != nil {
		t.Fatalf("db.Open() returned error: %v", err)
	}
	defer store.Close()
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("store.Migrate() returned error: %v", err)
	}
	if err := store.UpsertUser(context.Background(), db.User{ID: "user_1"}); err != nil {
		t.Fatalf("store.UpsertUser() returned error: %v", err)
	}

	service := NewService(NewHTTPClient("", nil), t.TempDir())

	activeUntil := time.Now().Add(5 * time.Minute).UTC()
	expiredUntil := time.Now().Add(-5 * time.Minute).UTC()
	if err := store.PutSyncState(context.Background(), db.SyncState{
		BaseID:          "appActive",
		ActiveUntil:     &activeUntil,
		SyncTokenUserID: ptrString("user_1"),
	}); err != nil {
		t.Fatalf("PutSyncState(appActive) returned error: %v", err)
	}
	if err := store.PutSyncState(context.Background(), db.SyncState{
		BaseID:          "appExpired",
		ActiveUntil:     &expiredUntil,
		SyncTokenUserID: ptrString("user_1"),
	}); err != nil {
		t.Fatalf("PutSyncState(appExpired) returned error: %v", err)
	}

	for _, baseID := range []string{"appActive", "appExpired", "appOrphan"} {
		if err := duckdb.WriteSnapshot(context.Background(), service.DatabasePath(baseID), duckdb.BaseSnapshot{
			BaseID:   baseID,
			BaseName: baseID,
			SyncedAt: time.Now().UTC(),
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
				},
			},
		}); err != nil {
			t.Fatalf("WriteSnapshot(%s) returned error: %v", baseID, err)
		}
	}

	manager := NewManager(service, store, staticTokenSource{}, time.Minute, time.Minute)
	if err := manager.SweepStaleDuckDBFiles(context.Background()); err != nil {
		t.Fatalf("SweepStaleDuckDBFiles() returned error: %v", err)
	}

	if _, err := os.Stat(service.DatabasePath("appActive")); err != nil {
		t.Fatalf("expected active DuckDB file to remain, got %v", err)
	}
	if _, err := os.Stat(service.DatabasePath("appExpired")); !os.IsNotExist(err) {
		t.Fatalf("expected expired DuckDB file to be removed, got %v", err)
	}
	if _, err := os.Stat(service.DatabasePath("appOrphan")); !os.IsNotExist(err) {
		t.Fatalf("expected orphan DuckDB file to be removed, got %v", err)
	}
}

func TestManagerSweepStaleDuckDBFilesIgnoresMissingDataDir(t *testing.T) {
	manager := NewManager(NewService(NewHTTPClient("", nil), filepath.Join(t.TempDir(), "missing")), nil, staticTokenSource{}, time.Minute, time.Minute)
	if err := manager.SweepStaleDuckDBFiles(context.Background()); err != nil {
		t.Fatalf("SweepStaleDuckDBFiles() returned error for missing dir: %v", err)
	}
}

func syncManagerFreePort(t *testing.T) int {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() returned error: %v", err)
	}
	defer listener.Close()

	return listener.Addr().(*net.TCPAddr).Port
}

func ptrString(value string) *string {
	return &value
}

func writeRestoreJSON(t *testing.T, w http.ResponseWriter, payload any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		t.Fatalf("json.NewEncoder().Encode() returned error: %v", err)
	}
}
