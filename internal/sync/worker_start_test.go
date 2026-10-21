package syncer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hackclub/better-airtable-mcp/internal/duckdb"
)

// failingSchemaClient lists one base and fails every sync at the schema
// request, so a worker never touches the cache file itself.
type failingSchemaClient struct{ hangingPageClient }

func (failingSchemaClient) ListBases(context.Context, string) ([]Base, error) {
	return []Base{{ID: "appRace", Name: "Race", PermissionLevel: "create"}}, nil
}

func (failingSchemaClient) GetBaseSchema(context.Context, string, string) ([]Table, error) {
	return nil, errors.New("schema unavailable")
}

// A new worker's goroutine must not see the zero activeUntil it has before
// activateBase records the caller. When it did, the worker judged itself
// expired at once, deleted the base's cache file and removed itself, leaving
// the caller waiting on a worker that would never run.
func TestActivatingABaseKeepsItsWorkerAndCacheFile(t *testing.T) {
	for i := 0; i < 300; i++ {
		service := NewService(failingSchemaClient{}, t.TempDir())
		if err := duckdb.WriteSnapshot(context.Background(), service.DatabasePath("appRace"), duckdb.BaseSnapshot{
			BaseID: "appRace", BaseName: "Race", SyncedAt: time.Now().UTC(),
		}); err != nil {
			t.Fatal(err)
		}
		manager := NewManager(service, nil, staticTokenSource{}, time.Hour, time.Hour)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := manager.EnsureBaseReadable(ctx, "user_1", "appRace")
		cancel()
		if err != nil {
			t.Fatalf("iteration %d: EnsureBaseReadable: %v", i, err)
		}

		time.Sleep(5 * time.Millisecond)
		manager.mu.Lock()
		_, alive := manager.workers["appRace"]
		manager.mu.Unlock()
		if !alive || !duckdb.DatabaseFileExists(service.DatabasePath("appRace")) {
			t.Fatalf("iteration %d: worker alive=%v, cache file exists=%v", i, alive, duckdb.DatabaseFileExists(service.DatabasePath("appRace")))
		}
	}
}
