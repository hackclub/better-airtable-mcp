package syncer

import (
	"context"
	"os"
	"testing"
	"time"
)

func waitForCondition(t *testing.T, timeout time.Duration, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func managerWorkerCount(manager *Manager) int {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	return len(manager.workers)
}

// The reason delta sync exists: a base that went idle must answer the next
// read from its cache straight away and catch up with a handful of requests,
// not download every record again.
func TestManagerReusesCacheAfterIdleAndOnlyFetchesChanges(t *testing.T) {
	sim := newSimAirtable()
	_, _, projects := seedSim(sim)
	for i := 0; i < 40; i++ {
		sim.create(simProjects, map[string]string{"Name": "bulk"}, map[string]float64{"Hours": 1})
	}
	// The records are older than the window every delta re-reads.
	sim.advance(time.Hour)
	service := newSimService(t, sim)
	manager := NewManager(service, nil, staticTokenSource{}, time.Hour, 300*time.Millisecond)
	manager.SetReconcileInterval(0)

	base, err := manager.EnsureBaseReady(context.Background(), "user_1", "appSim")
	if err != nil {
		t.Fatalf("EnsureBaseReady: %v", err)
	}
	_, fullSyncPages := sim.counts()

	waitForCondition(t, 5*time.Second, "the idle worker to retire", func() bool { return managerWorkerCount(manager) == 0 })
	if _, err := os.Stat(service.DatabasePath(base.ID)); err != nil {
		t.Fatalf("cache file should survive idling: %v", err)
	}

	// Time passes and somebody edits a record in Airtable.
	sim.advance(3 * time.Hour)
	sim.setNumber(simProjects, projects[0], "Hours", 77)

	if _, err := manager.EnsureBaseReadable(context.Background(), "user_1", "appSim"); err != nil {
		t.Fatalf("EnsureBaseReadable: %v", err)
	}
	result, err := service.QueryResolvedBase(context.Background(), base.ID, `SELECT hours FROM projects WHERE id = '`+projects[0]+`'`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0] != 77.0 {
		t.Fatalf("read after idle returned %v, want the edited value 77", result.Rows)
	}

	_, pagesNow := sim.counts()
	if delta := pagesNow - fullSyncPages; delta >= fullSyncPages/2 {
		t.Fatalf("catching up after idle made %d record requests; the first full download made %d", delta, fullSyncPages)
	}
	freshness, ok := manager.Freshness(context.Background(), base.ID)
	if !ok || freshness.State != FreshnessFresh {
		t.Fatalf("freshness = %+v (ok %v), want fresh", freshness, ok)
	}
}

func TestManagerRunsReconcileWhenBaseGoesIdle(t *testing.T) {
	sim := newSimAirtable()
	seedSim(sim)
	orphan := sim.create(simProjects, map[string]string{"Name": "Orphan"}, nil)
	service := newSimService(t, sim)
	manager := NewManager(service, nil, staticTokenSource{}, time.Hour, 300*time.Millisecond)
	manager.SetReconcileInterval(time.Hour)

	base, err := manager.EnsureBaseReady(context.Background(), "user_1", "appSim")
	if err != nil {
		t.Fatalf("EnsureBaseReady: %v", err)
	}
	waitForCondition(t, 5*time.Second, "the idle worker to retire", func() bool { return managerWorkerCount(manager) == 0 })

	// An unlinked record is deleted: only a full fetch can notice.
	sim.advance(2 * time.Hour)
	sim.delete(simProjects, orphan)

	// Using the base again makes the reconcile due; it runs once the base
	// goes idle, and the deleted row is gone afterwards.
	if _, err := manager.EnsureBaseReadable(context.Background(), "user_1", "appSim"); err != nil {
		t.Fatalf("EnsureBaseReadable: %v", err)
	}
	waitForCondition(t, 8*time.Second, "the reconcile to remove the deleted row", func() bool {
		if managerWorkerCount(manager) != 0 {
			return false
		}
		result, err := service.QueryResolvedBase(context.Background(), base.ID, `SELECT count(*) FROM projects WHERE id = '`+orphan+`'`)
		return err == nil && result.Rows[0][0] == int64(0)
	})
}

func TestManagerFullSyncRequestDownloadsEverythingAgain(t *testing.T) {
	sim := newSimAirtable()
	seedSim(sim)
	orphan := sim.create(simProjects, map[string]string{"Name": "Orphan"}, nil)
	service := newSimService(t, sim)
	manager := NewManager(service, nil, staticTokenSource{}, time.Hour, time.Minute)
	manager.SetReconcileInterval(0)

	base, err := manager.EnsureBaseReady(context.Background(), "user_1", "appSim")
	if err != nil {
		t.Fatalf("EnsureBaseReady: %v", err)
	}
	sim.advance(time.Hour)
	sim.delete(simProjects, orphan)

	if _, err := manager.RequestFullSync(context.Background(), "user_1", "appSim"); err != nil {
		t.Fatalf("RequestFullSync: %v", err)
	}
	waitForCondition(t, 8*time.Second, "the full sync to remove the deleted row", func() bool {
		result, err := service.QueryResolvedBase(context.Background(), base.ID, `SELECT count(*) FROM projects WHERE id = '`+orphan+`'`)
		return err == nil && result.Rows[0][0] == int64(0)
	})
}
