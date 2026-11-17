package syncer

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/hackclub/better-airtable-mcp/internal/duckdb"
)

var simBase = Base{ID: "appSim", Name: "Simulated"}

var simTableNames = map[string]string{simProjects: "projects", simPrograms: "programs", simOrgs: "orgs"}

func newSimService(t *testing.T, sim *simAirtable) *Service {
	t.Helper()
	service := NewService(sim, t.TempDir())
	service.now = sim.now
	return service
}

func simFullSync(t *testing.T, service *Service) {
	t.Helper()
	if _, err := service.runSync(context.Background(), StaticAccessToken("token"), simBase, syncRunOptions{}, nil); err != nil {
		t.Fatalf("full sync: %v", err)
	}
}

func simDelta(t *testing.T, service *Service) DeltaResult {
	t.Helper()
	result, err := service.runDelta(context.Background(), StaticAccessToken("token"), simBase, nil)
	if err != nil {
		t.Fatalf("delta sync: %v", err)
	}
	if result.NeedsFull {
		t.Fatalf("delta sync unexpectedly needs a full sync: %s", result.Reason)
	}
	return result
}

// dumpSimCache reads every data table of a cache, ordered by record ID,
// leaving out the given record IDs.
func dumpSimCache(t *testing.T, service *Service, skip map[string]bool) map[string][][]any {
	t.Helper()
	dump := map[string][][]any{}
	for _, name := range simTableNames {
		result, err := duckdb.Query(context.Background(), service.basePath(simBase.ID), fmt.Sprintf(`SELECT * FROM %s ORDER BY id`, name))
		if err != nil {
			t.Fatalf("dump %s: %v", name, err)
		}
		rows := make([][]any, 0, len(result.Rows))
		for _, row := range result.Rows {
			if id, _ := row[0].(string); skip[id] {
				continue
			}
			rows = append(rows, row)
		}
		dump[name] = rows
	}
	return dump
}

// requireCacheMatchesAirtable compares the service's cache with a brand new
// full download of the simulated base.
func requireCacheMatchesAirtable(t *testing.T, service *Service, sim *simAirtable, skip map[string]bool, context string) {
	t.Helper()
	truthService := newSimService(t, sim)
	simFullSync(t, truthService)
	want := dumpSimCache(t, truthService, skip)
	got := dumpSimCache(t, service, skip)
	for _, name := range simTableNames {
		if reflect.DeepEqual(got[name], want[name]) {
			continue
		}
		t.Fatalf("%s: table %s differs from Airtable\n got: %v\nwant: %v", context, name, got[name], want[name])
	}
}

// seedSim builds one org, two programs and five projects, all linked.
func seedSim(sim *simAirtable) (org string, programs, projects []string) {
	org = sim.create(simOrgs, map[string]string{"Name": "HQ"}, nil)
	for _, name := range []string{"Arcade", "Summit"} {
		programs = append(programs, sim.create(simPrograms, map[string]string{"Name": name}, nil))
	}
	sim.setLinks(simOrgs, org, "Programs", programs)
	for i := 0; i < 5; i++ {
		project := sim.create(simProjects, map[string]string{"Name": fmt.Sprintf("Project %d", i)}, map[string]float64{"Hours": float64(i + 1)})
		sim.setLinks(simProjects, project, "Program", []string{programs[i%2]})
		projects = append(projects, project)
	}
	return org, programs, projects
}

func TestDeltaAppliesEditsLinksComputedValuesAndDeletes(t *testing.T) {
	sim := newSimAirtable()
	_, programs, projects := seedSim(sim)
	service := newSimService(t, sim)
	simFullSync(t, service)
	requireCacheMatchesAirtable(t, service, sim, nil, "after full sync")

	// Let the overlap window pass, so anything the delta picks up below it
	// picks up because it tracked the change, not because it re-read
	// everything recent.
	sim.advance(10 * time.Minute)
	simDelta(t, service)
	sim.advance(10 * time.Minute)

	sim.setNumber(simProjects, projects[0], "Hours", 40)       // moves a rollup, and a rollup of that rollup
	sim.setText(simPrograms, programs[1], "Name", "Summit II") // moves a lookup and a formula in other rows
	blank := sim.create(simProjects, nil, nil)                 // a row nobody has edited yet
	linked := sim.create(simProjects, map[string]string{"Name": "New"}, map[string]float64{"Hours": 7})
	sim.setLinks(simProjects, linked, "Program", []string{programs[0]})
	sim.setLinks(simProjects, projects[2], "Program", []string{programs[1]}) // relink
	if !sim.delete(simProjects, projects[4]) {
		t.Fatalf("expected the deleted project to have been linked")
	}

	_, pagesBefore := sim.counts()
	result := simDelta(t, service)
	_, pagesAfter := sim.counts()
	requireCacheMatchesAirtable(t, service, sim, nil, "after delta")

	if result.RecordsDeleted != 1 {
		t.Fatalf("RecordsDeleted = %d, want 1 (the linked project that was deleted)", result.RecordsDeleted)
	}
	if result.RecordsFollowed == 0 {
		t.Fatalf("expected the delta to follow computed dependencies")
	}
	if len(result.StaleTables) != 0 {
		t.Fatalf("unexpected stale tables: %v", result.StaleTables)
	}
	exists, err := duckdb.ExistingRecordIDs(context.Background(), service.basePath(simBase.ID), "projects", []string{blank})
	if err != nil || len(exists) != 1 {
		t.Fatalf("blank new record missing from cache (found %v, err %v)", exists, err)
	}
	t.Logf("delta used %d record requests", pagesAfter-pagesBefore)
}

func TestDeltaWithNothingChangedCostsOneRequestPerTable(t *testing.T) {
	sim := newSimAirtable()
	seedSim(sim)
	service := newSimService(t, sim)
	simFullSync(t, service)
	sim.advance(time.Hour)
	simDelta(t, service)
	sim.advance(time.Hour)

	schemaBefore, pagesBefore := sim.counts()
	result := simDelta(t, service)
	schemaAfter, pagesAfter := sim.counts()
	if got := pagesAfter - pagesBefore; got != 3 {
		t.Fatalf("idle delta made %d record requests, want 3 (one per table)", got)
	}
	if got := schemaAfter - schemaBefore; got != 1 {
		t.Fatalf("idle delta made %d schema requests, want 1", got)
	}
	if result.RecordsUpserted != 0 || result.RecordsDeleted != 0 {
		t.Fatalf("idle delta wrote %d rows and deleted %d", result.RecordsUpserted, result.RecordsDeleted)
	}
}

func TestDeltaRandomizedEditsMatchFullSync(t *testing.T) {
	// DELTA_FUZZ_SEEDS and DELTA_FUZZ_STEPS widen the run for a deep soak.
	seeds, steps := int64(6), 40
	if testing.Short() {
		seeds, steps = 3, 25
	}
	if value, err := strconv.ParseInt(os.Getenv("DELTA_FUZZ_SEEDS"), 10, 64); err == nil && value > 0 {
		seeds = value
	}
	if value, err := strconv.Atoi(os.Getenv("DELTA_FUZZ_STEPS")); err == nil && value > 0 {
		steps = value
	}
	for seed := int64(1); seed <= seeds; seed++ {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			t.Parallel()
			rng := rand.New(rand.NewSource(seed))
			sim := newSimAirtable()
			sim.pageSize = 2 + rng.Intn(4)
			seedSim(sim)
			service := newSimService(t, sim)
			simFullSync(t, service)
			// A delta sees a deletion through the records that linked to the
			// deleted one: they lose the link and count as modified. So a
			// deletion is only guaranteed to be noticed if, at the previous
			// sync, some record that is still alive linked to it. The rest
			// are left for a reconcile; ghosts lists those.
			ghosts := map[string]bool{}
			linkersAtSync := map[string][]string{}
			var deleted []string
			snapshotLinkers := func() {
				linkersAtSync = map[string][]string{}
				for tableID, fields := range map[string][]string{simProjects: {"Program"}, simPrograms: {"Projects", "Org"}, simOrgs: {"Programs"}} {
					for _, id := range sim.ids(tableID) {
						for _, field := range fields {
							linkersAtSync[id] = append(linkersAtSync[id], sim.linksOf(tableID, id, field)...)
						}
					}
				}
				deleted = nil
			}
			alive := func(id string) bool {
				for _, tableID := range []string{simProjects, simPrograms, simOrgs} {
					if containsString(sim.ids(tableID), id) {
						return true
					}
				}
				return false
			}
			settleGhosts := func() {
				for _, id := range deleted {
					noticed := false
					for _, linker := range linkersAtSync[id] {
						if alive(linker) {
							noticed = true
						}
					}
					if !noticed {
						ghosts[id] = true
					}
				}
			}
			snapshotLinkers()

			pick := func(tableID string) (string, bool) {
				ids := sim.ids(tableID)
				if len(ids) == 0 {
					return "", false
				}
				return ids[rng.Intn(len(ids))], true
			}
			for step := 0; step < steps; step++ {
				for op := 0; op < 1+rng.Intn(4); op++ {
					switch rng.Intn(12) {
					case 0:
						sim.create(simProjects, nil, nil)
					case 1:
						project := sim.create(simProjects, map[string]string{"Name": fmt.Sprintf("P%d-%d", step, op)}, map[string]float64{"Hours": float64(rng.Intn(20))})
						if program, ok := pick(simPrograms); ok && rng.Intn(3) > 0 {
							sim.setLinks(simProjects, project, "Program", []string{program})
						}
					case 2, 3:
						if project, ok := pick(simProjects); ok {
							sim.setNumber(simProjects, project, "Hours", float64(rng.Intn(50)))
						}
					case 4:
						if project, ok := pick(simProjects); ok {
							sim.setText(simProjects, project, "Name", fmt.Sprintf("renamed-%d", step))
						}
					case 5:
						if program, ok := pick(simPrograms); ok {
							sim.setText(simPrograms, program, "Name", fmt.Sprintf("program-%d", step))
						}
					case 6:
						if project, ok := pick(simProjects); ok {
							var targets []string
							if program, ok := pick(simPrograms); ok && rng.Intn(4) > 0 {
								targets = []string{program}
							}
							sim.setLinks(simProjects, project, "Program", targets)
						}
					case 7:
						if program, ok := pick(simPrograms); ok {
							var targets []string
							if org, ok := pick(simOrgs); ok && rng.Intn(4) > 0 {
								targets = []string{org}
							}
							sim.setLinks(simPrograms, program, "Org", targets)
						}
					case 8:
						if project, ok := pick(simProjects); ok {
							sim.delete(simProjects, project)
							deleted = append(deleted, project)
						}
					case 9:
						if rng.Intn(3) == 0 {
							if program, ok := pick(simPrograms); ok {
								sim.delete(simPrograms, program)
								deleted = append(deleted, program)
							}
						}
					case 10:
						sim.create(simPrograms, map[string]string{"Name": fmt.Sprintf("G%d", step)}, nil)
					case 11:
						if rng.Intn(4) == 0 {
							sim.create(simOrgs, map[string]string{"Name": fmt.Sprintf("O%d", step)}, nil)
						} else if org, ok := pick(simOrgs); ok {
							sim.setText(simOrgs, org, "Name", fmt.Sprintf("org-%d", step))
						}
					}
				}
				// Sometimes sync right away, sometimes long after the overlap
				// window has passed.
				sim.advance(time.Duration(1+rng.Intn(600)) * time.Second)
				// Now and then Airtable fails partway through a delta. The
				// run after it has to end up correct all the same.
				if rng.Intn(4) == 0 {
					sim.failSoon(1 + rng.Intn(8))
					_, _ = service.runDelta(context.Background(), StaticAccessToken("token"), simBase, nil)
					sim.recover()
					sim.advance(time.Duration(1+rng.Intn(300)) * time.Second)
				}
				if result := simDelta(t, service); len(result.StaleTables) != 0 {
					t.Fatalf("step %d: unexpected stale tables %v", step, result.StaleTables)
				}
				settleGhosts()
				requireCacheMatchesAirtable(t, service, sim, ghosts, fmt.Sprintf("step %d", step))
				snapshotLinkers()
			}

			// A full reconcile clears whatever only it can see.
			simFullSync(t, service)
			requireCacheMatchesAirtable(t, service, sim, nil, "after reconcile")
		})
	}
}

func TestDeltaLeavesUnlinkedDeletionForReconcile(t *testing.T) {
	sim := newSimAirtable()
	seedSim(sim)
	orphan := sim.create(simProjects, map[string]string{"Name": "Orphan"}, nil)
	service := newSimService(t, sim)
	simFullSync(t, service)
	sim.advance(time.Hour)

	if sim.delete(simProjects, orphan) {
		t.Fatalf("orphan should not have been linked")
	}
	simDelta(t, service)
	found, err := duckdb.ExistingRecordIDs(context.Background(), service.basePath(simBase.ID), "projects", []string{orphan})
	if err != nil {
		t.Fatalf("look up orphan: %v", err)
	}
	if len(found) != 1 {
		t.Fatalf("a delta cannot see an unlinked deletion; the row should still be cached until a reconcile")
	}

	simFullSync(t, service)
	requireCacheMatchesAirtable(t, service, sim, nil, "after reconcile")
}

func TestDeltaAsksForFullSyncWhenItCannotPatch(t *testing.T) {
	sim := newSimAirtable()
	seedSim(sim)
	service := newSimService(t, sim)

	result, err := service.runDelta(context.Background(), StaticAccessToken("token"), simBase, nil)
	if err != nil || !result.NeedsFull {
		t.Fatalf("delta on a missing cache = %+v, %v; want NeedsFull", result, err)
	}

	simFullSync(t, service)
	sim.advance(time.Hour)
	sim.mu.Lock()
	sim.extraField = true
	sim.mu.Unlock()
	result, err = service.runDelta(context.Background(), StaticAccessToken("token"), simBase, nil)
	if err != nil || !result.NeedsFull || result.Reason != "airtable schema changed" {
		t.Fatalf("delta after a schema change = %+v, %v; want NeedsFull for a schema change", result, err)
	}

	simFullSync(t, service)
	sim.advance(time.Hour)
	simDelta(t, service)
}

func TestDeltaFlagsTablesWhenFollowingComputedValuesIsTooExpensive(t *testing.T) {
	sim := newSimAirtable()
	_, programs, _ := seedSim(sim)
	service := newSimService(t, sim)
	simFullSync(t, service)
	sim.advance(time.Hour)
	simDelta(t, service) // move past the first delta, which re-reads everything recent
	sim.advance(time.Hour)
	service.deltaBudget = 1

	// Renaming a program changes a lookup in every project linked to it:
	// more rows than the budget of one allows.
	sim.setText(simPrograms, programs[0], "Name", "Renamed")
	result := simDelta(t, service)
	if !reflect.DeepEqual(result.StaleTables, []string{"projects"}) {
		t.Fatalf("StaleTables = %v, want [projects]", result.StaleTables)
	}
	state, err := duckdb.ReadCacheState(context.Background(), service.basePath(simBase.ID))
	if err != nil {
		t.Fatalf("read cache state: %v", err)
	}
	for _, table := range state.Tables {
		if want := table.TableName == "projects"; table.ComputedStale != want {
			t.Fatalf("computed_stale for %s = %v, want %v", table.TableName, table.ComputedStale, want)
		}
	}

	simFullSync(t, service)
	state, err = duckdb.ReadCacheState(context.Background(), service.basePath(simBase.ID))
	if err != nil {
		t.Fatalf("read cache state: %v", err)
	}
	for _, table := range state.Tables {
		if table.ComputedStale {
			t.Fatalf("reconcile left %s flagged stale", table.TableName)
		}
	}
	requireCacheMatchesAirtable(t, service, sim, nil, "after reconcile")
}

func TestRefreshRecordsWritesAMutationThrough(t *testing.T) {
	sim := newSimAirtable()
	_, programs, projects := seedSim(sim)
	service := newSimService(t, sim)
	simFullSync(t, service)
	sim.advance(time.Hour)
	before, err := duckdb.ReadCacheState(context.Background(), service.basePath(simBase.ID))
	if err != nil {
		t.Fatalf("read cache state: %v", err)
	}

	// What an approved mutation does in Airtable: an update, a create and a
	// delete in one table.
	sim.setNumber(simProjects, projects[1], "Hours", 99)
	created := sim.create(simProjects, map[string]string{"Name": "Via MCP"}, map[string]float64{"Hours": 3})
	sim.setLinks(simProjects, created, "Program", []string{programs[0]})
	sim.delete(simProjects, projects[3])

	_, pagesBefore := sim.counts()
	err = service.RefreshRecords(context.Background(), StaticAccessToken("token"), simBase, simProjects, []string{projects[1], created}, []string{projects[3]})
	if err != nil {
		t.Fatalf("RefreshRecords: %v", err)
	}
	_, pagesAfter := sim.counts()
	requireCacheMatchesAirtable(t, service, sim, nil, "after write-through")
	if got := pagesAfter - pagesBefore; got > 6 {
		t.Fatalf("write-through made %d record requests, want a handful", got)
	}

	after, err := duckdb.ReadCacheState(context.Background(), service.basePath(simBase.ID))
	if err != nil {
		t.Fatalf("read cache state: %v", err)
	}
	for i, table := range after.Tables {
		if !table.Watermark.Equal(before.Tables[i].Watermark) {
			t.Fatalf("write-through moved the watermark of %s", table.TableName)
		}
	}
}

// A delta that dies after writing a changed row must not lose the rows that
// compute from it: the next run compares that row with the cache, finds it
// equal, and would see nothing to follow.
func TestDeltaFailureDoesNotLoseComputedFollowUps(t *testing.T) {
	sim := newSimAirtable()
	_, programs, _ := seedSim(sim)
	service := newSimService(t, sim)
	simFullSync(t, service)
	sim.advance(time.Hour)
	simDelta(t, service)
	sim.advance(time.Hour)

	// The rename is written, then the run fails on another table before the
	// projects that look the name up are refetched.
	sim.setText(simPrograms, programs[0], "Name", "Renamed")
	sim.mu.Lock()
	sim.failTable = simOrgs
	sim.mu.Unlock()
	if _, err := service.runDelta(context.Background(), StaticAccessToken("token"), simBase, nil); err == nil {
		t.Fatalf("expected the delta to fail")
	}
	result, err := duckdb.Query(context.Background(), service.basePath(simBase.ID), `SELECT name FROM programs WHERE id = '`+programs[0]+`'`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if result.Rows[0][0] != "Renamed" {
		t.Skipf("the failing run did not write the rename (table order), nothing to prove here")
	}

	sim.recover()
	sim.advance(10 * time.Minute)
	simDelta(t, service)
	requireCacheMatchesAirtable(t, service, sim, nil, "after the retry")
}

// A full download reads its tables one after another. An edit that lands in
// between can leave a row cached with its new value while a row computed
// from it was read before the edit; the next delta has to repair that even
// though the edited row already matches the cache.
func TestDeltaRepairsEditsThatLandDuringAFullDownload(t *testing.T) {
	for _, editAfter := range []string{simProjects, simPrograms, simOrgs} {
		t.Run(editAfter, func(t *testing.T) {
			sim := newSimAirtable()
			sim.pageSize = 100
			_, programs, projects := seedSim(sim)
			sim.advance(time.Hour)
			service := newSimService(t, sim)

			edited := false
			sim.mu.Lock()
			sim.afterPage = func(tableID string) {
				if tableID != editAfter || edited {
					return
				}
				edited = true
				sim.setText(simPrograms, programs[0], "Name", "Renamed mid-download")
				sim.setNumber(simProjects, projects[0], "Hours", 500)
			}
			sim.mu.Unlock()
			simFullSync(t, service)
			sim.mu.Lock()
			sim.afterPage = nil
			sim.mu.Unlock()

			sim.advance(30 * time.Minute)
			simDelta(t, service)
			requireCacheMatchesAirtable(t, service, sim, nil, "after the first delta")
		})
	}
}

func TestFetchByIDsRefusesMalformedRecordIDs(t *testing.T) {
	sim := newSimAirtable()
	seedSim(sim)
	service := newSimService(t, sim)
	simFullSync(t, service)

	err := service.RefreshRecords(context.Background(), StaticAccessToken("token"), simBase, simProjects, []string{`rec1')),TRUE(`}, nil)
	if err == nil {
		t.Fatalf("expected a malformed record id to be refused")
	}
}
