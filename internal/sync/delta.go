package syncer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/hackclub/better-airtable-mcp/internal/duckdb"
	"github.com/hackclub/better-airtable-mcp/internal/logx"
)

const (
	// deltaOverlap is how far behind a table's watermark each delta asks
	// again. Upserts are idempotent, so re-reading a short window is free and
	// covers clock skew and computed values that settle a moment after an
	// edit.
	deltaOverlap = 2 * time.Minute

	// deltaFlushRecords bounds how many fetched records are held in memory
	// before they are written.
	deltaFlushRecords = 2000

	// deltaPropagationBudget caps how many rows one delta refetches only
	// because a row they compute from changed. Past it the affected tables
	// are flagged computed_stale and wait for the next full reconcile.
	deltaPropagationBudget = 5000

	// deltaMaxRounds bounds how many hops of computed dependencies one delta
	// follows.
	deltaMaxRounds = 8

	maxParallelDeltaTables = 5
)

// DeltaResult describes one delta sync run.
type DeltaResult struct {
	BaseID   string
	BaseName string

	// NeedsFull means the cache cannot be brought up to date by a delta and
	// a full sync has to run instead. Reason says why.
	NeedsFull bool
	Reason    string

	CheckedAt       time.Time
	TablesChecked   int
	TotalRecords    int64
	RecordsUpserted int
	RecordsDeleted  int
	RecordsFollowed int
	PagesFetched    int64
	StaleTables     []string
	Duration        time.Duration
}

// readCacheState reads the active file's sync position. ok is false when the
// base has no cache file yet.
func (s *Service) readCacheState(ctx context.Context, baseID string) (state duckdb.CacheState, ok bool, err error) {
	unlock, err := s.lockDuckRead(ctx, baseID)
	if err != nil {
		return duckdb.CacheState{}, false, err
	}
	defer unlock()
	if !duckdb.DatabaseFileExists(s.basePath(baseID)) {
		return duckdb.CacheState{}, false, nil
	}
	state, err = duckdb.ReadCacheState(ctx, s.basePath(baseID))
	if err != nil {
		return duckdb.CacheState{}, false, err
	}
	return state, true, nil
}

// deltaBlocker reports why a cache file cannot take a delta, or "" if it can.
func deltaBlocker(state duckdb.CacheState, plans []syncTablePlan, hash string) string {
	if state.CacheFormat != duckdb.CacheFormat {
		return "cache file predates delta sync"
	}
	if state.Info.Status != "completed" || state.Info.LastSyncedAt.IsZero() {
		return "cache has no complete snapshot"
	}
	if state.SchemaHash != hash {
		return "airtable schema changed"
	}
	positions := make(map[string]duckdb.TableDeltaState, len(state.Tables))
	for _, table := range state.Tables {
		positions[table.TableName] = table
	}
	for _, plan := range plans {
		position, ok := positions[plan.Table.DuckDBTableName]
		if !ok || position.Watermark.IsZero() {
			return "table " + plan.Table.DuckDBTableName + " has no sync position"
		}
	}
	return ""
}

type deltaRun struct {
	service *Service
	ctx     context.Context
	tokens  AccessTokenFunc
	base    Base
	touch   func()

	plans      map[string]syncTablePlan // by Airtable table ID
	positions  map[string]duckdb.TableDeltaState
	graph      schemaGraph
	tableOrder []string

	mu       sync.Mutex
	written  map[string]map[string]struct{} // table ID -> record IDs written this run
	changed  map[computedEdge][]string      // edge -> source record IDs whose watched value changed
	linkEnds map[string][]string            // table ID -> record IDs at the other end of a link that changed
	stale    map[string]struct{}            // table IDs whose computed columns may lag
	budget   int
	result   DeltaResult
	lastPage time.Time
}

func (s *Service) newDeltaRun(ctx context.Context, tokens AccessTokenFunc, base Base, tables []Table, plans []syncTablePlan, state duckdb.CacheState, touch func()) *deltaRun {
	run := &deltaRun{
		service:   s,
		ctx:       ctx,
		tokens:    tokens,
		base:      base,
		touch:     touch,
		plans:     make(map[string]syncTablePlan, len(plans)),
		positions: make(map[string]duckdb.TableDeltaState, len(state.Tables)),
		graph:     buildSchemaGraph(tables, plans),
		written:   map[string]map[string]struct{}{},
		changed:   map[computedEdge][]string{},
		linkEnds:  map[string][]string{},
		stale:     map[string]struct{}{},
		budget:    s.propagationBudget(),
		result:    DeltaResult{BaseID: base.ID, BaseName: base.Name},
	}
	for _, plan := range plans {
		run.plans[plan.Table.AirtableTableID] = plan
		run.tableOrder = append(run.tableOrder, plan.Table.AirtableTableID)
	}
	for _, table := range state.Tables {
		run.positions[table.TableName] = table
	}
	return run
}

// watermarkAdvance asks apply to move a table's delta watermark.
type watermarkAdvance struct {
	basedOn   time.Time
	to        time.Time
	checkedAt time.Time
}

// apply writes one batch of fetched records (and deletions) for a table and
// collects what the write says changed.
func (r *deltaRun) apply(tableID string, records []Record, deleteIDs []string, advance *watermarkAdvance) error {
	plan, ok := r.plans[tableID]
	if !ok {
		return fmt.Errorf("delta for unknown table %s", tableID)
	}

	// A paged, filtered listing can return a record twice when it changes
	// mid-iteration; the later copy wins.
	index := make(map[string]int, len(records))
	snapshots := make([]duckdb.RecordSnapshot, 0, len(records))
	for _, record := range records {
		snapshot := duckdb.RecordSnapshot{ID: record.ID, CreatedTime: record.CreatedTime, Fields: record.Fields}
		if at, seen := index[record.ID]; seen {
			snapshots[at] = snapshot
			continue
		}
		index[record.ID] = len(snapshots)
		snapshots = append(snapshots, snapshot)
	}

	columns, watchAny := r.graph.watchFor(tableID)
	table := duckdb.DeltaTable{
		Table: duckdb.TableSnapshot{
			AirtableTableID: plan.Table.AirtableTableID,
			OriginalName:    plan.Table.OriginalName,
			DuckDBTableName: plan.Table.DuckDBTableName,
			Fields:          plan.Table.Fields,
			Records:         snapshots,
		},
		DeleteIDs:    deleteIDs,
		WatchColumns: columns,
		WatchAny:     watchAny,
		LinkColumns:  r.graph.linkColumnNames(tableID),
	}
	if advance != nil {
		table.AdvanceWatermark = true
		table.BasedOnWatermark = advance.basedOn
		table.NewWatermark = advance.to
		table.CheckedAt = advance.checkedAt
	}

	unlock, err := r.service.lockDuckWrite(r.ctx, r.base.ID)
	if err != nil {
		return err
	}
	results, err := duckdb.ApplyDelta(r.ctx, r.service.basePath(r.base.ID), []duckdb.DeltaTable{table}, time.Time{})
	unlock()
	if err != nil {
		return err
	}
	written := results[0]

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.written[tableID] == nil {
		r.written[tableID] = map[string]struct{}{}
	}
	for _, snapshot := range snapshots {
		r.written[tableID][snapshot.ID] = struct{}{}
	}
	for _, id := range deleteIDs {
		r.written[tableID][id] = struct{}{}
	}
	for _, edge := range r.graph.edgesBySource[tableID] {
		ids := written.ChangedAny
		if edge.SourceColumn != "" {
			ids = written.ChangedByColumn[edge.SourceColumn]
		}
		if len(ids) > 0 {
			r.changed[edge] = append(r.changed[edge], ids...)
		}
	}
	for _, links := range []map[string][]string{written.DroppedLinks, written.AddedLinks} {
		for column, ids := range links {
			if target := r.graph.linkColumns[tableID][column]; target != "" {
				r.linkEnds[target] = append(r.linkEnds[target], ids...)
			}
		}
	}
	r.result.RecordsUpserted += written.Upserted
	r.result.RecordsDeleted += written.Deleted
	return nil
}

func (r *deltaRun) listPage(tableID string, options ListRecordsPageOptions) (ListRecordsPageResult, error) {
	page, err := withAccessToken(r.ctx, r.tokens, func(token string) (ListRecordsPageResult, error) {
		return r.service.client.ListRecordsPage(r.ctx, token, r.base.ID, tableID, options)
	})
	if err != nil {
		return ListRecordsPageResult{}, err
	}
	r.mu.Lock()
	r.result.PagesFetched++
	r.mu.Unlock()
	if r.touch != nil {
		r.touch()
	}
	return page, nil
}

// syncTable pulls everything created or edited in one table since its
// watermark and advances the watermark once the listing completes.
func (r *deltaRun) syncTable(tableID string) error {
	plan := r.plans[tableID]
	position := r.positions[plan.Table.DuckDBTableName]
	fetchStartedAt := r.service.now().UTC()

	options := ListRecordsPageOptions{ModifiedSince: position.Watermark.Add(-deltaOverlap)}
	var batch []Record
	for {
		page, err := r.listPage(tableID, options)
		if err != nil {
			return err
		}
		batch = append(batch, page.Records...)
		if page.Offset == "" {
			break
		}
		options.Offset = page.Offset
		if len(batch) >= deltaFlushRecords {
			if err := r.apply(tableID, batch, nil, nil); err != nil {
				return err
			}
			batch = nil
		}
	}
	return r.apply(tableID, batch, nil, &watermarkAdvance{
		basedOn:   position.Watermark,
		to:        fetchStartedAt,
		checkedAt: r.service.now().UTC(),
	})
}

// fetchByIDs reads specific records of a table. It returns the records that
// still exist and the IDs that do not.
func (r *deltaRun) fetchByIDs(tableID string, ids []string) (records []Record, missing []string, err error) {
	for start := 0; start < len(ids); start += MaxRecordIDsPerRequest {
		chunk := ids[start:min(start+MaxRecordIDsPerRequest, len(ids))]
		found := make(map[string]struct{}, len(chunk))
		options := ListRecordsPageOptions{RecordIDs: chunk}
		for {
			page, err := r.listPage(tableID, options)
			if err != nil {
				return nil, nil, err
			}
			for _, record := range page.Records {
				found[record.ID] = struct{}{}
			}
			records = append(records, page.Records...)
			if page.Offset == "" {
				break
			}
			options.Offset = page.Offset
		}
		for _, id := range chunk {
			if _, ok := found[id]; !ok {
				missing = append(missing, id)
			}
		}
	}
	return records, missing, nil
}

// refetch brings specific rows of a table up to date, deleting the ones that
// no longer exist in Airtable.
func (r *deltaRun) refetch(tableID string, ids []string) error {
	for start := 0; start < len(ids); start += deltaFlushRecords {
		chunk := ids[start:min(start+deltaFlushRecords, len(ids))]
		records, missing, err := r.fetchByIDs(tableID, chunk)
		if err != nil {
			return err
		}
		if err := r.apply(tableID, records, missing, nil); err != nil {
			return err
		}
	}
	return nil
}

func (r *deltaRun) notWritten(tableID string, ids []string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	seen := r.written[tableID]
	unique := map[string]struct{}{}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		if _, ok := unique[id]; ok {
			continue
		}
		unique[id] = struct{}{}
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// refetchLinkEnds brings the records at the other end of every changed link
// up to date. Airtable mirrors a link edit on both records, so in a normal
// delta those records were already fetched as modified and nothing is left
// to do. What remains are records the listing could not return: one that was
// deleted (it vanishes from the links that pointed at it, and from Airtable),
// and, when a mutation is written through, the records the mutation linked
// to. A record Airtable no longer has is removed from the cache.
func (r *deltaRun) refetchLinkEnds() (fetched bool, err error) {
	r.mu.Lock()
	ends := r.linkEnds
	r.linkEnds = map[string][]string{}
	r.mu.Unlock()

	tableIDs := make([]string, 0, len(ends))
	for tableID := range ends {
		tableIDs = append(tableIDs, tableID)
	}
	sort.Strings(tableIDs)
	for _, tableID := range tableIDs {
		if _, ok := r.plans[tableID]; !ok {
			continue
		}
		ids := r.notWritten(tableID, ends[tableID])
		if len(ids) == 0 {
			continue
		}
		r.mu.Lock()
		affordable := len(ids) <= r.budget
		if affordable {
			r.budget -= len(ids)
			r.result.RecordsFollowed += len(ids)
		}
		r.mu.Unlock()
		if !affordable {
			// These records really were edited in Airtable, so the next
			// delta's modified-since listing returns them anyway.
			continue
		}
		if err := r.refetch(tableID, ids); err != nil {
			return fetched, err
		}
		fetched = true
	}
	return fetched, nil
}

// settle follows link ends and computed dependencies until a pass changes
// nothing more.
func (r *deltaRun) settle() error {
	for pass := 0; pass < deltaMaxRounds; pass++ {
		fetched, err := r.refetchLinkEnds()
		if err != nil {
			return err
		}
		if err := r.propagate(); err != nil {
			return err
		}
		r.mu.Lock()
		pending := len(r.linkEnds) > 0
		r.mu.Unlock()
		if !fetched && !pending {
			return nil
		}
	}
	return nil
}

// propagate refetches rows whose computed values depend on rows this run
// changed, following the dependency edges until nothing more changes.
func (r *deltaRun) propagate() error {
	for round := 0; ; round++ {
		r.mu.Lock()
		work := r.changed
		r.changed = map[computedEdge][]string{}
		r.mu.Unlock()
		if len(work) == 0 {
			return nil
		}

		edges := make([]computedEdge, 0, len(work))
		for edge := range work {
			edges = append(edges, edge)
		}
		sort.Slice(edges, func(i, j int) bool {
			a, b := edges[i], edges[j]
			if a.DependentTableID != b.DependentTableID {
				return a.DependentTableID < b.DependentTableID
			}
			if a.LinkColumn != b.LinkColumn {
				return a.LinkColumn < b.LinkColumn
			}
			return a.SourceColumn < b.SourceColumn
		})

		// Collect every dependent row of this round per table first, so a
		// row reached through several edges is fetched once.
		dependents := map[string][]string{}
		for _, edge := range edges {
			plan, ok := r.plans[edge.DependentTableID]
			if !ok || edge.LinkColumn == "" {
				continue
			}
			if round >= deltaMaxRounds {
				r.markStale(edge.DependentTableID)
				continue
			}
			unlock, err := r.service.lockDuckRead(r.ctx, r.base.ID)
			if err != nil {
				return err
			}
			rows, err := duckdb.FindRowsLinking(r.ctx, r.service.basePath(r.base.ID), plan.Table.DuckDBTableName, edge.LinkColumn, work[edge])
			unlock()
			if err != nil {
				return err
			}
			dependents[edge.DependentTableID] = append(dependents[edge.DependentTableID], rows...)
		}

		tableIDs := make([]string, 0, len(dependents))
		for tableID := range dependents {
			tableIDs = append(tableIDs, tableID)
		}
		sort.Strings(tableIDs)
		for _, tableID := range tableIDs {
			ids := r.notWritten(tableID, dependents[tableID])
			if len(ids) == 0 {
				continue
			}
			r.mu.Lock()
			affordable := len(ids) <= r.budget
			if affordable {
				r.budget -= len(ids)
				r.result.RecordsFollowed += len(ids)
			}
			r.mu.Unlock()
			if !affordable {
				r.markStale(tableID)
				continue
			}
			if err := r.refetch(tableID, ids); err != nil {
				return err
			}
		}
	}
}

func (r *deltaRun) markStale(tableID string) {
	r.mu.Lock()
	r.stale[tableID] = struct{}{}
	r.mu.Unlock()
}

// finish stamps the run on the cache file: tables whose computed columns
// could not be fully followed, and (when syncedAt is set) the base-level
// freshness time.
func (r *deltaRun) finish(syncedAt time.Time) error {
	r.mu.Lock()
	staleIDs := make([]string, 0, len(r.stale))
	for tableID := range r.stale {
		staleIDs = append(staleIDs, tableID)
	}
	r.mu.Unlock()
	sort.Strings(staleIDs)

	stale := true
	tables := make([]duckdb.DeltaTable, 0, len(staleIDs))
	for _, tableID := range staleIDs {
		plan := r.plans[tableID]
		tables = append(tables, duckdb.DeltaTable{
			Table:         duckdb.TableSnapshot{DuckDBTableName: plan.Table.DuckDBTableName, Fields: plan.Table.Fields},
			ComputedStale: &stale,
		})
		r.result.StaleTables = append(r.result.StaleTables, plan.Table.DuckDBTableName)
	}
	if len(tables) == 0 && syncedAt.IsZero() {
		return nil
	}

	unlock, err := r.service.lockDuckWrite(r.ctx, r.base.ID)
	if err != nil {
		return err
	}
	defer unlock()
	_, err = duckdb.ApplyDelta(r.ctx, r.service.basePath(r.base.ID), tables, syncedAt)
	return err
}

// runDelta brings a base's cache up to date by fetching only what changed in
// Airtable since each table's watermark. It reports NeedsFull instead of
// syncing when the cache cannot be patched (no snapshot yet, an older file
// layout, or a schema change).
func (s *Service) runDelta(ctx context.Context, tokens AccessTokenFunc, base Base, touch func()) (DeltaResult, error) {
	startedAt := s.now().UTC()
	result := DeltaResult{BaseID: base.ID, BaseName: base.Name}

	state, ok, err := s.readCacheState(ctx, base.ID)
	if err != nil {
		return result, err
	}
	if !ok {
		result.NeedsFull, result.Reason = true, "no cache file"
		return result, nil
	}

	tables, err := withAccessToken(ctx, tokens, func(token string) ([]Table, error) {
		return s.client.GetBaseSchema(ctx, token, base.ID)
	})
	if err != nil {
		return result, err
	}
	plans, err := buildSyncPlans(tables)
	if err != nil {
		return result, err
	}
	if reason := deltaBlocker(state, plans, schemaHash(tables)); reason != "" {
		result.NeedsFull, result.Reason = true, reason
		return result, nil
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	run := s.newDeltaRun(runCtx, tokens, base, tables, plans, state, touch)

	jobs := make(chan string, len(run.tableOrder))
	for _, tableID := range run.tableOrder {
		jobs <- tableID
	}
	close(jobs)
	var (
		workers  sync.WaitGroup
		errMu    sync.Mutex
		firstErr error
	)
	for worker := 0; worker < min(maxParallelDeltaTables, len(run.tableOrder)); worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for tableID := range jobs {
				if runCtx.Err() != nil {
					return
				}
				if err := run.syncTable(tableID); err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
					cancel()
					return
				}
			}
		}()
	}
	workers.Wait()
	if firstErr != nil {
		return result, firstErr
	}
	if err := ctx.Err(); err != nil {
		return result, fmt.Errorf("delta sync aborted: %w", context.Cause(ctx))
	}

	if err := run.settle(); err != nil {
		return result, err
	}
	if err := run.finish(startedAt); err != nil {
		return result, err
	}

	result = run.result
	result.CheckedAt = startedAt
	result.TablesChecked = len(plans)
	result.Duration = s.now().UTC().Sub(startedAt)
	if final, ok, err := s.readCacheState(ctx, base.ID); err == nil && ok {
		for _, table := range final.Tables {
			result.TotalRecords += table.RecordCount
		}
	}
	logx.Event(ctx, "sync_service", "sync.delta_completed",
		"base_id", base.ID,
		"tables_checked", result.TablesChecked,
		"pages_fetched", result.PagesFetched,
		"records_upserted", result.RecordsUpserted,
		"records_deleted", result.RecordsDeleted,
		"records_followed", result.RecordsFollowed,
		"stale_tables", len(result.StaleTables),
		"sync_duration_ms", result.Duration.Milliseconds(),
	)
	return result, nil
}

// errCacheNotPatchable is returned by RefreshRecords when the cache needs a
// full sync before individual records can be written into it.
var errCacheNotPatchable = errors.New("cache cannot be patched in place")

// RefreshRecords writes specific records of one table through to the cache
// right after they were changed in Airtable: changedIDs are refetched (and
// removed if Airtable no longer has them), deletedIDs are removed, and rows
// computed from them are followed. It leaves the table's watermark alone, so
// the next delta still covers everything else.
func (s *Service) RefreshRecords(ctx context.Context, tokens AccessTokenFunc, base Base, tableID string, changedIDs, deletedIDs []string) error {
	state, ok, err := s.readCacheState(ctx, base.ID)
	if err != nil {
		return err
	}
	if !ok {
		return errCacheNotPatchable
	}
	tables, err := withAccessToken(ctx, tokens, func(token string) ([]Table, error) {
		return s.client.GetBaseSchema(ctx, token, base.ID)
	})
	if err != nil {
		return err
	}
	plans, err := buildSyncPlans(tables)
	if err != nil {
		return err
	}
	if reason := deltaBlocker(state, plans, schemaHash(tables)); reason != "" {
		return fmt.Errorf("%w: %s", errCacheNotPatchable, reason)
	}

	run := s.newDeltaRun(ctx, tokens, base, tables, plans, state, nil)
	if _, ok := run.plans[tableID]; !ok {
		return fmt.Errorf("%w: table %s is not cached", errCacheNotPatchable, tableID)
	}
	if len(deletedIDs) > 0 {
		if err := run.apply(tableID, nil, deletedIDs, nil); err != nil {
			return err
		}
	}
	if len(changedIDs) > 0 {
		if err := run.refetch(tableID, changedIDs); err != nil {
			return err
		}
	}
	if err := run.settle(); err != nil {
		return err
	}
	return run.finish(time.Time{})
}

// oldestReconciledAt returns the oldest table's last full fetch, or the zero
// time when the cache has no reconcile record.
func (s *Service) oldestReconciledAt(ctx context.Context, baseID string) time.Time {
	state, ok, err := s.readCacheState(ctx, baseID)
	if err != nil || !ok {
		return time.Time{}
	}
	var oldest time.Time
	for _, table := range state.Tables {
		if table.ReconciledAt.IsZero() {
			return time.Time{}
		}
		if oldest.IsZero() || table.ReconciledAt.Before(oldest) {
			oldest = table.ReconciledAt
		}
	}
	return oldest
}

// cacheFileBytes is the on-disk size of a base's cache.
func (s *Service) cacheFileBytes(baseID string) int64 {
	var total int64
	for _, path := range []string{s.basePath(baseID), s.basePath(baseID) + ".wal"} {
		if info, err := os.Stat(path); err == nil {
			total += info.Size()
		}
	}
	return total
}

// propagationBudget is deltaPropagationBudget unless a test lowered it.
func (s *Service) propagationBudget() int {
	if s.deltaBudget > 0 {
		return s.deltaBudget
	}
	return deltaPropagationBudget
}
