package syncer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
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

	plans       map[string]syncTablePlan // by Airtable table ID
	tableIDs    map[string]string        // DuckDB table name -> Airtable table ID
	positions   map[string]duckdb.TableDeltaState
	graph       schemaGraph
	tableOrder  []string
	generation0 int // highest generation already queued in the cache file

	mu sync.Mutex
	// generation is what new follow-ups are queued under. The follow phase
	// reads everything up to a generation, bumps it, and clears what it read
	// once the fetches that work caused have been written.
	generation int
	// following is set once the per-table listings are done. Only rows
	// fetched after that are known to reflect every change this run saw.
	following bool
	followed  map[string]map[string]struct{} // table ID -> record IDs fetched in the follow phase
	stale     map[string]struct{}            // table IDs whose rows may lag until a full fetch
	budget    int
	result    DeltaResult
}

func (s *Service) newDeltaRun(ctx context.Context, tokens AccessTokenFunc, base Base, tables []Table, plans []syncTablePlan, state duckdb.CacheState, touch func()) (*deltaRun, error) {
	run := &deltaRun{
		service:   s,
		ctx:       ctx,
		tokens:    tokens,
		base:      base,
		touch:     touch,
		plans:     make(map[string]syncTablePlan, len(plans)),
		tableIDs:  make(map[string]string, len(plans)),
		positions: make(map[string]duckdb.TableDeltaState, len(state.Tables)),
		graph:     buildSchemaGraph(tables, plans),
		followed:  map[string]map[string]struct{}{},
		stale:     map[string]struct{}{},
		budget:    s.propagationBudget(),
		result:    DeltaResult{BaseID: base.ID, BaseName: base.Name},
	}
	for _, plan := range plans {
		run.plans[plan.Table.AirtableTableID] = plan
		run.tableIDs[plan.Table.DuckDBTableName] = plan.Table.AirtableTableID
		run.tableOrder = append(run.tableOrder, plan.Table.AirtableTableID)
	}
	for _, table := range state.Tables {
		run.positions[table.TableName] = table
	}

	// Follow-ups a previous run queued but never got to are still in the
	// file. Queue this run's work after them so one pass picks up both.
	unlock, err := s.lockDuckRead(ctx, base.ID)
	if err != nil {
		return nil, err
	}
	_, highest, err := duckdb.ReadPending(ctx, s.basePath(base.ID), 0)
	unlock()
	if err != nil {
		return nil, err
	}
	run.generation0 = highest
	run.generation = highest + 1
	return run, nil
}

// watermarkAdvance asks apply to move a table's delta watermark.
type watermarkAdvance struct {
	basedOn   time.Time
	to        time.Time
	checkedAt time.Time
}

// apply writes one batch of fetched records (and deletions) for a table. The
// write queues, in the cache file, whatever follow-up fetches it calls for.
func (r *deltaRun) apply(tableID string, records []Record, deleteIDs []string, advance *watermarkAdvance, forceChanged bool) error {
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

	r.mu.Lock()
	generation, following := r.generation, r.following
	r.mu.Unlock()

	columns, watchAny := r.graph.watchFor(tableID)
	table := duckdb.DeltaTable{
		Table: duckdb.TableSnapshot{
			AirtableTableID: plan.Table.AirtableTableID,
			OriginalName:    plan.Table.OriginalName,
			DuckDBTableName: plan.Table.DuckDBTableName,
			Fields:          plan.Table.Fields,
			Records:         snapshots,
		},
		DeleteIDs:         deleteIDs,
		WatchColumns:      columns,
		WatchAny:          watchAny,
		LinkColumns:       r.graph.linkColumnNames(tableID),
		PendingGeneration: generation,
		ForceChanged:      forceChanged,
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
	if following {
		if r.followed[tableID] == nil {
			r.followed[tableID] = map[string]struct{}{}
		}
		for _, snapshot := range snapshots {
			r.followed[tableID][snapshot.ID] = struct{}{}
		}
		for _, id := range deleteIDs {
			r.followed[tableID][id] = struct{}{}
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

	// The first delta after a full fetch cannot trust a comparison with the
	// cache. The full fetch read its tables one after another, so a row
	// edited while it ran may already be cached with its new value while a
	// row computed from it was read before the edit. Every row this listing
	// returns is therefore treated as changed, once.
	forceChanged := !position.ReconciledAt.IsZero() && position.CheckedAt.Equal(position.ReconciledAt)

	options := ListRecordsPageOptions{ModifiedSince: position.Watermark.Add(-r.service.overlap())}
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
			if err := r.apply(tableID, batch, nil, nil, forceChanged); err != nil {
				return err
			}
			batch = nil
		}
	}
	return r.apply(tableID, batch, nil, &watermarkAdvance{
		basedOn:   position.Watermark,
		to:        fetchStartedAt,
		checkedAt: r.service.now().UTC(),
	}, forceChanged)
}

var validRecordID = regexp.MustCompile(`^rec[A-Za-z0-9]{1,40}$`)

// fetchByIDs reads specific records of a table. It returns the records that
// still exist and the IDs that do not.
func (r *deltaRun) fetchByIDs(tableID string, ids []string) (records []Record, missing []string, err error) {
	for _, id := range ids {
		// IDs end up inside an Airtable formula, and some come from tool
		// arguments.
		if !validRecordID.MatchString(id) {
			return nil, nil, fmt.Errorf("invalid airtable record id %q", id)
		}
	}
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
		if err := r.apply(tableID, records, missing, nil, false); err != nil {
			return err
		}
	}
	return nil
}

// notFollowed drops IDs already fetched in the follow phase. Rows written by
// the per-table listings are not skipped: a listing may have read a row
// before the edit that made it a dependent.
func (r *deltaRun) notFollowed(tableID string, ids []string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	seen := r.followed[tableID]
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

// settle works through the follow-up queue until it is empty.
//
// Two kinds of follow-up exist. A changed value means the rows that compute
// from it (lookups, rollups, formulas over a link) must be refetched, because
// Airtable's modified time ignores computed fields. A changed link means the
// record at its other end must be refetched: Airtable mirrors a link edit on
// both records, so normally that record was already listed as modified, but
// a record that was deleted can only be noticed this way (it vanishes from
// the links that pointed at it and is no longer returned by ID), and a
// write-through never lists anything. Fetching a row can queue more work, so
// this repeats; work the budget or the round limit cannot cover flags the
// table as stale instead of being dropped.
func (r *deltaRun) settle() error {
	r.mu.Lock()
	r.following = true
	r.mu.Unlock()

	for pass := 0; ; pass++ {
		r.mu.Lock()
		through := r.generation
		r.generation++
		r.mu.Unlock()

		unlock, err := r.service.lockDuckRead(r.ctx, r.base.ID)
		if err != nil {
			return err
		}
		pending, _, err := duckdb.ReadPending(r.ctx, r.service.basePath(r.base.ID), through)
		unlock()
		if err != nil {
			return err
		}
		if len(pending) == 0 {
			return nil
		}

		refetch := map[string][]string{}
		changed := map[computedEdge][]string{}
		for _, change := range pending {
			sourceID, ok := r.tableIDs[change.SourceTable]
			if !ok {
				continue
			}
			switch change.Kind {
			case duckdb.PendingLinkEnd:
				if target := r.graph.linkColumns[sourceID][change.Column]; target != "" {
					refetch[target] = append(refetch[target], change.RecordID)
				}
			case duckdb.PendingChanged, duckdb.PendingChangedAny:
				for _, edge := range r.graph.edgesBySource[sourceID] {
					if (change.Kind == duckdb.PendingChangedAny) == (edge.SourceColumn == "") && edge.SourceColumn == change.Column {
						changed[edge] = append(changed[edge], change.RecordID)
					}
				}
			}
		}

		edges := make([]computedEdge, 0, len(changed))
		for edge := range changed {
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
		for _, edge := range edges {
			plan, ok := r.plans[edge.DependentTableID]
			if !ok || edge.LinkColumn == "" {
				continue
			}
			unlock, err := r.service.lockDuckRead(r.ctx, r.base.ID)
			if err != nil {
				return err
			}
			rows, err := duckdb.FindRowsLinking(r.ctx, r.service.basePath(r.base.ID), plan.Table.DuckDBTableName, edge.LinkColumn, changed[edge])
			unlock()
			if err != nil {
				return err
			}
			refetch[edge.DependentTableID] = append(refetch[edge.DependentTableID], rows...)
		}

		lastPass := pass >= deltaMaxRounds
		tableIDs := make([]string, 0, len(refetch))
		for tableID := range refetch {
			tableIDs = append(tableIDs, tableID)
		}
		sort.Strings(tableIDs)
		for _, tableID := range tableIDs {
			if _, ok := r.plans[tableID]; !ok {
				continue
			}
			ids := r.notFollowed(tableID, refetch[tableID])
			if len(ids) == 0 {
				continue
			}
			r.mu.Lock()
			affordable := !lastPass && len(ids) <= r.budget
			if affordable {
				r.budget -= len(ids)
				r.result.RecordsFollowed += len(ids)
			} else {
				r.stale[tableID] = struct{}{}
			}
			r.mu.Unlock()
			if !affordable {
				continue
			}
			if err := r.refetch(tableID, ids); err != nil {
				return err
			}
		}

		// Everything this pass read is now either written or accounted for
		// by a stale flag (recorded below before the queue is cleared).
		if err := r.flagStale(); err != nil {
			return err
		}
		unlock, err = r.service.lockDuckWrite(r.ctx, r.base.ID)
		if err != nil {
			return err
		}
		err = duckdb.ClearPending(r.ctx, r.service.basePath(r.base.ID), through)
		unlock()
		if err != nil {
			return err
		}
		if lastPass {
			return nil
		}
	}
}

// flagStale records, in the cache file, the tables whose rows could not all
// be followed.
func (r *deltaRun) flagStale() error {
	r.mu.Lock()
	staleIDs := make([]string, 0, len(r.stale))
	for tableID := range r.stale {
		staleIDs = append(staleIDs, tableID)
	}
	r.mu.Unlock()
	if len(staleIDs) == 0 {
		return nil
	}
	sort.Strings(staleIDs)

	stale := true
	tables := make([]duckdb.DeltaTable, 0, len(staleIDs))
	names := make([]string, 0, len(staleIDs))
	for _, tableID := range staleIDs {
		plan := r.plans[tableID]
		tables = append(tables, duckdb.DeltaTable{
			Table:         duckdb.TableSnapshot{DuckDBTableName: plan.Table.DuckDBTableName, Fields: plan.Table.Fields},
			ComputedStale: &stale,
		})
		names = append(names, plan.Table.DuckDBTableName)
	}
	unlock, err := r.service.lockDuckWrite(r.ctx, r.base.ID)
	if err != nil {
		return err
	}
	_, err = duckdb.ApplyDelta(r.ctx, r.service.basePath(r.base.ID), tables, time.Time{})
	unlock()
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.result.StaleTables = names
	r.mu.Unlock()
	return nil
}

// stampSynced records the base-level freshness time of a completed delta.
func (r *deltaRun) stampSynced(syncedAt time.Time) error {
	unlock, err := r.service.lockDuckWrite(r.ctx, r.base.ID)
	if err != nil {
		return err
	}
	defer unlock()
	_, err = duckdb.ApplyDelta(r.ctx, r.service.basePath(r.base.ID), nil, syncedAt)
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
		if ctx.Err() != nil {
			return result, err
		}
		// A cache file that cannot be read is no use to a delta; a full sync
		// rebuilds it.
		result.NeedsFull, result.Reason = true, "cache file is unreadable: "+logx.ErrorPreview(err)
		return result, nil
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
	run, err := s.newDeltaRun(runCtx, tokens, base, tables, plans, state, touch)
	if err != nil {
		return result, err
	}

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
	if err := run.stampSynced(startedAt); err != nil {
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

	run, err := s.newDeltaRun(ctx, tokens, base, tables, plans, state, nil)
	if err != nil {
		return err
	}
	if _, ok := run.plans[tableID]; !ok {
		return fmt.Errorf("%w: table %s is not cached", errCacheNotPatchable, tableID)
	}
	// Everything here is fetched by ID after the mutation ran, so it counts
	// as followed from the start.
	run.following = true
	if len(deletedIDs) > 0 {
		for _, id := range deletedIDs {
			if !validRecordID.MatchString(id) {
				return fmt.Errorf("invalid airtable record id %q", id)
			}
		}
		if err := run.apply(tableID, nil, deletedIDs, nil, false); err != nil {
			return err
		}
	}
	if len(changedIDs) > 0 {
		if err := run.refetch(tableID, changedIDs); err != nil {
			return err
		}
	}
	return run.settle()
}

// reconcileState returns the oldest table's last full fetch (the zero time
// when the cache has no reconcile record) and whether any table is flagged
// as lagging until the next full fetch.
func (s *Service) reconcileState(ctx context.Context, baseID string) (oldest time.Time, stale bool) {
	state, ok, err := s.readCacheState(ctx, baseID)
	if err != nil || !ok {
		return time.Time{}, false
	}
	complete := true
	for _, table := range state.Tables {
		if table.ComputedStale {
			stale = true
		}
		if table.ReconciledAt.IsZero() {
			complete = false
			continue
		}
		if oldest.IsZero() || table.ReconciledAt.Before(oldest) {
			oldest = table.ReconciledAt
		}
	}
	if !complete {
		return time.Time{}, stale
	}
	return oldest, stale
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

// overlap is deltaOverlap unless a test shortened it.
func (s *Service) overlap() time.Duration {
	if s.deltaOverlapOverride > 0 {
		return s.deltaOverlapOverride
	}
	return deltaOverlap
}
