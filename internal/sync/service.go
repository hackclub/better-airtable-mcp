// Package syncer runs the continuous Airtable-to-DuckDB sync workers.
package syncer

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/hackclub/better-airtable-mcp/internal/duckdb"
	"github.com/hackclub/better-airtable-mcp/internal/logx"
)

type Service struct {
	client  Client
	dataDir string

	mu         sync.Mutex
	syncLocks  map[string]*sync.Mutex
	duckLocks  map[string]*ctxRWLock
	syncSlots  chan struct{}
	basesCache map[string]basesCacheEntry
	now        func() time.Time
}

// baseListCacheTTL bounds how long a fetched base list is reused for base
// resolution. Short enough that access changes surface quickly, long enough
// to cover a burst of closely-spaced requests without re-fetching the
// paginated meta API each time.
const baseListCacheTTL = 15 * time.Second

type basesCacheEntry struct {
	bases     []Base
	fetchedAt time.Time
}

type SyncResult struct {
	BaseID        string
	BaseName      string
	LastSyncedAt  time.Time
	TablesSynced  int
	RecordsSynced int
	SyncDuration  time.Duration
}

type SyncProgress struct {
	BaseID               string
	BaseName             string
	SyncStartedAt        time.Time
	LastSyncedAt         time.Time
	Status               string
	TablesTotal          int
	TablesStarted        int
	TablesCompleted      int
	PagesFetched         int64
	RecordsSyncedThisRun int64
	Error                string
	ReadSnapshot         string
}

const maxParallelRecordRequests = 5

// An active-file sync flushes its buffered pages once either limit is hit.
const (
	activeFlushRecords  = 2000
	activeFlushInterval = 3 * time.Second
)

// defaultMaxConcurrentSyncs caps how many base syncs run at once across the
// whole process. Every active base re-syncs on its interval, and after a
// restart every restored worker starts a full sync at the same moment. With no
// cap, ~15 simultaneous syncs plus user queries pinned all 4 CPUs for 40
// minutes and tool calls timed out at the gateway (production, 2026-10-13).
const defaultMaxConcurrentSyncs = 4

// AccessTokenFunc returns the Airtable access token to use for the next
// request. Sync runs call it per request instead of capturing one token at
// the start: Airtable revokes the previous access token as soon as the refresh
// loop rotates it, which used to fail every long sync with a 401.
type AccessTokenFunc func(ctx context.Context) (string, error)

// StaticAccessToken adapts a fixed token to AccessTokenFunc.
func StaticAccessToken(token string) AccessTokenFunc {
	return func(context.Context) (string, error) { return token, nil }
}

// SetMaxConcurrentSyncs changes the process-wide sync cap. Call it before any
// sync starts.
func (s *Service) SetMaxConcurrentSyncs(limit int) {
	if limit < 1 {
		limit = 1
	}
	s.syncSlots = make(chan struct{}, limit)
}

// acquireSyncSlot waits for one of the process-wide sync slots.
func (s *Service) acquireSyncSlot(ctx context.Context) (func(), error) {
	select {
	case s.syncSlots <- struct{}{}:
		return func() { <-s.syncSlots }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// withAccessToken runs call with the current token. If Airtable answers 401
// and the token source now returns a different token (it was rotated while
// the request was in flight), it retries once with the fresh token.
func withAccessToken[T any](ctx context.Context, tokens AccessTokenFunc, call func(token string) (T, error)) (T, error) {
	var zero T
	token, err := tokens(ctx)
	if err != nil {
		return zero, err
	}
	result, err := call(token)
	if err == nil || !IsUnauthorized(err) {
		return result, err
	}
	fresh, tokenErr := tokens(ctx)
	if tokenErr != nil || fresh == token {
		return result, err
	}
	logx.Event(ctx, "sync_service", "sync.access_token_rotated_retry")
	return call(fresh)
}

type syncJob struct {
	TableIndex int
	Offset     string
}

type syncPageResult struct {
	TableIndex int
	Offset     string
	NextOffset string
	Records    []duckdb.RecordSnapshot
	Err        error
}

type syncTablePlan struct {
	Table duckdb.TableSnapshot
	Sort  ListRecordsPageOptions
}

type syncTableRuntime struct {
	Plan           syncTablePlan
	PagesFetched   int64
	VisibleRecords int64
	Started        bool
	Completed      bool
}

func NewService(client Client, dataDir string) *Service {
	return &Service{
		client:     client,
		dataDir:    dataDir,
		syncLocks:  make(map[string]*sync.Mutex),
		duckLocks:  make(map[string]*ctxRWLock),
		syncSlots:  make(chan struct{}, defaultMaxConcurrentSyncs),
		basesCache: make(map[string]basesCacheEntry),
		now:        time.Now,
	}
}

// listBasesCached returns the caller's base list, reusing a recent fetch for
// the same access token. The cache key is a hash of the token (never the raw
// token), entries are per-token so one user's bases can never serve another,
// and stale entries are pruned on every access.
func (s *Service) listBasesCached(ctx context.Context, accessToken string) ([]Base, error) {
	key := fmt.Sprintf("%x", sha256.Sum256([]byte(accessToken)))

	s.mu.Lock()
	now := s.now()
	for cacheKey, entry := range s.basesCache {
		if now.Sub(entry.fetchedAt) > baseListCacheTTL {
			delete(s.basesCache, cacheKey)
		}
	}
	entry, ok := s.basesCache[key]
	s.mu.Unlock()
	if ok {
		return entry.bases, nil
	}

	bases, err := s.client.ListBases(ctx, accessToken)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	s.basesCache[key] = basesCacheEntry{bases: bases, fetchedAt: s.now()}
	s.mu.Unlock()
	return bases, nil
}

func (s *Service) SearchBases(ctx context.Context, accessToken, query string) ([]Base, error) {
	bases, err := s.client.ListBases(ctx, accessToken)
	if err != nil {
		return nil, err
	}

	query = strings.TrimSpace(strings.ToLower(query))
	if query == "" {
		return bases, nil
	}

	filtered := make([]Base, 0, len(bases))
	for _, base := range bases {
		if strings.Contains(strings.ToLower(base.Name), query) {
			filtered = append(filtered, base)
		}
	}

	return filtered, nil
}

func (s *Service) SyncBase(ctx context.Context, accessToken, baseRef string) (SyncResult, error) {
	base, err := s.resolveBase(ctx, accessToken, baseRef)
	if err != nil {
		return SyncResult{}, err
	}

	unlockSync := s.lockSync(base.ID)
	defer unlockSync()
	releaseSlot, err := s.acquireSyncSlot(ctx)
	if err != nil {
		return SyncResult{}, err
	}
	defer releaseSlot()
	return s.runSync(ctx, StaticAccessToken(accessToken), base, nil)
}

func (s *Service) ListSchema(ctx context.Context, accessToken, baseRef string) (duckdb.BaseSchema, error) {
	base, err := s.ensureSynced(ctx, accessToken, baseRef)
	if err != nil {
		return duckdb.BaseSchema{}, err
	}

	return s.ListResolvedSchema(ctx, base.ID, base.Name)
}

// ListResolvedSchema reads the schema of an already-resolved, already-synced
// base. Callers must have verified access and sync state first — no
// resolution, authorization, or sync is performed here.
func (s *Service) ListResolvedSchema(ctx context.Context, baseID, baseName string) (duckdb.BaseSchema, error) {
	unlock, err := s.lockDuckRead(ctx, baseID)
	if err != nil {
		return duckdb.BaseSchema{}, err
	}
	defer unlock()
	return duckdb.ReadSchema(ctx, s.basePath(baseID), baseID, baseName)
}

func (s *Service) QueryBase(ctx context.Context, accessToken, baseRef, query string) (duckdb.QueryResult, error) {
	base, err := s.ensureSynced(ctx, accessToken, baseRef)
	if err != nil {
		return duckdb.QueryResult{}, err
	}

	return s.QueryResolvedBase(ctx, base.ID, query)
}

// QueryResolvedBase runs a read against an already-resolved, already-synced
// base ID. Callers must have verified access and sync state first (e.g. via
// the sync manager's EnsureBaseReadable) — no resolution, authorization, or
// sync is performed here.
func (s *Service) QueryResolvedBase(ctx context.Context, baseID, query string) (duckdb.QueryResult, error) {
	unlock, err := s.lockDuckRead(ctx, baseID)
	if err != nil {
		return duckdb.QueryResult{}, err
	}
	defer unlock()
	return duckdb.Query(ctx, s.basePath(baseID), query)
}

func (s *Service) ReadTableRowsByIDs(ctx context.Context, baseID, tableName string, ids []string) ([]map[string]any, error) {
	unlock, err := s.lockDuckRead(ctx, baseID)
	if err != nil {
		return nil, err
	}
	defer unlock()
	return duckdb.ReadTableRowsByIDs(ctx, s.basePath(baseID), tableName, ids)
}

func (s *Service) ensureSynced(ctx context.Context, accessToken, baseRef string) (Base, error) {
	base, err := s.resolveBase(ctx, accessToken, baseRef)
	if err != nil {
		return Base{}, err
	}

	if duckdb.DatabaseFileExists(s.basePath(base.ID)) {
		return base, nil
	}

	if _, err := s.SyncBase(ctx, accessToken, base.ID); err != nil {
		return Base{}, err
	}

	return base, nil
}

func (s *Service) resolveBase(ctx context.Context, accessToken, baseRef string) (Base, error) {
	baseRef = strings.TrimSpace(baseRef)
	if baseRef == "" {
		return Base{}, fmt.Errorf("base reference is required")
	}

	bases, err := s.listBasesCached(ctx, accessToken)
	if err != nil {
		return Base{}, err
	}

	for _, base := range bases {
		if base.ID == baseRef {
			return base, nil
		}
	}

	var exactMatches []Base
	for _, base := range bases {
		if strings.EqualFold(base.Name, baseRef) {
			exactMatches = append(exactMatches, base)
		}
	}
	if len(exactMatches) == 1 {
		return exactMatches[0], nil
	}
	if len(exactMatches) > 1 {
		return Base{}, fmt.Errorf("base name %q is ambiguous", baseRef)
	}

	var partialMatches []Base
	for _, base := range bases {
		if strings.Contains(strings.ToLower(base.Name), strings.ToLower(baseRef)) {
			partialMatches = append(partialMatches, base)
		}
	}
	if len(partialMatches) == 1 {
		return partialMatches[0], nil
	}
	if len(partialMatches) > 1 {
		return Base{}, fmt.Errorf("base reference %q matched multiple bases", baseRef)
	}

	return Base{}, fmt.Errorf("base %q was not found", baseRef)
}

func (s *Service) lockSync(baseID string) func() {
	s.mu.Lock()
	lock, ok := s.syncLocks[baseID]
	if !ok {
		lock = &sync.Mutex{}
		s.syncLocks[baseID] = lock
	}
	s.mu.Unlock()

	lock.Lock()
	return lock.Unlock
}

func (s *Service) duckLock(baseID string) *ctxRWLock {
	s.mu.Lock()
	defer s.mu.Unlock()
	lock, ok := s.duckLocks[baseID]
	if !ok {
		lock = newCtxRWLock()
		s.duckLocks[baseID] = lock
	}
	return lock
}

// lockDuckRead takes the base's read lock, giving up when ctx ends so a
// request whose caller has gone away stops queueing for the file.
func (s *Service) lockDuckRead(ctx context.Context, baseID string) (func(), error) {
	lock := s.duckLock(baseID)
	if err := lock.RLock(ctx); err != nil {
		return nil, fmt.Errorf("wait for base %s cache (a sync is writing to it): %w", baseID, err)
	}
	return lock.RUnlock, nil
}

func (s *Service) lockDuckWrite(ctx context.Context, baseID string) (func(), error) {
	lock := s.duckLock(baseID)
	if err := lock.Lock(ctx); err != nil {
		return nil, fmt.Errorf("wait for base %s cache write lock: %w", baseID, err)
	}
	return lock.Unlock, nil
}

func (s *Service) basePath(baseID string) string {
	return filepath.Join(s.dataDir, baseID+".db")
}

func (s *Service) DatabasePath(baseID string) string {
	return s.basePath(baseID)
}

func (s *Service) stagingPath(baseID string) string {
	return filepath.Join(s.dataDir, baseID+".staging.db")
}

func (s *Service) runSync(ctx context.Context, tokens AccessTokenFunc, base Base, progress func(SyncProgress)) (SyncResult, error) {
	startedAt := time.Now().UTC()
	tables, err := withAccessToken(ctx, tokens, func(token string) ([]Table, error) {
		return s.client.GetBaseSchema(ctx, token, base.ID)
	})
	if err != nil {
		return SyncResult{}, err
	}

	plans, err := buildSyncPlans(tables)
	if err != nil {
		return SyncResult{}, err
	}

	activePath := s.basePath(base.ID)
	useStaging := s.shouldUseStaging(base.ID)
	targetPath := activePath
	logx.Event(ctx, "sync_service", "sync.run_mode_selected",
		"base_id", base.ID,
		"staging_snapshot", useStaging,
		"table_count", len(plans),
	)
	fail := func(err error) (SyncResult, error) {
		logx.Event(ctx, "sync_service", "sync.run_storage_failed",
			"base_id", base.ID,
			"staging_snapshot", useStaging,
			"error_kind", logx.ErrorKind(err),
			"error_message", logx.ErrorPreview(err),
		)
		return SyncResult{}, err
	}
	if useStaging {
		targetPath = s.stagingPath(base.ID)
		if err := os.Remove(targetPath); err != nil && !os.IsNotExist(err) {
			return fail(fmt.Errorf("remove stale staging db: %w", err))
		}
	}

	init := duckdb.SnapshotInit{
		BaseID:        base.ID,
		BaseName:      base.Name,
		Tables:        make([]duckdb.TableSnapshot, 0, len(plans)),
		SyncStartedAt: startedAt,
	}
	for _, plan := range plans {
		init.Tables = append(init.Tables, duckdb.TableSnapshot{
			AirtableTableID: plan.Table.AirtableTableID,
			OriginalName:    plan.Table.OriginalName,
			DuckDBTableName: plan.Table.DuckDBTableName,
			Fields:          append([]duckdb.FieldSnapshot(nil), plan.Table.Fields...),
		})
	}

	// In staging mode the run writes to a private file no reader opens, so
	// one write connection is reused for the whole run instead of reopening
	// the file per page. In active-file mode each write must open and close
	// under the write lock so readers can interleave between pages.
	var session *duckdb.WriteSession
	var unlockDuck func()
	if !useStaging {
		unlockDuck, err = s.lockDuckWrite(ctx, base.ID)
		if err != nil {
			return fail(err)
		}
		if err := duckdb.InitializeSnapshot(ctx, targetPath, init); err != nil {
			unlockDuck()
			return fail(err)
		}
		unlockDuck()
	} else {
		var err error
		session, err = duckdb.OpenWriteSession(targetPath)
		if err != nil {
			return fail(err)
		}
		defer func() {
			if session != nil {
				_ = session.Close()
			}
		}()
		if err := session.InitializeSnapshot(ctx, init); err != nil {
			return fail(err)
		}
	}

	currentInfo := SyncProgress{
		BaseID:        base.ID,
		BaseName:      base.Name,
		SyncStartedAt: startedAt,
		Status:        "syncing",
		TablesTotal:   len(plans),
		ReadSnapshot:  "partial",
	}
	if useStaging {
		currentInfo.ReadSnapshot = "complete"
	}
	if progress != nil {
		progress(currentInfo)
	}

	runtimes := make([]syncTableRuntime, len(plans))
	for index, plan := range plans {
		runtimes[index] = syncTableRuntime{Plan: plan}
	}

	if len(plans) > 0 {
		workerCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		jobs := make(chan syncJob, len(plans))
		results := make(chan syncPageResult, len(plans))

		for index := range plans {
			jobs <- syncJob{TableIndex: index}
		}

		workerCount := minInt(len(plans), maxParallelRecordRequests)
		var workerWG sync.WaitGroup
		for workerIndex := 0; workerIndex < workerCount; workerIndex++ {
			workerWG.Add(1)
			go func() {
				defer workerWG.Done()
				for job := range jobs {
					runtime := runtimes[job.TableIndex]
					page, err := withAccessToken(workerCtx, tokens, func(token string) (ListRecordsPageResult, error) {
						return s.client.ListRecordsPage(workerCtx, token, base.ID, runtime.Plan.Table.AirtableTableID, ListRecordsPageOptions{
							Offset:        job.Offset,
							SortFieldName: runtime.Plan.Sort.SortFieldName,
							SortDirection: runtime.Plan.Sort.SortDirection,
						})
					})
					if err != nil {
						select {
						case results <- syncPageResult{TableIndex: job.TableIndex, Offset: job.Offset, Err: err}:
						case <-workerCtx.Done():
						}
						return
					}

					records := make([]duckdb.RecordSnapshot, 0, len(page.Records))
					for _, record := range page.Records {
						records = append(records, duckdb.RecordSnapshot{
							ID:          record.ID,
							CreatedTime: record.CreatedTime,
							Fields:      record.Fields,
						})
					}

					select {
					case results <- syncPageResult{
						TableIndex: job.TableIndex,
						Offset:     job.Offset,
						NextOffset: page.Offset,
						Records:    records,
					}:
					case <-workerCtx.Done():
						return
					}
				}
			}()
		}

		pendingTables := len(plans)
		var totalVisibleRecords int64
		var activeBatch []duckdb.TablePage
		var activeBatchRecords int
		lastActiveFlush := time.Now()
		for pendingTables > 0 {
			var result syncPageResult
			select {
			case result = <-results:
			case <-ctx.Done():
				// Workers bail out on workerCtx.Done() without sending a result,
				// so a bare receive here would block forever once the run times
				// out. Treat the deadline like any other page error.
				result = syncPageResult{Err: fmt.Errorf("sync run aborted: %w", context.Cause(ctx))}
			}
			if result.Err != nil {
				cancel()
				close(jobs)
				workerWG.Wait()
				if !useStaging {
					_ = s.markActiveSyncFailed(ctx, base.ID, currentInfo, result.Err)
				} else {
					_ = os.Remove(targetPath)
				}
				return fail(result.Err)
			}

			runtime := &runtimes[result.TableIndex]
			if !runtime.Started {
				runtime.Started = true
				currentInfo.TablesStarted++
			}
			runtime.PagesFetched++
			runtime.VisibleRecords += int64(len(result.Records))
			currentInfo.PagesFetched++
			currentInfo.RecordsSyncedThisRun += int64(len(result.Records))
			totalVisibleRecords += int64(len(result.Records))

			tableState := duckdb.TableSyncState{
				TableName:          runtime.Plan.Table.DuckDBTableName,
				SyncStatus:         "syncing",
				VisibleRecordCount: runtime.VisibleRecords,
				PagesFetched:       runtime.PagesFetched,
				HasMore:            result.NextOffset != "",
			}
			if result.NextOffset == "" {
				runtime.Completed = true
				tableState.SyncStatus = "completed"
				currentInfo.TablesCompleted++
				pendingTables--
			}

			syncInfo := duckdb.SyncInfo{
				SyncStartedAt:        currentInfo.SyncStartedAt,
				LastSyncedAt:         currentInfo.LastSyncedAt,
				TotalRecords:         totalVisibleRecords,
				TotalTables:          currentInfo.TablesTotal,
				Status:               "syncing",
				TablesStarted:        currentInfo.TablesStarted,
				TablesCompleted:      currentInfo.TablesCompleted,
				PagesFetched:         currentInfo.PagesFetched,
				RecordsSyncedThisRun: currentInfo.RecordsSyncedThisRun,
			}

			// Queue the table's next page before writing this one, so the
			// Airtable round trip overlaps the write. A table's pages are
			// sequential (each offset comes from the previous page), so for
			// the largest table the run time is the sum of these round trips.
			// Each table has at most one job in flight, so the send never
			// blocks, and per-table write order is unchanged because results
			// are still applied one at a time by this loop.
			if result.NextOffset != "" {
				jobs <- syncJob{TableIndex: result.TableIndex, Offset: result.NextOffset}
			}

			flushed := true
			if useStaging {
				err = session.ApplyTablePage(ctx, runtime.Plan.Table, result.Records, tableState, syncInfo)
			} else {
				// Active-file writes reopen (and so checkpoint) the file every
				// time, so pages are batched and flushed together. Readers see
				// new rows a few seconds later instead of per page.
				activeBatch = append(activeBatch, duckdb.TablePage{
					Table:   runtime.Plan.Table,
					Records: result.Records,
					State:   tableState,
				})
				activeBatchRecords += len(result.Records)
				flushed = pendingTables == 0 ||
					activeBatchRecords >= activeFlushRecords ||
					time.Since(lastActiveFlush) >= activeFlushInterval
				if flushed {
					unlockDuck, err = s.lockDuckWrite(ctx, base.ID)
					if err == nil {
						err = duckdb.ApplyTablePages(ctx, targetPath, activeBatch, syncInfo)
						unlockDuck()
					}
					activeBatch = activeBatch[:0]
					activeBatchRecords = 0
					lastActiveFlush = time.Now()
				}
			}
			if err != nil {
				cancel()
				close(jobs)
				workerWG.Wait()
				if !useStaging {
					_ = s.markActiveSyncFailed(ctx, base.ID, currentInfo, err)
				} else {
					_ = os.Remove(targetPath)
				}
				return fail(err)
			}

			// Progress is reported only once rows are written, since partial
			// reads use it as the count of visible records.
			if progress != nil && flushed {
				progress(currentInfo)
			}
		}

		close(jobs)
		workerWG.Wait()
	}

	completedAt := time.Now().UTC()
	finalInfo := duckdb.SyncInfo{
		SyncStartedAt:        startedAt,
		LastSyncedAt:         completedAt,
		SyncDurationMS:       completedAt.Sub(startedAt).Milliseconds(),
		TotalRecords:         currentInfo.RecordsSyncedThisRun,
		TotalTables:          len(plans),
		Status:               "completed",
		TablesStarted:        len(plans),
		TablesCompleted:      len(plans),
		PagesFetched:         currentInfo.PagesFetched,
		RecordsSyncedThisRun: currentInfo.RecordsSyncedThisRun,
	}

	if !useStaging {
		unlockDuck, err = s.lockDuckWrite(ctx, base.ID)
		if err == nil {
			err = duckdb.FinalizeSnapshot(ctx, targetPath, finalInfo)
			unlockDuck()
		}
		if err != nil {
			return fail(err)
		}
	} else {
		if err := session.FinalizeSnapshot(ctx, finalInfo); err != nil {
			_ = os.Remove(targetPath)
			return fail(err)
		}
		// Release the write handle (and DuckDB's file lock) before the
		// staging file is renamed over the active one.
		closeErr := session.Close()
		session = nil
		if closeErr != nil {
			_ = os.Remove(targetPath)
			return fail(fmt.Errorf("close staging write session: %w", closeErr))
		}
		unlockDuck, err = s.lockDuckWrite(ctx, base.ID)
		if err == nil {
			err = swapDatabaseFiles(targetPath, activePath)
			unlockDuck()
		}
		if err != nil {
			_ = os.Remove(targetPath)
			return fail(err)
		}
	}

	currentInfo.LastSyncedAt = completedAt
	currentInfo.Status = "completed"
	currentInfo.TablesStarted = len(plans)
	currentInfo.TablesCompleted = len(plans)
	currentInfo.ReadSnapshot = "complete"
	if progress != nil {
		progress(currentInfo)
	}

	result := SyncResult{
		BaseID:        base.ID,
		BaseName:      base.Name,
		LastSyncedAt:  completedAt,
		TablesSynced:  len(plans),
		RecordsSynced: int(currentInfo.RecordsSyncedThisRun),
		SyncDuration:  completedAt.Sub(startedAt),
	}
	logx.Event(ctx, "sync_service", "sync.run_storage_completed",
		"base_id", base.ID,
		"staging_snapshot", useStaging,
		"tables_synced", result.TablesSynced,
		"records_synced", result.RecordsSynced,
		"sync_duration_ms", result.SyncDuration.Milliseconds(),
	)
	return result, nil
}

func buildSyncPlans(tables []Table) ([]syncTablePlan, error) {
	tableNames := make([]string, 0, len(tables))
	for _, table := range tables {
		tableNames = append(tableNames, table.Name)
	}
	sanitizedTableNames := duckdb.SanitizeIdentifiers(tableNames)

	plans := make([]syncTablePlan, 0, len(tables))
	for tableIndex, table := range tables {
		fieldNames := make([]string, 0, len(table.Fields))
		sortFieldName := ""
		for _, field := range table.Fields {
			mapping, ok := duckdb.AirtableTypeToDuckDBType(field.Type)
			if ok && !mapping.Omitted {
				fieldNames = append(fieldNames, field.Name)
			}
			if sortFieldName == "" && field.Type == "createdTime" {
				sortFieldName = field.Name
			}
		}

		sanitizedFieldNames := duckdb.SanitizeFieldIdentifiers(fieldNames)

		fields := make([]duckdb.FieldSnapshot, 0, len(fieldNames))
		sanitizedIndex := 0
		for _, field := range table.Fields {
			mapping, ok := duckdb.AirtableTypeToDuckDBType(field.Type)
			if !ok || mapping.Omitted {
				continue
			}
			fields = append(fields, duckdb.FieldSnapshot{
				AirtableFieldID:   field.ID,
				OriginalFieldName: field.Name,
				DuckDBColumnName:  sanitizedFieldNames[sanitizedIndex].Sanitized,
				AirtableFieldType: field.Type,
				DuckDBType:        mapping.DuckDBType,
			})
			sanitizedIndex++
		}

		plans = append(plans, syncTablePlan{
			Table: duckdb.TableSnapshot{
				AirtableTableID: table.ID,
				OriginalName:    table.Name,
				DuckDBTableName: sanitizedTableNames[tableIndex].Sanitized,
				Fields:          fields,
			},
			Sort: ListRecordsPageOptions{
				SortFieldName: sortFieldName,
				SortDirection: "desc",
			},
		})
	}

	return plans, nil
}

func (s *Service) shouldUseStaging(baseID string) bool {
	if !duckdb.DatabaseFileExists(s.basePath(baseID)) {
		return false
	}

	info, err := s.readSyncInfo(context.Background(), baseID)
	if err != nil {
		return false
	}
	return !info.LastSyncedAt.IsZero() && info.Status == "completed"
}

// readSyncInfo reads the active snapshot's sync metadata under the read lock,
// so it never opens the file while a writer or the staging swap holds it.
func (s *Service) readSyncInfo(ctx context.Context, baseID string) (duckdb.SyncInfo, error) {
	unlock, err := s.lockDuckRead(ctx, baseID)
	if err != nil {
		return duckdb.SyncInfo{}, err
	}
	defer unlock()
	return duckdb.ReadSyncInfo(ctx, s.basePath(baseID))
}

func (s *Service) markActiveSyncFailed(ctx context.Context, baseID string, progress SyncProgress, syncErr error) error {
	// The run's context is often the reason the sync failed (deadline or
	// cancellation), so record the failure on a short detached context.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), markFailedTimeout)
	defer cancel()

	// A read-write handle must never coexist with readers' read-only handles on
	// the same file in this process.
	unlock, err := s.lockDuckWrite(ctx, baseID)
	if err != nil {
		return err
	}
	defer unlock()
	return duckdb.UpdateSyncFailure(ctx, s.basePath(baseID), duckdb.SyncInfo{
		SyncStartedAt:        progress.SyncStartedAt,
		LastSyncedAt:         progress.LastSyncedAt,
		TotalRecords:         progress.RecordsSyncedThisRun,
		TotalTables:          progress.TablesTotal,
		Status:               "failed",
		TablesStarted:        progress.TablesStarted,
		TablesCompleted:      progress.TablesCompleted,
		PagesFetched:         progress.PagesFetched,
		RecordsSyncedThisRun: progress.RecordsSyncedThisRun,
		Error:                syncErr.Error(),
	})
}

const markFailedTimeout = 30 * time.Second

func swapDatabaseFiles(sourcePath, destinationPath string) error {
	if err := os.Rename(sourcePath, destinationPath); err == nil {
		return nil
	}
	if err := os.Remove(destinationPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return os.Rename(sourcePath, destinationPath)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
