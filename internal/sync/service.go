package syncer

import (
	"context"
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

	mu        sync.Mutex
	syncLocks map[string]*sync.Mutex
	duckLocks map[string]*sync.RWMutex
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
		client:    client,
		dataDir:   dataDir,
		syncLocks: make(map[string]*sync.Mutex),
		duckLocks: make(map[string]*sync.RWMutex),
	}
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
	return s.runSync(ctx, accessToken, base, nil)
}

func (s *Service) ListSchema(ctx context.Context, accessToken, baseRef string) (duckdb.BaseSchema, error) {
	base, err := s.ensureSynced(ctx, accessToken, baseRef)
	if err != nil {
		return duckdb.BaseSchema{}, err
	}

	unlock := s.lockDuckRead(base.ID)
	defer unlock()
	return duckdb.ReadSchema(ctx, s.basePath(base.ID), base.ID, base.Name)
}

func (s *Service) QueryBase(ctx context.Context, accessToken, baseRef, query string) (duckdb.QueryResult, error) {
	base, err := s.ensureSynced(ctx, accessToken, baseRef)
	if err != nil {
		return duckdb.QueryResult{}, err
	}

	unlock := s.lockDuckRead(base.ID)
	defer unlock()
	return duckdb.Query(ctx, s.basePath(base.ID), query)
}

func (s *Service) ReadTableRowsByIDs(ctx context.Context, baseID, tableName string, ids []string) ([]map[string]any, error) {
	unlock := s.lockDuckRead(baseID)
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

	bases, err := s.client.ListBases(ctx, accessToken)
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

func (s *Service) lockDuckRead(baseID string) func() {
	s.mu.Lock()
	lock, ok := s.duckLocks[baseID]
	if !ok {
		lock = &sync.RWMutex{}
		s.duckLocks[baseID] = lock
	}
	s.mu.Unlock()

	lock.RLock()
	return lock.RUnlock
}

func (s *Service) lockDuckWrite(baseID string) func() {
	s.mu.Lock()
	lock, ok := s.duckLocks[baseID]
	if !ok {
		lock = &sync.RWMutex{}
		s.duckLocks[baseID] = lock
	}
	s.mu.Unlock()

	lock.Lock()
	return lock.Unlock
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

func (s *Service) runSync(ctx context.Context, accessToken string, base Base, progress func(SyncProgress)) (SyncResult, error) {
	startedAt := time.Now().UTC()
	tables, err := s.client.GetBaseSchema(ctx, accessToken, base.ID)
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

	var unlockDuck func()
	if !useStaging {
		unlockDuck = s.lockDuckWrite(base.ID)
		if err := duckdb.InitializeSnapshot(ctx, targetPath, init); err != nil {
			unlockDuck()
			return fail(err)
		}
		unlockDuck()
	} else {
		if err := duckdb.InitializeSnapshot(ctx, targetPath, init); err != nil {
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
					page, err := s.client.ListRecordsPage(workerCtx, accessToken, base.ID, runtime.Plan.Table.AirtableTableID, ListRecordsPageOptions{
						Offset:        job.Offset,
						SortFieldName: runtime.Plan.Sort.SortFieldName,
						SortDirection: runtime.Plan.Sort.SortDirection,
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
		for pendingTables > 0 {
			result := <-results
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

			writePath := targetPath
			if !useStaging {
				unlockDuck = s.lockDuckWrite(base.ID)
			}
			err = duckdb.ApplyTablePage(ctx, writePath, runtime.Plan.Table, result.Records, tableState, syncInfo)
			if !useStaging {
				unlockDuck()
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

			if progress != nil {
				progress(currentInfo)
			}

			if result.NextOffset != "" {
				jobs <- syncJob{TableIndex: result.TableIndex, Offset: result.NextOffset}
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
		unlockDuck = s.lockDuckWrite(base.ID)
		err = duckdb.FinalizeSnapshot(ctx, targetPath, finalInfo)
		unlockDuck()
		if err != nil {
			return fail(err)
		}
	} else {
		if err := duckdb.FinalizeSnapshot(ctx, targetPath, finalInfo); err != nil {
			_ = os.Remove(targetPath)
			return fail(err)
		}
		unlockDuck = s.lockDuckWrite(base.ID)
		err = swapDatabaseFiles(targetPath, activePath)
		unlockDuck()
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
		fieldNameMap := make(map[string]string, len(sanitizedFieldNames))
		for index, sanitized := range sanitizedFieldNames {
			fieldNameMap[fieldNames[index]] = sanitized.Sanitized
		}

		fields := make([]duckdb.FieldSnapshot, 0, len(fieldNames))
		for _, field := range table.Fields {
			mapping, ok := duckdb.AirtableTypeToDuckDBType(field.Type)
			if !ok || mapping.Omitted {
				continue
			}
			fields = append(fields, duckdb.FieldSnapshot{
				AirtableFieldID:   field.ID,
				OriginalFieldName: field.Name,
				DuckDBColumnName:  fieldNameMap[field.Name],
				AirtableFieldType: field.Type,
				DuckDBType:        mapping.DuckDBType,
			})
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

	info, err := duckdb.ReadSyncInfo(context.Background(), s.basePath(baseID))
	if err != nil {
		return false
	}
	return !info.LastSyncedAt.IsZero() && info.Status == "completed"
}

func (s *Service) markActiveSyncFailed(ctx context.Context, baseID string, progress SyncProgress, syncErr error) error {
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
