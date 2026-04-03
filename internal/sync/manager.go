package syncer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/hackclub/better-airtable-mcp/internal/db"
	"github.com/hackclub/better-airtable-mcp/internal/duckdb"
	"github.com/hackclub/better-airtable-mcp/internal/logx"
)

type TokenSource interface {
	AirtableAccessToken(ctx context.Context, userID string) (string, error)
}

type Manager struct {
	service  *Service
	store    *db.Store
	tokens   TokenSource
	interval time.Duration
	ttl      time.Duration
	now      func() time.Time

	mu      sync.Mutex
	workers map[string]*workerState
}

type SyncOperationStatus struct {
	OperationID      string
	BaseID           string
	BaseName         string
	Type             string
	Status           string
	ReadSnapshot     string
	SyncStartedAt    *time.Time
	EstimatedSeconds int
	LastSyncedAt     *time.Time
	CompletedAt      *time.Time
	TablesTotal      int
	TablesStarted    int
	TablesSynced     int
	PagesFetched     int64
	RecordsVisible   int64
	RecordsSynced    int
	Error            string
}

type workerState struct {
	baseID   string
	baseName string
	opID     string

	manager *Manager
	wakeCh  chan struct{}

	mu                   sync.Mutex
	activeUntil          time.Time
	syncTokenUserID      string
	inProgress           bool
	syncRequested        bool
	readable             bool
	readSnapshotComplete bool
	needsInitialRefresh  bool
	lastStartedAt        time.Time
	lastCompletedAt      *time.Time
	lastResult           SyncResult
	lastError            string
	tablesTotal          int
	tablesStarted        int
	tablesCompleted      int
	pagesFetched         int64
	recordsVisible       int64
	recordsSyncedThisRun int64
}

func NewManager(service *Service, store *db.Store, tokens TokenSource, interval, ttl time.Duration) *Manager {
	return &Manager{
		service:  service,
		store:    store,
		tokens:   tokens,
		interval: interval,
		ttl:      ttl,
		now:      time.Now,
		workers:  make(map[string]*workerState),
	}
}

func (m *Manager) RestoreActiveWorkers(ctx context.Context) error {
	if m == nil || m.store == nil {
		return nil
	}

	states, err := m.store.ListActiveSyncStates(ctx, m.now().UTC())
	if err != nil {
		return err
	}

	for _, state := range states {
		if state.SyncTokenUserID == nil || strings.TrimSpace(*state.SyncTokenUserID) == "" {
			continue
		}
		m.restoreWorker(state)
	}

	return nil
}

func (m *Manager) SweepStaleDuckDBFiles(ctx context.Context) error {
	if m == nil || m.service == nil || m.store == nil {
		return nil
	}

	states, err := m.store.ListActiveSyncStates(ctx, m.now().UTC())
	if err != nil {
		return err
	}

	activeBaseIDs := make(map[string]struct{}, len(states))
	for _, state := range states {
		activeBaseIDs[state.BaseID] = struct{}{}
	}

	entries, err := os.ReadDir(m.service.dataDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read duckdb data dir: %w", err)
	}

	var removeErr error
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".db" {
			continue
		}

		baseID := strings.TrimSuffix(entry.Name(), ".db")
		if _, ok := activeBaseIDs[baseID]; ok {
			continue
		}

		if err := os.Remove(filepath.Join(m.service.dataDir, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			removeErr = errors.Join(removeErr, fmt.Errorf("remove stale duckdb file %s: %w", entry.Name(), err))
		}
	}

	return removeErr
}

func (m *Manager) EnsureBaseReady(ctx context.Context, userID, baseRef string) (Base, error) {
	base, worker, err := m.activateBase(ctx, userID, baseRef)
	if err != nil {
		return Base{}, err
	}

	if worker.isCompleteReadable() && !worker.requiresInitialRefresh() {
		return base, nil
	}

	hadSnapshot := duckdb.DatabaseFileExists(m.service.basePath(base.ID))
	worker.requestImmediateSync()
	if err := worker.waitUntilComplete(ctx); err != nil {
		if hadSnapshot {
			return base, nil
		}
		return Base{}, err
	}
	return base, nil
}

func (m *Manager) EnsureBaseReadable(ctx context.Context, userID, baseRef string) (Base, error) {
	base, worker, err := m.activateBase(ctx, userID, baseRef)
	if err != nil {
		return Base{}, err
	}

	if worker.isReadable() {
		return base, nil
	}

	worker.requestImmediateSync()
	if err := worker.waitUntilReadable(ctx); err != nil {
		return Base{}, err
	}
	return base, nil
}

func (m *Manager) EnsureBaseSchemaSampled(ctx context.Context, userID, baseRef string) (Base, error) {
	base, worker, err := m.activateBase(ctx, userID, baseRef)
	if err != nil {
		return Base{}, err
	}

	if worker.isSchemaSampled() {
		return base, nil
	}

	worker.requestImmediateSync()
	if err := worker.waitUntilSchemaSampled(ctx); err != nil {
		return Base{}, err
	}
	return base, nil
}

func (m *Manager) RequestSync(ctx context.Context, userID, baseRef string) (SyncOperationStatus, error) {
	base, worker, err := m.activateBase(ctx, userID, baseRef)
	if err != nil {
		return SyncOperationStatus{}, err
	}
	worker.requestImmediateSync()
	status := worker.snapshotStatus()
	status.BaseID = base.ID
	status.BaseName = base.Name
	return status, nil
}

func (m *Manager) TriggerSync(ctx context.Context, userID, baseID string) error {
	_, worker, err := m.activateBase(ctx, userID, baseID)
	if err != nil {
		return err
	}
	worker.requestImmediateSync()
	return nil
}

func (m *Manager) CheckOperation(ctx context.Context, operationID string) (SyncOperationStatus, bool, error) {
	baseID, ok := strings.CutPrefix(operationID, "sync_")
	if !ok || strings.TrimSpace(baseID) == "" {
		return SyncOperationStatus{}, false, nil
	}

	m.mu.Lock()
	worker := m.workers[baseID]
	m.mu.Unlock()
	if worker != nil {
		status := worker.snapshotStatus()
		status.BaseID = baseID
		return status, true, nil
	}

	if m.store == nil {
		return SyncOperationStatus{}, false, nil
	}

	state, err := m.store.GetSyncState(ctx, baseID)
	if err != nil {
		return SyncOperationStatus{}, false, nil
	}

	status := SyncOperationStatus{
		OperationID:  "sync_" + baseID,
		BaseID:       baseID,
		Type:         "sync",
		Status:       "completed",
		ReadSnapshot: "complete",
	}
	if state.LastSyncedAt != nil {
		status.LastSyncedAt = state.LastSyncedAt
		status.CompletedAt = state.LastSyncedAt
	}
	if state.LastSyncDurationMS != nil {
		status.EstimatedSeconds = int((*state.LastSyncDurationMS + 999) / 1000)
	}
	if state.TotalTables != nil {
		status.TablesTotal = *state.TotalTables
		status.TablesStarted = *state.TotalTables
		status.TablesSynced = *state.TotalTables
	}
	if state.TotalRecords != nil {
		status.RecordsVisible = *state.TotalRecords
		status.RecordsSynced = int(*state.TotalRecords)
	}

	return status, true, nil
}

func (m *Manager) BaseStatus(baseID string) (SyncOperationStatus, bool) {
	m.mu.Lock()
	worker := m.workers[baseID]
	m.mu.Unlock()
	if worker == nil {
		return SyncOperationStatus{}, false
	}
	status := worker.snapshotStatus()
	status.BaseID = baseID
	return status, true
}

func (m *Manager) activateBase(ctx context.Context, userID, baseRef string) (Base, *workerState, error) {
	if m == nil || m.service == nil || m.tokens == nil {
		return Base{}, nil, fmt.Errorf("sync manager is not configured")
	}

	accessToken, err := m.tokens.AirtableAccessToken(ctx, userID)
	if err != nil {
		return Base{}, nil, err
	}

	base, err := m.service.resolveBase(ctx, accessToken, baseRef)
	if err != nil {
		return Base{}, nil, err
	}
	if m.store != nil {
		_ = m.store.UpsertUserBaseAccess(ctx, db.UserBaseAccess{
			UserID:          userID,
			BaseID:          base.ID,
			PermissionLevel: base.PermissionLevel,
			LastVerifiedAt:  m.now().UTC(),
		})
	}

	worker := m.getOrCreateWorker(base)
	activeUntil := m.now().Add(m.ttl)
	worker.touch(userID, activeUntil)
	if m.store != nil {
		_ = m.store.TouchSyncState(ctx, base.ID, activeUntil.UTC(), userID)
	}
	logx.Event(ctx, "sync_manager", "sync.base_activated",
		"user_id", userID,
		"base_id", base.ID,
		"active_until", activeUntil.UTC().Format(time.RFC3339),
	)
	return base, worker, nil
}

func (m *Manager) getOrCreateWorker(base Base) *workerState {
	m.mu.Lock()
	defer m.mu.Unlock()

	if worker, ok := m.workers[base.ID]; ok {
		worker.baseName = base.Name
		return worker
	}

	worker := &workerState{
		baseID:   base.ID,
		baseName: base.Name,
		opID:     "sync_" + base.ID,
		manager:  m,
		wakeCh:   make(chan struct{}, 1),
	}
	worker.seedFromSnapshot()
	m.workers[base.ID] = worker
	logx.Event(context.Background(), "sync_manager", "sync.worker_created",
		"base_id", base.ID,
	)
	go worker.run()
	return worker
}

func (m *Manager) restoreWorker(state db.SyncState) {
	m.mu.Lock()
	defer m.mu.Unlock()

	worker, ok := m.workers[state.BaseID]
	if !ok {
		worker = &workerState{
			baseID:   state.BaseID,
			baseName: state.BaseID,
			opID:     "sync_" + state.BaseID,
			manager:  m,
			wakeCh:   make(chan struct{}, 1),
		}
		worker.seedFromSnapshot()
		m.workers[state.BaseID] = worker
		go worker.run()
	}

	worker.restoreFromState(state)
	logx.Event(context.Background(), "sync_manager", "sync.worker_restored",
		"base_id", state.BaseID,
		"active_until", timeValue(state.ActiveUntil),
	)
}

func (m *Manager) removeWorker(baseID string) {
	m.mu.Lock()
	delete(m.workers, baseID)
	m.mu.Unlock()
}

func (w *workerState) run() {
	for {
		now := w.manager.now()
		shouldSync, waitFor, userID := w.nextAction(now)
		if !shouldSync {
			if waitFor <= 0 {
				w.cleanupExpired()
				return
			}
			timer := time.NewTimer(waitFor)
			select {
			case <-timer.C:
			case <-w.wakeCh:
				if !timer.Stop() {
					<-timer.C
				}
			}
			continue
		}

		startedAt := now.UTC()
		w.mu.Lock()
		w.lastStartedAt = startedAt
		w.lastError = ""
		w.tablesTotal = 0
		w.tablesStarted = 0
		w.tablesCompleted = 0
		w.pagesFetched = 0
		w.recordsSyncedThisRun = 0
		w.mu.Unlock()

		result, err := w.manager.syncOnce(context.Background(), userID, w.baseID, w.baseName, w.handleProgress)
		completedAt := w.manager.now().UTC()

		w.mu.Lock()
		w.inProgress = false
		w.lastCompletedAt = &completedAt
		if err != nil {
			w.lastError = err.Error()
			if w.readSnapshotComplete {
				// A failed refresh should not block readers when a complete snapshot already exists.
				// Fall back to the existing snapshot and retry on the normal schedule.
				w.needsInitialRefresh = false
			}
		} else {
			w.lastError = ""
			w.readable = true
			w.readSnapshotComplete = true
			w.needsInitialRefresh = false
			w.lastResult = result
			w.baseName = result.BaseName
			w.tablesTotal = result.TablesSynced
			w.tablesStarted = result.TablesSynced
			w.tablesCompleted = result.TablesSynced
			w.recordsVisible = int64(result.RecordsSynced)
			w.recordsSyncedThisRun = int64(result.RecordsSynced)
		}
		w.mu.Unlock()
	}
}

func (w *workerState) nextAction(now time.Time) (shouldSync bool, waitFor time.Duration, userID string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !now.Before(w.activeUntil) {
		return false, 0, ""
	}

	if !w.inProgress && (!duckdb.DatabaseFileExists(w.manager.service.basePath(w.baseID)) || w.syncRequested || w.needsInitialRefresh || (!w.readSnapshotComplete && (w.lastStartedAt.IsZero() || !now.Before(w.lastStartedAt.Add(w.manager.interval)))) || (w.readSnapshotComplete && (w.lastStartedAt.IsZero() || !now.Before(w.lastStartedAt.Add(w.manager.interval))))) {
		w.inProgress = true
		w.syncRequested = false
		w.lastError = ""
		return true, 0, w.syncTokenUserID
	}

	nextDue := w.activeUntil.Sub(now)
	if !w.lastStartedAt.IsZero() {
		if dueIn := w.lastStartedAt.Add(w.manager.interval).Sub(now); dueIn < nextDue {
			nextDue = dueIn
		}
	}

	return false, nextDue, ""
}

func (w *workerState) cleanupExpired() {
	_ = os.Remove(w.manager.service.basePath(w.baseID))
	logx.Event(context.Background(), "sync_manager", "sync.worker_expired",
		"base_id", w.baseID,
	)
	w.manager.removeWorker(w.baseID)
}

func (w *workerState) seedFromSnapshot() {
	path := w.manager.service.basePath(w.baseID)
	if !duckdb.DatabaseFileExists(path) {
		return
	}

	w.readable = true
	info, err := duckdb.ReadSyncInfo(context.Background(), path)
	if err != nil {
		return
	}
	w.recordsVisible = info.TotalRecords
	w.recordsSyncedThisRun = info.RecordsSyncedThisRun
	w.tablesTotal = info.TotalTables
	if !info.LastSyncedAt.IsZero() && info.Status == "completed" {
		w.readSnapshotComplete = true
		w.needsInitialRefresh = true
		completedAt := info.LastSyncedAt
		w.lastCompletedAt = &completedAt
		w.lastResult = SyncResult{
			BaseID:        w.baseID,
			BaseName:      w.baseName,
			LastSyncedAt:  info.LastSyncedAt,
			TablesSynced:  info.TotalTables,
			RecordsSynced: int(info.TotalRecords),
			SyncDuration:  time.Duration(info.SyncDurationMS) * time.Millisecond,
		}
		w.tablesStarted = info.TotalTables
		w.tablesCompleted = info.TotalTables
	}
}

func (w *workerState) touch(userID string, activeUntil time.Time) {
	w.mu.Lock()
	w.syncTokenUserID = userID
	w.activeUntil = activeUntil.UTC()
	w.mu.Unlock()
	w.notify()
}

func (w *workerState) handleProgress(progress SyncProgress) {
	var checkpoint string
	var attrs []any
	w.mu.Lock()
	previousTablesStarted := w.tablesStarted
	previousTablesCompleted := w.tablesCompleted

	w.baseName = progress.BaseName
	w.readable = true
	w.tablesTotal = progress.TablesTotal
	w.tablesStarted = progress.TablesStarted
	w.tablesCompleted = progress.TablesCompleted
	w.pagesFetched = progress.PagesFetched
	w.recordsSyncedThisRun = progress.RecordsSyncedThisRun
	if !progress.SyncStartedAt.IsZero() {
		startedAt := progress.SyncStartedAt.UTC()
		w.lastStartedAt = startedAt
	}
	if progress.ReadSnapshot == "complete" {
		w.readSnapshotComplete = true
	} else if progress.ReadSnapshot == "partial" {
		w.readSnapshotComplete = false
		w.recordsVisible = progress.RecordsSyncedThisRun
	}
	if progress.Status == "completed" {
		w.readSnapshotComplete = true
		w.needsInitialRefresh = false
		w.recordsVisible = progress.RecordsSyncedThisRun
		if !progress.LastSyncedAt.IsZero() {
			w.lastResult.LastSyncedAt = progress.LastSyncedAt.UTC()
		}
		checkpoint = "sync_completed"
	} else if progress.Status == "syncing" && !w.readSnapshotComplete {
		w.needsInitialRefresh = false
	}
	if checkpoint == "" && progress.TablesCompleted > previousTablesCompleted {
		checkpoint = "table_completed"
	} else if checkpoint == "" && progress.TablesStarted > previousTablesStarted {
		checkpoint = "table_started"
	}
	if checkpoint != "" {
		attrs = []any{
			"base_id", progress.BaseID,
			"checkpoint", checkpoint,
			"status", progress.Status,
			"read_snapshot", progress.ReadSnapshot,
			"tables_total", progress.TablesTotal,
			"tables_started", progress.TablesStarted,
			"tables_completed", progress.TablesCompleted,
			"pages_fetched", progress.PagesFetched,
			"records_synced_this_run", progress.RecordsSyncedThisRun,
		}
	}
	w.mu.Unlock()
	if checkpoint != "" {
		logx.Event(context.Background(), "sync_manager", "sync.progress_checkpoint", attrs...)
	}
}

func (w *workerState) restoreFromState(state db.SyncState) {
	w.mu.Lock()
	if state.ActiveUntil != nil {
		w.activeUntil = state.ActiveUntil.UTC()
	}
	if state.SyncTokenUserID != nil {
		w.syncTokenUserID = *state.SyncTokenUserID
	}
	if state.LastSyncedAt != nil {
		completedAt := state.LastSyncedAt.UTC()
		w.lastCompletedAt = &completedAt
		w.lastResult.LastSyncedAt = completedAt
		w.readSnapshotComplete = duckdb.DatabaseFileExists(w.manager.service.basePath(w.baseID))
		w.readable = w.readSnapshotComplete
		if state.LastSyncDurationMS != nil {
			duration := time.Duration(*state.LastSyncDurationMS) * time.Millisecond
			w.lastResult.SyncDuration = duration
			startedAt := completedAt.Add(-duration)
			if startedAt.After(w.manager.now().UTC()) {
				startedAt = w.manager.now().UTC()
			}
			w.lastStartedAt = startedAt
		} else {
			w.lastStartedAt = completedAt
		}
	}
	if state.TotalTables != nil {
		w.lastResult.TablesSynced = *state.TotalTables
		w.tablesTotal = *state.TotalTables
		w.tablesStarted = *state.TotalTables
		w.tablesCompleted = *state.TotalTables
	}
	if state.TotalRecords != nil {
		w.lastResult.RecordsSynced = int(*state.TotalRecords)
		w.recordsVisible = *state.TotalRecords
	}
	w.lastResult.BaseID = state.BaseID
	if !w.readable {
		w.readable = duckdb.DatabaseFileExists(w.manager.service.basePath(w.baseID))
	}
	w.mu.Unlock()
	w.notify()
}

func (w *workerState) requestImmediateSync() {
	w.mu.Lock()
	if !w.inProgress {
		w.syncRequested = true
	}
	w.mu.Unlock()
	w.notify()
}

func (w *workerState) isReadable() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.readable
}

func (w *workerState) isCompleteReadable() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.readable && w.readSnapshotComplete
}

func (w *workerState) isSchemaSampled() bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.readable && w.readSnapshotComplete {
		return true
	}
	if !w.readable {
		return false
	}
	return w.tablesTotal == 0 || w.tablesStarted >= w.tablesTotal
}

func (w *workerState) requiresInitialRefresh() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.needsInitialRefresh
}

func (w *workerState) notify() {
	select {
	case w.wakeCh <- struct{}{}:
	default:
	}
}

func (w *workerState) waitUntilReadable(ctx context.Context) error {
	for {
		if w.isReadable() {
			return nil
		}

		w.mu.Lock()
		lastError := w.lastError
		inProgress := w.inProgress
		w.mu.Unlock()
		if lastError != "" && !inProgress {
			return fmt.Errorf("sync base %s: %s", w.baseID, lastError)
		}

		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (w *workerState) waitUntilComplete(ctx context.Context) error {
	for {
		if w.isCompleteReadable() && !w.requiresInitialRefresh() {
			return nil
		}

		w.mu.Lock()
		lastError := w.lastError
		inProgress := w.inProgress
		w.mu.Unlock()
		if lastError != "" && !inProgress {
			return fmt.Errorf("sync base %s: %s", w.baseID, lastError)
		}

		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (w *workerState) waitUntilSchemaSampled(ctx context.Context) error {
	for {
		if w.isSchemaSampled() {
			return nil
		}

		w.mu.Lock()
		lastError := w.lastError
		inProgress := w.inProgress
		w.mu.Unlock()
		if lastError != "" && !inProgress {
			return fmt.Errorf("sync base %s: %s", w.baseID, lastError)
		}

		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (w *workerState) snapshotStatus() SyncOperationStatus {
	w.mu.Lock()
	defer w.mu.Unlock()

	status := SyncOperationStatus{
		OperationID:  w.opID,
		BaseID:       w.baseID,
		BaseName:     w.baseName,
		Type:         "sync",
		Status:       "completed",
		ReadSnapshot: "partial",
	}
	if w.inProgress || w.syncRequested {
		status.Status = "syncing"
	}
	if w.lastError != "" && !w.inProgress {
		status.Status = "failed"
		status.Error = w.lastError
	}
	if w.readSnapshotComplete {
		status.ReadSnapshot = "complete"
	}
	if !w.lastStartedAt.IsZero() {
		startedAt := w.lastStartedAt
		status.SyncStartedAt = &startedAt
	}
	if !w.lastResult.LastSyncedAt.IsZero() {
		lastSyncedAt := w.lastResult.LastSyncedAt
		status.LastSyncedAt = &lastSyncedAt
	}
	if w.lastCompletedAt != nil {
		completedAt := *w.lastCompletedAt
		status.CompletedAt = &completedAt
	}
	status.TablesTotal = w.tablesTotal
	status.TablesStarted = w.tablesStarted
	status.TablesSynced = w.tablesCompleted
	status.PagesFetched = w.pagesFetched
	status.RecordsVisible = w.recordsVisible
	status.RecordsSynced = int(w.recordsSyncedThisRun)
	if w.lastResult.SyncDuration > 0 {
		status.EstimatedSeconds = int((w.lastResult.SyncDuration + time.Second - 1) / time.Second)
	}
	if status.EstimatedSeconds == 0 {
		status.EstimatedSeconds = 15
	}
	return status
}

func (m *Manager) syncOnce(ctx context.Context, userID, baseID, baseName string, progress func(SyncProgress)) (SyncResult, error) {
	logx.Event(ctx, "sync_manager", "sync.run_started",
		"user_id", userID,
		"base_id", baseID,
	)
	accessToken, err := m.tokens.AirtableAccessToken(ctx, userID)
	if err != nil {
		logx.Event(ctx, "sync_manager", "sync.run_failed",
			"user_id", userID,
			"base_id", baseID,
			"error_kind", logx.ErrorKind(err),
			"error_message", logx.ErrorPreview(err),
		)
		return SyncResult{}, err
	}

	result, err := m.service.runSync(ctx, accessToken, Base{ID: baseID, Name: baseName}, progress)
	if err != nil {
		logx.Event(ctx, "sync_manager", "sync.run_failed",
			"user_id", userID,
			"base_id", baseID,
			"error_kind", logx.ErrorKind(err),
			"error_message", logx.ErrorPreview(err),
		)
		return SyncResult{}, err
	}

	if m.store != nil {
		lastSyncedAt := result.LastSyncedAt.UTC()
		durationMS := result.SyncDuration.Milliseconds()
		totalRecords := int64(result.RecordsSynced)
		totalTables := result.TablesSynced
		activeUntil := m.now().Add(m.ttl).UTC()
		syncTokenUserID := userID

		_ = m.store.PutSyncState(ctx, db.SyncState{
			BaseID:             result.BaseID,
			LastSyncedAt:       &lastSyncedAt,
			LastSyncDurationMS: &durationMS,
			TotalRecords:       &totalRecords,
			TotalTables:        &totalTables,
			ActiveUntil:        &activeUntil,
			SyncTokenUserID:    &syncTokenUserID,
		})
	}
	logx.Event(ctx, "sync_manager", "sync.run_completed",
		"user_id", userID,
		"base_id", result.BaseID,
		"tables_synced", result.TablesSynced,
		"records_synced", result.RecordsSynced,
		"sync_duration_ms", result.SyncDuration.Milliseconds(),
	)

	return result, nil
}

func timeValue(value *time.Time) string {
	if value == nil {
		return ""
	}
	return value.UTC().Format(time.RFC3339)
}
