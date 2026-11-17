package syncer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
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

	// stallTimeout is syncStallTimeout, a field so tests can shorten it.
	stallTimeout time.Duration

	// reconcileInterval is how old a base's last full fetch may get before
	// another one is scheduled. Zero disables scheduled reconciles.
	reconcileInterval time.Duration
	// reconcileRate caps a scheduled reconcile's Airtable requests per second.
	reconcileRate float64
	// freshenWait bounds how long a read waits for an in-flight delta.
	freshenWait time.Duration
	// cacheMaxBytes is the disk budget for cache files. Zero means no limit.
	cacheMaxBytes int64

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
	rerunRequested       bool // a sync was requested while one was in progress; run once more after it completes
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

	lastFailedAt        time.Time
	consecutiveFailures int

	// fullRequested makes the next requested sync a full one.
	fullRequested bool
	// runningMode is the mode of the run in progress.
	runningMode string
	// lastFreshAt is when the newest successful sync of any kind started:
	// the cache holds every edit made up to then.
	lastFreshAt time.Time
	// oldestReconciledAt is the oldest table's last full fetch.
	oldestReconciledAt time.Time
	// cacheStale means some table is flagged as lagging until a full fetch.
	cacheStale bool
	// reconcileRetryAt holds off another scheduled reconcile after one failed.
	reconcileRetryAt time.Time
	// idleReconcileTried stops an expired worker retrying its reconcile.
	idleReconcileTried bool
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

		stallTimeout: syncStallTimeout,

		reconcileInterval: defaultReconcileInterval,
		reconcileRate:     defaultReconcileRate,
		freshenWait:       defaultFreshenWait,
	}
}

const (
	defaultReconcileInterval = 6 * time.Hour
	// defaultReconcileRate leaves most of Airtable's 5 requests per second
	// per base to interactive work and to every other integration.
	defaultReconcileRate = 2.0
	defaultFreshenWait   = 5 * time.Second
	reconcileRetryDelay  = 30 * time.Minute
)

// SetReconcileInterval changes how often a base in use gets a full fetch.
func (m *Manager) SetReconcileInterval(interval time.Duration) {
	m.reconcileInterval = interval
}

// SetCacheMaxBytes sets the disk budget for cache files.
func (m *Manager) SetCacheMaxBytes(limit int64) {
	m.cacheMaxBytes = limit
}

// Sync modes a worker run can take.
const (
	syncModeAuto      = "auto"      // delta when the cache allows it, otherwise a full sync
	syncModeFull      = "full"      // a full sync at full speed, asked for explicitly
	syncModeReconcile = "reconcile" // a scheduled full fetch at the reconcile rate
)

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

// SweepStaleDuckDBFiles runs at startup. Cache files are kept across idle
// periods and restarts, so the sweep only removes staging files a crashed
// refresh left behind and then enforces the disk budget.
func (m *Manager) SweepStaleDuckDBFiles(ctx context.Context) error {
	if m == nil || m.service == nil {
		return nil
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
		name := entry.Name()
		if entry.IsDir() || !(strings.HasSuffix(name, ".staging.db") || strings.HasSuffix(name, ".staging.db.wal")) {
			continue
		}
		if err := os.Remove(filepath.Join(m.service.dataDir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			removeErr = errors.Join(removeErr, fmt.Errorf("remove stale staging file %s: %w", name, err))
		}
	}

	return errors.Join(removeErr, m.enforceCacheBudget(ctx))
}

// enforceCacheBudget deletes the least recently used cache files until the
// directory fits the disk budget. Bases with a live worker are never evicted.
func (m *Manager) enforceCacheBudget(ctx context.Context) error {
	if m == nil || m.service == nil || m.cacheMaxBytes <= 0 {
		return nil
	}

	entries, err := os.ReadDir(m.service.dataDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read duckdb data dir: %w", err)
	}

	type cacheFile struct {
		baseID   string
		bytes    int64
		lastUsed time.Time
	}
	files := map[string]*cacheFile{}
	var total int64
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || strings.Contains(name, ".staging.") {
			continue
		}
		baseID, isCache := strings.CutSuffix(strings.TrimSuffix(name, ".wal"), ".db")
		if !isCache {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		file := files[baseID]
		if file == nil {
			file = &cacheFile{baseID: baseID}
			files[baseID] = file
		}
		file.bytes += info.Size()
		if info.ModTime().After(file.lastUsed) {
			file.lastUsed = info.ModTime()
		}
		total += info.Size()
	}
	if total <= m.cacheMaxBytes {
		return nil
	}

	if m.store != nil {
		if usage, err := m.store.ListCacheUsage(ctx); err == nil {
			for _, entry := range usage {
				if file := files[entry.BaseID]; file != nil {
					file.lastUsed = entry.LastUsedAt
				}
			}
		}
	}
	candidates := make([]*cacheFile, 0, len(files))
	m.mu.Lock()
	for baseID, file := range files {
		if _, active := m.workers[baseID]; !active {
			candidates = append(candidates, file)
		}
	}
	m.mu.Unlock()
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].lastUsed.Before(candidates[j].lastUsed) })

	var removeErr error
	for _, file := range candidates {
		if total <= m.cacheMaxBytes {
			break
		}
		unlock, err := m.service.lockDuckWrite(ctx, file.baseID)
		if err != nil {
			return errors.Join(removeErr, err)
		}
		// A worker may have started for this base since the candidates were
		// listed; its cache is in use again.
		m.mu.Lock()
		_, active := m.workers[file.baseID]
		m.mu.Unlock()
		if active {
			unlock()
			continue
		}
		path := m.service.basePath(file.baseID)
		for _, target := range []string{path, path + ".wal"} {
			if err := os.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
				removeErr = errors.Join(removeErr, fmt.Errorf("evict cache file %s: %w", filepath.Base(target), err))
			}
		}
		unlock()
		total -= file.bytes
		logx.Event(ctx, "sync_manager", "sync.cache_evicted",
			"base_id", file.baseID,
			"cache_bytes", file.bytes,
			"last_used_at", file.lastUsed.UTC().Format(time.RFC3339),
		)
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
		// The cache answers immediately; give a delta a short, bounded
		// chance to land first so the answer includes recent edits.
		worker.waitForFresh(ctx, m.interval, m.freshenWait)
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

// RequestFullSync is RequestSync for a caller that wants every record
// downloaded again instead of a delta.
func (m *Manager) RequestFullSync(ctx context.Context, userID, baseRef string) (SyncOperationStatus, error) {
	base, worker, err := m.activateBase(ctx, userID, baseRef)
	if err != nil {
		return SyncOperationStatus{}, err
	}
	worker.requestFullSync()
	status := worker.snapshotStatus()
	status.BaseID = base.ID
	status.BaseName = base.Name
	return status, nil
}

// RefreshRecords writes records an approved mutation just changed straight
// into the cache, so the next read sees them without waiting for a sync. A
// cache that cannot be patched in place is left to the follow-up sync.
func (m *Manager) RefreshRecords(ctx context.Context, userID, baseID, tableID string, changedIDs, deletedIDs []string) error {
	if m == nil || m.service == nil || m.tokens == nil {
		return fmt.Errorf("sync manager is not configured")
	}
	tokens := func(ctx context.Context) (string, error) {
		return m.tokens.AirtableAccessToken(ctx, userID)
	}
	err := m.service.RefreshRecords(ctx, tokens, Base{ID: baseID}, tableID, changedIDs, deletedIDs)
	if err != nil {
		logx.Event(ctx, "sync_manager", "sync.write_through_failed",
			"base_id", baseID,
			"error_kind", logx.ErrorKind(err),
			"error_message", logx.ErrorPreview(err),
		)
		return err
	}
	logx.Event(ctx, "sync_manager", "sync.write_through_completed",
		"base_id", baseID,
		"changed_records", len(changedIDs),
		"deleted_records", len(deletedIDs),
	)
	return nil
}

func (m *Manager) TriggerSync(ctx context.Context, userID, baseID string) error {
	_, worker, err := m.activateBase(ctx, userID, baseID)
	if err != nil {
		return err
	}
	worker.requestFollowUpSync()
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

	activeUntil := m.now().Add(m.ttl)
	worker := m.getOrCreateWorker(base, userID, activeUntil)
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

// getOrCreateWorker returns the base's worker, recording the caller and its
// activity window on it. A new worker gets them before its goroutine starts:
// started first, it could see a zero activeUntil, judge itself expired, delete
// the base's cache file and exit while the caller waited on it.
func (m *Manager) getOrCreateWorker(base Base, userID string, activeUntil time.Time) *workerState {
	m.mu.Lock()
	if worker, ok := m.workers[base.ID]; ok {
		worker.baseName = base.Name
		m.mu.Unlock()
		worker.touch(userID, activeUntil)
		return worker
	}
	defer m.mu.Unlock()

	worker := &workerState{
		baseID:   base.ID,
		baseName: base.Name,
		opID:     "sync_" + base.ID,
		manager:  m,
		wakeCh:   make(chan struct{}, 1),
	}
	worker.seedFromSnapshot()
	worker.syncTokenUserID = userID
	worker.activeUntil = activeUntil.UTC()
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
		// Restore before starting, for the same reason as getOrCreateWorker.
		worker.restoreFromState(state)
		go worker.run()
	} else {
		worker.restoreFromState(state)
	}
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
		shouldSync, waitFor, userID, mode := w.nextAction(now)
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

		runCtx, cancelRun := context.WithTimeout(context.Background(), syncRunTimeout)
		outcome, err := w.manager.syncOnce(runCtx, userID, w.baseID, w.baseName, mode, w.handleProgress)
		cancelRun()
		completedAt := w.manager.now().UTC()
		result := outcome.Result

		w.mu.Lock()
		w.inProgress = false
		w.runningMode = ""
		if w.rerunRequested {
			w.rerunRequested = false
			w.syncRequested = true
		}
		w.lastCompletedAt = &completedAt
		if err != nil {
			w.lastError = err.Error()
			w.lastFailedAt = completedAt
			w.consecutiveFailures++
			if mode == syncModeReconcile {
				// A reconcile that keeps failing must not crowd out the
				// deltas that keep the cache fresh.
				w.reconcileRetryAt = completedAt.Add(reconcileRetryDelay)
			}
			if w.readSnapshotComplete {
				// A failed refresh should not block readers when a complete snapshot already exists.
				// Fall back to the existing snapshot and retry on the normal schedule.
				w.needsInitialRefresh = false
			}
		} else {
			w.lastError = ""
			w.lastFailedAt = time.Time{}
			w.consecutiveFailures = 0
			w.readable = true
			w.readSnapshotComplete = true
			w.needsInitialRefresh = false
			w.lastFreshAt = startedAt
			w.oldestReconciledAt = outcome.OldestReconciledAt
			w.cacheStale = outcome.CacheStale
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

// reconcileOverdue reports how far past its reconcile interval the base is.
// Callers must hold w.mu.
func (w *workerState) reconcileOverdue(now time.Time) (overdue time.Duration, due bool) {
	interval := w.manager.reconcileInterval
	if interval <= 0 || w.oldestReconciledAt.IsZero() || !w.readSnapshotComplete || now.Before(w.reconcileRetryAt) {
		return 0, false
	}
	age := now.Sub(w.oldestReconciledAt)
	// A table flagged stale wants its full fetch at the next idle moment,
	// whatever its age; it does not make an in-use base more overdue.
	return age - interval, age >= interval || w.cacheStale
}

func (w *workerState) nextAction(now time.Time) (shouldSync bool, waitFor time.Duration, userID string, mode string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	start := func(mode string) (bool, time.Duration, string, string) {
		w.inProgress = true
		w.syncRequested = false
		w.lastError = ""
		w.runningMode = mode
		return true, 0, w.syncTokenUserID, mode
	}

	if !now.Before(w.activeUntil) {
		// Nobody has used the base for a whole activity window. If its full
		// fetch is due, this is the moment to run it: no reader is waiting on
		// a delta. The worker exits afterwards either way.
		if _, due := w.reconcileOverdue(now); due && !w.inProgress && !w.idleReconcileTried && w.consecutiveFailures == 0 {
			w.idleReconcileTried = true
			return start(syncModeReconcile)
		}
		return false, 0, "", ""
	}

	if !w.inProgress {
		// A manual sync request always runs immediately, overriding any backoff.
		if w.syncRequested {
			mode := syncModeAuto
			if w.fullRequested {
				mode = syncModeFull
				w.fullRequested = false
			}
			return start(mode)
		}

		// After a failed run, wait out an exponential backoff before retrying so a
		// persistent failure (inaccessible base, unwritable storage, repeated 429s)
		// cannot spin against the Airtable API. This gates every automatic retry
		// path below, including the "no snapshot file yet" case that would otherwise
		// re-fire immediately.
		if backoff := w.failureBackoff(); backoff > 0 {
			if remaining := w.lastFailedAt.Add(backoff).Sub(now); remaining > 0 {
				return false, minDuration(remaining, w.activeUntil.Sub(now)), "", ""
			}
		}

		dueForRefresh := w.lastStartedAt.IsZero() || !now.Before(w.lastStartedAt.Add(w.manager.interval))
		if !duckdb.DatabaseFileExists(w.manager.service.basePath(w.baseID)) || w.needsInitialRefresh || dueForRefresh {
			// A base in constant use never goes idle, so its reconcile runs
			// here once it is a full interval overdue.
			if overdue, due := w.reconcileOverdue(now); due && overdue >= w.manager.reconcileInterval && !w.needsInitialRefresh {
				return start(syncModeReconcile)
			}
			return start(syncModeAuto)
		}
	}

	nextDue := w.activeUntil.Sub(now)
	if !w.lastStartedAt.IsZero() {
		if dueIn := w.lastStartedAt.Add(w.manager.interval).Sub(now); dueIn < nextDue {
			nextDue = dueIn
		}
	}
	// Wake when the failure backoff expires, if that comes first.
	if backoff := w.failureBackoff(); backoff > 0 {
		if remaining := w.lastFailedAt.Add(backoff).Sub(now); remaining > 0 && remaining < nextDue {
			nextDue = remaining
		}
	}

	return false, nextDue, "", ""
}

const (
	failureBackoffBase = 2 * time.Second
	failureBackoffMax  = 5 * time.Minute

	// syncStallTimeout fails a run that has written no page for this long,
	// turning an indefinitely stalled run into a normal retriable failure.
	// It must exceed the slowest legitimate page: one Airtable request with
	// every retry is up to 6 x 30s timeouts plus 5 x 90s backoffs (~10.5m).
	syncStallTimeout = 15 * time.Minute

	// syncRunTimeout is only a backstop behind the stall watch. Large bases
	// page through one table sequentially and legitimately run for tens of
	// minutes when Airtable is slow, so this must stay well above that.
	syncRunTimeout = 3 * time.Hour
)

// failureBackoff returns how long to wait after the last failed run before the
// next automatic retry, growing exponentially with consecutive failures and
// capped at failureBackoffMax. Returns 0 when there is no active failure.
// Callers must hold w.mu.
func (w *workerState) failureBackoff() time.Duration {
	if w.consecutiveFailures <= 0 || w.lastFailedAt.IsZero() {
		return 0
	}
	backoff := failureBackoffBase
	for i := 1; i < w.consecutiveFailures; i++ {
		backoff *= 2
		if backoff >= failureBackoffMax {
			return failureBackoffMax
		}
	}
	return backoff
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// cleanupExpired retires an idle worker. The cache file stays on disk: the
// next tool call reads it immediately and only fetches what changed since.
func (w *workerState) cleanupExpired() {
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
	info, err := w.manager.service.readSyncInfo(context.Background(), w.baseID)
	if err != nil {
		return
	}
	w.recordsVisible = info.TotalRecords
	w.recordsSyncedThisRun = info.RecordsSyncedThisRun
	w.tablesTotal = info.TotalTables
	if !info.LastSyncedAt.IsZero() && info.Status == "completed" {
		w.readSnapshotComplete = true
		w.needsInitialRefresh = true
		w.lastFreshAt = info.LastSyncedAt
		w.oldestReconciledAt, w.cacheStale = w.manager.service.reconcileState(context.Background(), w.baseID)
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
	w.idleReconcileTried = false
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
	switch progress.ReadSnapshot {
	case "complete":
		w.readSnapshotComplete = true
	case "partial":
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

// requestFullSync asks for a full download instead of a delta. If a run is
// already in progress the full sync follows it.
func (w *workerState) requestFullSync() {
	w.mu.Lock()
	w.fullRequested = true
	if w.inProgress {
		w.rerunRequested = true
	} else {
		w.syncRequested = true
	}
	w.mu.Unlock()
	w.notify()
}

// waitForFresh gives a complete cache a bounded chance to catch up before a
// read: if the last successful sync started longer than maxAge ago it asks
// for a delta and waits up to maxWait for one to finish. It never fails the
// read; a delta that is slow or broken just means the read is served from
// what the cache already holds.
func (w *workerState) waitForFresh(ctx context.Context, maxAge, maxWait time.Duration) {
	if maxWait <= 0 {
		return
	}
	now := w.manager.now()
	w.mu.Lock()
	complete := w.readable && w.readSnapshotComplete
	freshAt := w.lastFreshAt
	fresh := !freshAt.IsZero() && now.Sub(freshAt) < maxAge
	skip := !complete || fresh || (w.inProgress && w.runningMode != syncModeAuto) || w.failureBackoff() > 0
	if !skip && !w.inProgress {
		w.syncRequested = true
	}
	failures := w.consecutiveFailures
	w.mu.Unlock()
	if skip {
		return
	}
	w.notify()

	deadline := time.NewTimer(maxWait)
	defer deadline.Stop()
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-deadline.C:
			return
		case <-tick.C:
			w.mu.Lock()
			done := w.lastFreshAt.After(freshAt) || w.consecutiveFailures > failures ||
				(w.inProgress && w.runningMode != syncModeAuto)
			w.mu.Unlock()
			if done {
				return
			}
		}
	}
}

// requestFollowUpSync is requestImmediateSync for callers that just changed
// data in Airtable. An in-flight sync started before the change, so it may
// not capture it; instead of dropping the request, run once more after the
// current sync completes. Multiple requests during one in-flight sync still
// coalesce into a single follow-up run.
func (w *workerState) requestFollowUpSync() {
	w.mu.Lock()
	if w.inProgress {
		w.rerunRequested = true
	} else {
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

// syncOutcome is what one worker run produced.
type syncOutcome struct {
	Result             SyncResult
	Mode               string // "delta" or "full": what actually ran
	OldestReconciledAt time.Time
	CacheStale         bool
}

func (m *Manager) syncOnce(ctx context.Context, userID, baseID, baseName, mode string, progress func(SyncProgress)) (syncOutcome, error) {
	logx.Event(ctx, "sync_manager", "sync.run_started",
		"user_id", userID,
		"base_id", baseID,
		"mode", mode,
	)
	fail := func(err error, message string) (syncOutcome, error) {
		logx.Event(ctx, "sync_manager", "sync.run_failed",
			"user_id", userID,
			"base_id", baseID,
			"error_kind", logx.ErrorKind(err),
			"error_message", message,
		)
		return syncOutcome{}, err
	}
	// Fail fast when the user's Airtable authorization is gone, before
	// queueing for a sync slot.
	if _, err := m.tokens.AirtableAccessToken(ctx, userID); err != nil {
		return fail(err, logx.ErrorPreview(err))
	}

	releaseSlot, err := m.service.acquireSyncSlot(ctx)
	if err != nil {
		return fail(err, "timed out waiting for a sync slot: "+logx.ErrorPreview(err))
	}
	defer releaseSlot()

	// Resolve the token per Airtable request: the refresh loop may rotate it
	// mid-run, and Airtable revokes the old one immediately.
	tokens := func(ctx context.Context) (string, error) {
		return m.tokens.AirtableAccessToken(ctx, userID)
	}
	// The stall watch starts after the slot is acquired, so time spent queued
	// behind other syncs never counts as a stall.
	stallTimeout := m.stallTimeout
	if stallTimeout <= 0 {
		stallTimeout = syncStallTimeout
	}
	runCtx, stall := watchForStall(ctx, stallTimeout)
	defer stall.Stop()
	withStallCause := func(err error) error {
		if err != nil && !errors.Is(err, errSyncStalled) && errors.Is(context.Cause(runCtx), errSyncStalled) {
			return fmt.Errorf("%w: %w", context.Cause(runCtx), err)
		}
		return err
	}
	base := Base{ID: baseID, Name: baseName}

	outcome := syncOutcome{Mode: "full"}
	ranDelta := false
	if mode == syncModeAuto {
		delta, err := m.service.runDelta(runCtx, tokens, base, stall.Touch)
		if err = withStallCause(err); err != nil {
			return fail(err, logx.ErrorPreview(err))
		}
		if delta.NeedsFull {
			logx.Event(ctx, "sync_manager", "sync.delta_needs_full",
				"base_id", baseID,
				"reason", delta.Reason,
			)
		} else {
			ranDelta = true
			outcome.Mode = "delta"
			outcome.Result = SyncResult{
				BaseID:        baseID,
				BaseName:      baseName,
				LastSyncedAt:  delta.CheckedAt,
				TablesSynced:  delta.TablesChecked,
				RecordsSynced: int(delta.TotalRecords),
				SyncDuration:  delta.Duration,
			}
		}
	}
	if !ranDelta {
		options := syncRunOptions{}
		if mode == syncModeReconcile {
			options.requestsPerSecond = m.reconcileRate
		}
		result, err := m.service.runSync(runCtx, tokens, base, options, func(p SyncProgress) {
			stall.Touch()
			if progress != nil {
				progress(p)
			}
		})
		if err = withStallCause(err); err != nil {
			return fail(err, logx.ErrorPreview(err))
		}
		outcome.Result = result
	}

	result := outcome.Result
	outcome.OldestReconciledAt, outcome.CacheStale = m.service.reconcileState(ctx, baseID)
	if m.store != nil {
		lastSyncedAt := result.LastSyncedAt.UTC()
		durationMS := result.SyncDuration.Milliseconds()
		totalRecords := int64(result.RecordsSynced)
		totalTables := result.TablesSynced
		activeUntil := m.now().Add(m.ttl).UTC()
		syncTokenUserID := userID

		state := db.SyncState{
			BaseID:          result.BaseID,
			LastSyncedAt:    &lastSyncedAt,
			TotalRecords:    &totalRecords,
			TotalTables:     &totalTables,
			ActiveUntil:     &activeUntil,
			SyncTokenUserID: &syncTokenUserID,
		}
		if !ranDelta {
			// Only a full sync's duration is a meaningful estimate of how
			// long a full sync of this base takes.
			state.LastSyncDurationMS = &durationMS
		}
		_ = m.store.PutSyncState(ctx, state)

		var reconciledAt *time.Time
		if !outcome.OldestReconciledAt.IsZero() {
			reconciledAt = &outcome.OldestReconciledAt
		}
		_ = m.store.RecordCacheStats(ctx, result.BaseID, m.service.cacheFileBytes(result.BaseID), reconciledAt)
	}
	if !ranDelta {
		_ = m.enforceCacheBudget(ctx)
	}
	logx.Event(ctx, "sync_manager", "sync.run_completed",
		"user_id", userID,
		"base_id", result.BaseID,
		"mode", outcome.Mode,
		"tables_synced", result.TablesSynced,
		"records_synced", result.RecordsSynced,
		"sync_duration_ms", result.SyncDuration.Milliseconds(),
	)

	return outcome, nil
}

func timeValue(value *time.Time) string {
	if value == nil {
		return ""
	}
	return value.UTC().Format(time.RFC3339)
}

// Freshness says how current a base's cache is.
type Freshness struct {
	// State is "first_sync" while the base has never been fully downloaded
	// (reads see only what has loaded so far), "catching_up" when a complete
	// cache is known to lag, and "fresh" otherwise.
	State string
	// AsOf is the time up to which edits and new records are in the cache.
	AsOf time.Time
	// ReconciledAt is the time up to which deletions are verified.
	ReconciledAt time.Time
	// TablesCatchingUp lists tables whose computed columns may lag.
	TablesCatchingUp []string
}

const (
	FreshnessFresh      = "fresh"
	FreshnessCatchingUp = "catching_up"
	FreshnessFirstSync  = "first_sync"
)

// Freshness reports how current the cache of a base is. ok is false when the
// base has no cache file at all.
func (m *Manager) Freshness(ctx context.Context, baseID string) (Freshness, bool) {
	if m == nil || m.service == nil {
		return Freshness{}, false
	}
	state, ok, err := m.service.readCacheState(ctx, baseID)
	if err != nil || !ok {
		return Freshness{}, false
	}
	if state.Info.Status != "completed" || state.Info.LastSyncedAt.IsZero() || state.CacheFormat != duckdb.CacheFormat {
		// Either the first download is still running, or the file predates
		// delta sync and is about to be rebuilt; a partial or unverified
		// cache is reported the same way.
		freshness := Freshness{State: FreshnessFirstSync}
		m.mu.Lock()
		worker := m.workers[baseID]
		m.mu.Unlock()
		if worker != nil && worker.isCompleteReadable() {
			freshness.State = FreshnessCatchingUp
			freshness.AsOf = state.Info.LastSyncedAt
		}
		return freshness, true
	}

	// AsOf was stamped with the service's clock, so age is measured on it too.
	freshness := Freshness{State: FreshnessFresh, AsOf: state.Info.LastSyncedAt}
	for _, table := range state.Tables {
		if table.ComputedStale {
			freshness.TablesCatchingUp = append(freshness.TablesCatchingUp, table.TableName)
		}
		if !table.ReconciledAt.IsZero() && (freshness.ReconciledAt.IsZero() || table.ReconciledAt.Before(freshness.ReconciledAt)) {
			freshness.ReconciledAt = table.ReconciledAt
		}
	}
	if len(freshness.TablesCatchingUp) > 0 || (m.interval > 0 && m.service.now().Sub(freshness.AsOf) > 2*m.interval+m.freshenWait) {
		freshness.State = FreshnessCatchingUp
	}
	return freshness, true
}
