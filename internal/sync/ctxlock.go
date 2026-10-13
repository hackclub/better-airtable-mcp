package syncer

import (
	"context"

	"golang.org/x/sync/semaphore"
)

// duckLockWeight is the semaphore capacity behind ctxRWLock: a reader takes
// one unit, a writer takes all of them. It only has to exceed any realistic
// number of concurrent readers of one base.
const duckLockWeight = 1 << 20

// ctxRWLock is a per-base reader/writer lock whose acquisition honours context
// cancellation. sync.RWMutex cannot: a request whose client had already given
// up kept waiting for the lock, then opened DuckDB anyway, and under load these
// abandoned waiters piled up for 20+ minutes (production, 2026-10-13).
//
// semaphore.Weighted serves waiters in FIFO order, so a waiting writer still
// holds back later readers (no writer starvation, same as sync.RWMutex).
type ctxRWLock struct {
	sem *semaphore.Weighted
}

func newCtxRWLock() *ctxRWLock {
	return &ctxRWLock{sem: semaphore.NewWeighted(duckLockWeight)}
}

func (l *ctxRWLock) RLock(ctx context.Context) error {
	return l.sem.Acquire(ctx, 1)
}

func (l *ctxRWLock) RUnlock() {
	l.sem.Release(1)
}

func (l *ctxRWLock) Lock(ctx context.Context) error {
	return l.sem.Acquire(ctx, duckLockWeight)
}

func (l *ctxRWLock) Unlock() {
	l.sem.Release(duckLockWeight)
}
