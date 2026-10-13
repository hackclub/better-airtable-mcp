package syncer

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCtxRWLockReaderGivesUpWhenContextEnds(t *testing.T) {
	lock := newCtxRWLock()
	if err := lock.Lock(context.Background()); err != nil {
		t.Fatalf("Lock: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	startedAt := time.Now()
	err := lock.RLock(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded while a writer holds the lock, got %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed > 2*time.Second {
		t.Fatalf("reader should stop waiting at its deadline, waited %s", elapsed)
	}

	// The abandoned reader must not leak a unit: once the writer releases,
	// another writer can take the whole lock.
	lock.Unlock()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	if err := lock.Lock(ctx2); err != nil {
		t.Fatalf("writer could not reacquire after an abandoned reader: %v", err)
	}
	lock.Unlock()
}

func TestCtxRWLockAllowsConcurrentReadersAndExcludesWriter(t *testing.T) {
	lock := newCtxRWLock()
	ctx := context.Background()
	if err := lock.RLock(ctx); err != nil {
		t.Fatalf("first RLock: %v", err)
	}
	if err := lock.RLock(ctx); err != nil {
		t.Fatalf("second RLock: %v", err)
	}

	writerCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	if err := lock.Lock(writerCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("writer should wait while readers hold the lock, got %v", err)
	}

	lock.RUnlock()
	lock.RUnlock()
	if err := lock.Lock(ctx); err != nil {
		t.Fatalf("writer after readers released: %v", err)
	}
	lock.Unlock()
}

func TestQueryResolvedBaseStopsWaitingWhenCallerGivesUp(t *testing.T) {
	service := NewService(nil, t.TempDir())
	unlock, err := service.lockDuckWrite(context.Background(), "appBusy")
	if err != nil {
		t.Fatalf("lockDuckWrite: %v", err)
	}
	defer unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err = service.QueryResolvedBase(ctx, "appBusy", "SELECT 1")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected the query to give up with the caller's deadline, got %v", err)
	}
}

func TestSyncSlotsCapConcurrentSyncs(t *testing.T) {
	service := NewService(nil, t.TempDir())
	service.SetMaxConcurrentSyncs(2)
	ctx := context.Background()

	release1, err := service.acquireSyncSlot(ctx)
	if err != nil {
		t.Fatalf("slot 1: %v", err)
	}
	release2, err := service.acquireSyncSlot(ctx)
	if err != nil {
		t.Fatalf("slot 2: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	if _, err := service.acquireSyncSlot(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("third sync should wait for a slot, got %v", err)
	}

	release1()
	release3, err := service.acquireSyncSlot(ctx)
	if err != nil {
		t.Fatalf("slot after release: %v", err)
	}
	release2()
	release3()
}
