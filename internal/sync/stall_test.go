package syncer

import (
	"context"
	"errors"
	"testing"
	"time"
)

// hangingPageClient serves a schema, then blocks every page request until the
// caller's context ends, the way a slow Airtable request does when the run's
// deadline expires mid-flight.
type hangingPageClient struct{}

func (hangingPageClient) ListBases(context.Context, string) ([]Base, error) { return nil, nil }

func (hangingPageClient) ListRecords(context.Context, string, string, string) ([]Record, error) {
	return nil, nil
}

func (hangingPageClient) GetBaseSchema(context.Context, string, string) ([]Table, error) {
	return []Table{{
		ID:     "tblRecords",
		Name:   "Records",
		Fields: []Field{{ID: "fldName", Name: "Name", Type: "singleLineText"}},
	}}, nil
}

func (hangingPageClient) ListRecordsPage(ctx context.Context, _, _, _ string, _ ListRecordsPageOptions) (ListRecordsPageResult, error) {
	<-ctx.Done()
	return ListRecordsPageResult{}, ctx.Err()
}

func TestStallWatchCancelsWithoutProgress(t *testing.T) {
	ctx, watch := watchForStall(context.Background(), 40*time.Millisecond)
	defer watch.Stop()

	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("stall watch did not cancel an idle run")
	}
	if !errors.Is(context.Cause(ctx), errSyncStalled) {
		t.Fatalf("cause = %v, want errSyncStalled", context.Cause(ctx))
	}
}

func TestStallWatchKeepsProgressingRunAlive(t *testing.T) {
	ctx, watch := watchForStall(context.Background(), 80*time.Millisecond)
	defer watch.Stop()

	// Run for several idle windows while touching well inside each one.
	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		watch.Touch()
		select {
		case <-ctx.Done():
			t.Fatalf("progressing run was cancelled: %v", context.Cause(ctx))
		case <-time.After(15 * time.Millisecond):
		}
	}
}

func TestStallWatchStopIsNotAStall(t *testing.T) {
	ctx, watch := watchForStall(context.Background(), time.Hour)
	watch.Stop()

	<-ctx.Done()
	if errors.Is(context.Cause(ctx), errSyncStalled) {
		t.Fatal("Stop reported a stall")
	}
}

// A run whose page fetch never returns must fail as a stall through the real
// syncOnce path, instead of hanging or waiting out the run's backstop cap.
func TestSyncOnceFailsStalledRun(t *testing.T) {
	service := NewService(hangingPageClient{}, t.TempDir())
	manager := NewManager(service, nil, staticTokenSource{}, time.Minute, time.Minute)
	manager.stallTimeout = 80 * time.Millisecond

	done := make(chan error, 1)
	go func() {
		_, err := manager.syncOnce(context.Background(), "usr1", "appStall", "Stall", nil)
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, errSyncStalled) {
			t.Fatalf("err = %v, want errSyncStalled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stalled run never failed")
	}
}
