package syncer

import (
	"context"
	"errors"
	"testing"
	"time"
)

// stallingRecordsClient never returns a records page until its context is
// done, like an Airtable call that is still in flight when the sync run's
// deadline hits.
type stallingRecordsClient struct {
	entered chan struct{}
}

func (stallingRecordsClient) ListBases(ctx context.Context, accessToken string) ([]Base, error) {
	return nil, nil
}

func (stallingRecordsClient) GetBaseSchema(ctx context.Context, accessToken, baseID string) ([]Table, error) {
	return []Table{{
		ID:     "tblStall",
		Name:   "Stall",
		Fields: []Field{{ID: "fldName", Name: "Name", Type: "singleLineText"}},
	}}, nil
}

func (c stallingRecordsClient) ListRecordsPage(ctx context.Context, accessToken, baseID, tableID string, options ListRecordsPageOptions) (ListRecordsPageResult, error) {
	select {
	case c.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return ListRecordsPageResult{}, ctx.Err()
}

func (stallingRecordsClient) ListRecords(ctx context.Context, accessToken, baseID, tableID string) ([]Record, error) {
	return nil, nil
}

// When the run deadline expires, a worker may take the workerCtx.Done() branch
// and exit without sending a result. runSync must still return instead of
// blocking forever on the results channel (which left the base stuck in
// "syncing" until the process restarted). The worker's choice is random, so
// run it several times to make a hang near-certain without the fix.
func TestRunSyncReturnsWhenRunDeadlineExpires(t *testing.T) {
	for attempt := 0; attempt < 20; attempt++ {
		client := stallingRecordsClient{entered: make(chan struct{}, 1)}
		service := NewService(client, t.TempDir())

		// Expire the run only once a records request is in flight, so the
		// deadline lands where it does in production rather than during setup.
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := service.runSync(ctx, StaticAccessToken("token"), Base{ID: "appStall", Name: "Stall"}, syncRunOptions{}, nil)
			done <- err
		}()

		select {
		case <-client.entered:
		case <-time.After(5 * time.Second):
			cancel()
			t.Fatalf("attempt %d: no records request was made", attempt)
		}
		cancel()

		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("attempt %d: runSync error = %v, want context.Canceled", attempt, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("attempt %d: runSync hung after its context was done", attempt)
		}
	}
}
