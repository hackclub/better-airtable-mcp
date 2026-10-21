package syncer

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

var errSyncStalled = errors.New("sync stalled")

// stallWatch cancels a run's context once Touch has not been called for the
// idle window. A wall-clock cap on the whole run cannot tell a stalled run
// from a large base that is still paging: the Unified YSWS DB needs 450+
// sequential pages for one table, and at Airtable's slower response times
// that alone took longer than the old 15 minute cap (production, 2026-10-13),
// so every run failed and the next one rebuilt the cache from scratch.
type stallWatch struct {
	last   atomic.Int64
	cancel context.CancelCauseFunc
	done   chan struct{}
}

func watchForStall(parent context.Context, idle time.Duration) (context.Context, *stallWatch) {
	ctx, cancel := context.WithCancelCause(parent)
	w := &stallWatch{cancel: cancel, done: make(chan struct{})}
	w.Touch()

	go func() {
		ticker := time.NewTicker(idle / 4)
		defer ticker.Stop()
		for {
			select {
			case <-w.done:
				return
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				if now.Sub(time.Unix(0, w.last.Load())) >= idle {
					cancel(fmt.Errorf("%w: no progress for %s", errSyncStalled, idle))
					return
				}
			}
		}
	}()

	return ctx, w
}

func (w *stallWatch) Touch() {
	w.last.Store(time.Now().UnixNano())
}

// Stop ends the watch and releases the context. Call it once the run returns.
func (w *stallWatch) Stop() {
	close(w.done)
	w.cancel(context.Canceled)
}
