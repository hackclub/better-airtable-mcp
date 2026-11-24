package syncer

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hackclub/better-airtable-mcp/internal/logx"
)

func TestHTTPClientRetriesRateLimitedRequests(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"rate limited"}`))
			return
		}
		json.NewEncoder(w).Encode(map[string]any{
			"bases": []map[string]any{
				{"id": "app123", "name": "Test Base", "permissionLevel": "create"},
			},
		})
	}))
	defer server.Close()

	client := NewHTTPClient(server.URL, server.Client())
	client.clock = func() time.Time {
		return time.Unix(0, 0)
	}

	bases, err := client.ListBases(context.Background(), "token")
	if err != nil {
		t.Fatalf("ListBases() returned error: %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected 2 HTTP calls, got %d", calls)
	}
	if len(bases) != 1 || bases[0].ID != "app123" {
		t.Fatalf("unexpected bases %#v", bases)
	}
}

func TestAirtableBaseIDFromPath(t *testing.T) {
	tests := []struct {
		path   string
		baseID string
		ok     bool
	}{
		{path: "/v0/meta/bases/app123/tables", baseID: "app123", ok: true},
		{path: "/v0/app123/tbl456", baseID: "app123", ok: true},
		{path: "/v0/meta/bases", ok: false},
	}

	for _, test := range tests {
		baseID, ok := airtableBaseIDFromPath(test.path)
		if ok != test.ok || baseID != test.baseID {
			t.Fatalf("airtableBaseIDFromPath(%q) = (%q, %v), want (%q, %v)", test.path, baseID, ok, test.baseID, test.ok)
		}
	}
}

func TestHTTPClientAppliesPerUserRateLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"bases": []map[string]any{
				{"id": "app123", "name": "Test Base", "permissionLevel": "create"},
			},
		})
	}))
	defer server.Close()

	client := NewHTTPClient(server.URL, server.Client())
	now := time.Unix(0, 0)
	client.clock = func() time.Time {
		return now
	}
	var delays []time.Duration
	client.sleep = func(ctx context.Context, delay time.Duration) error {
		delays = append(delays, delay)
		now = now.Add(delay)
		return nil
	}

	for range 2 {
		if _, err := client.ListBases(context.Background(), "token-a"); err != nil {
			t.Fatalf("ListBases() returned error: %v", err)
		}
	}

	if len(delays) != 1 {
		t.Fatalf("expected one rate-limit sleep, got %d (%v)", len(delays), delays)
	}
	if delays[0] != 250*time.Millisecond {
		t.Fatalf("expected a 250ms wait for the user's next slot (4 per second), got %s", delays[0])
	}
}

func TestHTTPClientUserRateLimitIsPerToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"bases": []map[string]any{
				{"id": "app123", "name": "Test Base", "permissionLevel": "create"},
			},
		})
	}))
	defer server.Close()

	client := NewHTTPClient(server.URL, server.Client())
	client.clock = func() time.Time {
		return time.Unix(0, 0)
	}
	var delays []time.Duration
	client.sleep = func(ctx context.Context, delay time.Duration) error {
		delays = append(delays, delay)
		return nil
	}

	if _, err := client.ListBases(context.Background(), "token-a"); err != nil {
		t.Fatalf("ListBases() returned error: %v", err)
	}
	if _, err := client.ListBases(context.Background(), "token-b"); err != nil {
		t.Fatalf("ListBases() returned error: %v", err)
	}

	if len(delays) != 0 {
		t.Fatalf("expected distinct tokens to avoid shared delay, got %v", delays)
	}
}

func TestHTTPClient429BackoffGrowsExponentiallyAndRespectsRetryAfter(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls <= 3 {
			w.Header().Set("Retry-After", "2")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"rate limited"}`))
			return
		}
		json.NewEncoder(w).Encode(map[string]any{
			"tables": []map[string]any{
				{"id": "tbl1", "name": "T", "fields": []map[string]any{}},
			},
		})
	}))
	defer server.Close()

	client := NewHTTPClient(server.URL, server.Client())
	now := time.Unix(0, 0)
	client.clock = func() time.Time { return now }
	client.randomFloat = func() float64 { return 0 } // disable jitter for determinism
	var sleepDelays []time.Duration
	client.sleep = func(ctx context.Context, delay time.Duration) error {
		sleepDelays = append(sleepDelays, delay)
		now = now.Add(delay)
		return nil
	}

	if _, err := client.GetBaseSchema(context.Background(), "token", "appXYZ"); err != nil {
		t.Fatalf("GetBaseSchema() returned error: %v", err)
	}
	if calls != 4 {
		t.Fatalf("expected 4 HTTP calls (3 retries), got %d", calls)
	}

	// Filter out per-user and per-base limiter sleeps;
	// keep only the multi-second 429 backoff sleeps.
	var backoffs []time.Duration
	for _, d := range sleepDelays {
		if d >= time.Second {
			backoffs = append(backoffs, d)
		}
	}
	if len(backoffs) != 3 {
		t.Fatalf("expected 3 backoff sleeps, got %d (%v)", len(backoffs), backoffs)
	}
	// Retry-After=2s, no jitter, multipliers 1.0, 1.5, 2.25
	want := []time.Duration{2 * time.Second, 3 * time.Second, 4500 * time.Millisecond}
	for i, w := range want {
		if backoffs[i] != w {
			t.Fatalf("backoff[%d]: want %s, got %s (all=%v)", i, w, backoffs[i], backoffs)
		}
	}
}

func TestHTTPClient429ExhaustsRetriesAndReturnsError(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"rate limited"}`))
	}))
	defer server.Close()

	client := NewHTTPClient(server.URL, server.Client())
	client.clock = func() time.Time { return time.Unix(0, 0) }
	client.randomFloat = func() float64 { return 0 }
	client.sleep = func(ctx context.Context, delay time.Duration) error { return nil }

	_, err := client.GetBaseSchema(context.Background(), "token", "appPersistent429")
	if err == nil {
		t.Fatalf("expected error after exhausting retries, got nil")
	}
	if !strings.Contains(err.Error(), "repeated rate limits") {
		t.Fatalf("expected rate-limit error, got %v", err)
	}
	if calls != defaultMaxRetryAttempts {
		t.Fatalf("expected %d HTTP calls (max attempts), got %d", defaultMaxRetryAttempts, calls)
	}
}

func TestHTTPClient429BackoffCapsAtMaxBackoff(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls < 3 {
			w.Header().Set("Retry-After", "120")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{
			"tables": []map[string]any{},
		})
	}))
	defer server.Close()

	client := NewHTTPClient(server.URL, server.Client())
	client.clock = func() time.Time { return time.Unix(0, 0) }
	client.randomFloat = func() float64 { return 0 }
	var sleepDelays []time.Duration
	client.sleep = func(ctx context.Context, delay time.Duration) error {
		sleepDelays = append(sleepDelays, delay)
		return nil
	}

	if _, err := client.GetBaseSchema(context.Background(), "token", "appCap"); err != nil {
		t.Fatalf("GetBaseSchema() returned error: %v", err)
	}

	for _, d := range sleepDelays {
		if d > defaultMaxRetryBackoff {
			t.Fatalf("backoff sleep %s exceeded cap %s (all=%v)", d, defaultMaxRetryBackoff, sleepDelays)
		}
	}
}

func TestHTTPClientPerBaseTokenBucketSpacesSteadyState(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"tables": []map[string]any{},
		})
	}))
	defer server.Close()

	client := NewHTTPClient(server.URL, server.Client())
	now := time.Unix(0, 0)
	client.clock = func() time.Time { return now }
	var sleepDelays []time.Duration
	client.sleep = func(ctx context.Context, delay time.Duration) error {
		sleepDelays = append(sleepDelays, delay)
		now = now.Add(delay)
		return nil
	}
	client.baseRateBurst = 2

	// Use distinct tokens so the per-user limiter never fires; we want to
	// isolate the per-base bucket's spacing here.
	tokens := []string{"a", "b", "c", "d", "e"}
	for _, token := range tokens {
		if _, err := client.GetBaseSchema(context.Background(), token, "appBucket"); err != nil {
			t.Fatalf("GetBaseSchema() returned error: %v", err)
		}
	}

	if len(sleepDelays) != 3 {
		t.Fatalf("expected 3 bucket-throttled sleeps after burst of 2, got %d (%v)", len(sleepDelays), sleepDelays)
	}
	want := 250 * time.Millisecond // 1/4s at rate=4 req/s
	for i, d := range sleepDelays {
		if d != want {
			t.Fatalf("bucket sleep[%d] = %s, want %s (all=%v)", i, d, want, sleepDelays)
		}
	}
}

func TestHTTPClientPerBaseLimiterIsolatesBases(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"tables": []map[string]any{},
		})
	}))
	defer server.Close()

	client := NewHTTPClient(server.URL, server.Client())
	client.clock = func() time.Time { return time.Unix(0, 0) }
	var sleepDelays []time.Duration
	client.sleep = func(ctx context.Context, delay time.Duration) error {
		sleepDelays = append(sleepDelays, delay)
		return nil
	}
	client.baseRateBurst = 1

	// Distinct users, so only the per-base buckets are in play.
	if _, err := client.GetBaseSchema(context.Background(), "token-a", "appA"); err != nil {
		t.Fatalf("GetBaseSchema(appA) returned error: %v", err)
	}
	if _, err := client.GetBaseSchema(context.Background(), "token-b", "appB"); err != nil {
		t.Fatalf("GetBaseSchema(appB) returned error: %v", err)
	}

	for _, d := range sleepDelays {
		if d >= 100*time.Millisecond {
			t.Fatalf("expected no per-base bucket sleep across distinct bases, got %s (all=%v)", d, sleepDelays)
		}
	}
}

// virtualClockClient returns a client whose sleeps advance a fake clock, plus
// a way to read that clock.
func virtualClockClient(t *testing.T, handler http.HandlerFunc) (*HTTPClient, func() time.Time, func(time.Duration)) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	client := NewHTTPClient(server.URL, server.Client())
	now := time.Unix(0, 0)
	client.clock = func() time.Time { return now }
	client.randomFloat = func() float64 { return 0 }
	client.sleep = func(ctx context.Context, delay time.Duration) error {
		now = now.Add(delay)
		return nil
	}
	return client, func() time.Time { return now }, func(d time.Duration) { now = now.Add(d) }
}

func emptySchemaHandler(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]any{"tables": []map[string]any{}})
}

func TestHTTPClientSharesOneUserBudgetAcrossBases(t *testing.T) {
	client, clock, _ := virtualClockClient(t, emptySchemaHandler)

	// One user, eight different bases: no per-base bucket ever fills, so any
	// spacing comes from the user's own budget.
	started := clock()
	for index := 0; index < 8; index++ {
		baseID := "appUser" + string(rune('A'+index))
		if _, err := client.GetBaseSchema(context.Background(), "one-user", baseID); err != nil {
			t.Fatalf("GetBaseSchema(%s) returned error: %v", baseID, err)
		}
	}
	if elapsed, want := clock().Sub(started), 7*250*time.Millisecond; elapsed != want {
		t.Fatalf("eight requests for one user across bases took %s, want %s (4 per second in total)", elapsed, want)
	}

	// A second user is not held back by the first.
	before := clock()
	if _, err := client.GetBaseSchema(context.Background(), "another-user", "appOther"); err != nil {
		t.Fatalf("GetBaseSchema() returned error: %v", err)
	}
	if waited := clock().Sub(before); waited != 0 {
		t.Fatalf("a different user waited %s behind the first user's requests", waited)
	}
}

func TestHTTPClientKeepsRoomForSmallRequestsDuringBulkDownload(t *testing.T) {
	client, clock, _ := virtualClockClient(t, emptySchemaHandler)
	bulk := WithBulkRequests(context.Background())

	started := clock()
	for index := 0; index < 13; index++ {
		baseID := "appBulk" + string(rune('A'+index))
		if _, err := client.GetBaseSchema(bulk, "one-user", baseID); err != nil {
			t.Fatalf("bulk GetBaseSchema(%s) returned error: %v", baseID, err)
		}
	}
	// Thirteen bulk requests at 3 per second span four seconds, where the
	// user's full 4 per second would have taken three.
	if elapsed := clock().Sub(started); elapsed < 3900*time.Millisecond || elapsed > 4100*time.Millisecond {
		t.Fatalf("thirteen bulk requests took %s, want about 4s (3 per second)", elapsed)
	}

	// A small request for the same user gets the next slot, not a place at
	// the back of the download.
	before := clock()
	if _, err := client.GetBaseSchema(context.Background(), "one-user", "appSmall"); err != nil {
		t.Fatalf("GetBaseSchema() returned error: %v", err)
	}
	if waited := clock().Sub(before); waited > 250*time.Millisecond {
		t.Fatalf("a small request waited %s behind a bulk download, want at most 250ms", waited)
	}
}

func TestHTTPClientDownloadStandsAsideForSmallRequests(t *testing.T) {
	client, clock, advance := virtualClockClient(t, emptySchemaHandler)
	bulk := WithBulkRequests(context.Background())

	if _, err := client.GetBaseSchema(context.Background(), "one-user", "appSmall"); err != nil {
		t.Fatalf("GetBaseSchema() returned error: %v", err)
	}
	// A download page asked for right behind a small request waits out the
	// yield window, which is longer than the user's 250ms slot spacing.
	before := clock()
	if _, err := client.GetBaseSchema(bulk, "one-user", "appBig"); err != nil {
		t.Fatalf("bulk GetBaseSchema() returned error: %v", err)
	}
	if waited := clock().Sub(before); waited < bulkYieldWindow {
		t.Fatalf("a download page waited %s behind a small request, want at least %s", waited, bulkYieldWindow)
	}

	// Once the small requests stop, the download runs at its own pace again.
	advance(5 * time.Second)
	before = clock()
	if _, err := client.GetBaseSchema(bulk, "one-user", "appBigTwo"); err != nil {
		t.Fatalf("bulk GetBaseSchema() returned error: %v", err)
	}
	if waited := clock().Sub(before); waited != 0 {
		t.Fatalf("a download page waited %s with no small requests around", waited)
	}

	// Another user's small requests do not hold this user's download back.
	if _, err := client.GetBaseSchema(context.Background(), "another-user", "appOther"); err != nil {
		t.Fatalf("GetBaseSchema() returned error: %v", err)
	}
	advance(time.Second)
	before = clock()
	if _, err := client.GetBaseSchema(bulk, "one-user", "appBigThree"); err != nil {
		t.Fatalf("bulk GetBaseSchema() returned error: %v", err)
	}
	if waited := clock().Sub(before); waited != 0 {
		t.Fatalf("a download page waited %s behind a different user's request", waited)
	}
}

func TestHTTPClientHalvesUserBudgetAfterRateLimit(t *testing.T) {
	var calls int
	client, clock, advance := virtualClockClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set("Retry-After", "30")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		emptySchemaHandler(w, r)
	})
	// The rejected request's own wait must not move the clock, so the
	// requests below land inside the slow period like other goroutines would.
	baseSleep := client.sleep
	client.sleep = func(ctx context.Context, delay time.Duration) error {
		if delay >= time.Second {
			return nil
		}
		return baseSleep(ctx, delay)
	}

	if _, err := client.GetBaseSchema(context.Background(), "one-user", "appFirst"); err != nil {
		t.Fatalf("GetBaseSchema() returned error: %v", err)
	}

	spacing := func(label string) time.Duration {
		t.Helper()
		if _, err := client.GetBaseSchema(context.Background(), "one-user", "app"+label+"1"); err != nil {
			t.Fatalf("GetBaseSchema() returned error: %v", err)
		}
		before := clock()
		if _, err := client.GetBaseSchema(context.Background(), "one-user", "app"+label+"2"); err != nil {
			t.Fatalf("GetBaseSchema() returned error: %v", err)
		}
		return clock().Sub(before)
	}

	if got := spacing("Slow"); got != 500*time.Millisecond {
		t.Fatalf("spacing right after a 429 was %s, want 500ms (half rate)", got)
	}
	advance(31 * time.Second)
	if got := spacing("Recovered"); got != 250*time.Millisecond {
		t.Fatalf("spacing after the slow period was %s, want 250ms", got)
	}
}

// TestHTTPClientConcurrentDownloadsStayInsideOneUsersLimit runs on the real
// clock: two bulk downloads and a stream of small requests share one user,
// against a server that counts that user's requests like Airtable does.
func TestHTTPClientConcurrentDownloadsStayInsideOneUsersLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("runs for a few seconds on the real clock")
	}

	var mu sync.Mutex
	var arrivals []time.Time
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		arrivals = append(arrivals, time.Now())
		mu.Unlock()
		emptySchemaHandler(w, r)
	}))
	defer server.Close()
	client := NewHTTPClient(server.URL, server.Client())

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for _, baseID := range []string{"appBigOne", "appBigTwo"} {
		for worker := 0; worker < 5; worker++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				bulk := WithBulkRequests(ctx)
				for ctx.Err() == nil {
					_, _ = client.GetBaseSchema(bulk, "one-user", baseID)
				}
			}()
		}
	}

	var slowestSmall time.Duration
	var smallCount int
	for ctx.Err() == nil {
		started := time.Now()
		if _, err := client.GetBaseSchema(ctx, "one-user", "appSmall"); err != nil {
			break
		}
		smallCount++
		if waited := time.Since(started); waited > slowestSmall {
			slowestSmall = waited
		}
		select {
		case <-ctx.Done():
		case <-time.After(700 * time.Millisecond):
		}
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	busiest := 0
	for start := range arrivals {
		count := 0
		for _, at := range arrivals[start:] {
			if at.Sub(arrivals[start]) < time.Second {
				count++
			}
		}
		if count > busiest {
			busiest = count
		}
	}
	if busiest > 5 {
		t.Fatalf("one user sent %d requests inside a second, want at most 5", busiest)
	}
	if len(arrivals) < 12 {
		t.Fatalf("expected the downloads to keep moving, saw only %d requests in 4s", len(arrivals))
	}
	if smallCount < 3 || slowestSmall > time.Second {
		t.Fatalf("small requests were starved: %d completed, slowest waited %s", smallCount, slowestSmall)
	}
}

func TestComputeRateLimitBackoffJitterAndCap(t *testing.T) {
	c := NewHTTPClient("https://example", nil)
	c.randomFloat = func() float64 { return 1.0 } // max jitter

	// Retry-After=10s, attempt 0: base=10s, mult=1.0, +30% jitter = 13s
	got := c.computeRateLimitBackoff("10", 0)
	if got != 13*time.Second {
		t.Fatalf("attempt 0 with full jitter: want 13s, got %s", got)
	}

	// Attempt 4 with Retry-After=30s would compute 30s * 1.5^4 = 151.875s,
	// then +30% jitter, capped at maxRetryBackoff (90s).
	got = c.computeRateLimitBackoff("30", 4)
	if got != defaultMaxRetryBackoff {
		t.Fatalf("attempt 4 should cap at %s, got %s", defaultMaxRetryBackoff, got)
	}
}

func TestHTTPClientLogsRetryWithoutTokenLeakage(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"rate limited"}`))
			return
		}
		json.NewEncoder(w).Encode(map[string]any{
			"bases": []map[string]any{
				{"id": "app123", "name": "Test Base", "permissionLevel": "create"},
			},
		})
	}))
	defer server.Close()

	// Sync workers left running by other tests also log through the default
	// logger, so the buffer must tolerate concurrent writes.
	var output lockedBuffer
	previous := slog.Default()
	slog.SetDefault(logx.NewLogger(&output))
	t.Cleanup(func() {
		slog.SetDefault(previous)
	})

	client := NewHTTPClient(server.URL, server.Client())
	client.clock = func() time.Time {
		return time.Unix(0, 0)
	}

	if _, err := client.ListBases(context.Background(), "token-secret-value"); err != nil {
		t.Fatalf("ListBases() returned error: %v", err)
	}

	logText := output.String()
	if !strings.Contains(logText, `"event":"airtable.request.retry"`) {
		t.Fatalf("expected retry log event, got %s", logText)
	}
	if strings.Contains(logText, "token-secret-value") {
		t.Fatalf("expected raw access token to stay out of logs, got %s", logText)
	}
}

func TestNewHTTPClientDefaultsToTimeoutBearingClient(t *testing.T) {
	client := NewHTTPClient("", nil)
	if client.httpClient.Timeout <= 0 {
		t.Fatalf("expected default http client to carry a timeout, got %v", client.httpClient.Timeout)
	}
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
