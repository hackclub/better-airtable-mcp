package syncer

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
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
	if delays[0] != 20*time.Millisecond {
		t.Fatalf("expected 20ms user-token delay, got %s", delays[0])
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
	client.clock = func() time.Time { return time.Unix(0, 0) }
	client.randomFloat = func() float64 { return 0 } // disable jitter for determinism
	var sleepDelays []time.Duration
	client.sleep = func(ctx context.Context, delay time.Duration) error {
		sleepDelays = append(sleepDelays, delay)
		return nil
	}

	if _, err := client.GetBaseSchema(context.Background(), "token", "appXYZ"); err != nil {
		t.Fatalf("GetBaseSchema() returned error: %v", err)
	}
	if calls != 4 {
		t.Fatalf("expected 4 HTTP calls (3 retries), got %d", calls)
	}

	// Filter out per-user (20ms) and per-base (token bucket) limiter sleeps;
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

	if _, err := client.GetBaseSchema(context.Background(), "token", "appA"); err != nil {
		t.Fatalf("GetBaseSchema(appA) returned error: %v", err)
	}
	if _, err := client.GetBaseSchema(context.Background(), "token", "appB"); err != nil {
		t.Fatalf("GetBaseSchema(appB) returned error: %v", err)
	}

	for _, d := range sleepDelays {
		if d >= 100*time.Millisecond {
			t.Fatalf("expected no per-base bucket sleep across distinct bases, got %s (all=%v)", d, sleepDelays)
		}
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

	var output bytes.Buffer
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
