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
