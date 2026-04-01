package syncer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
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
