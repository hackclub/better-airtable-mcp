package oauth

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPublicRateLimitThrottlesPerClientIP(t *testing.T) {
	var served int
	handler := PublicRateLimit(NewRequestLimiter(1, 2), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served++
		w.WriteHeader(http.StatusNoContent)
	}))

	send := func(remoteAddr string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/oauth/register", nil)
		request.RemoteAddr = remoteAddr
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		return recorder
	}

	for i := 0; i < 2; i++ {
		if code := send("203.0.113.7:1234").Code; code != http.StatusNoContent {
			t.Fatalf("request %d from first ip = %d, want %d", i, code, http.StatusNoContent)
		}
	}

	limited := send("203.0.113.7:1234")
	if limited.Code != http.StatusTooManyRequests {
		t.Fatalf("expected burst-exhausted request to be limited, got %d", limited.Code)
	}
	if limited.Header().Get("Retry-After") != "1" {
		t.Fatalf("expected Retry-After=1, got %q", limited.Header().Get("Retry-After"))
	}

	if code := send("203.0.113.8:1234").Code; code != http.StatusNoContent {
		t.Fatalf("expected a different ip to have its own bucket, got %d", code)
	}
	if served != 3 {
		t.Fatalf("expected 3 served requests, got %d", served)
	}
}

func TestClientIPKeyUsesForwardedHeaderOnlyFromPrivatePeers(t *testing.T) {
	testCases := []struct {
		name       string
		remoteAddr string
		forwarded  string
		want       string
	}{
		{
			name:       "private peer with forwarded header uses rightmost entry",
			remoteAddr: "10.244.4.1:39000",
			forwarded:  "198.51.100.9, 203.0.113.7",
			want:       "203.0.113.7",
		},
		{
			name:       "private peer without forwarded header falls back to peer",
			remoteAddr: "10.244.4.1:39000",
			forwarded:  "",
			want:       "10.244.4.1",
		},
		{
			name:       "public peer never honors forwarded header",
			remoteAddr: "203.0.113.7:1234",
			forwarded:  "198.51.100.9",
			want:       "203.0.113.7",
		},
		{
			name:       "loopback peer honors forwarded header",
			remoteAddr: "127.0.0.1:5678",
			forwarded:  "203.0.113.7",
			want:       "203.0.113.7",
		},
		{
			name:       "garbage forwarded value falls back to peer",
			remoteAddr: "10.244.4.1:39000",
			forwarded:  "not-an-ip",
			want:       "10.244.4.1",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/oauth/token", nil)
			request.RemoteAddr = testCase.remoteAddr
			if testCase.forwarded != "" {
				request.Header.Set("X-Forwarded-For", testCase.forwarded)
			}
			if got := clientIPKey(request); got != testCase.want {
				t.Fatalf("clientIPKey() = %q, want %q", got, testCase.want)
			}
		})
	}
}
