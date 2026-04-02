package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type stubPinger struct {
	err error
}

func (s *stubPinger) Ping(context.Context) error { return s.err }

func TestHealthy(t *testing.T) {
	handler := NewHandler(&stubPinger{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("expected JSON content type, got %q", ct)
	}

	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["status"] != "healthy" {
		t.Fatalf("expected status healthy, got %q", body["status"])
	}
}

func TestUnhealthy(t *testing.T) {
	handler := NewHandler(&stubPinger{err: errors.New("connection refused")})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}

	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["status"] != "unhealthy" {
		t.Fatalf("expected status unhealthy, got %q", body["status"])
	}
	if body["error"] != "database unreachable" {
		t.Fatalf("expected error 'database unreachable', got %q", body["error"])
	}
}

func TestMethodNotAllowed(t *testing.T) {
	handler := NewHandler(&stubPinger{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/healthz", nil)

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
	if allow := rec.Header().Get("Allow"); allow != "GET" {
		t.Fatalf("expected Allow: GET, got %q", allow)
	}
}
