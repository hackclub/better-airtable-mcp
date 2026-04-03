package oauth

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"

	"github.com/hackclub/better-airtable-mcp/internal/db"
	"github.com/hackclub/better-airtable-mcp/internal/logx"
)

func TestMiddlewareRateLimitsByBearerToken(t *testing.T) {
	store, cleanup := openMiddlewareTestStore(t)
	defer cleanup()

	if err := store.UpsertUser(context.Background(), db.User{ID: "user_1"}); err != nil {
		t.Fatalf("UpsertUser() returned error: %v", err)
	}

	bearerToken := "mcp-token"
	if err := store.PutMCPToken(context.Background(), db.MCPTokenRecord{
		TokenHash:  HashToken(bearerToken),
		UserID:     "user_1",
		ClientID:   ptr("client_123"),
		ClientName: ptr("Claude"),
		CreatedAt:  time.Now().UTC(),
		ExpiresAt:  time.Now().Add(time.Hour).UTC(),
	}); err != nil {
		t.Fatalf("PutMCPToken() returned error: %v", err)
	}

	middleware := NewMiddlewareWithRateLimit(store, 1, 1)
	handler := middleware.RequireBearer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if tokenHash, ok := TokenHashFromContext(r.Context()); !ok || tokenHash != HashToken(bearerToken) {
			t.Fatalf("expected token hash in context, got %q ok=%v", tokenHash, ok)
		}
		if clientID, ok := ClientIDFromContext(r.Context()); !ok || clientID != "client_123" {
			t.Fatalf("expected client id in context, got %q ok=%v", clientID, ok)
		}
		if clientName, ok := ClientNameFromContext(r.Context()); !ok || clientName != "Claude" {
			t.Fatalf("expected client name in context, got %q ok=%v", clientName, ok)
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	first := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	first.Header.Set("Authorization", "Bearer "+bearerToken)
	firstRecorder := httptest.NewRecorder()
	handler.ServeHTTP(firstRecorder, first)
	if firstRecorder.Code != http.StatusNoContent {
		t.Fatalf("expected first request to pass, got %d", firstRecorder.Code)
	}

	second := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	second.Header.Set("Authorization", "Bearer "+bearerToken)
	secondRecorder := httptest.NewRecorder()
	handler.ServeHTTP(secondRecorder, second)
	if secondRecorder.Code != http.StatusTooManyRequests {
		t.Fatalf("expected second request to be rate limited, got %d", secondRecorder.Code)
	}
	if retryAfter := secondRecorder.Header().Get("Retry-After"); retryAfter != "1" {
		t.Fatalf("expected Retry-After=1, got %q", retryAfter)
	}
}

func TestMiddlewareLogsRateLimitWithoutTokenLeakage(t *testing.T) {
	store, cleanup := openMiddlewareTestStore(t)
	defer cleanup()

	if err := store.UpsertUser(context.Background(), db.User{ID: "user_1"}); err != nil {
		t.Fatalf("UpsertUser() returned error: %v", err)
	}

	bearerToken := "mcp-super-secret-token"
	if err := store.PutMCPToken(context.Background(), db.MCPTokenRecord{
		TokenHash:  HashToken(bearerToken),
		UserID:     "user_1",
		ClientID:   ptr("client_123"),
		ClientName: ptr("Claude"),
		CreatedAt:  time.Now().UTC(),
		ExpiresAt:  time.Now().Add(time.Hour).UTC(),
	}); err != nil {
		t.Fatalf("PutMCPToken() returned error: %v", err)
	}

	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(logx.NewLogger(&output))
	t.Cleanup(func() {
		slog.SetDefault(previous)
	})

	middleware := NewMiddlewareWithRateLimit(store, 1, 1)
	handler := logx.HTTPMiddleware(logx.Route("/mcp", middleware.RequireBearer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))))

	first := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	first.Header.Set("Authorization", "Bearer "+bearerToken)
	firstRecorder := httptest.NewRecorder()
	handler.ServeHTTP(firstRecorder, first)

	second := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	second.Header.Set("Authorization", "Bearer "+bearerToken)
	secondRecorder := httptest.NewRecorder()
	handler.ServeHTTP(secondRecorder, second)

	if secondRecorder.Code != http.StatusTooManyRequests {
		t.Fatalf("expected second request to be rate limited, got %d", secondRecorder.Code)
	}

	logText := output.String()
	if !strings.Contains(logText, `"event":"oauth.rate_limited"`) {
		t.Fatalf("expected oauth.rate_limited log, got %s", logText)
	}
	if !strings.Contains(logText, HashToken(bearerToken)) {
		t.Fatalf("expected token hash in log output, got %s", logText)
	}
	if strings.Contains(logText, bearerToken) {
		t.Fatalf("expected raw bearer token to stay out of logs, got %s", logText)
	}
}

func ptr[T any](value T) *T {
	return &value
}

func openMiddlewareTestStore(t *testing.T) (*db.Store, func()) {
	t.Helper()

	port := oauthFreePort(t)
	postgres := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Port(uint32(port)).
			Database("better_airtable_middleware_test").
			Username("postgres").
			Password("postgres").
			BinariesPath(filepath.Join(t.TempDir(), "postgres-binaries")).
			DataPath(filepath.Join(t.TempDir(), "postgres-data")).
			RuntimePath(filepath.Join(t.TempDir(), "postgres-runtime")),
	)
	if err := postgres.Start(); err != nil {
		t.Fatalf("embedded postgres start failed: %v", err)
	}

	store, err := db.Open(context.Background(), fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/better_airtable_middleware_test?sslmode=disable", port))
	if err != nil {
		_ = postgres.Stop()
		t.Fatalf("db.Open() returned error: %v", err)
	}
	if err := store.Migrate(context.Background()); err != nil {
		store.Close()
		_ = postgres.Stop()
		t.Fatalf("store.Migrate() returned error: %v", err)
	}

	return store, func() {
		store.Close()
		_ = postgres.Stop()
	}
}
