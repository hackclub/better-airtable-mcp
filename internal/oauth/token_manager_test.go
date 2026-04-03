package oauth

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"

	"github.com/hackclub/better-airtable-mcp/internal/cryptoutil"
	"github.com/hackclub/better-airtable-mcp/internal/db"
	"github.com/hackclub/better-airtable-mcp/internal/logx"
)

func TestTokenManagerRefreshesExpiringTokenOnDemand(t *testing.T) {
	store, cleanup := openOAuthTestStore(t)
	defer cleanup()

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		expectedAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("client-id:client-secret"))
		if got := r.Header.Get("Authorization"); got != expectedAuth {
			t.Fatalf("unexpected Authorization header %q", got)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("ParseForm() returned error: %v", err)
		}
		if r.PostForm.Get("grant_type") != "refresh_token" {
			t.Fatalf("unexpected grant_type %q", r.PostForm.Get("grant_type"))
		}
		writeOAuthJSON(t, w, map[string]any{
			"access_token":  "new-access",
			"refresh_token": "new-refresh",
			"expires_in":    3600,
			"scope":         defaultAirtableScopes,
			"token_type":    "Bearer",
		})
	}))
	defer server.Close()

	cipher := mustNewCipher(t)
	putEncryptedToken(t, store, cipher, "user_1", "old-access", "old-refresh", time.Now().Add(5*time.Minute))

	manager := NewTokenManager(store, cipher, NewAirtableOAuthClient("client-id", "client-secret", "https://example.test/callback", server.Client(), "", server.URL))

	token, err := manager.AirtableAccessToken(context.Background(), "user_1")
	if err != nil {
		t.Fatalf("AirtableAccessToken() returned error: %v", err)
	}
	if token != "new-access" {
		t.Fatalf("expected refreshed access token, got %q", token)
	}
	if calls.Load() != 1 {
		t.Fatalf("expected one refresh call, got %d", calls.Load())
	}

	record, err := store.GetAirtableToken(context.Background(), "user_1")
	if err != nil {
		t.Fatalf("GetAirtableToken() returned error: %v", err)
	}
	decryptedAccess, _ := cipher.Decrypt(record.AccessTokenCiphertext)
	decryptedRefresh, _ := cipher.Decrypt(record.RefreshTokenCiphertext)
	if string(decryptedAccess) != "new-access" || string(decryptedRefresh) != "new-refresh" {
		t.Fatalf("unexpected stored token pair access=%q refresh=%q", decryptedAccess, decryptedRefresh)
	}

	token, err = manager.AirtableAccessToken(context.Background(), "user_1")
	if err != nil {
		t.Fatalf("second AirtableAccessToken() returned error: %v", err)
	}
	if token != "new-access" || calls.Load() != 1 {
		t.Fatalf("expected cached refreshed token on second call, token=%q calls=%d", token, calls.Load())
	}
}

func TestTokenManagerMarksInvalidGrantAsReauthorizationRequired(t *testing.T) {
	store, cleanup := openOAuthTestStore(t)
	defer cleanup()

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		writeOAuthJSON(t, w, map[string]any{
			"error":             "invalid_grant",
			"error_description": "refresh token revoked",
		})
	}))
	defer server.Close()

	cipher := mustNewCipher(t)
	putEncryptedToken(t, store, cipher, "user_1", "old-access", "old-refresh", time.Now().Add(-time.Minute))

	manager := NewTokenManager(store, cipher, NewAirtableOAuthClient("client-id", "client-secret", "https://example.test/callback", server.Client(), "", server.URL))

	_, err := manager.AirtableAccessToken(context.Background(), "user_1")
	if err == nil {
		t.Fatal("expected AirtableAccessToken() to require reauthorization")
	}
	var reauthErr ReauthorizationRequiredError
	if !strings.Contains(err.Error(), reauthErr.Error()) {
		t.Fatalf("expected reauthorization error, got %v", err)
	}

	record, err := store.GetAirtableToken(context.Background(), "user_1")
	if err != nil {
		t.Fatalf("GetAirtableToken() returned error: %v", err)
	}
	if record.ReauthRequiredAt == nil {
		t.Fatal("expected token to be marked for reauthorization")
	}

	_, err = manager.AirtableAccessToken(context.Background(), "user_1")
	if err == nil {
		t.Fatal("expected repeated AirtableAccessToken() call to still fail")
	}
	if calls.Load() != 1 {
		t.Fatalf("expected invalid_grant path to avoid repeated refresh attempts, got %d calls", calls.Load())
	}
}

func TestTokenManagerLogsReauthorizationWithoutTokenLeakage(t *testing.T) {
	store, cleanup := openOAuthTestStore(t)
	defer cleanup()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		writeOAuthJSON(t, w, map[string]any{
			"error":             "invalid_grant",
			"error_description": "refresh token revoked",
		})
	}))
	defer server.Close()

	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(logx.NewLogger(&output))
	t.Cleanup(func() {
		slog.SetDefault(previous)
	})

	cipher := mustNewCipher(t)
	putEncryptedToken(t, store, cipher, "user_1", "old-access-token", "old-refresh-token", time.Now().Add(-time.Minute))

	manager := NewTokenManager(store, cipher, NewAirtableOAuthClient("client-id", "client-secret", "https://example.test/callback", server.Client(), "", server.URL))
	if _, err := manager.AirtableAccessToken(context.Background(), "user_1"); err == nil {
		t.Fatal("expected AirtableAccessToken() to require reauthorization")
	}

	logText := output.String()
	if !strings.Contains(logText, `"event":"oauth.airtable_token.reauth_required_marked"`) {
		t.Fatalf("expected reauth marker log, got %s", logText)
	}
	for _, forbidden := range []string{"old-access-token", "old-refresh-token"} {
		if strings.Contains(logText, forbidden) {
			t.Fatalf("expected logs to exclude %q, got %s", forbidden, logText)
		}
	}
}

func TestTokenManagerRunRefreshLoopRefreshesExpiringTokens(t *testing.T) {
	store, cleanup := openOAuthTestStore(t)
	defer cleanup()

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeOAuthJSON(t, w, map[string]any{
			"access_token":  "loop-access",
			"refresh_token": "loop-refresh",
			"expires_in":    3600,
			"scope":         defaultAirtableScopes,
			"token_type":    "Bearer",
		})
	}))
	defer server.Close()

	cipher := mustNewCipher(t)
	putEncryptedToken(t, store, cipher, "user_1", "old-access", "old-refresh", time.Now().Add(2*time.Minute))

	manager := NewTokenManager(store, cipher, NewAirtableOAuthClient("client-id", "client-secret", "https://example.test/callback", server.Client(), "", server.URL))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go manager.RunRefreshLoop(ctx, 20*time.Millisecond)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		record, err := store.GetAirtableToken(context.Background(), "user_1")
		if err != nil {
			t.Fatalf("GetAirtableToken() returned error: %v", err)
		}
		accessPlaintext, _ := cipher.Decrypt(record.AccessTokenCiphertext)
		if string(accessPlaintext) == "loop-access" {
			if calls.Load() == 0 {
				t.Fatal("expected refresh loop to hit Airtable before updating stored token")
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("timed out waiting for background refresh loop to update the token")
}

func TestTokenManagerSingleFlightsConcurrentRefreshes(t *testing.T) {
	store, cleanup := openOAuthTestStore(t)
	defer cleanup()

	var calls atomic.Int32
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		<-release
		writeOAuthJSON(t, w, map[string]any{
			"access_token":  "shared-access",
			"refresh_token": "shared-refresh",
			"expires_in":    3600,
			"scope":         defaultAirtableScopes,
			"token_type":    "Bearer",
		})
	}))
	defer server.Close()

	cipher := mustNewCipher(t)
	putEncryptedToken(t, store, cipher, "user_1", "old-access", "old-refresh", time.Now().Add(-time.Minute))

	manager := NewTokenManager(store, cipher, NewAirtableOAuthClient("client-id", "client-secret", "https://example.test/callback", server.Client(), "", server.URL))

	type result struct {
		token string
		err   error
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			token, err := manager.AirtableAccessToken(context.Background(), "user_1")
			results <- result{token: token, err: err}
		}()
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if calls.Load() == 1 {
			close(release)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	wg.Wait()
	close(results)

	if calls.Load() != 1 {
		t.Fatalf("expected a single refresh call, got %d", calls.Load())
	}
	for outcome := range results {
		if outcome.err != nil {
			t.Fatalf("concurrent AirtableAccessToken() returned error: %v", outcome.err)
		}
		if outcome.token != "shared-access" {
			t.Fatalf("expected shared refreshed token, got %q", outcome.token)
		}
	}
}

func openOAuthTestStore(t *testing.T) (*db.Store, func()) {
	t.Helper()

	port := oauthFreePort(t)
	postgres := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Port(uint32(port)).
			Database("better_airtable_token_manager_test").
			Username("postgres").
			Password("postgres").
			BinariesPath(filepath.Join(t.TempDir(), "postgres-binaries")).
			DataPath(filepath.Join(t.TempDir(), "postgres-data")).
			RuntimePath(filepath.Join(t.TempDir(), "postgres-runtime")),
	)
	if err := postgres.Start(); err != nil {
		t.Fatalf("embedded postgres start failed: %v", err)
	}

	store, err := db.Open(context.Background(), fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/better_airtable_token_manager_test?sslmode=disable", port))
	if err != nil {
		_ = postgres.Stop()
		t.Fatalf("db.Open() returned error: %v", err)
	}
	if err := store.Migrate(context.Background()); err != nil {
		store.Close()
		_ = postgres.Stop()
		t.Fatalf("store.Migrate() returned error: %v", err)
	}
	if err := store.UpsertUser(context.Background(), db.User{ID: "user_1"}); err != nil {
		store.Close()
		_ = postgres.Stop()
		t.Fatalf("store.UpsertUser() returned error: %v", err)
	}

	return store, func() {
		store.Close()
		if err := postgres.Stop(); err != nil {
			t.Fatalf("embedded postgres stop failed: %v", err)
		}
	}
}

func putEncryptedToken(t *testing.T, store *db.Store, cipher *cryptoutil.Cipher, userID, accessToken, refreshToken string, expiresAt time.Time) {
	t.Helper()

	accessCiphertext, err := cipher.Encrypt([]byte(accessToken))
	if err != nil {
		t.Fatalf("cipher.Encrypt(access) returned error: %v", err)
	}
	refreshCiphertext, err := cipher.Encrypt([]byte(refreshToken))
	if err != nil {
		t.Fatalf("cipher.Encrypt(refresh) returned error: %v", err)
	}

	if err := store.PutAirtableToken(context.Background(), db.AirtableTokenRecord{
		UserID:                 userID,
		AccessTokenCiphertext:  accessCiphertext,
		RefreshTokenCiphertext: refreshCiphertext,
		ExpiresAt:              expiresAt.UTC(),
		Scopes:                 defaultAirtableScopes,
	}); err != nil {
		t.Fatalf("store.PutAirtableToken() returned error: %v", err)
	}
}

func mustNewCipher(t *testing.T) *cryptoutil.Cipher {
	t.Helper()
	cipher, err := cryptoutil.New([]byte(strings.Repeat("k", 32)))
	if err != nil {
		t.Fatalf("cryptoutil.New() returned error: %v", err)
	}
	return cipher
}

func writeOAuthJSON(t *testing.T, w http.ResponseWriter, payload any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		t.Fatalf("json.NewEncoder().Encode() returned error: %v", err)
	}
}
