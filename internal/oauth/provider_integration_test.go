package oauth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"

	"github.com/hackclub/better-airtable-mcp/internal/config"
	"github.com/hackclub/better-airtable-mcp/internal/cryptoutil"
	"github.com/hackclub/better-airtable-mcp/internal/db"
)

func TestOAuthProviderEndToEnd(t *testing.T) {
	const cfgClientID = "airtable-client-id"
	const cfgClientSecret = "airtable-client-secret"

	port := oauthFreePort(t)
	postgres := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Port(uint32(port)).
			Database("better_airtable_oauth_test").
			Username("postgres").
			Password("postgres").
			BinariesPath(filepath.Join(t.TempDir(), "postgres-binaries")).
			DataPath(filepath.Join(t.TempDir(), "postgres-data")).
			RuntimePath(filepath.Join(t.TempDir(), "postgres-runtime")),
	)
	if err := postgres.Start(); err != nil {
		t.Fatalf("embedded postgres start failed: %v", err)
	}
	defer postgres.Stop()

	store, err := db.Open(context.Background(), fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/better_airtable_oauth_test?sslmode=disable", port))
	if err != nil {
		t.Fatalf("db.Open() returned error: %v", err)
	}
	defer store.Close()

	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("store.Migrate() returned error: %v", err)
	}

	airtableTokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/oauth2/v1/token" {
			t.Fatalf("unexpected Airtable OAuth path %q", r.URL.Path)
		}
		expectedAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte(cfgClientID+":"+cfgClientSecret))
		if got := r.Header.Get("Authorization"); got != expectedAuth {
			t.Fatalf("unexpected Authorization header %q", got)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("ParseForm() returned error: %v", err)
		}
		if r.PostForm.Get("grant_type") != "authorization_code" {
			t.Fatalf("unexpected grant_type %q", r.PostForm.Get("grant_type"))
		}
		json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "airtable-access-token",
			"refresh_token": "airtable-refresh-token",
			"expires_in":    3600,
			"scope":         defaultAirtableScopes,
			"token_type":    "Bearer",
		})
	}))
	defer airtableTokenServer.Close()

	secret, err := cryptoutil.New([]byte(strings.Repeat("k", 32)))
	if err != nil {
		t.Fatalf("cryptoutil.New() returned error: %v", err)
	}

	cfg := config.Config{
		BaseURL:              mustParseURL(t, "https://provider.example"),
		AirtableClientID:     cfgClientID,
		AirtableClientSecret: cfgClientSecret,
	}
	handler := NewHandler(
		cfg,
		store,
		secret,
		NewAirtableOAuthClient(
			cfgClientID,
			cfgClientSecret,
			cfg.BaseURLString()+"/oauth/airtable/callback",
			airtableTokenServer.Client(),
			airtableTokenServer.URL+"/oauth2/v1/authorize",
			airtableTokenServer.URL+"/oauth2/v1/token",
		),
	)

	registerRecorder := httptest.NewRecorder()
	registerRequest := httptest.NewRequest(http.MethodPost, "/oauth/register", strings.NewReader(`{"redirect_uris":["https://client.example/callback"],"client_name":"Test Client"}`))
	handler.Register(registerRecorder, registerRequest)
	if registerRecorder.Code != http.StatusCreated {
		t.Fatalf("expected register to return 201, got %d", registerRecorder.Code)
	}

	var registration map[string]any
	if err := json.Unmarshal(registerRecorder.Body.Bytes(), &registration); err != nil {
		t.Fatalf("json.Unmarshal() returned error: %v", err)
	}
	clientID := registration["client_id"].(string)

	codeVerifier := "test-verifier"
	authorizeRecorder := httptest.NewRecorder()
	authorizeRequest := httptest.NewRequest(http.MethodGet, "/oauth/authorize?response_type=code&client_id="+url.QueryEscape(clientID)+"&redirect_uri="+url.QueryEscape("https://client.example/callback")+"&state=client-state&code_challenge="+url.QueryEscape(S256Challenge(codeVerifier))+"&code_challenge_method=S256", nil)
	handler.Authorize(authorizeRecorder, authorizeRequest)
	if authorizeRecorder.Code != http.StatusFound {
		t.Fatalf("expected authorize redirect, got %d", authorizeRecorder.Code)
	}

	airtableAuthorizeURL, err := url.Parse(authorizeRecorder.Header().Get("Location"))
	if err != nil {
		t.Fatalf("url.Parse() returned error: %v", err)
	}
	requestState := airtableAuthorizeURL.Query().Get("state")
	if requestState == "" {
		t.Fatal("expected authorize redirect to include Airtable state")
	}

	callbackRecorder := httptest.NewRecorder()
	callbackRequest := httptest.NewRequest(http.MethodGet, "/oauth/airtable/callback?code=airtable-code&state="+url.QueryEscape(requestState), nil)
	handler.AirtableCallback(callbackRecorder, callbackRequest)
	if callbackRecorder.Code != http.StatusFound {
		t.Fatalf("expected callback redirect, got %d", callbackRecorder.Code)
	}

	clientRedirect, err := url.Parse(callbackRecorder.Header().Get("Location"))
	if err != nil {
		t.Fatalf("url.Parse() returned error: %v", err)
	}
	if clientRedirect.Query().Get("state") != "client-state" {
		t.Fatalf("expected original client state, got %q", clientRedirect.Query().Get("state"))
	}
	authorizationCode := clientRedirect.Query().Get("code")
	if authorizationCode == "" {
		t.Fatal("expected callback redirect to include authorization code")
	}

	tokenRecorder := httptest.NewRecorder()
	tokenBody := strings.NewReader(url.Values{
		"grant_type":    {"authorization_code"},
		"client_id":     {clientID},
		"redirect_uri":  {"https://client.example/callback"},
		"code":          {authorizationCode},
		"code_verifier": {codeVerifier},
	}.Encode())
	tokenRequest := httptest.NewRequest(http.MethodPost, "/oauth/token", tokenBody)
	tokenRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	handler.Token(tokenRecorder, tokenRequest)
	if tokenRecorder.Code != http.StatusOK {
		t.Fatalf("expected token exchange to return 200, got %d with body %s", tokenRecorder.Code, tokenRecorder.Body.String())
	}

	var tokenResponse map[string]any
	if err := json.Unmarshal(tokenRecorder.Body.Bytes(), &tokenResponse); err != nil {
		t.Fatalf("json.Unmarshal() returned error: %v", err)
	}
	accessToken := tokenResponse["access_token"].(string)
	refreshToken := tokenResponse["refresh_token"].(string)
	if accessToken == "" || refreshToken == "" {
		t.Fatalf("expected access and refresh tokens, got %#v", tokenResponse)
	}

	record, err := store.GetMCPToken(context.Background(), HashToken(accessToken))
	if err != nil {
		t.Fatalf("store.GetMCPToken() returned error: %v", err)
	}
	if record.UserID == "" {
		t.Fatalf("expected persisted mcp token to include a user ID, got %#v", record)
	}
	if record.ClientID == nil || *record.ClientID != clientID {
		t.Fatalf("expected persisted mcp token to include client ID %q, got %#v", clientID, record)
	}
	if record.ClientName == nil || *record.ClientName != "Test Client" {
		t.Fatalf("expected persisted mcp token to include client name, got %#v", record)
	}

	refreshRecorder := httptest.NewRecorder()
	refreshRequest := httptest.NewRequest(http.MethodPost, "/oauth/token", strings.NewReader(url.Values{
		"grant_type":    {"refresh_token"},
		"client_id":     {clientID},
		"refresh_token": {refreshToken},
	}.Encode()))
	refreshRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	handler.Token(refreshRecorder, refreshRequest)
	if refreshRecorder.Code != http.StatusOK {
		t.Fatalf("expected refresh token exchange to return 200, got %d with body %s", refreshRecorder.Code, refreshRecorder.Body.String())
	}
}

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("url.Parse() returned error: %v", err)
	}
	return parsed
}

func oauthFreePort(t *testing.T) int {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() returned error: %v", err)
	}
	defer listener.Close()

	return listener.Addr().(*net.TCPAddr).Port
}
