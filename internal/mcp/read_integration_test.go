package mcp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"

	"github.com/hackclub/better-airtable-mcp/internal/config"
	"github.com/hackclub/better-airtable-mcp/internal/cryptoutil"
	"github.com/hackclub/better-airtable-mcp/internal/db"
	"github.com/hackclub/better-airtable-mcp/internal/mcp"
	"github.com/hackclub/better-airtable-mcp/internal/oauth"
	syncer "github.com/hackclub/better-airtable-mcp/internal/sync"
	"github.com/hackclub/better-airtable-mcp/internal/tools"
)

func TestAuthenticatedReadToolsOverMCP(t *testing.T) {
	port := mcpFreePort(t)
	postgres := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Port(uint32(port)).
			Database("better_airtable_mcp_test").
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

	store, err := db.Open(context.Background(), fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/better_airtable_mcp_test?sslmode=disable", port))
	if err != nil {
		t.Fatalf("db.Open() returned error: %v", err)
	}
	defer store.Close()
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("store.Migrate() returned error: %v", err)
	}

	fakeAirtable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v0/meta/bases":
			writeMCPJSON(t, w, map[string]any{
				"bases": []map[string]any{
					{"id": "appProjects", "name": "Project Tracker", "permissionLevel": "create"},
				},
			})
		case "/v0/meta/bases/appProjects/tables":
			writeMCPJSON(t, w, map[string]any{
				"tables": []map[string]any{
					{
						"id":   "tblProjects",
						"name": "Projects",
						"fields": []map[string]any{
							{"id": "fldName", "name": "Name", "type": "singleLineText"},
							{"id": "fldLinkedTasks", "name": "Linked Tasks", "type": "multipleRecordLinks"},
						},
					},
					{
						"id":   "tblTasks",
						"name": "Tasks",
						"fields": []map[string]any{
							{"id": "fldTaskName", "name": "Name", "type": "singleLineText"},
						},
					},
				},
			})
		case "/v0/appProjects/tblProjects":
			writeMCPJSON(t, w, map[string]any{
				"records": []map[string]any{
					{
						"id":          "recProject1",
						"createdTime": "2026-04-01T12:00:00Z",
						"fields": map[string]any{
							"Name":         "Website Redesign",
							"Linked Tasks": []string{"recTask1"},
						},
					},
				},
			})
		case "/v0/appProjects/tblTasks":
			writeMCPJSON(t, w, map[string]any{
				"records": []map[string]any{
					{
						"id":          "recTask1",
						"createdTime": "2026-04-01T13:00:00Z",
						"fields": map[string]any{
							"Name": "Design new homepage",
						},
					},
				},
			})
		default:
			t.Fatalf("unexpected Airtable path %q", r.URL.Path)
		}
	}))
	defer fakeAirtable.Close()

	secret, err := cryptoutil.New([]byte(strings.Repeat("k", 32)))
	if err != nil {
		t.Fatalf("cryptoutil.New() returned error: %v", err)
	}

	if err := store.UpsertUser(context.Background(), db.User{ID: "user_1"}); err != nil {
		t.Fatalf("store.UpsertUser() returned error: %v", err)
	}

	encryptedToken, err := secret.Encrypt([]byte("airtable-access-token"))
	if err != nil {
		t.Fatalf("secret.Encrypt() returned error: %v", err)
	}
	if err := store.PutAirtableToken(context.Background(), db.AirtableTokenRecord{
		UserID:                 "user_1",
		AccessTokenCiphertext:  encryptedToken,
		RefreshTokenCiphertext: encryptedToken,
		ExpiresAt:              time.Now().Add(time.Hour),
		Scopes:                 "data.records:read data.records:write schema.bases:read",
	}); err != nil {
		t.Fatalf("store.PutAirtableToken() returned error: %v", err)
	}

	bearerToken := "mcp-access-token"
	if err := store.PutMCPToken(context.Background(), db.MCPTokenRecord{
		TokenHash:  oauth.HashToken(bearerToken),
		UserID:     "user_1",
		ClientID:   ptr("client_claude"),
		ClientName: ptr("Claude"),
		CreatedAt:  time.Now().UTC(),
		ExpiresAt:  time.Now().Add(time.Hour).UTC(),
	}); err != nil {
		t.Fatalf("store.PutMCPToken() returned error: %v", err)
	}

	cfg := config.Config{
		SyncInterval:      time.Minute,
		QueryDefaultLimit: 100,
		QueryMaxLimit:     1000,
	}
	runtime := &tools.Runtime{
		Store:  store,
		Cipher: secret,
		Syncer: syncer.NewService(syncer.NewHTTPClient(fakeAirtable.URL, fakeAirtable.Client()), t.TempDir()),
		Config: cfg,
	}
	runtime.SyncManager = syncer.NewManager(runtime.Syncer, store, runtime, cfg.SyncInterval, 10*time.Minute)

	handler := oauth.NewMiddleware(store).RequireBearer(mcp.NewHandler("better-airtable-mcp", "0.1.0", tools.NewCatalog(cfg, runtime)))

	searchResponse := performAuthenticatedToolCall(t, handler, bearerToken, "search_bases", map[string]any{"query": "project"})
	searchResult := searchResponse["result"].(map[string]any)["structuredContent"].(map[string]any)
	bases := searchResult["bases"].([]any)
	if len(bases) != 1 {
		t.Fatalf("expected 1 base, got %#v", bases)
	}

	queryResponse := performAuthenticatedToolCall(t, handler, bearerToken, "query", map[string]any{
		"base": "Project Tracker",
		"sql":  "SELECT p.name, t.name AS task_name FROM projects p, UNNEST(p.linked_tasks) AS u(task_id) JOIN tasks t ON t.id = u.task_id",
	})
	queryResult := queryResponse["result"].(map[string]any)["structuredContent"].(map[string]any)
	rows := queryResult["rows"].([]any)
	if len(rows) != 1 {
		t.Fatalf("expected 1 query row, got %#v", rows)
	}
}

func TestAuthenticatedQueryReportsTruncationWhenServerAppliesDefaultLimit(t *testing.T) {
	port := mcpFreePort(t)
	postgres := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Port(uint32(port)).
			Database("better_airtable_mcp_truncation_test").
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

	store, err := db.Open(context.Background(), fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/better_airtable_mcp_truncation_test?sslmode=disable", port))
	if err != nil {
		t.Fatalf("db.Open() returned error: %v", err)
	}
	defer store.Close()
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("store.Migrate() returned error: %v", err)
	}

	fakeAirtable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v0/meta/bases":
			writeMCPJSON(t, w, map[string]any{
				"bases": []map[string]any{
					{"id": "appProjects", "name": "Project Tracker", "permissionLevel": "create"},
				},
			})
		case "/v0/meta/bases/appProjects/tables":
			writeMCPJSON(t, w, map[string]any{
				"tables": []map[string]any{
					{
						"id":   "tblProjects",
						"name": "Projects",
						"fields": []map[string]any{
							{"id": "fldName", "name": "Name", "type": "singleLineText"},
						},
					},
				},
			})
		case "/v0/appProjects/tblProjects":
			records := make([]map[string]any, 0, 101)
			for index := range 101 {
				records = append(records, map[string]any{
					"id":          fmt.Sprintf("rec%03d", index+1),
					"createdTime": "2026-04-01T12:00:00Z",
					"fields": map[string]any{
						"Name": fmt.Sprintf("Project %03d", index+1),
					},
				})
			}
			writeMCPJSON(t, w, map[string]any{"records": records})
		default:
			t.Fatalf("unexpected Airtable path %q", r.URL.Path)
		}
	}))
	defer fakeAirtable.Close()

	secret, err := cryptoutil.New([]byte(strings.Repeat("k", 32)))
	if err != nil {
		t.Fatalf("cryptoutil.New() returned error: %v", err)
	}
	if err := store.UpsertUser(context.Background(), db.User{ID: "user_1"}); err != nil {
		t.Fatalf("store.UpsertUser() returned error: %v", err)
	}

	encryptedToken, err := secret.Encrypt([]byte("airtable-access-token"))
	if err != nil {
		t.Fatalf("secret.Encrypt() returned error: %v", err)
	}
	if err := store.PutAirtableToken(context.Background(), db.AirtableTokenRecord{
		UserID:                 "user_1",
		AccessTokenCiphertext:  encryptedToken,
		RefreshTokenCiphertext: encryptedToken,
		ExpiresAt:              time.Now().Add(time.Hour),
		Scopes:                 "data.records:read data.records:write schema.bases:read",
	}); err != nil {
		t.Fatalf("store.PutAirtableToken() returned error: %v", err)
	}

	bearerToken := "mcp-access-token"
	if err := store.PutMCPToken(context.Background(), db.MCPTokenRecord{
		TokenHash:  oauth.HashToken(bearerToken),
		UserID:     "user_1",
		ClientID:   ptr("client_claude"),
		ClientName: ptr("Claude"),
		CreatedAt:  time.Now().UTC(),
		ExpiresAt:  time.Now().Add(time.Hour).UTC(),
	}); err != nil {
		t.Fatalf("store.PutMCPToken() returned error: %v", err)
	}

	cfg := config.Config{
		SyncInterval:      time.Minute,
		QueryDefaultLimit: 100,
		QueryMaxLimit:     1000,
	}
	runtime := &tools.Runtime{
		Store:  store,
		Cipher: secret,
		Syncer: syncer.NewService(syncer.NewHTTPClient(fakeAirtable.URL, fakeAirtable.Client()), t.TempDir()),
		Config: cfg,
	}
	runtime.SyncManager = syncer.NewManager(runtime.Syncer, store, runtime, cfg.SyncInterval, 10*time.Minute)

	handler := oauth.NewMiddleware(store).RequireBearer(mcp.NewHandler("better-airtable-mcp", "0.1.0", tools.NewCatalog(cfg, runtime)))

	queryResponse := performAuthenticatedToolCall(t, handler, bearerToken, "query", map[string]any{
		"base": "Project Tracker",
		"sql":  "SELECT id, name FROM projects ORDER BY id",
	})
	queryResult := queryResponse["result"].(map[string]any)["structuredContent"].(map[string]any)
	if truncated, ok := queryResult["truncated"].(bool); !ok || !truncated {
		t.Fatalf("expected query to report truncation, got %#v", queryResult)
	}
	if rowCount := int(queryResult["row_count"].(float64)); rowCount != 100 {
		t.Fatalf("expected row_count 100 after truncation, got %#v", queryResult["row_count"])
	}
	rows := queryResult["rows"].([]any)
	if len(rows) != 100 {
		t.Fatalf("expected 100 rows after truncation, got %d", len(rows))
	}
}

func performAuthenticatedToolCall(t *testing.T, handler http.Handler, bearerToken, toolName string, arguments map[string]any) map[string]any {
	t.Helper()

	sessionID := initializeAuthenticatedMCPSession(t, handler, bearerToken)

	payload, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tools/call",
		"params": map[string]any{
			"name":      toolName,
			"arguments": arguments,
		},
	})
	if err != nil {
		t.Fatalf("json.Marshal() returned error: %v", err)
	}

	request := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(payload))
	request.Header.Set("Authorization", "Bearer "+bearerToken)
	request.Header.Set(mcp.SessionHeader, sessionID)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d: %s", recorder.Code, recorder.Body.String())
	}

	var response map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("json.Unmarshal() returned error: %v", err)
	}

	return response
}

func initializeAuthenticatedMCPSession(t *testing.T, handler http.Handler, bearerToken string) string {
	t.Helper()

	payload, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "initialize",
	})
	if err != nil {
		t.Fatalf("json.Marshal() returned error: %v", err)
	}

	request := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(payload))
	request.Header.Set("Authorization", "Bearer "+bearerToken)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected initialize HTTP 200, got %d: %s", recorder.Code, recorder.Body.String())
	}

	sessionID := recorder.Header().Get(mcp.SessionHeader)
	if sessionID == "" {
		t.Fatal("expected initialize to return MCP session id")
	}
	return sessionID
}

func writeMCPJSON(t *testing.T, w http.ResponseWriter, payload any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		t.Fatalf("json.NewEncoder().Encode() returned error: %v", err)
	}
}

func mcpFreePort(t *testing.T) int {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() returned error: %v", err)
	}
	defer listener.Close()

	return listener.Addr().(*net.TCPAddr).Port
}

func ptr[T any](value T) *T {
	return &value
}
