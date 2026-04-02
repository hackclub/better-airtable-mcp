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

	searchResponse := performAuthenticatedToolCall(t, handler, bearerToken, "list_bases", map[string]any{"query": "project"})
	searchText := firstToolText(t, searchResponse)
	if !strings.Contains(searchText, "Project Tracker") {
		t.Fatalf("expected list_bases text to include base name, got %q", searchText)
	}
	searchResult := searchResponse["result"].(map[string]any)["structuredContent"].(map[string]any)
	bases := searchResult["bases"].([]any)
	if len(bases) != 1 {
		t.Fatalf("expected 1 base, got %#v", bases)
	}

	schemaResponse := performAuthenticatedToolCall(t, handler, bearerToken, "list_schema", map[string]any{
		"base": "Project Tracker",
	})
	schemaText := firstToolText(t, schemaResponse)
	if !strings.Contains(schemaText, "sync_status\n") ||
		!strings.Contains(schemaText, "base\n\nbase_id,base_name\nappProjects,Project Tracker\n") ||
		!strings.Contains(schemaText, "tables\n\n# projects\n") ||
		!strings.Contains(schemaText, "# projects\n\nid,created_time,name,linked_tasks\n") ||
		!strings.Contains(schemaText, "# tasks\n\nid,created_time,name\n") {
		t.Fatalf("expected list_schema text to contain sync status, base, and per-table sample sections, got %q", schemaText)
	}
	schemaResult := schemaResponse["result"].(map[string]any)["structuredContent"].(map[string]any)
	if _, ok := schemaResult["sync"].(map[string]any); !ok {
		t.Fatalf("expected list_schema structured content to include sync metadata, got %#v", schemaResult)
	}

	queryResponse := waitForToolResult(t, func() map[string]any {
		return performAuthenticatedToolCall(t, handler, bearerToken, "query", map[string]any{
			"base": "Project Tracker",
			"sql": []string{
				"SELECT p.name, t.name AS task_name FROM projects p, UNNEST(p.linked_tasks) AS u(task_id) JOIN tasks t ON t.id = u.task_id",
			},
		})
	}, func(response map[string]any) bool {
		queryResult := response["result"].(map[string]any)["structuredContent"].(map[string]any)
		results := queryResult["results"].([]any)
		rows := results[0].(map[string]any)["rows"].([]any)
		return len(rows) == 1
	})
	queryText := firstToolText(t, queryResponse)
	if !strings.Contains(queryText, "query_rows\n") || !strings.Contains(queryText, "Website Redesign,Design new homepage") {
		t.Fatalf("expected query text to contain CSV rows, got %q", queryText)
	}
	queryResult := queryResponse["result"].(map[string]any)["structuredContent"].(map[string]any)
	results := queryResult["results"].([]any)
	if len(results) != 1 {
		t.Fatalf("expected 1 query result, got %#v", queryResult)
	}
	rows := results[0].(map[string]any)["rows"].([]any)
	if len(rows) != 1 {
		t.Fatalf("expected 1 query row, got %#v", rows)
	}

	batchResponse := waitForToolResult(t, func() map[string]any {
		return performAuthenticatedToolCall(t, handler, bearerToken, "query", map[string]any{
			"base": "Project Tracker",
			"sql": []string{
				"SELECT name FROM projects ORDER BY id",
				"SELECT name FROM tasks ORDER BY id",
			},
		})
	}, func(response map[string]any) bool {
		batchResult := response["result"].(map[string]any)["structuredContent"].(map[string]any)
		results := batchResult["results"].([]any)
		return len(results[0].(map[string]any)["rows"].([]any)) == 1 &&
			len(results[1].(map[string]any)["rows"].([]any)) == 1
	})
	batchText := firstToolText(t, batchResponse)
	if !strings.Contains(batchText, "query_1_rows\n") || !strings.Contains(batchText, "query_2_rows\n") {
		t.Fatalf("expected batch query text to contain indexed sections, got %q", batchText)
	}
	batchResult := batchResponse["result"].(map[string]any)["structuredContent"].(map[string]any)
	batchResults := batchResult["results"].([]any)
	if len(batchResults) != 2 {
		t.Fatalf("expected 2 batch query results, got %#v", batchResult)
	}
	firstRows := batchResults[0].(map[string]any)["rows"].([]any)
	secondRows := batchResults[1].(map[string]any)["rows"].([]any)
	if len(firstRows) != 1 || len(secondRows) != 1 {
		t.Fatalf("expected 1 row per batch result, got first=%#v second=%#v", firstRows, secondRows)
	}
}

func TestAuthenticatedReadToolsRejectCachedBaseWithoutAirtableAccess(t *testing.T) {
	port := mcpFreePort(t)
	postgres := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Port(uint32(port)).
			Database("better_airtable_mcp_cross_user_read_test").
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

	store, err := db.Open(context.Background(), fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/better_airtable_mcp_cross_user_read_test?sslmode=disable", port))
	if err != nil {
		t.Fatalf("db.Open() returned error: %v", err)
	}
	defer store.Close()
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("store.Migrate() returned error: %v", err)
	}

	fakeAirtable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		switch r.URL.Path {
		case "/v0/meta/bases":
			switch authHeader {
			case "Bearer airtable-access-token-user-1":
				writeMCPJSON(t, w, map[string]any{
					"bases": []map[string]any{
						{"id": "appProjects", "name": "Project Tracker", "permissionLevel": "create"},
					},
				})
			case "Bearer airtable-access-token-user-2":
				writeMCPJSON(t, w, map[string]any{"bases": []map[string]any{}})
			default:
				t.Fatalf("unexpected Authorization header %q on %s", authHeader, r.URL.Path)
			}
		case "/v0/meta/bases/appProjects/tables":
			if authHeader != "Bearer airtable-access-token-user-1" {
				t.Fatalf("unexpected schema request with Authorization %q", authHeader)
			}
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
			if authHeader != "Bearer airtable-access-token-user-1" {
				t.Fatalf("unexpected record request with Authorization %q", authHeader)
			}
			writeMCPJSON(t, w, map[string]any{
				"records": []map[string]any{
					{
						"id":          "recProject1",
						"createdTime": "2026-04-01T12:00:00Z",
						"fields": map[string]any{
							"Name": "Website Redesign",
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

	for _, userID := range []string{"user_1", "user_2"} {
		if err := store.UpsertUser(context.Background(), db.User{ID: userID}); err != nil {
			t.Fatalf("store.UpsertUser(%q) returned error: %v", userID, err)
		}
	}

	user1Token, err := secret.Encrypt([]byte("airtable-access-token-user-1"))
	if err != nil {
		t.Fatalf("secret.Encrypt(user1) returned error: %v", err)
	}
	if err := store.PutAirtableToken(context.Background(), db.AirtableTokenRecord{
		UserID:                 "user_1",
		AccessTokenCiphertext:  user1Token,
		RefreshTokenCiphertext: user1Token,
		ExpiresAt:              time.Now().Add(time.Hour),
		Scopes:                 "data.records:read data.records:write schema.bases:read",
	}); err != nil {
		t.Fatalf("store.PutAirtableToken(user_1) returned error: %v", err)
	}
	user2Token, err := secret.Encrypt([]byte("airtable-access-token-user-2"))
	if err != nil {
		t.Fatalf("secret.Encrypt(user2) returned error: %v", err)
	}
	if err := store.PutAirtableToken(context.Background(), db.AirtableTokenRecord{
		UserID:                 "user_2",
		AccessTokenCiphertext:  user2Token,
		RefreshTokenCiphertext: user2Token,
		ExpiresAt:              time.Now().Add(time.Hour),
		Scopes:                 "data.records:read data.records:write schema.bases:read",
	}); err != nil {
		t.Fatalf("store.PutAirtableToken(user_2) returned error: %v", err)
	}

	for bearerToken, userID := range map[string]string{
		"mcp-access-token-user-1": "user_1",
		"mcp-access-token-user-2": "user_2",
	} {
		if err := store.PutMCPToken(context.Background(), db.MCPTokenRecord{
			TokenHash: oauth.HashToken(bearerToken),
			UserID:    userID,
			CreatedAt: time.Now().UTC(),
			ExpiresAt: time.Now().Add(time.Hour).UTC(),
		}); err != nil {
			t.Fatalf("store.PutMCPToken(%q) returned error: %v", userID, err)
		}
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

	warmResponse := performAuthenticatedToolCall(t, handler, "mcp-access-token-user-1", "list_schema", map[string]any{
		"base": "appProjects",
	})
	if isError, _ := warmResponse["result"].(map[string]any)["isError"].(bool); isError {
		t.Fatalf("expected authorized user to warm cache successfully, got %#v", warmResponse)
	}

	queryResponse := performAuthenticatedToolCall(t, handler, "mcp-access-token-user-2", "query", map[string]any{
		"base": "appProjects",
		"sql":  []string{"SELECT name FROM projects"},
	})
	queryResult := queryResponse["result"].(map[string]any)
	if isError, _ := queryResult["isError"].(bool); !isError {
		t.Fatalf("expected unauthorized query to fail, got %#v", queryResponse)
	}
	if text := firstToolText(t, queryResponse); !strings.Contains(text, `base "appProjects" was not found`) {
		t.Fatalf("expected unauthorized query error to mention missing base, got %q", text)
	}

	schemaResponse := performAuthenticatedToolCall(t, handler, "mcp-access-token-user-2", "list_schema", map[string]any{
		"base": "appProjects",
	})
	schemaResult := schemaResponse["result"].(map[string]any)
	if isError, _ := schemaResult["isError"].(bool); !isError {
		t.Fatalf("expected unauthorized list_schema to fail, got %#v", schemaResponse)
	}
	if text := firstToolText(t, schemaResponse); !strings.Contains(text, `base "appProjects" was not found`) {
		t.Fatalf("expected unauthorized schema error to mention missing base, got %q", text)
	}
}

func TestCheckOperationRejectsSyncStatusWithoutAirtableAccess(t *testing.T) {
	port := mcpFreePort(t)
	postgres := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Port(uint32(port)).
			Database("better_airtable_mcp_sync_access_test").
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

	store, err := db.Open(context.Background(), fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/better_airtable_mcp_sync_access_test?sslmode=disable", port))
	if err != nil {
		t.Fatalf("db.Open() returned error: %v", err)
	}
	defer store.Close()
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("store.Migrate() returned error: %v", err)
	}

	fakeAirtable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		switch r.URL.Path {
		case "/v0/meta/bases":
			switch authHeader {
			case "Bearer airtable-access-token-user-1":
				writeMCPJSON(t, w, map[string]any{
					"bases": []map[string]any{
						{"id": "appProjects", "name": "Project Tracker", "permissionLevel": "create"},
					},
				})
			case "Bearer airtable-access-token-user-2":
				writeMCPJSON(t, w, map[string]any{"bases": []map[string]any{}})
			default:
				t.Fatalf("unexpected Authorization header %q on %s", authHeader, r.URL.Path)
			}
		case "/v0/meta/bases/appProjects/tables":
			if authHeader != "Bearer airtable-access-token-user-1" {
				t.Fatalf("unexpected schema request with Authorization %q", authHeader)
			}
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
			if authHeader != "Bearer airtable-access-token-user-1" {
				t.Fatalf("unexpected record request with Authorization %q", authHeader)
			}
			writeMCPJSON(t, w, map[string]any{
				"records": []map[string]any{
					{
						"id":          "recProject1",
						"createdTime": "2026-04-01T12:00:00Z",
						"fields": map[string]any{
							"Name": "Website Redesign",
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

	for _, userID := range []string{"user_1", "user_2"} {
		if err := store.UpsertUser(context.Background(), db.User{ID: userID}); err != nil {
			t.Fatalf("store.UpsertUser(%q) returned error: %v", userID, err)
		}
	}

	user1Token, err := secret.Encrypt([]byte("airtable-access-token-user-1"))
	if err != nil {
		t.Fatalf("secret.Encrypt(user1) returned error: %v", err)
	}
	if err := store.PutAirtableToken(context.Background(), db.AirtableTokenRecord{
		UserID:                 "user_1",
		AccessTokenCiphertext:  user1Token,
		RefreshTokenCiphertext: user1Token,
		ExpiresAt:              time.Now().Add(time.Hour),
		Scopes:                 "data.records:read data.records:write schema.bases:read",
	}); err != nil {
		t.Fatalf("store.PutAirtableToken(user_1) returned error: %v", err)
	}
	user2Token, err := secret.Encrypt([]byte("airtable-access-token-user-2"))
	if err != nil {
		t.Fatalf("secret.Encrypt(user2) returned error: %v", err)
	}
	if err := store.PutAirtableToken(context.Background(), db.AirtableTokenRecord{
		UserID:                 "user_2",
		AccessTokenCiphertext:  user2Token,
		RefreshTokenCiphertext: user2Token,
		ExpiresAt:              time.Now().Add(time.Hour),
		Scopes:                 "data.records:read data.records:write schema.bases:read",
	}); err != nil {
		t.Fatalf("store.PutAirtableToken(user_2) returned error: %v", err)
	}

	for bearerToken, userID := range map[string]string{
		"mcp-access-token-user-1": "user_1",
		"mcp-access-token-user-2": "user_2",
	} {
		if err := store.PutMCPToken(context.Background(), db.MCPTokenRecord{
			TokenHash: oauth.HashToken(bearerToken),
			UserID:    userID,
			CreatedAt: time.Now().UTC(),
			ExpiresAt: time.Now().Add(time.Hour).UTC(),
		}); err != nil {
			t.Fatalf("store.PutMCPToken(%q) returned error: %v", userID, err)
		}
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

	syncResponse := performAuthenticatedToolCall(t, handler, "mcp-access-token-user-1", "sync", map[string]any{
		"base": "appProjects",
	})
	syncResult := syncResponse["result"].(map[string]any)["structuredContent"].(map[string]any)
	operationID := syncResult["operation_id"].(string)
	if operationID != "sync_appProjects" {
		t.Fatalf("expected sync operation id sync_appProjects, got %#v", syncResult)
	}

	checkResponse := performAuthenticatedToolCall(t, handler, "mcp-access-token-user-2", "check_operation", map[string]any{
		"operation_id": operationID,
	})
	checkResult := checkResponse["result"].(map[string]any)
	if isError, _ := checkResult["isError"].(bool); !isError {
		t.Fatalf("expected unauthorized sync status lookup to fail, got %#v", checkResponse)
	}
	if text := firstToolText(t, checkResponse); !strings.Contains(text, "operation was not found") {
		t.Fatalf("expected sync status lookup error to hide the operation, got %q", text)
	}
}

func firstToolText(t *testing.T, response map[string]any) string {
	t.Helper()
	result := response["result"].(map[string]any)
	content := result["content"].([]any)
	if len(content) == 0 {
		t.Fatal("expected tool result content")
	}
	block := content[0].(map[string]any)
	text, _ := block["text"].(string)
	return text
}

func waitForToolResult(t *testing.T, call func() map[string]any, done func(map[string]any) bool) map[string]any {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		response := call()
		if done(response) {
			return response
		}
		time.Sleep(50 * time.Millisecond)
	}

	response := call()
	if done(response) {
		return response
	}
	t.Fatalf("timed out waiting for tool result readiness: %#v", response)
	return nil
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

	queryResponse := waitForToolResult(t, func() map[string]any {
		return performAuthenticatedToolCall(t, handler, bearerToken, "query", map[string]any{
			"base": "Project Tracker",
			"sql": []string{
				"SELECT id, name FROM projects ORDER BY id",
			},
		})
	}, func(response map[string]any) bool {
		queryResult := response["result"].(map[string]any)["structuredContent"].(map[string]any)
		results := queryResult["results"].([]any)
		if len(results) != 1 {
			return false
		}
		firstResult := results[0].(map[string]any)
		truncated, ok := firstResult["truncated"].(bool)
		return ok && truncated
	})
	queryResult := queryResponse["result"].(map[string]any)["structuredContent"].(map[string]any)
	results := queryResult["results"].([]any)
	if len(results) != 1 {
		t.Fatalf("expected 1 query result, got %#v", queryResult)
	}
	firstResult := results[0].(map[string]any)
	if truncated, ok := firstResult["truncated"].(bool); !ok || !truncated {
		t.Fatalf("expected query to report truncation, got %#v", queryResult)
	}
	if rowCount := int(firstResult["row_count"].(float64)); rowCount != 100 {
		t.Fatalf("expected row_count 100 after truncation, got %#v", firstResult["row_count"])
	}
	rows := firstResult["rows"].([]any)
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
