//go:build integration

package mcp_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"

	"github.com/hackclub/better-airtable-mcp/internal/approval"
	"github.com/hackclub/better-airtable-mcp/internal/config"
	"github.com/hackclub/better-airtable-mcp/internal/cryptoutil"
	"github.com/hackclub/better-airtable-mcp/internal/db"
	"github.com/hackclub/better-airtable-mcp/internal/mcp"
	"github.com/hackclub/better-airtable-mcp/internal/oauth"
	syncer "github.com/hackclub/better-airtable-mcp/internal/sync"
	"github.com/hackclub/better-airtable-mcp/internal/tools"
)

// TestSchemaMutateStripsUncreatableFieldsOverMCP walks the full manage_schema
// approval flow for a create_table that includes a createdTime field: the tool
// response must carry a warning, the approval preview must already show the
// sanitized field list, and the create request that reaches Airtable on
// approval must not contain the stripped field.
func TestSchemaMutateStripsUncreatableFieldsOverMCP(t *testing.T) {
	port := mutateFreePort(t)
	postgres := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Port(uint32(port)).
			Database("better_airtable_schema_test").
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

	store, err := db.Open(context.Background(), fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/better_airtable_schema_test?sslmode=disable", port))
	if err != nil {
		t.Fatalf("db.Open() returned error: %v", err)
	}
	defer store.Close()
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("store.Migrate() returned error: %v", err)
	}

	var fakeMu sync.Mutex
	var createTableBody map[string]any
	tableCreated := false

	fakeAirtable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v0/meta/bases":
			writeMCPJSON(t, w, map[string]any{
				"bases": []map[string]any{
					{"id": "appProjects", "name": "Project Tracker", "permissionLevel": "create"},
				},
			})
		case r.URL.Path == "/v0/meta/bases/appProjects/tables" && r.Method == http.MethodGet:
			tables := []map[string]any{
				{
					"id":   "tblProjects",
					"name": "Projects",
					"fields": []map[string]any{
						{"id": "fldName", "name": "Name", "type": "singleLineText"},
					},
				},
			}
			fakeMu.Lock()
			if tableCreated {
				tables = append(tables, map[string]any{
					"id":   "tblShopOrders",
					"name": "Shop Orders",
					"fields": []map[string]any{
						{"id": "fldEmail", "name": "Email", "type": "email"},
						{"id": "fldPrize", "name": "Prize", "type": "singleLineText"},
					},
				})
			}
			fakeMu.Unlock()
			writeMCPJSON(t, w, map[string]any{"tables": tables})
		case r.URL.Path == "/v0/meta/bases/appProjects/tables" && r.Method == http.MethodPost:
			fakeMu.Lock()
			if err := json.NewDecoder(r.Body).Decode(&createTableBody); err != nil {
				fakeMu.Unlock()
				t.Errorf("decode create table payload: %v", err)
				return
			}
			tableCreated = true
			fakeMu.Unlock()
			writeMCPJSON(t, w, map[string]any{
				"id":   "tblShopOrders",
				"name": "Shop Orders",
				"fields": []map[string]any{
					{"id": "fldEmail", "name": "Email", "type": "email"},
					{"id": "fldPrize", "name": "Prize", "type": "singleLineText"},
				},
			})
		case strings.HasPrefix(r.URL.Path, "/v0/appProjects/") && r.Method == http.MethodGet:
			writeMCPJSON(t, w, map[string]any{"records": []map[string]any{}})
		default:
			t.Errorf("unexpected Airtable %s %s", r.Method, r.URL.Path)
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
		Scopes:                 "data.records:read schema.bases:read schema.bases:write",
	}); err != nil {
		t.Fatalf("store.PutAirtableToken() returned error: %v", err)
	}

	bearerToken := "mcp-access-token"
	if err := store.PutMCPToken(context.Background(), db.MCPTokenRecord{
		TokenHash: oauth.HashToken(bearerToken),
		UserID:    "user_1",
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().Add(time.Hour).UTC(),
	}); err != nil {
		t.Fatalf("store.PutMCPToken() returned error: %v", err)
	}

	cfg := config.Config{
		BaseURL:           mustParseTestURL(t, "http://example.test"),
		SyncInterval:      time.Minute,
		SyncTTL:           10 * time.Minute,
		ApprovalTTL:       60 * time.Minute,
		QueryDefaultLimit: 100,
		QueryMaxLimit:     1000,
	}
	syncService := syncer.NewService(syncer.NewHTTPClient(fakeAirtable.URL, fakeAirtable.Client()), t.TempDir())
	runtime := &tools.Runtime{
		Store:  store,
		Cipher: secret,
		Syncer: syncService,
		Config: cfg,
	}
	runtime.SyncManager = syncer.NewManager(syncService, store, runtime, cfg.SyncInterval, cfg.SyncTTL)
	runtime.Approval = approval.NewService(store, secret, syncService, runtime.SyncManager, runtime, syncer.NewHTTPClient(fakeAirtable.URL, fakeAirtable.Client()), cfg.BaseURLString(), cfg.ApprovalTTL)

	mux := http.NewServeMux()
	mux.Handle("/mcp", oauth.NewMiddleware(store, "").RequireBearer(mcp.NewHandler("better-airtable-mcp", "0.1.0", tools.NewCatalog(cfg, runtime))))
	approvalHandler := approval.NewHandler(runtime.Approval)
	mux.HandleFunc("/api/operations/", approvalHandler.ServeOperationAPI)

	ensureBaseSyncedForMutationTest(t, runtime, "user_1", "Project Tracker")

	schemaResponse := performAuthenticatedToolCall(t, mux, bearerToken, "manage_schema", map[string]any{
		"base": "Project Tracker",
		"operations": []map[string]any{
			{
				"type": "create_table",
				"name": "Shop Orders",
				"fields": []map[string]any{
					{"name": "Email", "type": "email"},
					{"name": "created_at", "type": "createdTime"},
					{"name": "Prize", "type": "singleLineText"},
				},
			},
		},
	})
	schemaText := firstToolText(t, schemaResponse)
	if !strings.Contains(schemaText, "warnings") {
		t.Fatalf("expected warnings column in manage_schema text, got %q", schemaText)
	}
	structured := schemaResponse["result"].(map[string]any)["structuredContent"].(map[string]any)
	warnings, _ := structured["warnings"].(string)
	if !strings.Contains(warnings, "created_at") || !strings.Contains(warnings, "createdTime") {
		t.Fatalf("expected warning naming the stripped createdTime field, got %#v", structured)
	}
	if structured["status"].(string) != "pending_approval" {
		t.Fatalf("expected pending_approval, got %#v", structured)
	}
	operationID := structured["operation_id"].(string)

	getRequest := httptest.NewRequest(http.MethodGet, "/api/operations/"+operationID, nil)
	getRecorder := httptest.NewRecorder()
	mux.ServeHTTP(getRecorder, getRequest)
	if getRecorder.Code != http.StatusOK {
		t.Fatalf("expected GET operation to return 200, got %d: %s", getRecorder.Code, getRecorder.Body.String())
	}
	var operation approval.OperationView
	if err := json.Unmarshal(getRecorder.Body.Bytes(), &operation); err != nil {
		t.Fatalf("json.Unmarshal() returned error: %v", err)
	}
	if len(operation.SchemaOperations) != 1 {
		t.Fatalf("expected one schema operation in preview, got %#v", operation.SchemaOperations)
	}
	previewFields := operation.SchemaOperations[0].Fields
	if len(previewFields) != 2 || previewFields[0].Name != "Email" || previewFields[1].Name != "Prize" {
		t.Fatalf("expected approval preview to show only creatable fields, got %#v", previewFields)
	}

	approveRequest := httptest.NewRequest(http.MethodPost, "/api/operations/"+operationID+"/approve", strings.NewReader(`{}`))
	approveRecorder := httptest.NewRecorder()
	mux.ServeHTTP(approveRecorder, approveRequest)
	if approveRecorder.Code != http.StatusOK {
		t.Fatalf("expected approve to return 200, got %d: %s", approveRecorder.Code, approveRecorder.Body.String())
	}
	var approved approval.OperationView
	if err := json.Unmarshal(approveRecorder.Body.Bytes(), &approved); err != nil {
		t.Fatalf("json.Unmarshal() returned error: %v", err)
	}
	if approved.Status != "completed" {
		t.Fatalf("expected approved schema operation to complete, got %#v", approved)
	}
	if approved.SchemaResult == nil || len(approved.SchemaResult.CreatedTableIDs) != 1 || approved.SchemaResult.CreatedTableIDs[0] != "tblShopOrders" {
		t.Fatalf("unexpected schema execution result %#v", approved.SchemaResult)
	}

	fakeMu.Lock()
	defer fakeMu.Unlock()
	if createTableBody == nil {
		t.Fatal("expected a create table request to reach Airtable")
	}
	sentFields, _ := createTableBody["fields"].([]any)
	if len(sentFields) != 2 {
		t.Fatalf("expected 2 fields in Airtable create request, got %#v", createTableBody["fields"])
	}
	for _, raw := range sentFields {
		field := raw.(map[string]any)
		if field["type"] == "createdTime" {
			t.Fatalf("createdTime field leaked into the Airtable create request: %#v", sentFields)
		}
	}
}
