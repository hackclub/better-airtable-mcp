package mcp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"

	"github.com/hackclub/better-airtable-mcp/internal/approval"
	"github.com/hackclub/better-airtable-mcp/internal/config"
	"github.com/hackclub/better-airtable-mcp/internal/cryptoutil"
	"github.com/hackclub/better-airtable-mcp/internal/db"
	"github.com/hackclub/better-airtable-mcp/internal/logx"
	"github.com/hackclub/better-airtable-mcp/internal/mcp"
	"github.com/hackclub/better-airtable-mcp/internal/oauth"
	syncer "github.com/hackclub/better-airtable-mcp/internal/sync"
	"github.com/hackclub/better-airtable-mcp/internal/tools"
)

func TestMutateApprovalFlowOverMCP(t *testing.T) {
	port := mutateFreePort(t)
	postgres := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Port(uint32(port)).
			Database("better_airtable_mutate_test").
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

	store, err := db.Open(context.Background(), fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/better_airtable_mutate_test?sslmode=disable", port))
	if err != nil {
		t.Fatalf("db.Open() returned error: %v", err)
	}
	defer store.Close()
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("store.Migrate() returned error: %v", err)
	}

	var recordListCalls atomic.Int32
	var recordsMu sync.Mutex
	records := map[string]map[string]any{
		"recProject1": {
			"Name":   "Website Redesign",
			"Status": "Planning",
		},
	}

	fakeAirtable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v0/meta/bases":
			writeMCPJSON(t, w, map[string]any{
				"bases": []map[string]any{
					{"id": "appProjects", "name": "Project Tracker", "permissionLevel": "create"},
				},
			})
		case r.URL.Path == "/v0/meta/bases/appProjects/tables":
			writeMCPJSON(t, w, map[string]any{
				"tables": []map[string]any{
					{
						"id":   "tblProjects",
						"name": "Projects",
						"fields": []map[string]any{
							{"id": "fldName", "name": "Name", "type": "singleLineText"},
							{"id": "fldStatus", "name": "Status", "type": "singleSelect"},
						},
					},
				},
			})
		case r.URL.Path == "/v0/appProjects/tblProjects" && r.Method == http.MethodGet:
			recordListCalls.Add(1)
			recordsMu.Lock()
			defer recordsMu.Unlock()
			payload := make([]map[string]any, 0, len(records))
			for id, fields := range records {
				payload = append(payload, map[string]any{
					"id":          id,
					"createdTime": "2026-04-01T12:00:00Z",
					"fields":      fields,
				})
			}
			writeMCPJSON(t, w, map[string]any{"records": payload})
		case r.URL.Path == "/v0/appProjects/tblProjects" && r.Method == http.MethodPatch:
			var request struct {
				Records []struct {
					ID     string         `json:"id"`
					Fields map[string]any `json:"fields"`
				} `json:"records"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode patch payload: %v", err)
			}

			recordsMu.Lock()
			defer recordsMu.Unlock()
			responseRecords := make([]map[string]any, 0, len(request.Records))
			for _, record := range request.Records {
				current := records[record.ID]
				for key, value := range record.Fields {
					current[key] = value
				}
				responseRecords = append(responseRecords, map[string]any{
					"id":          record.ID,
					"createdTime": "2026-04-01T12:00:00Z",
					"fields":      current,
				})
			}
			writeMCPJSON(t, w, map[string]any{"records": responseRecords})
		default:
			t.Fatalf("unexpected Airtable %s %s", r.Method, r.URL.Path)
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
		BaseURL:           mustParseTestURL(t, "http://example.test"),
		SyncInterval:      time.Minute,
		SyncTTL:           10 * time.Minute,
		ApprovalTTL:       10 * time.Minute,
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
	mux.Handle("/mcp", oauth.NewMiddleware(store).RequireBearer(mcp.NewHandler("better-airtable-mcp", "0.1.0", tools.NewCatalog(cfg, runtime))))
	approvalHandler := approval.NewHandler(runtime.Approval)
	mux.HandleFunc("/api/operations/", approvalHandler.ServeOperationAPI)

	ensureBaseSyncedForMutationTest(t, runtime, "user_1", "Project Tracker")

	mutateResponse := performAuthenticatedToolCall(t, mux, bearerToken, "mutate", map[string]any{
		"base": "Project Tracker",
		"operations": []map[string]any{
			{
				"type":  "update_records",
				"table": "projects",
				"records": []map[string]any{
					{
						"id": "recProject1",
						"fields": map[string]any{
							"status": "Done",
						},
					},
				},
			},
		},
	})
	mutateText := firstToolText(t, mutateResponse)
	if !strings.Contains(mutateText, "operation_id,status,approval_url,expires_at,summary,assistant_instruction\n") {
		t.Fatalf("expected mutate text CSV header, got %q", mutateText)
	}
	mutateStructured := mutateResponse["result"].(map[string]any)["structuredContent"].(map[string]any)
	operationID := mutateStructured["operation_id"].(string)
	if mutateStructured["status"].(string) != "pending_approval" {
		t.Fatalf("expected pending_approval, got %#v", mutateStructured)
	}
	if mutateStructured["assistant_instruction"] != tools.AssistantInstructionForApprovalURL() {
		t.Fatalf("expected assistant instruction in mutate response, got %#v", mutateStructured["assistant_instruction"])
	}

	checkPending := performAuthenticatedToolCall(t, mux, bearerToken, "check_operation", map[string]any{
		"operation_id": operationID,
	})
	pendingText := firstToolText(t, checkPending)
	if !strings.Contains(pendingText, "operation_id,type,status,approval_url,summary,assistant_instruction,result,error\n") {
		t.Fatalf("expected check_operation CSV header, got %q", pendingText)
	}
	pendingPayload := checkPending["result"].(map[string]any)["structuredContent"].(map[string]any)
	if pendingPayload["status"].(string) != "pending_approval" {
		t.Fatalf("expected pending operation, got %#v", pendingPayload)
	}
	if pendingPayload["assistant_instruction"] != tools.AssistantInstructionForApprovalURL() {
		t.Fatalf("expected assistant instruction in pending operation, got %#v", pendingPayload["assistant_instruction"])
	}

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
	if got := operation.Operations[0].Records[0].CurrentFields["status"]; got != "Planning" {
		t.Fatalf("expected preview current status Planning, got %#v", got)
	}
	if operation.MCPSessionID == "" {
		t.Fatal("expected approval operation to include MCP session id")
	}
	if operation.MCPClientID != "client_claude" {
		t.Fatalf("expected approval operation to include MCP client id, got %#v", operation)
	}
	if operation.MCPClientName != "Claude" {
		t.Fatalf("expected approval operation to include MCP client name, got %#v", operation)
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
		t.Fatalf("expected approved operation to complete, got %#v", approved)
	}
	if approved.Result == nil || len(approved.Result.UpdatedRecordIDs) != 1 || approved.Result.UpdatedRecordIDs[0] != "recProject1" {
		t.Fatalf("unexpected execution result %#v", approved.Result)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if recordListCalls.Load() >= 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if recordListCalls.Load() < 2 {
		t.Fatalf("expected approval to trigger a follow-up sync, only saw %d record list call(s)", recordListCalls.Load())
	}

	checkCompleted := performAuthenticatedToolCall(t, mux, bearerToken, "check_operation", map[string]any{
		"operation_id": operationID,
	})
	completedText := firstToolText(t, checkCompleted)
	if !strings.Contains(completedText, "completed") {
		t.Fatalf("expected completed operation text to include CSV row, got %q", completedText)
	}
	completedPayload := checkCompleted["result"].(map[string]any)["structuredContent"].(map[string]any)
	if completedPayload["status"].(string) != "completed" {
		t.Fatalf("expected completed mutate status, got %#v", completedPayload)
	}
}

func TestMutateApprovalFlowLogsWithoutLeakingPayloadValues(t *testing.T) {
	port := mutateFreePort(t)
	postgres := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Port(uint32(port)).
			Database("better_airtable_mutate_log_test").
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

	store, err := db.Open(context.Background(), fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/better_airtable_mutate_log_test?sslmode=disable", port))
	if err != nil {
		t.Fatalf("db.Open() returned error: %v", err)
	}
	defer store.Close()
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("store.Migrate() returned error: %v", err)
	}

	records := map[string]map[string]any{
		"recProject1": {
			"Name":   "Website Redesign",
			"Status": "Planning",
		},
	}

	fakeAirtable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v0/meta/bases":
			writeMCPJSON(t, w, map[string]any{
				"bases": []map[string]any{
					{"id": "appProjects", "name": "Project Tracker", "permissionLevel": "create"},
				},
			})
		case r.URL.Path == "/v0/meta/bases/appProjects/tables":
			writeMCPJSON(t, w, map[string]any{
				"tables": []map[string]any{
					{
						"id":   "tblProjects",
						"name": "Projects",
						"fields": []map[string]any{
							{"id": "fldName", "name": "Name", "type": "singleLineText"},
							{"id": "fldStatus", "name": "Status", "type": "singleSelect"},
						},
					},
				},
			})
		case r.URL.Path == "/v0/appProjects/tblProjects" && r.Method == http.MethodGet:
			payload := make([]map[string]any, 0, len(records))
			for id, fields := range records {
				payload = append(payload, map[string]any{
					"id":          id,
					"createdTime": "2026-04-01T12:00:00Z",
					"fields":      cloneAnyMap(fields),
				})
			}
			writeMCPJSON(t, w, map[string]any{"records": payload})
		case r.URL.Path == "/v0/appProjects/tblProjects" && r.Method == http.MethodPatch:
			var request struct {
				Records []struct {
					ID     string         `json:"id"`
					Fields map[string]any `json:"fields"`
				} `json:"records"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("json.NewDecoder().Decode() returned error: %v", err)
			}
			responseRecords := make([]map[string]any, 0, len(request.Records))
			for _, record := range request.Records {
				current := records[record.ID]
				for key, value := range record.Fields {
					current[key] = value
				}
				responseRecords = append(responseRecords, map[string]any{
					"id":          record.ID,
					"createdTime": "2026-04-01T12:00:00Z",
					"fields":      cloneAnyMap(current),
				})
			}
			writeMCPJSON(t, w, map[string]any{"records": responseRecords})
		default:
			t.Fatalf("unexpected Airtable %s %s", r.Method, r.URL.Path)
		}
	}))
	defer fakeAirtable.Close()

	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(logx.NewLogger(&output))
	t.Cleanup(func() {
		slog.SetDefault(previous)
	})

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
		BaseURL:           mustParseTestURL(t, "http://example.test"),
		SyncInterval:      time.Minute,
		SyncTTL:           10 * time.Minute,
		ApprovalTTL:       10 * time.Minute,
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
	mux.Handle("/mcp", oauth.NewMiddleware(store).RequireBearer(mcp.NewHandler("better-airtable-mcp", "0.1.0", tools.NewCatalog(cfg, runtime))))
	approvalHandler := approval.NewHandler(runtime.Approval)
	mux.HandleFunc("/api/operations/", approvalHandler.ServeOperationAPI)

	ensureBaseSyncedForMutationTest(t, runtime, "user_1", "Project Tracker")

	mutateResponse := performAuthenticatedToolCall(t, mux, bearerToken, "mutate", map[string]any{
		"base": "Project Tracker",
		"operations": []map[string]any{
			{
				"type":  "update_records",
				"table": "projects",
				"records": []map[string]any{
					{
						"id": "recProject1",
						"fields": map[string]any{
							"status": "LEAK-CANDIDATE-STATUS",
						},
					},
				},
			},
		},
	})
	operationID := mutateResponse["result"].(map[string]any)["structuredContent"].(map[string]any)["operation_id"].(string)

	approveRequest := httptest.NewRequest(http.MethodPost, "/api/operations/"+operationID+"/approve", strings.NewReader(`{}`))
	approveRecorder := httptest.NewRecorder()
	mux.ServeHTTP(approveRecorder, approveRequest)
	if approveRecorder.Code != http.StatusOK {
		t.Fatalf("expected approve to return 200, got %d: %s", approveRecorder.Code, approveRecorder.Body.String())
	}

	logText := output.String()
	for _, required := range []string{`"event":"approval.prepare_completed"`, `"event":"approval.execute_batch"`, `"event":"approval.execute_completed"`, `"event":"mcp.rpc.completed"`} {
		if !strings.Contains(logText, required) {
			t.Fatalf("expected %s in logs, got %s", required, logText)
		}
	}
	for _, forbidden := range []string{"LEAK-CANDIDATE-STATUS", operationID, "/approve/" + operationID} {
		if strings.Contains(logText, forbidden) {
			t.Fatalf("expected logs to exclude %q, got %s", forbidden, logText)
		}
	}
}

func TestCheckOperationRejectsMutationStatusForDifferentUser(t *testing.T) {
	port := mutateFreePort(t)
	postgres := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Port(uint32(port)).
			Database("better_airtable_mutate_cross_user_test").
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

	store, err := db.Open(context.Background(), fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/better_airtable_mutate_cross_user_test?sslmode=disable", port))
	if err != nil {
		t.Fatalf("db.Open() returned error: %v", err)
	}
	defer store.Close()
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("store.Migrate() returned error: %v", err)
	}

	fakeAirtable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v0/meta/bases":
			writeMCPJSON(t, w, map[string]any{
				"bases": []map[string]any{
					{"id": "appProjects", "name": "Project Tracker", "permissionLevel": "create"},
				},
			})
		case r.URL.Path == "/v0/meta/bases/appProjects/tables":
			writeMCPJSON(t, w, map[string]any{
				"tables": []map[string]any{
					{
						"id":   "tblProjects",
						"name": "Projects",
						"fields": []map[string]any{
							{"id": "fldName", "name": "Name", "type": "singleLineText"},
							{"id": "fldStatus", "name": "Status", "type": "singleSelect"},
						},
					},
				},
			})
		case r.URL.Path == "/v0/appProjects/tblProjects" && r.Method == http.MethodGet:
			writeMCPJSON(t, w, map[string]any{
				"records": []map[string]any{
					{
						"id":          "recProject1",
						"createdTime": "2026-04-01T12:00:00Z",
						"fields": map[string]any{
							"Name":   "Website Redesign",
							"Status": "Planning",
						},
					},
				},
			})
		default:
			t.Fatalf("unexpected Airtable %s %s", r.Method, r.URL.Path)
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
		t.Fatalf("store.PutAirtableToken(user_1) returned error: %v", err)
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
		BaseURL:           mustParseTestURL(t, "http://example.test"),
		SyncInterval:      time.Minute,
		SyncTTL:           10 * time.Minute,
		ApprovalTTL:       10 * time.Minute,
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

	handler := oauth.NewMiddleware(store).RequireBearer(mcp.NewHandler("better-airtable-mcp", "0.1.0", tools.NewCatalog(cfg, runtime)))

	ensureBaseSyncedForMutationTest(t, runtime, "user_1", "Project Tracker")

	mutateResponse := performAuthenticatedToolCall(t, handler, "mcp-access-token-user-1", "mutate", map[string]any{
		"base": "Project Tracker",
		"operations": []map[string]any{
			{
				"type":  "update_records",
				"table": "projects",
				"records": []map[string]any{
					{
						"id": "recProject1",
						"fields": map[string]any{
							"status": "Done",
						},
					},
				},
			},
		},
	})
	mutateStructured := mutateResponse["result"].(map[string]any)["structuredContent"].(map[string]any)
	operationID := mutateStructured["operation_id"].(string)

	checkResponse := performAuthenticatedToolCall(t, handler, "mcp-access-token-user-2", "check_operation", map[string]any{
		"operation_id": operationID,
	})
	checkResult := checkResponse["result"].(map[string]any)
	if isError, _ := checkResult["isError"].(bool); !isError {
		t.Fatalf("expected cross-user mutation lookup to fail, got %#v", checkResponse)
	}
	if text := firstToolText(t, checkResponse); !strings.Contains(text, "operation was not found") {
		t.Fatalf("expected cross-user mutation lookup to hide the operation, got %q", text)
	}
}

func TestMutateCreateRecordsAcceptsOriginalAirtableFieldNamesOverMCP(t *testing.T) {
	port := mutateFreePort(t)
	postgres := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Port(uint32(port)).
			Database("better_airtable_create_mutate_test").
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

	store, err := db.Open(context.Background(), fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/better_airtable_create_mutate_test?sslmode=disable", port))
	if err != nil {
		t.Fatalf("db.Open() returned error: %v", err)
	}
	defer store.Close()
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("store.Migrate() returned error: %v", err)
	}

	var createBodies []map[string]any
	var recordsMu sync.Mutex
	records := map[string]map[string]any{}
	nextRecordNumber := 1

	fakeAirtable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v0/meta/bases":
			writeMCPJSON(t, w, map[string]any{
				"bases": []map[string]any{
					{"id": "appProjects", "name": "Project Tracker", "permissionLevel": "create"},
				},
			})
		case r.URL.Path == "/v0/meta/bases/appProjects/tables":
			writeMCPJSON(t, w, map[string]any{
				"tables": []map[string]any{
					{
						"id":   "tblTable1",
						"name": "Table 1",
						"fields": []map[string]any{
							{"id": "fldName", "name": "Name", "type": "singleLineText"},
							{"id": "fldNotes", "name": "Notes", "type": "multilineText"},
							{"id": "fldStatus", "name": "Status", "type": "singleSelect"},
						},
					},
				},
			})
		case r.URL.Path == "/v0/appProjects/tblTable1" && r.Method == http.MethodGet:
			recordsMu.Lock()
			defer recordsMu.Unlock()

			payload := make([]map[string]any, 0, len(records))
			for id, fields := range records {
				payload = append(payload, map[string]any{
					"id":          id,
					"createdTime": "2026-04-01T12:00:00Z",
					"fields":      fields,
				})
			}
			writeMCPJSON(t, w, map[string]any{"records": payload})
		case r.URL.Path == "/v0/appProjects/tblTable1" && r.Method == http.MethodPost:
			var request struct {
				Records []struct {
					Fields map[string]any `json:"fields"`
				} `json:"records"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode create payload: %v", err)
			}

			recordsMu.Lock()
			defer recordsMu.Unlock()

			responseRecords := make([]map[string]any, 0, len(request.Records))
			for _, record := range request.Records {
				createBodies = append(createBodies, record.Fields)
				recordID := fmt.Sprintf("recCreated%d", nextRecordNumber)
				nextRecordNumber++
				records[recordID] = cloneAnyMap(record.Fields)
				responseRecords = append(responseRecords, map[string]any{
					"id":          recordID,
					"createdTime": "2026-04-01T12:00:00Z",
					"fields":      record.Fields,
				})
			}
			writeMCPJSON(t, w, map[string]any{"records": responseRecords})
		default:
			t.Fatalf("unexpected Airtable %s %s", r.Method, r.URL.Path)
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
		BaseURL:           mustParseTestURL(t, "http://example.test"),
		SyncInterval:      time.Minute,
		SyncTTL:           10 * time.Minute,
		ApprovalTTL:       10 * time.Minute,
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
	mux.Handle("/mcp", oauth.NewMiddleware(store).RequireBearer(mcp.NewHandler("better-airtable-mcp", "0.1.0", tools.NewCatalog(cfg, runtime))))
	approvalHandler := approval.NewHandler(runtime.Approval)
	mux.HandleFunc("/api/operations/", approvalHandler.ServeOperationAPI)

	mutateResponse := performAuthenticatedToolCall(t, mux, bearerToken, "mutate", map[string]any{
		"base": "appProjects",
		"operations": []map[string]any{
			{
				"type":  "create_records",
				"table": "table_1",
				"records": []map[string]any{
					{
						"fields": map[string]any{
							"Name":   "Set up project repo",
							"Notes":  "Initialized with Next.js and Tailwind",
							"Status": "Done",
						},
					},
					{
						"fields": map[string]any{
							"Name":   "Implement user auth",
							"Notes":  "OAuth with Google and GitHub",
							"Status": "Todo",
						},
					},
				},
			},
		},
	})

	mutateStructured := mutateResponse["result"].(map[string]any)["structuredContent"].(map[string]any)
	if mutateStructured["status"].(string) != "pending_approval" {
		t.Fatalf("expected pending_approval, got %#v", mutateStructured)
	}
	operationID := mutateStructured["operation_id"].(string)

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
	if operation.Operations[0].Table != "table_1" || operation.Operations[0].OriginalTableName != "Table 1" {
		t.Fatalf("expected table aliases to resolve cleanly, got %#v", operation.Operations[0])
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
		t.Fatalf("expected approved operation to complete, got %#v", approved)
	}
	if approved.Result == nil || len(approved.Result.CreatedRecordIDs) != 2 {
		t.Fatalf("expected create result to include two record IDs, got %#v", approved.Result)
	}

	if len(createBodies) != 2 {
		t.Fatalf("expected two Airtable create payloads, got %#v", createBodies)
	}
	for _, body := range createBodies {
		if _, ok := body["Name"]; !ok {
			t.Fatalf("expected Airtable create payload to use original field names, got %#v", body)
		}
		if _, ok := body["Notes"]; !ok {
			t.Fatalf("expected Airtable create payload to use original field names, got %#v", body)
		}
		if _, ok := body["Status"]; !ok {
			t.Fatalf("expected Airtable create payload to use original field names, got %#v", body)
		}
		if _, ok := body["name"]; ok {
			t.Fatalf("expected Airtable create payload to avoid duckdb aliases, got %#v", body)
		}
	}
}

func TestMutateDeleteRecordIDsOverMCP(t *testing.T) {
	port := mutateFreePort(t)
	postgres := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Port(uint32(port)).
			Database("better_airtable_delete_mutate_test").
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

	store, err := db.Open(context.Background(), fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/better_airtable_delete_mutate_test?sslmode=disable", port))
	if err != nil {
		t.Fatalf("db.Open() returned error: %v", err)
	}
	defer store.Close()
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("store.Migrate() returned error: %v", err)
	}

	fakeAirtable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v0/meta/bases":
			writeMCPJSON(t, w, map[string]any{
				"bases": []map[string]any{
					{"id": "appProjects", "name": "Project Tracker", "permissionLevel": "create"},
				},
			})
		case r.URL.Path == "/v0/meta/bases/appProjects/tables":
			writeMCPJSON(t, w, map[string]any{
				"tables": []map[string]any{
					{
						"id":   "tblProjects",
						"name": "Projects",
						"fields": []map[string]any{
							{"id": "fldName", "name": "Name", "type": "singleLineText"},
							{"id": "fldStatus", "name": "Status", "type": "singleSelect"},
						},
					},
				},
			})
		case r.URL.Path == "/v0/appProjects/tblProjects" && r.Method == http.MethodGet:
			writeMCPJSON(t, w, map[string]any{
				"records": []map[string]any{
					{
						"id":          "recProject1",
						"createdTime": "2026-04-01T12:00:00Z",
						"fields": map[string]any{
							"Name":   "Website Redesign",
							"Status": "Planning",
						},
					},
				},
			})
		default:
			t.Fatalf("unexpected Airtable %s %s", r.Method, r.URL.Path)
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
		BaseURL:           mustParseTestURL(t, "http://example.test"),
		SyncInterval:      time.Minute,
		SyncTTL:           10 * time.Minute,
		ApprovalTTL:       10 * time.Minute,
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
	mux.Handle("/mcp", oauth.NewMiddleware(store).RequireBearer(mcp.NewHandler("better-airtable-mcp", "0.1.0", tools.NewCatalog(cfg, runtime))))
	approvalHandler := approval.NewHandler(runtime.Approval)
	mux.HandleFunc("/api/operations/", approvalHandler.ServeOperationAPI)

	ensureBaseSyncedForMutationTest(t, runtime, "user_1", "appProjects")

	mutateResponse := performAuthenticatedToolCall(t, mux, bearerToken, "mutate", map[string]any{
		"base": "appProjects",
		"operations": []map[string]any{
			{
				"type":    "delete_records",
				"table":   "projects",
				"records": []string{"recProject1"},
			},
		},
	})

	mutateStructured := mutateResponse["result"].(map[string]any)["structuredContent"].(map[string]any)
	if mutateStructured["status"].(string) != "pending_approval" {
		t.Fatalf("expected pending_approval, got %#v", mutateStructured)
	}
	operationID := mutateStructured["operation_id"].(string)

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
	if len(operation.Operations) != 1 || len(operation.Operations[0].Records) != 1 {
		t.Fatalf("unexpected approval preview %#v", operation.Operations)
	}
	if operation.Operations[0].Records[0].ID != "recProject1" {
		t.Fatalf("expected delete preview to reference recProject1, got %#v", operation.Operations[0].Records[0])
	}
	if got := operation.Operations[0].Records[0].CurrentFields["name"]; got != "Website Redesign" {
		t.Fatalf("expected delete preview current data, got %#v", operation.Operations[0].Records[0].CurrentFields)
	}
}

func TestMutateReturnsNotReadyWhenTargetRecordHasNotSyncedYetOverMCP(t *testing.T) {
	port := mutateFreePort(t)
	postgres := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Port(uint32(port)).
			Database("better_airtable_mutate_not_ready_test").
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

	store, err := db.Open(context.Background(), fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/better_airtable_mutate_not_ready_test?sslmode=disable", port))
	if err != nil {
		t.Fatalf("db.Open() returned error: %v", err)
	}
	defer store.Close()
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("store.Migrate() returned error: %v", err)
	}

	var recordRequests atomic.Int32
	fakeAirtable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v0/meta/bases":
			writeMCPJSON(t, w, map[string]any{
				"bases": []map[string]any{
					{"id": "appProjects", "name": "Project Tracker", "permissionLevel": "create"},
				},
			})
		case r.URL.Path == "/v0/meta/bases/appProjects/tables":
			writeMCPJSON(t, w, map[string]any{
				"tables": []map[string]any{
					{
						"id":   "tblProjects",
						"name": "Projects",
						"fields": []map[string]any{
							{"id": "fldName", "name": "Name", "type": "singleLineText"},
							{"id": "fldStatus", "name": "Status", "type": "singleSelect"},
						},
					},
				},
			})
		case r.URL.Path == "/v0/appProjects/tblProjects" && r.Method == http.MethodGet:
			recordRequests.Add(1)
			time.Sleep(250 * time.Millisecond)
			writeMCPJSON(t, w, map[string]any{
				"records": []map[string]any{
					{
						"id":          "recProject1",
						"createdTime": "2026-04-01T12:00:00Z",
						"fields": map[string]any{
							"Name":   "Website Redesign",
							"Status": "Planning",
						},
					},
				},
			})
		default:
			t.Fatalf("unexpected Airtable %s %s", r.Method, r.URL.Path)
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
		BaseURL:           mustParseTestURL(t, "http://example.test"),
		SyncInterval:      time.Minute,
		SyncTTL:           10 * time.Minute,
		ApprovalTTL:       10 * time.Minute,
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
	mux.Handle("/mcp", oauth.NewMiddleware(store).RequireBearer(mcp.NewHandler("better-airtable-mcp", "0.1.0", tools.NewCatalog(cfg, runtime))))

	mutateResponse := performAuthenticatedToolCall(t, mux, bearerToken, "mutate", map[string]any{
		"base": "Project Tracker",
		"operations": []map[string]any{
			{
				"type":  "update_records",
				"table": "projects",
				"records": []map[string]any{
					{
						"id": "recProject1",
						"fields": map[string]any{
							"status": "Done",
						},
					},
				},
			},
		},
	})

	result := mutateResponse["result"].(map[string]any)
	if isError, ok := result["isError"].(bool); !ok || !isError {
		t.Fatalf("expected mutate call to return error while sync is still partial, got %#v", result)
	}

	mutateText := firstToolText(t, mutateResponse)
	if !strings.Contains(mutateText, `records are not synced yet for table "projects": recProject1`) {
		t.Fatalf("expected partial-sync error text, got %q", mutateText)
	}

	mutateStructured := result["structuredContent"].(map[string]any)
	if got := mutateStructured["reason"]; got != "records_not_synced_yet" {
		t.Fatalf("expected records_not_synced_yet reason, got %#v", mutateStructured)
	}
	if got := mutateStructured["table"]; got != "projects" {
		t.Fatalf("expected projects table in error payload, got %#v", mutateStructured)
	}
	recordIDs := mutateStructured["record_ids"].([]any)
	if len(recordIDs) != 1 || recordIDs[0].(string) != "recProject1" {
		t.Fatalf("expected recProject1 in error payload, got %#v", mutateStructured)
	}
	syncPayload := mutateStructured["sync"].(map[string]any)
	if got := syncPayload["read_snapshot"]; got != "partial" {
		t.Fatalf("expected partial read snapshot, got %#v", syncPayload)
	}
	if got := syncPayload["status"]; got != "syncing" {
		t.Fatalf("expected syncing status, got %#v", syncPayload)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if recordRequests.Load() > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if recordRequests.Load() == 0 {
		t.Fatalf("expected sync to begin fetching records")
	}
}

func ensureBaseSyncedForMutationTest(t *testing.T, runtime *tools.Runtime, userID, baseRef string) {
	t.Helper()

	if runtime == nil || runtime.SyncManager == nil {
		t.Fatal("runtime sync manager is not configured")
	}
	if _, err := runtime.SyncManager.EnsureBaseReady(context.Background(), userID, baseRef); err != nil {
		t.Fatalf("runtime.SyncManager.EnsureBaseReady() returned error: %v", err)
	}
}

func mutateFreePort(t *testing.T) int {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() returned error: %v", err)
	}
	defer listener.Close()

	return listener.Addr().(*net.TCPAddr).Port
}

func mustParseTestURL(t *testing.T, raw string) *url.URL {
	t.Helper()

	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("url.Parse() returned error: %v", err)
	}
	return parsed
}

func cloneAnyMap(input map[string]any) map[string]any {
	cloned := make(map[string]any, len(input))
	for key, value := range input {
		cloned[key] = value
	}
	return cloned
}
