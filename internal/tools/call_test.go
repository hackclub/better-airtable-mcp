package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hackclub/better-airtable-mcp/internal/config"
	"github.com/hackclub/better-airtable-mcp/internal/duckdb"
	"github.com/hackclub/better-airtable-mcp/internal/oauth"
	syncer "github.com/hackclub/better-airtable-mcp/internal/sync"
)

// fakeSyncer implements the Syncer interface without a database.
type fakeSyncer struct {
	queryResolvedCalls int
	queryBaseCalls     int
	queryErr           error
	result             duckdb.QueryResult
}

func (f *fakeSyncer) SearchBases(ctx context.Context, accessToken, query string) ([]syncer.Base, error) {
	return []syncer.Base{{ID: "appProjects", Name: "Project Tracker", PermissionLevel: "create"}}, nil
}

func (f *fakeSyncer) SyncBase(ctx context.Context, accessToken, baseRef string) (syncer.SyncResult, error) {
	return syncer.SyncResult{}, nil
}

func (f *fakeSyncer) ListSchema(ctx context.Context, accessToken, baseRef string) (duckdb.BaseSchema, error) {
	return duckdb.BaseSchema{}, nil
}

func (f *fakeSyncer) ListResolvedSchema(ctx context.Context, baseID, baseName string) (duckdb.BaseSchema, error) {
	return duckdb.BaseSchema{}, nil
}

func (f *fakeSyncer) QueryBase(ctx context.Context, accessToken, baseRef, query string) (duckdb.QueryResult, error) {
	f.queryBaseCalls++
	if f.queryErr != nil {
		return duckdb.QueryResult{}, f.queryErr
	}
	return f.result, nil
}

func (f *fakeSyncer) QueryResolvedBase(ctx context.Context, baseID, query string) (duckdb.QueryResult, error) {
	f.queryResolvedCalls++
	if f.queryErr != nil {
		return duckdb.QueryResult{}, f.queryErr
	}
	return f.result, nil
}

// fakeSyncManager implements the SyncManager interface without a database.
type fakeSyncManager struct {
	ensureReadableCalls int
}

func (f *fakeSyncManager) base() syncer.Base {
	return syncer.Base{ID: "appProjects", Name: "Project Tracker", PermissionLevel: "create"}
}

func (f *fakeSyncManager) EnsureBaseReady(ctx context.Context, userID, baseRef string) (syncer.Base, error) {
	return f.base(), nil
}

func (f *fakeSyncManager) EnsureBaseReadable(ctx context.Context, userID, baseRef string) (syncer.Base, error) {
	f.ensureReadableCalls++
	return f.base(), nil
}

func (f *fakeSyncManager) EnsureBaseSchemaSampled(ctx context.Context, userID, baseRef string) (syncer.Base, error) {
	return f.base(), nil
}

func (f *fakeSyncManager) RequestSync(ctx context.Context, userID, baseRef string) (syncer.SyncOperationStatus, error) {
	return syncer.SyncOperationStatus{OperationID: "sync_appProjects", Status: "syncing"}, nil
}

func (f *fakeSyncManager) CheckOperation(ctx context.Context, operationID string) (syncer.SyncOperationStatus, bool, error) {
	if operationID != "sync_appProjects" {
		return syncer.SyncOperationStatus{}, false, nil
	}
	completedAt := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	return syncer.SyncOperationStatus{
		OperationID:  operationID,
		BaseID:       "appProjects",
		Type:         "sync",
		Status:       "completed",
		ReadSnapshot: "complete",
		CompletedAt:  &completedAt,
	}, true, nil
}

func (f *fakeSyncManager) BaseStatus(baseID string) (syncer.SyncOperationStatus, bool) {
	return syncer.SyncOperationStatus{OperationID: "sync_" + baseID, Status: "completed", ReadSnapshot: "complete"}, true
}

type staticTokens struct{}

func (staticTokens) AirtableAccessToken(ctx context.Context, userID string) (string, error) {
	return "token", nil
}

func newFakeRuntime(fakeSync *fakeSyncer, fakeManager *fakeSyncManager) *Runtime {
	return &Runtime{
		Syncer:         fakeSync,
		SyncManager:    fakeManager,
		AirtableTokens: staticTokens{},
		Config: config.Config{
			SyncInterval:      time.Minute,
			QueryDefaultLimit: 100,
			QueryMaxLimit:     1000,
		},
	}
}

func authedContext() context.Context {
	return oauth.ContextWithUserID(context.Background(), "user_1")
}

func TestQueryToolCallRunsBatchWithoutDatabase(t *testing.T) {
	fakeSync := &fakeSyncer{result: duckdb.QueryResult{
		Columns:      []string{"id", "name"},
		Rows:         [][]any{{"rec1", "First"}},
		RowCount:     1,
		LastSyncedAt: time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC),
	}}
	fakeManager := &fakeSyncManager{}
	tool := NewQueryTool(100, 1000, newFakeRuntime(fakeSync, fakeManager))

	result, err := tool.Call(authedContext(), json.RawMessage(`{
		"base": "Project Tracker",
		"sql": ["SELECT id, name FROM projects", "SELECT id FROM projects"]
	}`))
	if err != nil {
		t.Fatalf("Call() returned error: %v", err)
	}
	if result.IsError {
		t.Fatalf("Call() returned error result: %#v", result)
	}

	if fakeManager.ensureReadableCalls != 1 {
		t.Fatalf("expected the base to be resolved once per request, got %d", fakeManager.ensureReadableCalls)
	}
	if fakeSync.queryResolvedCalls != 2 {
		t.Fatalf("expected one resolved query per statement, got %d", fakeSync.queryResolvedCalls)
	}
	if fakeSync.queryBaseCalls != 0 {
		t.Fatalf("expected no re-resolving QueryBase calls, got %d", fakeSync.queryBaseCalls)
	}

	structured := result.StructuredContent.(map[string]any)
	results := structured["results"].([]map[string]any)
	if len(results) != 2 {
		t.Fatalf("expected 2 statement results, got %d", len(results))
	}
	if results[0]["row_count"] != 1 {
		t.Fatalf("unexpected first result %#v", results[0])
	}
	if len(result.Content) == 0 || !strings.Contains(result.Content[0].Text, "rec1") {
		t.Fatalf("expected CSV text with row data, got %#v", result.Content)
	}
}

func TestQueryToolCallPropagatesQueryErrors(t *testing.T) {
	fakeSync := &fakeSyncer{queryErr: fmt.Errorf("catalog error")}
	tool := NewQueryTool(100, 1000, newFakeRuntime(fakeSync, &fakeSyncManager{}))

	_, err := tool.Call(authedContext(), json.RawMessage(`{
		"base": "Project Tracker",
		"sql": ["SELECT 1"]
	}`))
	if err == nil || !strings.Contains(err.Error(), "catalog error") {
		t.Fatalf("expected the query error to propagate, got %v", err)
	}
}

func TestQueryToolCallRequiresAuthenticatedUser(t *testing.T) {
	tool := NewQueryTool(100, 1000, newFakeRuntime(&fakeSyncer{}, &fakeSyncManager{}))

	_, err := tool.Call(context.Background(), json.RawMessage(`{
		"base": "Project Tracker",
		"sql": ["SELECT 1"]
	}`))
	if err == nil || !strings.Contains(err.Error(), "missing authenticated user") {
		t.Fatalf("expected missing-user error, got %v", err)
	}
}

func TestCheckOperationToolCallReportsSyncStatusWithoutDatabase(t *testing.T) {
	tool := NewCheckOperationTool(newFakeRuntime(&fakeSyncer{}, &fakeSyncManager{}))

	result, err := tool.Call(authedContext(), json.RawMessage(`{"operation_id": "sync_appProjects"}`))
	if err != nil {
		t.Fatalf("Call() returned error: %v", err)
	}
	if result.IsError {
		t.Fatalf("Call() returned error result: %#v", result)
	}
	structured := result.StructuredContent.(map[string]any)
	if structured["status"] != "completed" || structured["type"] != "sync" {
		t.Fatalf("unexpected structured status %#v", structured)
	}
}

func TestCheckOperationToolCallReportsUnknownSyncOperation(t *testing.T) {
	tool := NewCheckOperationTool(newFakeRuntime(&fakeSyncer{}, &fakeSyncManager{}))

	result, err := tool.Call(authedContext(), json.RawMessage(`{"operation_id": "sync_appMissing"}`))
	if err != nil {
		t.Fatalf("Call() returned error: %v", err)
	}
	if !result.IsError {
		t.Fatalf("expected an error result for an unknown sync operation, got %#v", result)
	}
	if len(result.Content) == 0 || !strings.Contains(result.Content[0].Text, "operation was not found") {
		t.Fatalf("expected not-found message, got %#v", result.Content)
	}
}
