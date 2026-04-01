package db

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
)

func TestStoreMigrateAndRoundTripRecords(t *testing.T) {
	port := freePort(t)
	postgres := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Port(uint32(port)).
			Database("better_airtable_test").
			Username("postgres").
			Password("postgres").
			BinariesPath(filepath.Join(t.TempDir(), "postgres-binaries")).
			DataPath(filepath.Join(t.TempDir(), "postgres-data")).
			RuntimePath(filepath.Join(t.TempDir(), "postgres-runtime")),
	)
	if err := postgres.Start(); err != nil {
		t.Fatalf("embedded postgres start failed: %v", err)
	}
	defer func() {
		if err := postgres.Stop(); err != nil {
			t.Fatalf("embedded postgres stop failed: %v", err)
		}
	}()

	databaseURL := fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/better_airtable_test?sslmode=disable", port)
	store, err := Open(context.Background(), databaseURL)
	if err != nil {
		t.Fatalf("Open() returned error: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned error: %v", err)
	}

	email := "person@example.com"
	airtableUserID := "usr_123"
	if err := store.UpsertUser(ctx, User{
		ID:             "user_1",
		AirtableUserID: &airtableUserID,
		Email:          &email,
	}); err != nil {
		t.Fatalf("UpsertUser() returned error: %v", err)
	}

	user, err := store.GetUser(ctx, "user_1")
	if err != nil {
		t.Fatalf("GetUser() returned error: %v", err)
	}
	if user.Email == nil || *user.Email != email {
		t.Fatalf("unexpected user email %#v", user)
	}

	if err := store.UpsertUser(ctx, User{ID: "user_2"}); err != nil {
		t.Fatalf("UpsertUser() for user_2 returned error: %v", err)
	}

	if err := store.PutAirtableToken(ctx, AirtableTokenRecord{
		UserID:                 "user_1",
		AccessTokenCiphertext:  []byte("access"),
		RefreshTokenCiphertext: []byte("refresh"),
		ExpiresAt:              time.Now().Add(time.Hour).UTC(),
		Scopes:                 "data.records:read data.records:write schema.bases:read",
	}); err != nil {
		t.Fatalf("PutAirtableToken() returned error: %v", err)
	}

	token, err := store.GetAirtableToken(ctx, "user_1")
	if err != nil {
		t.Fatalf("GetAirtableToken() returned error: %v", err)
	}
	if string(token.AccessTokenCiphertext) != "access" {
		t.Fatalf("unexpected access token ciphertext %q", token.AccessTokenCiphertext)
	}
	if token.ReauthRequiredAt != nil {
		t.Fatalf("expected reauth marker to be nil, got %#v", token.ReauthRequiredAt)
	}

	reauthRequiredAt := time.Now().UTC()
	if err := store.MarkAirtableTokenReauthRequired(ctx, "user_1", reauthRequiredAt); err != nil {
		t.Fatalf("MarkAirtableTokenReauthRequired() returned error: %v", err)
	}

	token, err = store.GetAirtableToken(ctx, "user_1")
	if err != nil {
		t.Fatalf("GetAirtableToken() after reauth mark returned error: %v", err)
	}
	if token.ReauthRequiredAt == nil {
		t.Fatal("expected reauth marker to be set")
	}

	expiring, err := store.ListAirtableTokensExpiringBefore(ctx, time.Now().Add(2*time.Hour).UTC())
	if err != nil {
		t.Fatalf("ListAirtableTokensExpiringBefore() returned error: %v", err)
	}
	if len(expiring) != 0 {
		t.Fatalf("expected reauth-required token to be excluded from refresh list, got %#v", expiring)
	}

	if err := store.PutAirtableToken(ctx, AirtableTokenRecord{
		UserID:                 "user_1",
		AccessTokenCiphertext:  []byte("new-access"),
		RefreshTokenCiphertext: []byte("new-refresh"),
		ExpiresAt:              time.Now().Add(30 * time.Minute).UTC(),
		Scopes:                 "data.records:read data.records:write schema.bases:read",
	}); err != nil {
		t.Fatalf("PutAirtableToken() after reauth mark returned error: %v", err)
	}

	expiring, err = store.ListAirtableTokensExpiringBefore(ctx, time.Now().Add(time.Hour).UTC())
	if err != nil {
		t.Fatalf("ListAirtableTokensExpiringBefore() second call returned error: %v", err)
	}
	if len(expiring) != 1 || expiring[0].UserID != "user_1" {
		t.Fatalf("unexpected expiring token list %#v", expiring)
	}

	activeUntil := time.Now().Add(10 * time.Minute).UTC()
	if err := store.TouchSyncState(ctx, "app123", activeUntil, "user_1"); err != nil {
		t.Fatalf("TouchSyncState() returned error: %v", err)
	}

	syncState, err := store.GetSyncState(ctx, "app123")
	if err != nil {
		t.Fatalf("GetSyncState() after touch returned error: %v", err)
	}
	if syncState.ActiveUntil == nil || !syncState.ActiveUntil.Equal(activeUntil) {
		t.Fatalf("unexpected active_until after touch %#v", syncState)
	}
	if syncState.SyncTokenUserID == nil || *syncState.SyncTokenUserID != "user_1" {
		t.Fatalf("unexpected sync_token_user_id after touch %#v", syncState)
	}
	if syncState.LastSyncedAt != nil {
		t.Fatalf("expected touch-only sync state to preserve nil last_synced_at, got %#v", syncState)
	}

	lastSyncedAt := time.Now().UTC()
	durationMS := int64(1234)
	totalRecords := int64(42)
	totalTables := 3
	if err := store.PutSyncState(ctx, SyncState{
		BaseID:             "app123",
		LastSyncedAt:       &lastSyncedAt,
		LastSyncDurationMS: &durationMS,
		TotalRecords:       &totalRecords,
		TotalTables:        &totalTables,
		ActiveUntil:        &activeUntil,
		SyncTokenUserID:    ptr("user_1"),
	}); err != nil {
		t.Fatalf("PutSyncState() returned error: %v", err)
	}

	newActiveUntil := time.Now().Add(20 * time.Minute).UTC()
	if err := store.TouchSyncState(ctx, "app123", newActiveUntil, "user_2"); err != nil {
		t.Fatalf("TouchSyncState() second call returned error: %v", err)
	}

	syncState, err = store.GetSyncState(ctx, "app123")
	if err != nil {
		t.Fatalf("GetSyncState() after second touch returned error: %v", err)
	}
	if syncState.ActiveUntil == nil || !syncState.ActiveUntil.Equal(newActiveUntil) {
		t.Fatalf("unexpected active_until after second touch %#v", syncState)
	}
	if syncState.SyncTokenUserID == nil || *syncState.SyncTokenUserID != "user_2" {
		t.Fatalf("unexpected sync_token_user_id after second touch %#v", syncState)
	}
	if syncState.LastSyncedAt == nil || !syncState.LastSyncedAt.Equal(lastSyncedAt) {
		t.Fatalf("expected sync metrics to be preserved across touch, got %#v", syncState)
	}

	expiredUntil := time.Now().Add(-time.Minute).UTC()
	if err := store.PutSyncState(ctx, SyncState{
		BaseID:          "appExpired",
		ActiveUntil:     &expiredUntil,
		SyncTokenUserID: ptr("user_1"),
	}); err != nil {
		t.Fatalf("PutSyncState(appExpired) returned error: %v", err)
	}

	activeStates, err := store.ListActiveSyncStates(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("ListActiveSyncStates() returned error: %v", err)
	}
	if len(activeStates) != 1 || activeStates[0].BaseID != "app123" {
		t.Fatalf("unexpected active sync states %#v", activeStates)
	}

	if err := store.PutMCPToken(ctx, MCPTokenRecord{
		TokenHash:  "hash_123",
		UserID:     "user_1",
		ClientID:   ptr("client_123"),
		ClientName: ptr("Claude"),
		CreatedAt:  time.Now().UTC(),
		ExpiresAt:  time.Now().Add(time.Hour).UTC(),
	}); err != nil {
		t.Fatalf("PutMCPToken() returned error: %v", err)
	}

	mcpToken, err := store.GetMCPToken(ctx, "hash_123")
	if err != nil {
		t.Fatalf("GetMCPToken() returned error: %v", err)
	}
	if mcpToken.UserID != "user_1" {
		t.Fatalf("unexpected mcp token record %#v", mcpToken)
	}
	if mcpToken.ClientID == nil || *mcpToken.ClientID != "client_123" {
		t.Fatalf("unexpected mcp token client id %#v", mcpToken)
	}
	if mcpToken.ClientName == nil || *mcpToken.ClientName != "Claude" {
		t.Fatalf("unexpected mcp token client name %#v", mcpToken)
	}

	if err := store.UpsertUserBaseAccess(ctx, UserBaseAccess{
		UserID:          "user_1",
		BaseID:          "app123",
		PermissionLevel: "create",
		LastVerifiedAt:  time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertUserBaseAccess() returned error: %v", err)
	}

	access, err := store.GetUserBaseAccess(ctx, "user_1", "app123")
	if err != nil {
		t.Fatalf("GetUserBaseAccess() returned error: %v", err)
	}
	if access.PermissionLevel != "create" {
		t.Fatalf("unexpected base access %#v", access)
	}

	pendingErr := "failure"
	resolvedAt := time.Now().UTC()
	if err := store.PutPendingOperation(ctx, PendingOperation{
		ID:                "op_123",
		UserID:            "user_1",
		BaseID:            "app123",
		Status:            "pending_approval",
		OperationType:     "record_mutation",
		PayloadCiphertext: []byte("payload"),
		CreatedAt:         time.Now().UTC(),
		ExpiresAt:         time.Now().Add(10 * time.Minute).UTC(),
	}); err != nil {
		t.Fatalf("PutPendingOperation() returned error: %v", err)
	}

	if err := store.UpdatePendingOperationStatus(ctx, "op_123", "failed", []byte("result"), &pendingErr, &resolvedAt); err != nil {
		t.Fatalf("UpdatePendingOperationStatus() returned error: %v", err)
	}

	operation, err := store.GetPendingOperation(ctx, "op_123")
	if err != nil {
		t.Fatalf("GetPendingOperation() returned error: %v", err)
	}
	if operation.Status != "failed" || operation.Error == nil || *operation.Error != pendingErr {
		t.Fatalf("unexpected pending operation %#v", operation)
	}

	clientSecretHash := "secret-hash"
	clientName := "Test Client"
	if err := store.UpsertOAuthClient(ctx, OAuthClient{
		ClientID:         "client_123",
		ClientSecretHash: &clientSecretHash,
		ClientName:       &clientName,
		RedirectURIs:     []string{"https://example.com/callback"},
	}); err != nil {
		t.Fatalf("UpsertOAuthClient() returned error: %v", err)
	}

	client, err := store.GetOAuthClient(ctx, "client_123")
	if err != nil {
		t.Fatalf("GetOAuthClient() returned error: %v", err)
	}
	if len(client.RedirectURIs) != 1 || !strings.Contains(client.RedirectURIs[0], "example.com") {
		t.Fatalf("unexpected oauth client %#v", client)
	}
}

func freePort(t *testing.T) int {
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
