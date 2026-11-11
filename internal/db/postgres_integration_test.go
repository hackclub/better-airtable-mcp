//go:build integration

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

	// Truncate to microseconds: Postgres timestamps drop sub-microsecond
	// precision, and Linux clocks produce nanosecond-precision time.Now().
	activeUntil := time.Now().Add(10 * time.Minute).UTC().Truncate(time.Microsecond)
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

	lastSyncedAt := time.Now().UTC().Truncate(time.Microsecond)
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

	newActiveUntil := time.Now().Add(20 * time.Minute).UTC().Truncate(time.Microsecond)
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

	refreshNow := time.Now().UTC()
	if err := store.PutMCPRefreshToken(ctx, MCPRefreshTokenRecord{
		TokenHash: "refresh_hash_123",
		UserID:    "user_1",
		ClientID:  "client_123",
		CreatedAt: refreshNow,
		ExpiresAt: refreshNow.Add(time.Hour),
	}); err != nil {
		t.Fatalf("PutMCPRefreshToken() returned error: %v", err)
	}

	refreshRecord, ok, err := store.ConsumeMCPRefreshToken(ctx, "refresh_hash_123", refreshNow)
	if err != nil {
		t.Fatalf("ConsumeMCPRefreshToken() returned error: %v", err)
	}
	if !ok {
		t.Fatal("expected refresh token to be consumed")
	}
	if refreshRecord.UserID != "user_1" || refreshRecord.ClientID != "client_123" {
		t.Fatalf("unexpected refresh token record %#v", refreshRecord)
	}

	if _, ok, err := store.ConsumeMCPRefreshToken(ctx, "refresh_hash_123", refreshNow); err != nil {
		t.Fatalf("ConsumeMCPRefreshToken() second call returned error: %v", err)
	} else if ok {
		t.Fatal("expected consumed refresh token to be single-use")
	}

	if err := store.PutMCPRefreshToken(ctx, MCPRefreshTokenRecord{
		TokenHash: "expired_refresh_hash_123",
		UserID:    "user_1",
		ClientID:  "client_123",
		CreatedAt: refreshNow.Add(-2 * time.Hour),
		ExpiresAt: refreshNow.Add(-time.Hour),
	}); err != nil {
		t.Fatalf("PutMCPRefreshToken() for expired token returned error: %v", err)
	}
	if _, ok, err := store.ConsumeMCPRefreshToken(ctx, "expired_refresh_hash_123", refreshNow); err != nil {
		t.Fatalf("ConsumeMCPRefreshToken() expired call returned error: %v", err)
	} else if ok {
		t.Fatal("expected expired refresh token not to be consumed")
	}
	removedRefreshTokens, err := store.PruneExpiredMCPRefreshTokens(ctx, refreshNow)
	if err != nil {
		t.Fatalf("PruneExpiredMCPRefreshTokens() returned error: %v", err)
	}
	if removedRefreshTokens != 1 {
		t.Fatalf("expected one expired refresh token to be pruned, got %d", removedRefreshTokens)
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

func TestTransitionPendingOperationStatusIsCompareAndSwap(t *testing.T) {
	port := freePort(t)
	postgres := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Port(uint32(port)).
			Database("better_airtable_cas_test").
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

	ctx := context.Background()
	store, err := Open(ctx, fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/better_airtable_cas_test?sslmode=disable", port))
	if err != nil {
		t.Fatalf("Open() returned error: %v", err)
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned error: %v", err)
	}
	// Exercise an upgrade from the old schema as well as a fresh database.
	if _, err := store.pool.Exec(ctx, `ALTER TABLE pending_operations DROP COLUMN execution_started_at`); err != nil {
		t.Fatalf("simulate old schema: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() old schema returned error: %v", err)
	}
	if err := store.UpsertUser(ctx, User{ID: "user_1"}); err != nil {
		t.Fatalf("UpsertUser() returned error: %v", err)
	}
	if err := store.PutPendingOperation(ctx, PendingOperation{
		ID:                "op_cas",
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

	startedBefore := time.Now().UTC().Truncate(time.Microsecond)
	won, err := store.TransitionPendingOperationStatus(ctx, "op_cas", "pending_approval", "executing")
	if err != nil {
		t.Fatalf("TransitionPendingOperationStatus() returned error: %v", err)
	}
	if !won {
		t.Fatal("expected the first transition to win")
	}
	claimed, err := store.GetPendingOperation(ctx, "op_cas")
	if err != nil {
		t.Fatalf("GetPendingOperation() after claim returned error: %v", err)
	}
	if claimed.ExecutionStartedAt == nil || claimed.ExecutionStartedAt.Before(startedBefore) || claimed.ExecutionStartedAt.After(time.Now().UTC()) {
		t.Fatalf("expected execution start timestamp at claim time, got %v", claimed.ExecutionStartedAt)
	}
	if claimed.ResolvedAt != nil {
		t.Fatalf("executing operation has resolution timestamp %v", claimed.ResolvedAt)
	}

	won, err = store.TransitionPendingOperationStatus(ctx, "op_cas", "pending_approval", "executing")
	if err != nil {
		t.Fatalf("second TransitionPendingOperationStatus() returned error: %v", err)
	}
	if won {
		t.Fatal("expected the second transition to lose the compare-and-swap")
	}

	operation, err := store.GetPendingOperation(ctx, "op_cas")
	if err != nil {
		t.Fatalf("GetPendingOperation() returned error: %v", err)
	}
	if operation.Status != "executing" {
		t.Fatalf("status = %q, want executing", operation.Status)
	}
	if operation.ExecutionStartedAt == nil || !operation.ExecutionStartedAt.Equal(*claimed.ExecutionStartedAt) {
		t.Fatal("losing compare-and-swap changed execution start timestamp")
	}
	if operation.ResolvedAt != nil {
		t.Fatal("losing compare-and-swap set resolution timestamp")
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

func TestRecoverStaleExecutingOperations(t *testing.T) {
	port := freePort(t)
	postgres := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Port(uint32(port)).
			Database("better_airtable_recover_test").
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

	ctx := context.Background()
	store, err := Open(ctx, fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/better_airtable_recover_test?sslmode=disable", port))
	if err != nil {
		t.Fatalf("Open() returned error: %v", err)
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned error: %v", err)
	}
	if err := store.UpsertUser(ctx, User{ID: "user_1"}); err != nil {
		t.Fatalf("UpsertUser() returned error: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	put := func(id, status string, startedAt *time.Time, expiresAt time.Time) {
		t.Helper()
		var resolvedAt *time.Time
		if status == "completed" {
			resolvedAt = startedAt
		}
		if err := store.PutPendingOperation(ctx, PendingOperation{
			ID:                 id,
			UserID:             "user_1",
			BaseID:             "app123",
			Status:             status,
			OperationType:      "record_mutation",
			PayloadCiphertext:  []byte("payload"),
			CreatedAt:          now.Add(-2 * time.Hour),
			ExpiresAt:          expiresAt,
			ExecutionStartedAt: startedAt,
			ResolvedAt:         resolvedAt,
		}); err != nil {
			t.Fatalf("PutPendingOperation(%s) returned error: %v", id, err)
		}
	}

	put("op_stale", "executing", ptr(now.Add(-20*time.Minute)), now.Add(24*time.Hour))
	put("op_fresh", "executing", ptr(now.Add(-time.Minute)), now.Add(-time.Hour))
	put("op_boundary", "executing", ptr(now.Add(-15*time.Minute)), now.Add(24*time.Hour))
	put("op_pending", "pending_approval", nil, now.Add(24*time.Hour))
	put("op_completed", "completed", ptr(now.Add(-time.Hour)), now.Add(24*time.Hour))
	put("op_legacy", "executing", nil, now.Add(24*time.Hour))
	// Simulate an older version that wrote the start into resolved_at.
	if err := store.UpdatePendingOperationStatus(ctx, "op_legacy", "executing", nil, nil, ptr(now.Add(-time.Hour))); err != nil {
		t.Fatalf("set legacy resolved_at returned error: %v", err)
	}

	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() legacy backfill returned error: %v", err)
	}
	legacy, err := store.GetPendingOperation(ctx, "op_legacy")
	if err != nil {
		t.Fatalf("GetPendingOperation(op_legacy) returned error: %v", err)
	}
	if legacy.ExecutionStartedAt == nil || legacy.ExecutionStartedAt.Before(now) || legacy.ExecutionStartedAt.After(time.Now().UTC()) {
		t.Fatalf("expected legacy execution timestamp at migration time, got %v", legacy.ExecutionStartedAt)
	}
	if legacy.ResolvedAt != nil {
		t.Fatal("migration left a nonterminal resolution timestamp")
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("repeated Migrate() returned error: %v", err)
	}

	recovered, err := store.RecoverStaleExecutingOperations(ctx, now.Add(-15*time.Minute))
	if err != nil {
		t.Fatalf("RecoverStaleExecutingOperations() returned error: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("recovered = %d, want 1", recovered)
	}

	stale, err := store.GetPendingOperation(ctx, "op_stale")
	if err != nil {
		t.Fatalf("GetPendingOperation(op_stale) returned error: %v", err)
	}
	if stale.Status != "failed" || stale.Error == nil || *stale.Error == "" || stale.ResolvedAt == nil {
		t.Fatalf("expected stale executing op to be failed with a note, got %#v", stale)
	}

	for id, want := range map[string]string{
		"op_fresh": "executing", "op_boundary": "executing", "op_legacy": "executing",
		"op_pending": "pending_approval", "op_completed": "completed",
	} {
		op, err := store.GetPendingOperation(ctx, id)
		if err != nil {
			t.Fatalf("GetPendingOperation(%s) returned error: %v", id, err)
		}
		if op.Status != want {
			t.Fatalf("expected %s status %s, got %s", id, want, op.Status)
		}
		if op.Status == "executing" && op.ResolvedAt != nil {
			t.Fatalf("executing operation %s has resolution timestamp", id)
		}
		if id == "op_completed" && (op.ResolvedAt == nil || !op.ResolvedAt.Equal(now.Add(-time.Hour))) {
			t.Fatal("migration changed terminal resolution timestamp")
		}
		if id == "op_legacy" && (op.ExecutionStartedAt == nil || !op.ExecutionStartedAt.Equal(*legacy.ExecutionStartedAt)) {
			t.Fatal("repeated migration reset legacy execution timestamp")
		}
	}
	// A legacy row must also be recoverable once its startup grace window passes.
	recovered, err = store.RecoverStaleExecutingOperations(ctx, legacy.ExecutionStartedAt.Add(16*time.Minute))
	if err != nil || recovered != 3 {
		t.Fatalf("recovery after grace window = %d, %v; want 3, nil", recovered, err)
	}
}
