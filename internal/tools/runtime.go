package tools

import (
	"context"
	"fmt"
	"time"

	"github.com/hackclub/better-airtable-mcp/internal/approval"
	"github.com/hackclub/better-airtable-mcp/internal/config"
	"github.com/hackclub/better-airtable-mcp/internal/cryptoutil"
	"github.com/hackclub/better-airtable-mcp/internal/db"
	"github.com/hackclub/better-airtable-mcp/internal/duckdb"
	syncer "github.com/hackclub/better-airtable-mcp/internal/sync"
)

// Syncer is the sync-service surface the tools call. *syncer.Service
// satisfies it; tests substitute a fake so Call paths run without a database.
type Syncer interface {
	SearchBases(ctx context.Context, accessToken, query string) ([]syncer.Base, error)
	SyncBase(ctx context.Context, accessToken, baseRef string) (syncer.SyncResult, error)
	ListSchema(ctx context.Context, accessToken, baseRef string) (duckdb.BaseSchema, error)
	ListResolvedSchema(ctx context.Context, baseID, baseName string) (duckdb.BaseSchema, error)
	QueryBase(ctx context.Context, accessToken, baseRef, query string) (duckdb.QueryResult, error)
	QueryResolvedBase(ctx context.Context, baseID, query string) (duckdb.QueryResult, error)
}

// SyncManager is the sync-manager surface the tools call. *syncer.Manager
// satisfies it.
type SyncManager interface {
	EnsureBaseReady(ctx context.Context, userID, baseRef string) (syncer.Base, error)
	EnsureBaseReadable(ctx context.Context, userID, baseRef string) (syncer.Base, error)
	EnsureBaseSchemaSampled(ctx context.Context, userID, baseRef string) (syncer.Base, error)
	RequestSync(ctx context.Context, userID, baseRef string) (syncer.SyncOperationStatus, error)
	CheckOperation(ctx context.Context, operationID string) (syncer.SyncOperationStatus, bool, error)
	BaseStatus(baseID string) (syncer.SyncOperationStatus, bool)
}

// ApprovalService is the approval-service surface the tools call.
// *approval.Service satisfies it.
type ApprovalService interface {
	PrepareMutation(ctx context.Context, userID string, request approval.MutationRequest) (approval.PreparedMutation, error)
	PrepareSchemaMutation(ctx context.Context, userID string, request approval.SchemaMutationRequest) (approval.PreparedMutation, error)
	GetOperation(ctx context.Context, operationID string) (approval.OperationView, error)
}

type Runtime struct {
	Store          *db.Store
	Cipher         *cryptoutil.Cipher
	Syncer         Syncer
	SyncManager    SyncManager
	Approval       ApprovalService
	AirtableTokens syncer.TokenSource
	Config         config.Config
}

func (r *Runtime) AirtableAccessToken(ctx context.Context, userID string) (string, error) {
	if r != nil && r.AirtableTokens != nil {
		return r.AirtableTokens.AirtableAccessToken(ctx, userID)
	}
	if r == nil || r.Store == nil || r.Cipher == nil {
		return "", fmt.Errorf("tool runtime is not configured")
	}

	record, err := r.Store.GetAirtableToken(ctx, userID)
	if err != nil {
		return "", err
	}
	plaintext, err := r.Cipher.Decrypt(record.AccessTokenCiphertext)
	if err != nil {
		return "", err
	}
	return string(plaintext), nil
}

func (r *Runtime) NextSyncTime(lastSyncedAt time.Time, lastSyncDuration time.Duration) time.Time {
	if lastSyncDuration >= r.Config.SyncInterval {
		return lastSyncedAt
	}
	return lastSyncedAt.Add(r.Config.SyncInterval - lastSyncDuration)
}
