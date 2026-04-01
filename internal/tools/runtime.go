package tools

import (
	"context"
	"fmt"
	"time"

	"github.com/hackclub/better-airtable-mcp/internal/approval"
	"github.com/hackclub/better-airtable-mcp/internal/config"
	"github.com/hackclub/better-airtable-mcp/internal/cryptoutil"
	"github.com/hackclub/better-airtable-mcp/internal/db"
	syncer "github.com/hackclub/better-airtable-mcp/internal/sync"
)

type Runtime struct {
	Store          *db.Store
	Cipher         *cryptoutil.Cipher
	Syncer         *syncer.Service
	SyncManager    *syncer.Manager
	Approval       *approval.Service
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
