package oauth

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/hackclub/better-airtable-mcp/internal/cryptoutil"
	"github.com/hackclub/better-airtable-mcp/internal/db"
	"github.com/hackclub/better-airtable-mcp/internal/logx"
)

const defaultRefreshBefore = 10 * time.Minute

type ReauthorizationRequiredError struct{}

func (ReauthorizationRequiredError) Error() string {
	return "airtable authorization expired; reconnect Better Airtable MCP to Airtable"
}

type TokenManager struct {
	store         *db.Store
	cipher        *cryptoutil.Cipher
	airtable      *AirtableOAuthClient
	refreshBefore time.Duration
	now           func() time.Time

	mu       sync.Mutex
	inFlight map[string]*refreshCall
}

type refreshCall struct {
	done  chan struct{}
	token string
	err   error
}

func NewTokenManager(store *db.Store, cipher *cryptoutil.Cipher, airtable *AirtableOAuthClient) *TokenManager {
	return &TokenManager{
		store:         store,
		cipher:        cipher,
		airtable:      airtable,
		refreshBefore: defaultRefreshBefore,
		now:           time.Now,
		inFlight:      make(map[string]*refreshCall),
	}
}

func (m *TokenManager) AirtableAccessToken(ctx context.Context, userID string) (string, error) {
	record, err := m.store.GetAirtableToken(ctx, userID)
	if err != nil {
		return "", err
	}
	if record.ReauthRequiredAt != nil {
		return "", ReauthorizationRequiredError{}
	}
	if m.now().Add(m.refreshBefore).Before(record.ExpiresAt) {
		return m.decryptAccessToken(record)
	}
	return m.refreshAndGetAccess(ctx, userID)
}

func (m *TokenManager) RunRefreshLoop(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Minute
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.refreshExpiringTokens(ctx); err != nil {
				logx.Event(ctx, "oauth_token_manager", "oauth.airtable_token.refresh_loop_failed",
					"error_kind", logx.ErrorKind(err),
					"error_message", logx.ErrorPreview(err),
				)
				continue
			}
			logx.Event(ctx, "oauth_token_manager", "oauth.airtable_token.refresh_loop_completed")
		}
	}
}

func (m *TokenManager) refreshExpiringTokens(ctx context.Context) error {
	records, err := m.store.ListAirtableTokensExpiringBefore(ctx, m.now().Add(m.refreshBefore).UTC())
	if err != nil {
		return err
	}
	for _, record := range records {
		_, _ = m.refreshAndGetAccess(ctx, record.UserID)
	}
	return nil
}

func (m *TokenManager) refreshAndGetAccess(ctx context.Context, userID string) (string, error) {
	m.mu.Lock()
	if call, ok := m.inFlight[userID]; ok {
		m.mu.Unlock()
		logx.Event(ctx, "oauth_token_manager", "oauth.airtable_token.refresh_dedup_wait",
			"user_id", userID,
		)
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-call.done:
			return call.token, call.err
		}
	}

	call := &refreshCall{done: make(chan struct{})}
	m.inFlight[userID] = call
	m.mu.Unlock()

	call.token, call.err = m.refreshNow(ctx, userID)
	close(call.done)

	m.mu.Lock()
	delete(m.inFlight, userID)
	m.mu.Unlock()

	return call.token, call.err
}

func (m *TokenManager) refreshNow(ctx context.Context, userID string) (string, error) {
	logx.Event(ctx, "oauth_token_manager", "oauth.airtable_token.refresh_started",
		"user_id", userID,
	)
	record, err := m.store.GetAirtableToken(ctx, userID)
	if err != nil {
		logx.Event(ctx, "oauth_token_manager", "oauth.airtable_token.refresh_failed",
			"user_id", userID,
			"error_kind", logx.ErrorKind(err),
			"error_message", logx.ErrorPreview(err),
		)
		return "", err
	}
	if record.ReauthRequiredAt != nil {
		logx.Event(ctx, "oauth_token_manager", "oauth.airtable_token.refresh_failed",
			"user_id", userID,
			"error_kind", "auth",
			"error_message", ReauthorizationRequiredError{}.Error(),
		)
		return "", ReauthorizationRequiredError{}
	}
	if m.now().Add(m.refreshBefore).Before(record.ExpiresAt) {
		return m.decryptAccessToken(record)
	}

	refreshPlaintext, err := m.cipher.Decrypt(record.RefreshTokenCiphertext)
	if err != nil {
		logx.Event(ctx, "oauth_token_manager", "oauth.airtable_token.refresh_failed",
			"user_id", userID,
			"error_kind", logx.ErrorKind(err),
			"error_message", logx.ErrorPreview(err),
		)
		return "", fmt.Errorf("decrypt airtable refresh token: %w", err)
	}

	refreshed, err := m.airtable.Refresh(ctx, string(refreshPlaintext))
	if err != nil {
		if IsOAuthError(err, "invalid_grant") {
			requiredAt := m.now().UTC()
			if markErr := m.store.MarkAirtableTokenReauthRequired(ctx, userID, requiredAt); markErr != nil {
				logx.Event(ctx, "oauth_token_manager", "oauth.airtable_token.refresh_failed",
					"user_id", userID,
					"error_kind", logx.ErrorKind(markErr),
					"error_message", logx.ErrorPreview(markErr),
				)
				return "", markErr
			}
			logx.Event(ctx, "oauth_token_manager", "oauth.airtable_token.reauth_required_marked",
				"user_id", userID,
			)
			return "", ReauthorizationRequiredError{}
		}
		logx.Event(ctx, "oauth_token_manager", "oauth.airtable_token.refresh_failed",
			"user_id", userID,
			"error_kind", logx.ErrorKind(err),
			"error_message", logx.ErrorPreview(err),
		)
		return "", err
	}

	if strings.TrimSpace(refreshed.AccessToken) == "" {
		logx.Event(ctx, "oauth_token_manager", "oauth.airtable_token.refresh_failed",
			"user_id", userID,
			"error_kind", "external_api",
			"error_message", "airtable refresh response did not include an access token",
		)
		return "", fmt.Errorf("airtable refresh response did not include an access token")
	}

	newRefreshToken := refreshed.RefreshToken
	if strings.TrimSpace(newRefreshToken) == "" {
		newRefreshToken = string(refreshPlaintext)
	}

	accessCiphertext, err := m.cipher.Encrypt([]byte(refreshed.AccessToken))
	if err != nil {
		logx.Event(ctx, "oauth_token_manager", "oauth.airtable_token.refresh_failed",
			"user_id", userID,
			"error_kind", logx.ErrorKind(err),
			"error_message", logx.ErrorPreview(err),
		)
		return "", fmt.Errorf("encrypt refreshed airtable access token: %w", err)
	}
	refreshCiphertext, err := m.cipher.Encrypt([]byte(newRefreshToken))
	if err != nil {
		logx.Event(ctx, "oauth_token_manager", "oauth.airtable_token.refresh_failed",
			"user_id", userID,
			"error_kind", logx.ErrorKind(err),
			"error_message", logx.ErrorPreview(err),
		)
		return "", fmt.Errorf("encrypt refreshed airtable refresh token: %w", err)
	}

	expiresIn := refreshed.ExpiresIn
	if expiresIn <= 0 {
		expiresIn = 3600
	}
	scopes := strings.TrimSpace(refreshed.Scope)
	if scopes == "" {
		scopes = record.Scopes
	}

	if err := m.store.PutAirtableToken(ctx, db.AirtableTokenRecord{
		UserID:                 userID,
		AccessTokenCiphertext:  accessCiphertext,
		RefreshTokenCiphertext: refreshCiphertext,
		ExpiresAt:              m.now().Add(time.Duration(expiresIn) * time.Second).UTC(),
		Scopes:                 scopes,
	}); err != nil {
		logx.Event(ctx, "oauth_token_manager", "oauth.airtable_token.refresh_failed",
			"user_id", userID,
			"error_kind", logx.ErrorKind(err),
			"error_message", logx.ErrorPreview(err),
		)
		return "", err
	}
	logx.Event(ctx, "oauth_token_manager", "oauth.airtable_token.refresh_succeeded",
		"user_id", userID,
		"expires_in_seconds", expiresIn,
	)

	return refreshed.AccessToken, nil
}

func (m *TokenManager) decryptAccessToken(record db.AirtableTokenRecord) (string, error) {
	accessPlaintext, err := m.cipher.Decrypt(record.AccessTokenCiphertext)
	if err != nil {
		return "", fmt.Errorf("decrypt airtable access token: %w", err)
	}
	return string(accessPlaintext), nil
}
