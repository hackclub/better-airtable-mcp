package oauth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"strings"
	"time"

	"github.com/hackclub/better-airtable-mcp/internal/db"
	"github.com/hackclub/better-airtable-mcp/internal/httpx"
	"github.com/hackclub/better-airtable-mcp/internal/logx"
)

type Middleware struct {
	store   *db.Store
	limiter *RequestLimiter
}

type contextKey string

const userIDContextKey contextKey = "oauth_user_id"
const tokenHashContextKey contextKey = "oauth_token_hash"
const clientIDContextKey contextKey = "oauth_client_id"
const clientNameContextKey contextKey = "oauth_client_name"

func NewMiddleware(store *db.Store) *Middleware {
	return NewMiddlewareWithRateLimit(store, 50, 50)
}

func NewMiddlewareWithRateLimit(store *db.Store, requestsPerSecond float64, burst int) *Middleware {
	return &Middleware{
		store:   store,
		limiter: NewRequestLimiter(requestsPerSecond, burst),
	}
}

func (m *Middleware) RequireBearer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := r.Header.Get("Authorization")
		token, ok := strings.CutPrefix(header, "Bearer ")
		if !ok || strings.TrimSpace(token) == "" {
			logx.Event(r.Context(), "oauth_middleware", "oauth.middleware_rejected",
				"reason", "missing_bearer_token",
				"error_kind", "auth",
				"error_message", "missing bearer token",
			)
			httpx.WriteError(w, http.StatusUnauthorized, "missing bearer token")
			return
		}

		tokenHash := HashToken(token)
		record, err := m.store.GetMCPToken(r.Context(), tokenHash)
		if err != nil || time.Now().After(record.ExpiresAt) {
			logx.Event(r.Context(), "oauth_middleware", "oauth.middleware_rejected",
				"reason", "invalid_or_expired_bearer_token",
				"token_hash", tokenHash,
				"error_kind", "auth",
				"error_message", "invalid or expired bearer token",
			)
			httpx.WriteError(w, http.StatusUnauthorized, "invalid or expired bearer token")
			return
		}
		if m.limiter != nil && !m.limiter.Allow(tokenHash) {
			logx.Event(r.Context(), "oauth_middleware", "oauth.rate_limited",
				"token_hash", tokenHash,
				"user_id", record.UserID,
				"client_id", valueOrBlank(record.ClientID),
				"retry_after_seconds", 1,
				"error_kind", "rate_limit",
				"error_message", "rate limit exceeded",
			)
			w.Header().Set("Retry-After", "1")
			httpx.WriteError(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}

		ctx := context.WithValue(r.Context(), userIDContextKey, record.UserID)
		ctx = context.WithValue(ctx, tokenHashContextKey, tokenHash)
		if record.ClientID != nil {
			ctx = context.WithValue(ctx, clientIDContextKey, *record.ClientID)
		}
		if record.ClientName != nil {
			ctx = context.WithValue(ctx, clientNameContextKey, *record.ClientName)
		}
		logger := logx.FromContext(ctx).With(
			"user_id", record.UserID,
			"token_hash", tokenHash,
		)
		if record.ClientID != nil {
			logger = logger.With("client_id", *record.ClientID)
		}
		ctx = logx.WithLogger(ctx, logger)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func valueOrBlank(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func UserIDFromContext(ctx context.Context) (string, bool) {
	userID, ok := ctx.Value(userIDContextKey).(string)
	return userID, ok
}

func TokenHashFromContext(ctx context.Context) (string, bool) {
	tokenHash, ok := ctx.Value(tokenHashContextKey).(string)
	return tokenHash, ok
}

func ClientIDFromContext(ctx context.Context) (string, bool) {
	clientID, ok := ctx.Value(clientIDContextKey).(string)
	return clientID, ok
}

func ClientNameFromContext(ctx context.Context) (string, bool) {
	clientName, ok := ctx.Value(clientNameContextKey).(string)
	return clientName, ok
}

func HashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func generatePKCEVerifier() (string, error) {
	random := make([]byte, 32)
	if err := fillRandom(random); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(random), nil
}

func S256Challenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func fillRandom(buffer []byte) error {
	_, err := rand.Read(buffer)
	return err
}
