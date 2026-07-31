package oauth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/hackclub/better-airtable-mcp/internal/db"
	"github.com/hackclub/better-airtable-mcp/internal/httpx"
	"github.com/hackclub/better-airtable-mcp/internal/logx"
)

type Middleware struct {
	store               *db.Store
	limiter             *RequestLimiter
	resourceMetadataURL string
}

type contextKey string

const userIDContextKey contextKey = "oauth_user_id"
const tokenHashContextKey contextKey = "oauth_token_hash"
const clientIDContextKey contextKey = "oauth_client_id"
const clientNameContextKey contextKey = "oauth_client_name"

// NewMiddleware builds the bearer-auth middleware. resourceMetadataURL is the
// absolute URL of the RFC 9728 protected-resource metadata document; it is
// advertised in the WWW-Authenticate challenge on 401s so MCP clients can
// discover the authorization server and re-authenticate in place instead of
// surfacing an opaque error.
func NewMiddleware(store *db.Store, resourceMetadataURL string) *Middleware {
	m := NewMiddlewareWithRateLimit(store, 50, 50)
	m.resourceMetadataURL = resourceMetadataURL
	return m
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
			m.writeUnauthorized(w, "", "missing bearer token")
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
			m.writeUnauthorized(w, "invalid_token", "invalid or expired bearer token")
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

// writeUnauthorized emits a 401 with an RFC 6750 / RFC 9728 WWW-Authenticate
// challenge so MCP clients re-run the OAuth flow (reconnect in place) rather
// than surfacing an opaque error. errCode is empty when no credentials were
// presented (a missing token is not an invalid one).
func (m *Middleware) writeUnauthorized(w http.ResponseWriter, errCode, message string) {
	params := make([]string, 0, 3)
	if errCode != "" {
		params = append(params, fmt.Sprintf("error=%q", errCode), fmt.Sprintf("error_description=%q", message))
	}
	if m.resourceMetadataURL != "" {
		params = append(params, fmt.Sprintf("resource_metadata=%q", m.resourceMetadataURL))
	}
	challenge := "Bearer"
	if len(params) > 0 {
		challenge += " " + strings.Join(params, ", ")
	}
	w.Header().Set("WWW-Authenticate", challenge)
	httpx.WriteError(w, http.StatusUnauthorized, message)
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

// ContextWithUserID returns a context carrying an authenticated user id, the
// same way RequireBearer sets it. Intended for tests that exercise handlers
// or tools below the middleware.
func ContextWithUserID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, userIDContextKey, userID)
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
