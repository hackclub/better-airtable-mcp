package oauth

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/hackclub/better-airtable-mcp/internal/config"
	"github.com/hackclub/better-airtable-mcp/internal/cryptoutil"
	"github.com/hackclub/better-airtable-mcp/internal/db"
	"github.com/hackclub/better-airtable-mcp/internal/httpx"
	"github.com/hackclub/better-airtable-mcp/internal/logx"
)

type Handler struct {
	cfg      config.Config
	store    *db.Store
	cipher   *cryptoutil.Cipher
	airtable *AirtableOAuthClient
	now      func() time.Time

	mu            sync.Mutex
	authRequests  map[string]authorizationRequest
	authCodes     map[string]authorizationCode
	refreshGrants map[string]refreshGrant

	mcpTokenTTL        time.Duration
	mcpRefreshTokenTTL time.Duration
}

type authorizationRequest struct {
	ClientID            string
	RedirectURI         string
	OriginalState       string
	CodeChallenge       string
	CodeChallengeMethod string
	AirtableVerifier    string
	ExpiresAt           time.Time
}

type authorizationCode struct {
	ClientID      string
	RedirectURI   string
	UserID        string
	CodeChallenge string
	ExpiresAt     time.Time
}

type refreshGrant struct {
	ClientID  string
	UserID    string
	ExpiresAt time.Time
}

type registerRequest struct {
	RedirectURIs []string `json:"redirect_uris"`
	ClientName   string   `json:"client_name"`
}

func NewHandler(cfg config.Config, store *db.Store, cipher *cryptoutil.Cipher, airtable *AirtableOAuthClient) *Handler {
	if airtable == nil {
		airtable = NewAirtableOAuthClient(
			cfg.AirtableClientID,
			cfg.AirtableClientSecret,
			cfg.BaseURLString()+"/oauth/airtable/callback",
			nil,
			"",
			"",
		)
	}

	return &Handler{
		cfg:                cfg,
		store:              store,
		cipher:             cipher,
		airtable:           airtable,
		now:                time.Now,
		authRequests:       make(map[string]authorizationRequest),
		authCodes:          make(map[string]authorizationCode),
		refreshGrants:      make(map[string]refreshGrant),
		mcpTokenTTL:        24 * time.Hour,
		mcpRefreshTokenTTL: 30 * 24 * time.Hour,
	}
}

func (h *Handler) ProtectedResource(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpx.MethodNotAllowed(w, http.MethodGet)
		return
	}

	baseURL := h.cfg.BaseURLString()
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"resource":                 h.cfg.MCPURL(),
		"authorization_servers":    []string{baseURL},
		"bearer_methods_supported": []string{"header"},
	})
}

func (h *Handler) AuthorizationServer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpx.MethodNotAllowed(w, http.MethodGet)
		return
	}

	baseURL := h.cfg.BaseURLString()
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"issuer":                                baseURL,
		"authorization_endpoint":                baseURL + "/oauth/authorize",
		"token_endpoint":                        baseURL + "/oauth/token",
		"registration_endpoint":                 baseURL + "/oauth/register",
		"response_types_supported":              []string{"code"},
		"grant_types_supported":                 []string{"authorization_code", "refresh_token"},
		"code_challenge_methods_supported":      []string{"S256"},
		"token_endpoint_auth_methods_supported": []string{"none", "client_secret_post"},
	})
}

func (h *Handler) Register(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		httpx.MethodNotAllowed(w, http.MethodPost)
		return
	}

	var request registerRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		logx.Event(r.Context(), "oauth_handler", "oauth.client_registration_failed",
			"error_kind", "validation",
			"error_message", "invalid JSON payload",
		)
		httpx.WriteError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}
	if len(request.RedirectURIs) == 0 {
		logx.Event(r.Context(), "oauth_handler", "oauth.client_registration_failed",
			"error_kind", "validation",
			"error_message", "redirect_uris must contain at least one URI",
		)
		httpx.WriteError(w, http.StatusBadRequest, "redirect_uris must contain at least one URI")
		return
	}

	for _, redirectURI := range request.RedirectURIs {
		if _, err := url.ParseRequestURI(redirectURI); err != nil {
			logx.Event(r.Context(), "oauth_handler", "oauth.client_registration_failed",
				"redirect_uri", logx.SanitizeRedirectURI(redirectURI),
				"error_kind", "validation",
				"error_message", "redirect_uris must contain valid absolute URIs",
			)
			httpx.WriteError(w, http.StatusBadRequest, "redirect_uris must contain valid absolute URIs")
			return
		}
	}

	clientID, err := generateOpaqueToken(24)
	if err != nil {
		logx.Event(r.Context(), "oauth_handler", "oauth.client_registration_failed",
			"error_kind", logx.ErrorKind(err),
			"error_message", logx.ErrorPreview(err),
		)
		httpx.WriteError(w, http.StatusInternalServerError, "failed to generate client_id")
		return
	}

	var clientName *string
	if trimmed := strings.TrimSpace(request.ClientName); trimmed != "" {
		clientName = &trimmed
	}

	if err := h.store.UpsertOAuthClient(r.Context(), db.OAuthClient{
		ClientID:     clientID,
		RedirectURIs: request.RedirectURIs,
		ClientName:   clientName,
	}); err != nil {
		logx.Event(r.Context(), "oauth_handler", "oauth.client_registration_failed",
			"client_id", clientID,
			"error_kind", logx.ErrorKind(err),
			"error_message", logx.ErrorPreview(err),
		)
		httpx.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	logx.Event(r.Context(), "oauth_handler", "oauth.client_registered",
		"client_id", clientID,
		"redirect_uri_count", len(request.RedirectURIs),
	)

	httpx.WriteJSON(w, http.StatusCreated, map[string]any{
		"client_id":                  clientID,
		"redirect_uris":              request.RedirectURIs,
		"client_name":                clientName,
		"grant_types":                []string{"authorization_code", "refresh_token"},
		"response_types":             []string{"code"},
		"token_endpoint_auth_method": "none",
		"code_challenge_methods":     []string{"S256"},
	})
}

func (h *Handler) Authorize(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpx.MethodNotAllowed(w, http.MethodGet)
		return
	}

	query := r.URL.Query()
	if query.Get("response_type") != "code" {
		httpx.WriteError(w, http.StatusBadRequest, "response_type must be code")
		return
	}

	clientID := strings.TrimSpace(query.Get("client_id"))
	redirectURI := strings.TrimSpace(query.Get("redirect_uri"))
	state := query.Get("state")
	codeChallenge := query.Get("code_challenge")
	codeChallengeMethod := query.Get("code_challenge_method")
	logx.Event(r.Context(), "oauth_handler", "oauth.authorize_requested",
		"client_id", clientID,
		"redirect_uri", logx.SanitizeRedirectURI(redirectURI),
	)

	if clientID == "" || redirectURI == "" || state == "" || codeChallenge == "" || codeChallengeMethod != "S256" {
		logx.Event(r.Context(), "oauth_handler", "oauth.authorize_rejected",
			"client_id", clientID,
			"redirect_uri", logx.SanitizeRedirectURI(redirectURI),
			"error_kind", "validation",
			"error_message", "client_id, redirect_uri, state, code_challenge, and code_challenge_method=S256 are required",
		)
		httpx.WriteError(w, http.StatusBadRequest, "client_id, redirect_uri, state, code_challenge, and code_challenge_method=S256 are required")
		return
	}

	client, err := h.store.GetOAuthClient(r.Context(), clientID)
	if err != nil {
		logx.Event(r.Context(), "oauth_handler", "oauth.authorize_rejected",
			"client_id", clientID,
			"redirect_uri", logx.SanitizeRedirectURI(redirectURI),
			"error_kind", "auth",
			"error_message", "unknown client_id",
		)
		httpx.WriteError(w, http.StatusBadRequest, "unknown client_id")
		return
	}
	if !contains(client.RedirectURIs, redirectURI) {
		logx.Event(r.Context(), "oauth_handler", "oauth.authorize_rejected",
			"client_id", clientID,
			"redirect_uri", logx.SanitizeRedirectURI(redirectURI),
			"error_kind", "auth",
			"error_message", "redirect_uri is not registered for this client",
		)
		httpx.WriteError(w, http.StatusBadRequest, "redirect_uri is not registered for this client")
		return
	}

	airtableVerifier, err := generatePKCEVerifier()
	if err != nil {
		logx.Event(r.Context(), "oauth_handler", "oauth.authorize_rejected",
			"client_id", clientID,
			"error_kind", logx.ErrorKind(err),
			"error_message", logx.ErrorPreview(err),
		)
		httpx.WriteError(w, http.StatusInternalServerError, "failed to create PKCE verifier")
		return
	}
	requestID, err := generateOpaqueToken(24)
	if err != nil {
		logx.Event(r.Context(), "oauth_handler", "oauth.authorize_rejected",
			"client_id", clientID,
			"error_kind", logx.ErrorKind(err),
			"error_message", logx.ErrorPreview(err),
		)
		httpx.WriteError(w, http.StatusInternalServerError, "failed to create authorization request")
		return
	}

	h.mu.Lock()
	h.pruneExpiredLocked(h.now().UTC())
	h.authRequests[requestID] = authorizationRequest{
		ClientID:            clientID,
		RedirectURI:         redirectURI,
		OriginalState:       state,
		CodeChallenge:       codeChallenge,
		CodeChallengeMethod: codeChallengeMethod,
		AirtableVerifier:    airtableVerifier,
		ExpiresAt:           h.now().Add(10 * time.Minute),
	}
	h.mu.Unlock()

	logx.Event(r.Context(), "oauth_handler", "oauth.authorize_redirect_issued",
		"client_id", clientID,
		"redirect_uri", logx.SanitizeRedirectURI(redirectURI),
	)
	http.Redirect(w, r, h.airtable.AuthorizeURL(requestID, S256Challenge(airtableVerifier)), http.StatusFound)
}

func (h *Handler) Token(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		httpx.MethodNotAllowed(w, http.MethodPost)
		return
	}
	if err := r.ParseForm(); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "invalid form payload")
		return
	}

	switch r.PostForm.Get("grant_type") {
	case "authorization_code":
		h.handleAuthorizationCodeGrant(w, r)
	case "refresh_token":
		h.handleRefreshTokenGrant(w, r)
	default:
		logx.Event(r.Context(), "oauth_handler", "oauth.token_exchange_failed",
			"grant_type", r.PostForm.Get("grant_type"),
			"error_kind", "validation",
			"error_message", "grant_type must be authorization_code or refresh_token",
		)
		httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{
			"error":             "unsupported_grant_type",
			"error_description": "grant_type must be authorization_code or refresh_token",
		})
	}
}

func (h *Handler) AirtableCallback(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpx.MethodNotAllowed(w, http.MethodGet)
		return
	}

	requestID := r.URL.Query().Get("state")
	code := r.URL.Query().Get("code")
	logx.Event(r.Context(), "oauth_handler", "oauth.airtable_callback_received")
	if requestID == "" || code == "" {
		logx.Event(r.Context(), "oauth_handler", "oauth.airtable_callback_failed",
			"error_kind", "validation",
			"error_message", "state and code are required",
		)
		httpx.WriteError(w, http.StatusBadRequest, "state and code are required")
		return
	}

	request, ok := h.consumeAuthorizationRequest(requestID)
	if !ok {
		logx.Event(r.Context(), "oauth_handler", "oauth.airtable_callback_failed",
			"error_kind", "auth",
			"error_message", "authorization request is missing or expired",
		)
		httpx.WriteError(w, http.StatusBadRequest, "authorization request is missing or expired")
		return
	}

	airtableToken, err := h.airtable.Exchange(r.Context(), code, request.AirtableVerifier)
	if err != nil {
		logx.Event(r.Context(), "oauth_handler", "oauth.airtable_exchange_failed",
			"client_id", request.ClientID,
			"error_kind", logx.ErrorKind(err),
			"error_message", logx.ErrorPreview(err),
		)
		httpx.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}

	userID, err := h.createUserSession(r.Context(), airtableToken)
	if err != nil {
		logx.Event(r.Context(), "oauth_handler", "oauth.user_session_create_failed",
			"client_id", request.ClientID,
			"error_kind", logx.ErrorKind(err),
			"error_message", logx.ErrorPreview(err),
		)
		httpx.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}

	authCode, err := generateOpaqueToken(32)
	if err != nil {
		logx.Event(r.Context(), "oauth_handler", "oauth.airtable_callback_failed",
			"client_id", request.ClientID,
			"user_id", userID,
			"error_kind", logx.ErrorKind(err),
			"error_message", logx.ErrorPreview(err),
		)
		httpx.WriteError(w, http.StatusInternalServerError, "failed to issue authorization code")
		return
	}

	h.mu.Lock()
	h.pruneExpiredLocked(h.now().UTC())
	h.authCodes[authCode] = authorizationCode{
		ClientID:      request.ClientID,
		RedirectURI:   request.RedirectURI,
		UserID:        userID,
		CodeChallenge: request.CodeChallenge,
		ExpiresAt:     h.now().Add(5 * time.Minute),
	}
	h.mu.Unlock()

	redirectTarget, err := appendQuery(request.RedirectURI, map[string]string{
		"code":  authCode,
		"state": request.OriginalState,
	})
	if err != nil {
		logx.Event(r.Context(), "oauth_handler", "oauth.airtable_callback_failed",
			"client_id", request.ClientID,
			"user_id", userID,
			"redirect_uri", logx.SanitizeRedirectURI(request.RedirectURI),
			"error_kind", logx.ErrorKind(err),
			"error_message", logx.ErrorPreview(err),
		)
		httpx.WriteError(w, http.StatusInternalServerError, "failed to finalize redirect")
		return
	}

	logx.Event(r.Context(), "oauth_handler", "oauth.user_session_created",
		"client_id", request.ClientID,
		"user_id", userID,
		"redirect_uri", logx.SanitizeRedirectURI(request.RedirectURI),
	)
	http.Redirect(w, r, redirectTarget, http.StatusFound)
}

func (h *Handler) handleAuthorizationCodeGrant(w http.ResponseWriter, r *http.Request) {
	clientID := r.PostForm.Get("client_id")
	code := r.PostForm.Get("code")
	redirectURI := r.PostForm.Get("redirect_uri")
	codeVerifier := r.PostForm.Get("code_verifier")
	if clientID == "" || code == "" || redirectURI == "" || codeVerifier == "" {
		logx.Event(r.Context(), "oauth_handler", "oauth.token_exchange_failed",
			"grant_type", "authorization_code",
			"client_id", clientID,
			"redirect_uri", logx.SanitizeRedirectURI(redirectURI),
			"error_kind", "validation",
			"error_message", "client_id, code, redirect_uri, and code_verifier are required",
		)
		httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{
			"error":             "invalid_request",
			"error_description": "client_id, code, redirect_uri, and code_verifier are required",
		})
		return
	}

	authCode, ok := h.consumeAuthorizationCode(code)
	if !ok {
		logx.Event(r.Context(), "oauth_handler", "oauth.token_exchange_failed",
			"grant_type", "authorization_code",
			"client_id", clientID,
			"redirect_uri", logx.SanitizeRedirectURI(redirectURI),
			"error_kind", "auth",
			"error_message", "authorization code is invalid or expired",
		)
		httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{
			"error":             "invalid_grant",
			"error_description": "authorization code is invalid or expired",
		})
		return
	}
	if authCode.ClientID != clientID || authCode.RedirectURI != redirectURI {
		logx.Event(r.Context(), "oauth_handler", "oauth.token_exchange_failed",
			"grant_type", "authorization_code",
			"client_id", clientID,
			"redirect_uri", logx.SanitizeRedirectURI(redirectURI),
			"error_kind", "auth",
			"error_message", "authorization code does not match the client or redirect URI",
		)
		httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{
			"error":             "invalid_grant",
			"error_description": "authorization code does not match the client or redirect URI",
		})
		return
	}
	if S256Challenge(codeVerifier) != authCode.CodeChallenge {
		logx.Event(r.Context(), "oauth_handler", "oauth.token_exchange_failed",
			"grant_type", "authorization_code",
			"client_id", clientID,
			"redirect_uri", logx.SanitizeRedirectURI(redirectURI),
			"error_kind", "auth",
			"error_message", "code_verifier does not satisfy the original PKCE challenge",
		)
		httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{
			"error":             "invalid_grant",
			"error_description": "code_verifier does not satisfy the original PKCE challenge",
		})
		return
	}

	accessToken, refreshToken, expiresAt, err := h.issueMCPToken(r.Context(), authCode.UserID, authCode.ClientID)
	if err != nil {
		logx.Event(r.Context(), "oauth_handler", "oauth.token_exchange_failed",
			"grant_type", "authorization_code",
			"client_id", clientID,
			"user_id", authCode.UserID,
			"redirect_uri", logx.SanitizeRedirectURI(redirectURI),
			"error_kind", logx.ErrorKind(err),
			"error_message", logx.ErrorPreview(err),
		)
		httpx.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}

	logx.Event(r.Context(), "oauth_handler", "oauth.token_exchange_succeeded",
		"grant_type", "authorization_code",
		"client_id", clientID,
		"user_id", authCode.UserID,
		"expires_in_seconds", int(time.Until(expiresAt).Seconds()),
	)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"access_token":  accessToken,
		"refresh_token": refreshToken,
		"token_type":    "Bearer",
		"expires_in":    int(time.Until(expiresAt).Seconds()),
	})
}

func (h *Handler) handleRefreshTokenGrant(w http.ResponseWriter, r *http.Request) {
	refreshToken := r.PostForm.Get("refresh_token")
	clientID := r.PostForm.Get("client_id")
	if refreshToken == "" {
		logx.Event(r.Context(), "oauth_handler", "oauth.token_exchange_failed",
			"grant_type", "refresh_token",
			"client_id", clientID,
			"error_kind", "validation",
			"error_message", "refresh_token is required",
		)
		httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{
			"error":             "invalid_request",
			"error_description": "refresh_token is required",
		})
		return
	}

	grant, ok := h.consumeRefreshGrant(refreshToken)
	if !ok {
		logx.Event(r.Context(), "oauth_handler", "oauth.token_exchange_failed",
			"grant_type", "refresh_token",
			"client_id", clientID,
			"error_kind", "auth",
			"error_message", "refresh_token is invalid or expired",
		)
		httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{
			"error":             "invalid_grant",
			"error_description": "refresh_token is invalid or expired",
		})
		return
	}
	if clientID != "" && grant.ClientID != clientID {
		logx.Event(r.Context(), "oauth_handler", "oauth.token_exchange_failed",
			"grant_type", "refresh_token",
			"client_id", clientID,
			"user_id", grant.UserID,
			"error_kind", "auth",
			"error_message", "refresh_token does not belong to the supplied client_id",
		)
		httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{
			"error":             "invalid_grant",
			"error_description": "refresh_token does not belong to the supplied client_id",
		})
		return
	}

	accessToken, newRefreshToken, expiresAt, err := h.issueMCPToken(r.Context(), grant.UserID, grant.ClientID)
	if err != nil {
		logx.Event(r.Context(), "oauth_handler", "oauth.token_exchange_failed",
			"grant_type", "refresh_token",
			"client_id", clientID,
			"user_id", grant.UserID,
			"error_kind", logx.ErrorKind(err),
			"error_message", logx.ErrorPreview(err),
		)
		httpx.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}

	logx.Event(r.Context(), "oauth_handler", "oauth.token_exchange_succeeded",
		"grant_type", "refresh_token",
		"client_id", grant.ClientID,
		"user_id", grant.UserID,
		"expires_in_seconds", int(time.Until(expiresAt).Seconds()),
	)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"access_token":  accessToken,
		"refresh_token": newRefreshToken,
		"token_type":    "Bearer",
		"expires_in":    int(time.Until(expiresAt).Seconds()),
	})
}

func (h *Handler) createUserSession(ctx context.Context, token AirtableTokenResponse) (string, error) {
	userID, err := generateOpaqueToken(20)
	if err != nil {
		return "", fmt.Errorf("generate user ID: %w", err)
	}
	userID = "user_" + userID

	if err := h.store.UpsertUser(ctx, db.User{ID: userID}); err != nil {
		return "", err
	}

	accessCiphertext, err := h.cipher.Encrypt([]byte(token.AccessToken))
	if err != nil {
		return "", fmt.Errorf("encrypt airtable access token: %w", err)
	}
	refreshCiphertext, err := h.cipher.Encrypt([]byte(token.RefreshToken))
	if err != nil {
		return "", fmt.Errorf("encrypt airtable refresh token: %w", err)
	}

	if err := h.store.PutAirtableToken(ctx, db.AirtableTokenRecord{
		UserID:                 userID,
		AccessTokenCiphertext:  accessCiphertext,
		RefreshTokenCiphertext: refreshCiphertext,
		ExpiresAt:              h.now().Add(time.Duration(token.ExpiresIn) * time.Second),
		Scopes:                 token.Scope,
	}); err != nil {
		return "", err
	}

	logx.Event(ctx, "oauth_handler", "oauth.user_session_stored",
		"user_id", userID,
	)
	return userID, nil
}

func (h *Handler) issueMCPToken(ctx context.Context, userID, clientID string) (string, string, time.Time, error) {
	accessToken, err := generateOpaqueToken(32)
	if err != nil {
		return "", "", time.Time{}, fmt.Errorf("generate access token: %w", err)
	}
	refreshToken, err := generateOpaqueToken(32)
	if err != nil {
		return "", "", time.Time{}, fmt.Errorf("generate refresh token: %w", err)
	}

	var clientIDPtr *string
	var clientNamePtr *string
	if trimmed := strings.TrimSpace(clientID); trimmed != "" {
		clientIDPtr = &trimmed
		client, err := h.store.GetOAuthClient(ctx, trimmed)
		if err != nil {
			return "", "", time.Time{}, err
		}
		clientNamePtr = client.ClientName
	}

	expiresAt := h.now().Add(h.mcpTokenTTL)
	if err := h.store.PutMCPToken(ctx, db.MCPTokenRecord{
		TokenHash:  hashToken(accessToken),
		UserID:     userID,
		ClientID:   clientIDPtr,
		ClientName: clientNamePtr,
		CreatedAt:  h.now().UTC(),
		ExpiresAt:  expiresAt.UTC(),
	}); err != nil {
		return "", "", time.Time{}, err
	}

	h.mu.Lock()
	h.pruneExpiredLocked(h.now().UTC())
	h.refreshGrants[refreshToken] = refreshGrant{
		ClientID:  clientID,
		UserID:    userID,
		ExpiresAt: h.now().Add(h.mcpRefreshTokenTTL),
	}
	h.mu.Unlock()

	logx.Event(ctx, "oauth_handler", "oauth.token_issued",
		"user_id", userID,
		"client_id", clientID,
		"expires_in_seconds", int(time.Until(expiresAt).Seconds()),
	)
	return accessToken, refreshToken, expiresAt.UTC(), nil
}

func (h *Handler) consumeAuthorizationRequest(id string) (authorizationRequest, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	now := h.now().UTC()
	h.pruneExpiredLocked(now)
	request, ok := h.authRequests[id]
	if !ok || now.After(request.ExpiresAt) {
		delete(h.authRequests, id)
		return authorizationRequest{}, false
	}
	delete(h.authRequests, id)
	return request, true
}

func (h *Handler) consumeAuthorizationCode(code string) (authorizationCode, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	now := h.now().UTC()
	h.pruneExpiredLocked(now)
	authCode, ok := h.authCodes[code]
	if !ok || now.After(authCode.ExpiresAt) {
		delete(h.authCodes, code)
		return authorizationCode{}, false
	}
	delete(h.authCodes, code)
	return authCode, true
}

func (h *Handler) consumeRefreshGrant(refreshToken string) (refreshGrant, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	now := h.now().UTC()
	h.pruneExpiredLocked(now)
	grant, ok := h.refreshGrants[refreshToken]
	if !ok || now.After(grant.ExpiresAt) {
		delete(h.refreshGrants, refreshToken)
		return refreshGrant{}, false
	}
	delete(h.refreshGrants, refreshToken)
	return grant, true
}

func (h *Handler) RunCleanupLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			removed := h.PruneExpiredState()
			if removed > 0 {
				logx.Event(ctx, "oauth_handler", "oauth.state_pruned",
					"removed_entries", removed,
				)
			}
		}
	}
}

func (h *Handler) PruneExpiredState() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.pruneExpiredLocked(h.now().UTC())
}

func (h *Handler) pruneExpiredLocked(now time.Time) int {
	removed := 0

	for id, request := range h.authRequests {
		if !now.Before(request.ExpiresAt) {
			delete(h.authRequests, id)
			removed++
		}
	}
	for code, authCode := range h.authCodes {
		if !now.Before(authCode.ExpiresAt) {
			delete(h.authCodes, code)
			removed++
		}
	}
	for token, grant := range h.refreshGrants {
		if !now.Before(grant.ExpiresAt) {
			delete(h.refreshGrants, token)
			removed++
		}
	}

	return removed
}

func contains(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}

func appendQuery(base string, params map[string]string) (string, error) {
	parsed, err := url.Parse(base)
	if err != nil {
		return "", err
	}

	query := parsed.Query()
	for key, value := range params {
		query.Set(key, value)
	}
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func generateOpaqueToken(bytesLen int) (string, error) {
	token := make([]byte, bytesLen)
	if err := fillRandom(token); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(token), nil
}

func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
