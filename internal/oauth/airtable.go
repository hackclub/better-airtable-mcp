package oauth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const (
	defaultAirtableAuthorizeURL = "https://airtable.com/oauth2/v1/authorize"
	defaultAirtableTokenURL     = "https://airtable.com/oauth2/v1/token"
	defaultAirtableScopes       = "data.records:read data.records:write schema.bases:read"
)

type AirtableOAuthClient struct {
	clientID     string
	clientSecret string
	redirectURI  string
	httpClient   *http.Client
	authorizeURL string
	tokenURL     string
	scopes       string
}

type AirtableTokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
	Scope        string `json:"scope"`
	TokenType    string `json:"token_type"`
}

type TokenEndpointError struct {
	StatusCode       int
	Body             string
	OAuthError       string
	OAuthDescription string
}

func (e *TokenEndpointError) Error() string {
	return fmt.Sprintf("airtable token endpoint returned %d: %s", e.StatusCode, e.Body)
}

func NewAirtableOAuthClient(clientID, clientSecret, redirectURI string, httpClient *http.Client, authorizeURL, tokenURL string) *AirtableOAuthClient {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	if strings.TrimSpace(authorizeURL) == "" {
		authorizeURL = defaultAirtableAuthorizeURL
	}
	if strings.TrimSpace(tokenURL) == "" {
		tokenURL = defaultAirtableTokenURL
	}

	return &AirtableOAuthClient{
		clientID:     clientID,
		clientSecret: clientSecret,
		redirectURI:  redirectURI,
		httpClient:   httpClient,
		authorizeURL: authorizeURL,
		tokenURL:     tokenURL,
		scopes:       defaultAirtableScopes,
	}
}

func (c *AirtableOAuthClient) AuthorizeURL(state, codeChallenge string) string {
	query := url.Values{}
	query.Set("client_id", c.clientID)
	query.Set("redirect_uri", c.redirectURI)
	query.Set("response_type", "code")
	query.Set("scope", c.scopes)
	query.Set("state", state)
	query.Set("code_challenge", codeChallenge)
	query.Set("code_challenge_method", "S256")
	return c.authorizeURL + "?" + query.Encode()
}

func (c *AirtableOAuthClient) Exchange(ctx context.Context, code, verifier string) (AirtableTokenResponse, error) {
	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("client_id", c.clientID)
	form.Set("redirect_uri", c.redirectURI)
	form.Set("code", code)
	form.Set("code_verifier", verifier)
	return c.tokenRequest(ctx, form)
}

func (c *AirtableOAuthClient) Refresh(ctx context.Context, refreshToken string) (AirtableTokenResponse, error) {
	form := url.Values{}
	form.Set("grant_type", "refresh_token")
	form.Set("client_id", c.clientID)
	form.Set("refresh_token", refreshToken)
	return c.tokenRequest(ctx, form)
}

func (c *AirtableOAuthClient) tokenRequest(ctx context.Context, form url.Values) (AirtableTokenResponse, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return AirtableTokenResponse{}, fmt.Errorf("create airtable token request: %w", err)
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Accept", "application/json")
	if c.clientSecret != "" {
		request.Header.Set("Authorization", "Basic "+basicAuth(c.clientID, c.clientSecret))
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return AirtableTokenResponse{}, fmt.Errorf("perform airtable token request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		trimmedBody := strings.TrimSpace(string(body))
		var payload struct {
			Error            string `json:"error"`
			ErrorDescription string `json:"error_description"`
		}
		_ = json.Unmarshal(body, &payload)
		return AirtableTokenResponse{}, &TokenEndpointError{
			StatusCode:       response.StatusCode,
			Body:             trimmedBody,
			OAuthError:       payload.Error,
			OAuthDescription: payload.ErrorDescription,
		}
	}

	var token AirtableTokenResponse
	if err := json.NewDecoder(response.Body).Decode(&token); err != nil {
		return AirtableTokenResponse{}, fmt.Errorf("decode airtable token response: %w", err)
	}

	return token, nil
}

func IsOAuthError(err error, target string) bool {
	tokenErr, ok := err.(*TokenEndpointError)
	return ok && tokenErr.OAuthError == target
}

func basicAuth(username, password string) string {
	return base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
}
