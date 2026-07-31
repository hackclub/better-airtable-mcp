package oauth

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRequireBearerEmitsResourceMetadataChallenge(t *testing.T) {
	const resourceMeta = "https://example.test/.well-known/oauth-protected-resource"
	middleware := NewMiddleware(nil, resourceMeta)
	handler := middleware.RequireBearer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler should not run without a bearer token")
	}))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/mcp", nil))

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	challenge := rec.Header().Get("WWW-Authenticate")
	if !strings.HasPrefix(challenge, "Bearer") {
		t.Fatalf("WWW-Authenticate = %q, want a Bearer challenge", challenge)
	}
	if !strings.Contains(challenge, `resource_metadata="`+resourceMeta+`"`) {
		t.Fatalf("WWW-Authenticate = %q, want resource_metadata pointing at the metadata doc", challenge)
	}
}

func TestWriteUnauthorizedFormatsInvalidTokenChallenge(t *testing.T) {
	m := &Middleware{resourceMetadataURL: "https://example.test/.well-known/oauth-protected-resource"}
	rec := httptest.NewRecorder()
	m.writeUnauthorized(rec, "invalid_token", "invalid or expired bearer token")

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	challenge := rec.Header().Get("WWW-Authenticate")
	for _, want := range []string{
		`error="invalid_token"`,
		`error_description="invalid or expired bearer token"`,
		`resource_metadata="https://example.test/.well-known/oauth-protected-resource"`,
	} {
		if !strings.Contains(challenge, want) {
			t.Fatalf("WWW-Authenticate = %q, missing %q", challenge, want)
		}
	}
}

func ptr[T any](value T) *T {
	return &value
}
