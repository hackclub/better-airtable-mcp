package landing

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHandlerRendersREADMEAsHTML(t *testing.T) {
	dir := t.TempDir()
	readmePath := filepath.Join(dir, "README.md")
	if err := os.WriteFile(readmePath, []byte("# Better Airtable MCP\n\nA **test** landing page."), 0o644); err != nil {
		t.Fatalf("WriteFile() returned error: %v", err)
	}

	handler := NewHandler(readmePath)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/", nil)

	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", recorder.Code)
	}
	if got := recorder.Header().Get("Content-Type"); !strings.Contains(got, "text/html") {
		t.Fatalf("expected HTML content type, got %q", got)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "<h1>Better Airtable MCP</h1>") {
		t.Fatalf("expected rendered heading, got %q", body)
	}
	if !strings.Contains(body, "<strong>test</strong>") {
		t.Fatalf("expected rendered markdown emphasis, got %q", body)
	}
}

func TestHandlerRejectsNonRootPaths(t *testing.T) {
	handler := NewHandler("README.md")
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/nope", nil)

	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", recorder.Code)
	}
}
