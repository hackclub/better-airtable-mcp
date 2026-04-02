package approval

import (
	"context"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type stubOperationService struct {
	getOperation func(ctx context.Context, operationID string) (OperationView, error)
	approve      func(ctx context.Context, operationID string) (OperationView, error)
	reject       func(ctx context.Context, operationID string) (OperationView, error)
}

func (s stubOperationService) GetOperation(ctx context.Context, operationID string) (OperationView, error) {
	return s.getOperation(ctx, operationID)
}

func (s stubOperationService) Approve(ctx context.Context, operationID string) (OperationView, error) {
	return s.approve(ctx, operationID)
}

func (s stubOperationService) Reject(ctx context.Context, operationID string) (OperationView, error) {
	return s.reject(ctx, operationID)
}

func TestServeApprovalPageServesEmbeddedSPA(t *testing.T) {
	handler := NewHandler(stubOperationService{
		getOperation: func(context.Context, string) (OperationView, error) {
			return testOperationView(), nil
		},
		approve: func(context.Context, string) (OperationView, error) {
			return testOperationView(), nil
		},
		reject: func(context.Context, string) (OperationView, error) {
			return testOperationView(), nil
		},
	})

	request := httptest.NewRequest(http.MethodGet, "/approve/op_123", nil)
	recorder := httptest.NewRecorder()

	handler.ServeApprovalPage(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d", recorder.Code)
	}
	if got := recorder.Header().Get("Content-Type"); !strings.Contains(got, "text/html") {
		t.Fatalf("expected text/html content type, got %q", got)
	}
	if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("expected Cache-Control=no-store, got %q", got)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "<div id=\"root\"></div>") {
		t.Fatalf("expected embedded SPA root element, got %q", body)
	}
	if !strings.Contains(body, "/src/main.tsx") && !strings.Contains(body, "/approval-ui/assets/") {
		t.Fatalf("expected SPA entrypoint references in HTML, got %q", body)
	}
}

func TestServeApprovalPageReturns404ForUnknownOperation(t *testing.T) {
	handler := NewHandler(stubOperationService{
		getOperation: func(context.Context, string) (OperationView, error) {
			return OperationView{}, context.Canceled
		},
		approve: func(context.Context, string) (OperationView, error) {
			return OperationView{}, nil
		},
		reject: func(context.Context, string) (OperationView, error) {
			return OperationView{}, nil
		},
	})

	request := httptest.NewRequest(http.MethodGet, "/approve/op_missing", nil)
	recorder := httptest.NewRecorder()

	handler.ServeApprovalPage(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected HTTP 404, got %d", recorder.Code)
	}
}

func TestServeDebugPageServesEmbeddedSPAWithoutApprovalLookup(t *testing.T) {
	handler := NewHandler(nil)

	request := httptest.NewRequest(http.MethodGet, "/debug", nil)
	recorder := httptest.NewRecorder()

	handler.ServeDebugPage(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d", recorder.Code)
	}
	if got := recorder.Header().Get("Content-Type"); !strings.Contains(got, "text/html") {
		t.Fatalf("expected text/html content type, got %q", got)
	}
	if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("expected Cache-Control=no-store, got %q", got)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "<div id=\"root\"></div>") {
		t.Fatalf("expected embedded SPA root element, got %q", body)
	}
}

func TestServeAssetsServesEmbeddedBundleAsset(t *testing.T) {
	handler := NewHandler(stubOperationService{
		getOperation: func(context.Context, string) (OperationView, error) {
			return testOperationView(), nil
		},
		approve: func(context.Context, string) (OperationView, error) {
			return testOperationView(), nil
		},
		reject: func(context.Context, string) (OperationView, error) {
			return testOperationView(), nil
		},
	})

	assetPath := firstBundledAssetPath(t)
	request := httptest.NewRequest(http.MethodGet, "/approval-ui/"+assetPath, nil)
	recorder := httptest.NewRecorder()

	handler.ServeAssets(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d", recorder.Code)
	}
	if recorder.Body.Len() == 0 {
		t.Fatal("expected bundled asset body to be non-empty")
	}
}

func firstBundledAssetPath(t *testing.T) string {
	t.Helper()

	assets := approvalAssetFS()
	entries, err := fs.ReadDir(assets, "assets")
	if err != nil {
		t.Fatalf("read embedded assets directory: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("expected at least one bundled asset")
	}
	return "assets/" + entries[0].Name()
}

func testOperationView() OperationView {
	return OperationView{
		OperationID:  "op_123",
		Status:       "pending_approval",
		ApprovalURL:  "https://example.test/approve/op_123",
		BaseID:       "app123",
		BaseName:     "Project Tracker",
		Summary:      "Update 1 record in projects",
		CreatedAt:    time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC),
		ExpiresAt:    time.Date(2099, 4, 1, 12, 10, 0, 0, time.UTC),
		LastSyncedAt: time.Date(2026, 4, 1, 11, 59, 0, 0, time.UTC),
		CanApprove:   true,
		CanReject:    true,
		Operations:   []OperationPreview{},
	}
}
