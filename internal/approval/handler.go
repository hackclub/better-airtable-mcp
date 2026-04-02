package approval

import (
	"context"
	"io/fs"
	"net/http"
	"strings"

	"github.com/hackclub/better-airtable-mcp/internal/httpx"
)

type operationService interface {
	GetOperation(ctx context.Context, operationID string) (OperationView, error)
	Approve(ctx context.Context, operationID string) (OperationView, error)
	Reject(ctx context.Context, operationID string) (OperationView, error)
}

type Handler struct {
	service      operationService
	indexHTML    []byte
	assetHandler http.Handler
}

func NewHandler(service operationService) *Handler {
	assets := approvalAssetFS()
	indexHTML, err := fs.ReadFile(assets, "index.html")
	if err != nil {
		panic("approval index.html is missing from embedded assets: " + err.Error())
	}

	return &Handler{
		service:      service,
		indexHTML:    indexHTML,
		assetHandler: http.StripPrefix("/approval-ui/", http.FileServerFS(assets)),
	}
}

func (h *Handler) ServeApprovalPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpx.MethodNotAllowed(w, http.MethodGet)
		return
	}
	if h.service == nil {
		httpx.WriteError(w, http.StatusNotImplemented, "approval service is not configured")
		return
	}

	operationID := strings.TrimPrefix(r.URL.Path, "/approve/")
	if operationID == "" || operationID == r.URL.Path {
		http.NotFound(w, r)
		return
	}
	if _, err := h.service.GetOperation(r.Context(), operationID); err != nil {
		httpx.WriteError(w, http.StatusNotFound, "operation was not found")
		return
	}

	h.serveEmbeddedApp(w)
}

func (h *Handler) ServeDebugPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpx.MethodNotAllowed(w, http.MethodGet)
		return
	}

	h.serveEmbeddedApp(w)
}

func (h *Handler) serveEmbeddedApp(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(h.indexHTML)
}

func (h *Handler) ServeOperationAPI(w http.ResponseWriter, r *http.Request) {
	if h.service == nil {
		httpx.WriteError(w, http.StatusNotImplemented, "approval service is not configured")
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/operations/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}

	operationID := parts[0]
	switch {
	case len(parts) == 1 && r.Method == http.MethodGet:
		operation, err := h.service.GetOperation(r.Context(), operationID)
		if err != nil {
			httpx.WriteError(w, http.StatusNotFound, "operation was not found")
			return
		}
		httpx.WriteJSON(w, http.StatusOK, operation)
	case len(parts) == 2 && r.Method == http.MethodPost && parts[1] == "approve":
		operation, err := h.service.Approve(r.Context(), operationID)
		if err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		httpx.WriteJSON(w, http.StatusOK, operation)
	case len(parts) == 2 && r.Method == http.MethodPost && parts[1] == "reject":
		operation, err := h.service.Reject(r.Context(), operationID)
		if err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		httpx.WriteJSON(w, http.StatusOK, operation)
	default:
		switch len(parts) {
		case 1:
			httpx.MethodNotAllowed(w, http.MethodGet)
		case 2:
			httpx.MethodNotAllowed(w, http.MethodPost)
		default:
			http.NotFound(w, r)
		}
	}
}

func (h *Handler) ServeAssets(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		httpx.MethodNotAllowed(w, http.MethodGet, http.MethodHead)
		return
	}

	h.assetHandler.ServeHTTP(w, r)
}
