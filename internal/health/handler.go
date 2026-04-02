package health

import (
	"context"
	"net/http"
	"time"

	"github.com/hackclub/better-airtable-mcp/internal/httpx"
)

// Pinger is satisfied by *db.Store.
type Pinger interface {
	Ping(ctx context.Context) error
}

type Handler struct {
	db Pinger
}

func NewHandler(db Pinger) *Handler {
	return &Handler{db: db}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpx.MethodNotAllowed(w, http.MethodGet)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if err := h.db.Ping(ctx); err != nil {
		httpx.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "unhealthy",
			"error":  "database unreachable",
		})
		return
	}

	httpx.WriteJSON(w, http.StatusOK, map[string]string{
		"status": "healthy",
	})
}
