package landing

import (
	"net/http"
	"os"
)

type Handler struct {
	readmePath string
}

func NewHandler(readmePath string) *Handler {
	return &Handler{readmePath: readmePath}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	content, err := os.ReadFile(h.readmePath)
	if err != nil {
		http.Error(w, "README.md is unavailable", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/markdown; charset=utf-8")
	_, _ = w.Write(content)
}
