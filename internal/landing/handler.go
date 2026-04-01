package landing

import (
	"bytes"
	"html/template"
	"net/http"
	"os"

	"github.com/yuin/goldmark"
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

	var rendered bytes.Buffer
	if err := goldmark.Convert(content, &rendered); err != nil {
		http.Error(w, "README.md could not be rendered", http.StatusInternalServerError)
		return
	}

	page := template.Must(template.New("landing").Parse(`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Better Airtable MCP</title>
  <style>
    :root {
      color-scheme: light;
      font-family: ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      line-height: 1.5;
      background: #f5f7fb;
      color: #162033;
    }
    body {
      margin: 0;
      padding: 2rem 1rem 4rem;
    }
    main {
      max-width: 900px;
      margin: 0 auto;
      background: #fff;
      border: 1px solid #d6dfef;
      border-radius: 16px;
      box-shadow: 0 16px 40px rgba(22, 32, 51, 0.08);
      padding: 2rem;
    }
    h1, h2, h3 { line-height: 1.2; }
    code, pre {
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      background: #eef3fb;
      border-radius: 8px;
    }
    code { padding: 0.15rem 0.35rem; }
    pre {
      overflow-x: auto;
      padding: 1rem;
    }
    table {
      border-collapse: collapse;
      width: 100%;
      overflow: hidden;
      border-radius: 12px;
    }
    th, td {
      border: 1px solid #d6dfef;
      padding: 0.6rem 0.75rem;
      text-align: left;
      vertical-align: top;
    }
    th { background: #f2f6fd; }
    blockquote {
      margin: 1rem 0;
      padding-left: 1rem;
      border-left: 4px solid #9ab0d6;
      color: #41516f;
    }
    a { color: #0f5bd8; }
    img { max-width: 100%; }
  </style>
</head>
<body>
  <main>{{.}}</main>
</body>
</html>`))

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := page.Execute(w, template.HTML(rendered.String())); err != nil {
		http.Error(w, "README.md could not be rendered", http.StatusInternalServerError)
		return
	}
}
