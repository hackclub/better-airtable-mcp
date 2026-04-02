package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/hackclub/better-airtable-mcp/internal/httpx"
	"github.com/hackclub/better-airtable-mcp/internal/oauth"
)

type Handler struct {
	serverName    string
	serverVersion string
	tools         []Tool
	toolIndex     map[string]Tool
	sessions      *SessionManager
	heartbeat     time.Duration
}

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type toolCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

func NewHandler(serverName, serverVersion string, tools []Tool) *Handler {
	index := make(map[string]Tool, len(tools))
	for _, tool := range tools {
		definition := tool.Definition()
		index[definition.Name] = tool
	}

	return &Handler{
		serverName:    serverName,
		serverVersion: serverVersion,
		tools:         tools,
		toolIndex:     index,
		sessions:      NewSessionManager(),
		heartbeat:     25 * time.Second,
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.handleGet(w, r)
	case http.MethodPost:
		h.handlePost(w, r)
	case http.MethodDelete:
		h.handleDelete(w, r)
	default:
		httpx.MethodNotAllowed(w, http.MethodGet, http.MethodPost, http.MethodDelete)
	}
}

func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request) {
	sessionID, ok := h.touchSession(w, r)
	if !ok {
		return
	}
	ownerID := currentSessionOwnerID(r.Context())
	flusher, ok := w.(http.Flusher)
	if !ok {
		httpx.WriteError(w, http.StatusInternalServerError, "response writer does not support streaming")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set(SessionHeader, sessionID)
	w.WriteHeader(http.StatusOK)

	h.writeSSEEvent(w, "ready", "{}")
	flusher.Flush()

	ticker := time.NewTicker(h.heartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			_, _ = h.sessions.Touch(sessionID, ownerID)
			h.writeSSEEvent(w, "ping", "{}")
			flusher.Flush()
		}
	}
}

func (h *Handler) handlePost(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	var raw json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		h.writeRPCError(w, nil, -32700, "invalid JSON payload")
		return
	}

	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] == '[' {
		h.writeRPCError(w, nil, -32600, "batch requests are not supported")
		return
	}

	var request rpcRequest
	if err := json.Unmarshal(trimmed, &request); err != nil {
		h.writeRPCError(w, nil, -32600, "invalid JSON-RPC request")
		return
	}

	if request.JSONRPC != "2.0" {
		h.writeRPCError(w, request.ID, -32600, "jsonrpc must be \"2.0\"")
		return
	}

	ctx := r.Context()
	if request.Method == "initialize" {
		session, err := h.sessions.Create(currentSessionOwnerID(ctx))
		if err != nil {
			h.writeRPCError(w, request.ID, -32603, "failed to create session")
			return
		}
		w.Header().Set(SessionHeader, session.ID)
		ctx = WithSessionID(ctx, session.ID)
	} else {
		sessionID, ok := h.touchSession(w, r)
		if !ok {
			return
		}
		w.Header().Set(SessionHeader, sessionID)
		ctx = WithSessionID(ctx, sessionID)
	}

	result, rpcErr := h.dispatch(ctx, request)
	if len(request.ID) == 0 {
		w.WriteHeader(http.StatusAccepted)
		return
	}

	response := rpcResponse{
		JSONRPC: "2.0",
		ID:      request.ID,
		Result:  result,
		Error:   rpcErr,
	}
	httpx.WriteJSON(w, http.StatusOK, response)
}

func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request) {
	sessionID := r.Header.Get(SessionHeader)
	if sessionID == "" {
		httpx.WriteError(w, http.StatusBadRequest, "missing mcp session id")
		return
	}
	if !h.sessions.Delete(sessionID, currentSessionOwnerID(r.Context())) {
		httpx.WriteError(w, http.StatusNotFound, "session was not found")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) dispatch(ctx context.Context, request rpcRequest) (any, *rpcError) {
	switch request.Method {
	case "initialize":
		return map[string]any{
			"protocolVersion": "2025-11-25",
			"capabilities": map[string]any{
				"tools": map[string]any{
					"listChanged": false,
				},
			},
			"serverInfo": map[string]string{
				"name":    h.serverName,
				"version": h.serverVersion,
			},
		}, nil
	case "notifications/initialized":
		return nil, nil
	case "tools/list":
		definitions := make([]ToolDefinition, 0, len(h.tools))
		for _, tool := range h.tools {
			definitions = append(definitions, tool.Definition())
		}

		return map[string]any{"tools": definitions}, nil
	case "tools/call":
		var params toolCallParams
		if err := decodeParams(request.Params, &params); err != nil {
			return nil, &rpcError{Code: -32602, Message: err.Error()}
		}
		if params.Name == "" {
			return nil, &rpcError{Code: -32602, Message: "tools/call requires a tool name"}
		}

		tool, ok := h.toolIndex[params.Name]
		if !ok {
			return ErrorResult(fmt.Sprintf("unknown tool %q", params.Name), nil), nil
		}

		result, err := tool.Call(ctx, params.Arguments)
		if err != nil {
			return ErrorResult(err.Error(), nil), nil
		}
		return result, nil
	default:
		return nil, &rpcError{Code: -32601, Message: "method not found"}
	}
}

func (h *Handler) writeRPCError(w http.ResponseWriter, id json.RawMessage, code int, message string) {
	httpx.WriteJSON(w, http.StatusOK, rpcResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error: &rpcError{
			Code:    code,
			Message: message,
		},
	})
}

func decodeParams(raw json.RawMessage, dst any) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = []byte("{}")
	}

	if err := json.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("invalid params: %w", err)
	}

	return nil
}

func (h *Handler) touchSession(w http.ResponseWriter, r *http.Request) (string, bool) {
	sessionID := r.Header.Get(SessionHeader)
	if sessionID == "" {
		httpx.WriteError(w, http.StatusBadRequest, "missing mcp session id")
		return "", false
	}
	if _, ok := h.sessions.Touch(sessionID, currentSessionOwnerID(r.Context())); !ok {
		httpx.WriteError(w, http.StatusNotFound, "session was not found")
		return "", false
	}
	return sessionID, true
}

func currentSessionOwnerID(ctx context.Context) string {
	if userID, ok := oauth.UserIDFromContext(ctx); ok {
		return userID
	}
	return ""
}

func (h *Handler) writeSSEEvent(w http.ResponseWriter, eventName, data string) {
	_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", eventName, data)
}

func (h *Handler) RunSessionExpiryLoop(ctx context.Context, interval time.Duration) {
	h.sessions.RunExpiryLoop(ctx, interval)
}
