// Package logx provides structured JSON logging with secret redaction.
package logx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
)

type contextKey string

const (
	loggerContextKey      contextKey = "logx_logger"
	requestMetaContextKey contextKey = "logx_request_meta"
)

type RequestMeta struct {
	ID    string
	Route string
}

func NewLogger(output io.Writer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(output, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
}

func WithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerContextKey, logger)
}

func FromContext(ctx context.Context) *slog.Logger {
	if ctx != nil {
		if logger, ok := ctx.Value(loggerContextKey).(*slog.Logger); ok && logger != nil {
			return logger
		}
	}
	return slog.Default()
}

func Event(ctx context.Context, component, event string, attrs ...any) {
	baseAttrs := []any{
		"event", event,
		"component", component,
	}
	FromContext(ctx).Info(event, append(baseAttrs, attrs...)...)
}

// EventSummary logs like Event but appends summary to the message. Log stores
// that index only the message (Orchard's does) then still show why it
// happened. The summary must already be redacted, e.g. built from
// ErrorPreview.
func EventSummary(ctx context.Context, component, event, summary string, attrs ...any) {
	baseAttrs := []any{
		"event", event,
		"component", component,
	}
	message := event
	if summary != "" {
		message = event + " " + summary
	}
	FromContext(ctx).Info(message, append(baseAttrs, attrs...)...)
}

func RequestID(ctx context.Context) string {
	meta := requestMetaFromContext(ctx)
	if meta == nil {
		return ""
	}
	return meta.ID
}

func SetRoute(ctx context.Context, route string) {
	meta := requestMetaFromContext(ctx)
	if meta == nil {
		return
	}
	meta.Route = route
}

func withRequestMeta(ctx context.Context, meta *RequestMeta) context.Context {
	return context.WithValue(ctx, requestMetaContextKey, meta)
}

func requestMetaFromContext(ctx context.Context) *RequestMeta {
	if ctx == nil {
		return nil
	}
	meta, _ := ctx.Value(requestMetaContextKey).(*RequestMeta)
	return meta
}

func newRequestID() string {
	buffer := make([]byte, 12)
	if _, err := rand.Read(buffer); err != nil {
		return "000000000000000000000000"
	}
	return hex.EncodeToString(buffer)
}
