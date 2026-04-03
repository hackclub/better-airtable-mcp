package logx

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"
)

const (
	HTTPSlowRequestThreshold     = 2 * time.Second
	AirtableSlowRequestThreshold = 1 * time.Second
)

func HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		meta := &RequestMeta{ID: newRequestID()}
		logger := slog.Default().With("request_id", meta.ID)
		ctx := withRequestMeta(WithLogger(r.Context(), logger), meta)
		r = r.WithContext(ctx)

		recorder := newResponseRecorder(w)
		startedAt := time.Now()
		next.ServeHTTP(recorder, r)

		duration := time.Since(startedAt)
		route := meta.Route
		if route == "" {
			route = "unmatched"
		}

		Event(ctx, "http", "http.request.completed",
			"route", route,
			"method", r.Method,
			"status", recorder.Status(),
			"duration_ms", duration.Milliseconds(),
			"bytes_written", recorder.BytesWritten(),
			"canceled", errors.Is(r.Context().Err(), context.Canceled),
			"timed_out", errors.Is(r.Context().Err(), context.DeadlineExceeded),
			"slow", duration >= HTTPSlowRequestThreshold,
		)
	})
}

func Route(template string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		SetRoute(r.Context(), template)
		next.ServeHTTP(w, r)
	})
}

type responseRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
	bytes       int
}

func newResponseRecorder(w http.ResponseWriter) *responseRecorder {
	return &responseRecorder{
		ResponseWriter: w,
		status:         http.StatusOK,
	}
}

func (r *responseRecorder) Header() http.Header {
	return r.ResponseWriter.Header()
}

func (r *responseRecorder) WriteHeader(status int) {
	if r.wroteHeader {
		return
	}
	r.status = status
	r.wroteHeader = true
	r.ResponseWriter.WriteHeader(status)
}

func (r *responseRecorder) Write(body []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	written, err := r.ResponseWriter.Write(body)
	r.bytes += written
	return written, err
}

func (r *responseRecorder) ReadFrom(reader io.Reader) (int64, error) {
	if readFrom, ok := r.ResponseWriter.(io.ReaderFrom); ok {
		if !r.wroteHeader {
			r.WriteHeader(http.StatusOK)
		}
		written, err := readFrom.ReadFrom(reader)
		r.bytes += int(written)
		return written, err
	}
	return io.Copy(r, reader)
}

func (r *responseRecorder) Flush() {
	if flusher, ok := r.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (r *responseRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := r.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, http.ErrNotSupported
	}
	return hijacker.Hijack()
}

func (r *responseRecorder) Push(target string, options *http.PushOptions) error {
	pusher, ok := r.ResponseWriter.(http.Pusher)
	if !ok {
		return http.ErrNotSupported
	}
	return pusher.Push(target, options)
}

func (r *responseRecorder) Status() int {
	return r.status
}

func (r *responseRecorder) BytesWritten() int {
	return r.bytes
}
