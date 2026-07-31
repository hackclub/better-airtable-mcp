package oauth

import (
	"net"
	"net/http"
	"strings"

	"github.com/hackclub/better-airtable-mcp/internal/httpx"
)

// PublicRateLimit wraps a handler with per-client-IP token-bucket limiting for
// unauthenticated endpoints.
func PublicRateLimit(limiter *RequestLimiter, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if limiter != nil && !limiter.Allow(clientIPKey(r)) {
			w.Header().Set("Retry-After", "1")
			httpx.WriteError(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// clientIPKey returns the rate-limit key for an unauthenticated request.
// In production the server sits behind the cluster ingress, so the direct
// peer address is the proxy, not the client. When the peer is a
// private/loopback address (the trusted ingress), we use the rightmost
// X-Forwarded-For entry — the one appended by that proxy itself, which a
// client cannot forge. Requests arriving directly from public peers never
// have their spoofable forwarding headers honored.
func clientIPKey(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	peer := net.ParseIP(host)
	if peer != nil && (peer.IsPrivate() || peer.IsLoopback()) {
		if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
			parts := strings.Split(forwarded, ",")
			candidate := strings.TrimSpace(parts[len(parts)-1])
			if net.ParseIP(candidate) != nil {
				return candidate
			}
		}
	}
	return host
}
