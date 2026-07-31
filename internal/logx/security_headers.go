package logx

import "net/http"

// SecurityHeaders sets response-hardening headers on every response.
//
// Scripts stay locked to same-origin bundles via default-src 'self'.
// style-src allows inline styles because the landing page renders a small
// inline <style> block; inline *scripts* remain blocked, which is the
// XSS-relevant protection for the credential-bearing approval UI.
func SecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := w.Header()
		header.Set("X-Content-Type-Options", "nosniff")
		header.Set("X-Frame-Options", "DENY")
		header.Set("Referrer-Policy", "no-referrer")
		header.Set("Content-Security-Policy",
			"default-src 'self'; style-src 'self' 'unsafe-inline'; frame-ancestors 'none'; base-uri 'none'; object-src 'none'")
		next.ServeHTTP(w, r)
	})
}
