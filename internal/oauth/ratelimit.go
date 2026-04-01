package oauth

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type RequestLimiter struct {
	now   func() time.Time
	rate  rate.Limit
	burst int

	mu      sync.Mutex
	entries map[string]*limiterEntry
}

type limiterEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

func NewRequestLimiter(requestsPerSecond float64, burst int) *RequestLimiter {
	return &RequestLimiter{
		now:     time.Now,
		rate:    rate.Limit(requestsPerSecond),
		burst:   burst,
		entries: make(map[string]*limiterEntry),
	}
}

func (l *RequestLimiter) Allow(key string) bool {
	if key == "" {
		return true
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	entry, ok := l.entries[key]
	if !ok {
		entry = &limiterEntry{
			limiter:  rate.NewLimiter(l.rate, l.burst),
			lastSeen: now,
		}
		l.entries[key] = entry
	}
	entry.lastSeen = now
	l.pruneLocked(now)
	return entry.limiter.Allow()
}

func (l *RequestLimiter) pruneLocked(now time.Time) {
	const ttl = 10 * time.Minute
	for key, entry := range l.entries {
		if now.Sub(entry.lastSeen) > ttl {
			delete(l.entries, key)
		}
	}
}
