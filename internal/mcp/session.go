package mcp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

const SessionHeader = "Mcp-Session-Id"

type sessionContextKey string

const sessionIDContextKey sessionContextKey = "mcp_session_id"

type Session struct {
	ID         string
	OwnerID    string
	CreatedAt  time.Time
	LastSeenAt time.Time
}

type SessionManager struct {
	now     func() time.Time
	idleTTL time.Duration

	mu       sync.Mutex
	sessions map[string]Session
}

func NewSessionManager() *SessionManager {
	return &SessionManager{
		now:      time.Now,
		idleTTL:  30 * time.Minute,
		sessions: make(map[string]Session),
	}
}

func (m *SessionManager) Create(ownerID string) (Session, error) {
	id, err := newSessionID()
	if err != nil {
		return Session{}, err
	}

	session := Session{
		ID:         id,
		OwnerID:    ownerID,
		CreatedAt:  m.now().UTC(),
		LastSeenAt: m.now().UTC(),
	}

	m.mu.Lock()
	m.pruneExpiredLocked(m.now().UTC())
	m.sessions[session.ID] = session
	m.mu.Unlock()
	return session, nil
}

func (m *SessionManager) Touch(id, ownerID string) (Session, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.pruneExpiredLocked(m.now().UTC())
	session, ok := m.sessions[id]
	if !ok {
		return Session{}, false
	}
	if session.OwnerID != "" && ownerID != "" && session.OwnerID != ownerID {
		return Session{}, false
	}

	session.LastSeenAt = m.now().UTC()
	m.sessions[id] = session
	return session, true
}

func (m *SessionManager) Delete(id, ownerID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.pruneExpiredLocked(m.now().UTC())
	session, ok := m.sessions[id]
	if !ok {
		return false
	}
	if session.OwnerID != "" && ownerID != "" && session.OwnerID != ownerID {
		return false
	}

	delete(m.sessions, id)
	return true
}

func (m *SessionManager) RunExpiryLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.PruneExpired()
		}
	}
}

func (m *SessionManager) PruneExpired() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pruneExpiredLocked(m.now().UTC())
}

func (m *SessionManager) pruneExpiredLocked(now time.Time) int {
	if m.idleTTL <= 0 {
		return 0
	}

	removed := 0
	for id, session := range m.sessions {
		if !now.Before(session.LastSeenAt.Add(m.idleTTL)) {
			delete(m.sessions, id)
			removed++
		}
	}
	return removed
}

func WithSessionID(ctx context.Context, sessionID string) context.Context {
	return context.WithValue(ctx, sessionIDContextKey, sessionID)
}

func SessionIDFromContext(ctx context.Context) (string, bool) {
	sessionID, ok := ctx.Value(sessionIDContextKey).(string)
	return sessionID, ok
}

func newSessionID() (string, error) {
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return "", err
	}
	return "mcp_sess_" + hex.EncodeToString(random), nil
}
