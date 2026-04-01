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
	now func() time.Time

	mu       sync.Mutex
	sessions map[string]Session
}

func NewSessionManager() *SessionManager {
	return &SessionManager{
		now:      time.Now,
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
	m.sessions[session.ID] = session
	m.mu.Unlock()
	return session, nil
}

func (m *SessionManager) Touch(id, ownerID string) (Session, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

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
