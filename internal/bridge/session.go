package bridge

import (
	"crypto/rand"
	"fmt"
	"sync"
)

// newSessionID generates an RFC 4122 v4 UUID using only crypto/rand — no
// dependency on google/uuid or similar, per this milestone's
// stdlib-only constraint.
func newSessionID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failing is effectively unrecoverable on any real
		// platform; panicking here matches Node's randomUUID(), which
		// throws rather than returning a degraded id.
		panic(fmt.Sprintf("bridge: crypto/rand failed: %v", err))
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// sessionCheckKind is the result shape of Session.Check — ported from
// session.ts's SessionCheckResult.
type sessionCheckKind int

const (
	sessionStateless sessionCheckKind = iota
	sessionOK
	sessionUnknown
	sessionMissing
)

type sessionCheckResult struct {
	kind      sessionCheckKind
	sessionID string
	minted    bool
}

// session is minimal per-bridge session state: one child, one session.
// Ported from session.ts.
type session struct {
	mu        sync.Mutex
	currentID string
	hasID     bool
	newID     func() string
}

func newSession() *session {
	return &session{newID: newSessionID}
}

func (s *session) check(method string, headerSessionID string, hasHeader bool) sessionCheckResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	if method == "initialize" {
		minted := !s.hasID
		if minted {
			s.currentID = s.newID()
			s.hasID = true
		}
		return sessionCheckResult{kind: sessionOK, sessionID: s.currentID, minted: minted}
	}
	if !hasHeader {
		if s.hasID {
			return sessionCheckResult{kind: sessionMissing}
		}
		return sessionCheckResult{kind: sessionStateless}
	}
	if s.hasID && headerSessionID == s.currentID {
		return sessionCheckResult{kind: sessionOK, sessionID: s.currentID, minted: false}
	}
	return sessionCheckResult{kind: sessionUnknown}
}

// rotate handles DELETE /mcp: mints a fresh id if headerSessionID matches
// the current one, returning (newID, true); otherwise ("", false) — caller
// answers 404.
func (s *session) rotate(headerSessionID string, hasHeader bool) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasID || !hasHeader || headerSessionID != s.currentID {
		return "", false
	}
	s.currentID = s.newID()
	return s.currentID, true
}
