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

// sessionCheckKind is the result shape of session.check — ported from
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
}

// session is minimal per-bridge session state: one child, one session.
// Ported from session.ts.
type session struct {
	mu        sync.Mutex
	currentID string
	hasID     bool
	newID     func() string
	// provisional: currentID was minted ahead of the child's answer to an
	// initialize whose SSE head had to go out first (see provisionalMint),
	// held by `holders` such responses, and is not yet confirmed by a
	// successful initialize.
	provisional bool
	holders     int
}

func newSession() *session {
	return &session{newID: newSessionID}
}

// initialize confirms the session for a successful initialize, minting it if
// there is none yet.
func (s *session) initialize() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasID {
		s.currentID = s.newID()
		s.hasID = true
	}
	s.provisional = false
	return s.currentID
}

func (s *session) check(headerSessionID string, hasHeader bool) sessionCheckResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !hasHeader {
		if s.hasID {
			return sessionCheckResult{kind: sessionMissing}
		}
		return sessionCheckResult{kind: sessionStateless}
	}
	if s.hasID && headerSessionID == s.currentID {
		return sessionCheckResult{kind: sessionOK, sessionID: s.currentID}
	}
	return sessionCheckResult{kind: sessionUnknown}
}

// provisionalMint hands out a session id for an initialize response whose
// head cannot wait for the child any longer. tracked reports whether the
// caller now holds a claim that revoke must release if the initialize turns
// out to be rejected; it is false when a confirmed session already existed.
func (s *session) provisionalMint() (id string, tracked bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasID {
		s.currentID = s.newID()
		s.hasID = true
		s.provisional = true
	}
	if s.provisional {
		s.holders++
		return s.currentID, true
	}
	return s.currentID, false
}

// revoke releases one provisionalMint claim; the session disappears once no
// claim is left and no successful initialize has confirmed it.
func (s *session) revoke() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.holders > 0 {
		s.holders--
	}
	if s.provisional && s.holders == 0 {
		s.hasID = false
		s.provisional = false
		s.currentID = ""
	}
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
