package bridge

import "testing"

func TestSession_Stateless(t *testing.T) {
	s := newSession()
	check := s.check("", false)
	if check.kind != sessionStateless {
		t.Fatalf("want stateless, got %v", check.kind)
	}
}

func TestSession_InitializeMintsOnce(t *testing.T) {
	s := newSession()
	first := s.initialize()
	if first == "" {
		t.Fatal("no session id minted")
	}
	if second := s.initialize(); second != first {
		t.Fatalf("session id changed across repeat initialize: %q then %q", first, second)
	}
}

func TestSession_MissingHeaderOnceMinted(t *testing.T) {
	s := newSession()
	s.initialize()
	check := s.check("", false)
	if check.kind != sessionMissing {
		t.Fatalf("want missing, got %v", check.kind)
	}
}

func TestSession_UnknownHeader(t *testing.T) {
	s := newSession()
	sid := s.initialize()
	check := s.check("not-"+sid, true)
	if check.kind != sessionUnknown {
		t.Fatalf("want unknown, got %v", check.kind)
	}
}

func TestSession_OKWithMatchingHeader(t *testing.T) {
	s := newSession()
	sid := s.initialize()
	check := s.check(sid, true)
	if check.kind != sessionOK || check.sessionID != sid {
		t.Fatalf("want ok with same id, got %+v", check)
	}
}

func TestSession_Rotate(t *testing.T) {
	s := newSession()
	sid := s.initialize()

	if _, ok := s.rotate("wrong", true); ok {
		t.Fatal("rotate with wrong header should fail")
	}
	newID, ok := s.rotate(sid, true)
	if !ok || newID == sid {
		t.Fatalf("expected a fresh id, got %q ok=%v", newID, ok)
	}
	// The old id is no longer valid.
	check := s.check(sid, true)
	if check.kind != sessionUnknown {
		t.Fatalf("expected old id to be unknown post-rotate, got %v", check.kind)
	}
}
