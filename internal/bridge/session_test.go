package bridge

import "testing"

func TestSession_Stateless(t *testing.T) {
	s := newSession()
	check := s.check("ping", "", false)
	if check.kind != sessionStateless {
		t.Fatalf("want stateless, got %v", check.kind)
	}
}

func TestSession_InitializeMintsOnce(t *testing.T) {
	s := newSession()
	first := s.check("initialize", "", false)
	if first.kind != sessionOK || !first.minted {
		t.Fatalf("want minted ok, got %+v", first)
	}
	second := s.check("initialize", "", false)
	if second.kind != sessionOK || second.minted {
		t.Fatalf("want idempotent (not re-minted), got %+v", second)
	}
	if second.sessionID != first.sessionID {
		t.Fatalf("session id changed across repeat initialize")
	}
}

func TestSession_MissingHeaderOnceMinted(t *testing.T) {
	s := newSession()
	s.check("initialize", "", false)
	check := s.check("ping", "", false)
	if check.kind != sessionMissing {
		t.Fatalf("want missing, got %v", check.kind)
	}
}

func TestSession_UnknownHeader(t *testing.T) {
	s := newSession()
	init := s.check("initialize", "", false)
	check := s.check("ping", "not-"+init.sessionID, true)
	if check.kind != sessionUnknown {
		t.Fatalf("want unknown, got %v", check.kind)
	}
}

func TestSession_OKWithMatchingHeader(t *testing.T) {
	s := newSession()
	init := s.check("initialize", "", false)
	check := s.check("ping", init.sessionID, true)
	if check.kind != sessionOK || check.sessionID != init.sessionID {
		t.Fatalf("want ok with same id, got %+v", check)
	}
}

func TestSession_Rotate(t *testing.T) {
	s := newSession()
	init := s.check("initialize", "", false)

	if _, ok := s.rotate("wrong", true); ok {
		t.Fatal("rotate with wrong header should fail")
	}
	newID, ok := s.rotate(init.sessionID, true)
	if !ok || newID == init.sessionID {
		t.Fatalf("expected a fresh id, got %q ok=%v", newID, ok)
	}
	// The old id is no longer valid.
	check := s.check("ping", init.sessionID, true)
	if check.kind != sessionUnknown {
		t.Fatalf("expected old id to be unknown post-rotate, got %v", check.kind)
	}
}
