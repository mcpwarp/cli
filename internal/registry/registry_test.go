package registry

import (
	"testing"

	"github.com/mcpwarp/cli/internal/appproto"
)

func TestApplyRegisteredCreatesEntries(t *testing.T) {
	r := New()
	res := r.ApplyRegistered([]appproto.RegisteredService{
		{Name: "a", ID: "id1", URL: "https://a.tunnel.example/mcp", Created: true},
	}, map[string]string{"a": "stdio"})

	if len(res.Created) != 1 || res.Created[0] != "a" {
		t.Fatalf("expected a created, got %+v", res)
	}
	entry, ok := r.Get("a")
	if !ok || entry.ID != "id1" || entry.Kind != "stdio" || entry.Status != StatusActive {
		t.Fatalf("unexpected entry: %+v", entry)
	}
}

func TestApplyRegisteredIDChanged(t *testing.T) {
	r := New()
	r.ApplyRegistered([]appproto.RegisteredService{{Name: "a", ID: "id1", URL: "https://a.example/mcp"}}, nil)
	res := r.ApplyRegistered([]appproto.RegisteredService{{Name: "a", ID: "id2", URL: "https://a.example/mcp"}}, nil)
	if len(res.IDChanged) != 1 || res.IDChanged[0] != "a" {
		t.Fatalf("expected id-changed for a, got %+v", res)
	}
}

func TestApplyDisableBeforeName(t *testing.T) {
	r := New()
	name, known := r.ApplyDisable("id1", "quota")
	if known || name != "" {
		t.Fatalf("disable for an unknown id should return unknown, got %q known=%v", name, known)
	}
	if !r.IsDisabled("id1") {
		t.Fatalf("id1 should be recorded as disabled even before a name is known")
	}

	res := r.ApplyRegistered([]appproto.RegisteredService{{Name: "a", ID: "id1", URL: "https://a.example/mcp"}}, nil)
	if len(res.Resurrected) != 0 {
		t.Fatalf("a brand-new id honours disabledIds, it isn't a resurrection: %+v", res)
	}
	entry, _ := r.Get("a")
	if entry.Status != StatusDisabled {
		t.Fatalf("expected a disabled (disable arrived first), got %v", entry.Status)
	}

	resolved := r.ResolvePendingDisables()
	if len(resolved) != 1 || resolved[0].Name != "a" || resolved[0].Reason != "quota" {
		t.Fatalf("expected the deferred disable to resolve to a, got %+v", resolved)
	}
	// Resolving is one-shot.
	if again := r.ResolvePendingDisables(); len(again) != 0 {
		t.Fatalf("expected no further pending disables, got %+v", again)
	}
}

func TestApplyDisableKnownEntry(t *testing.T) {
	r := New()
	r.ApplyRegistered([]appproto.RegisteredService{{Name: "a", ID: "id1", URL: "https://a.example/mcp"}}, nil)
	name, known := r.ApplyDisable("id1", "quota")
	if !known || name != "a" {
		t.Fatalf("expected known disable of a, got name=%q known=%v", name, known)
	}
	entry, _ := r.Get("a")
	if entry.Status != StatusDisabled {
		t.Fatalf("expected disabled, got %v", entry.Status)
	}
}

func TestApplyEnableByName(t *testing.T) {
	r := New()
	r.ApplyRegistered([]appproto.RegisteredService{{Name: "a", ID: "id1", URL: "https://a.example/mcp"}}, nil)
	r.ApplyDisable("id1", "quota")

	hadEntry := r.ApplyEnable("a", "id1")
	if !hadEntry {
		t.Fatalf("expected a known entry")
	}
	entry, _ := r.Get("a")
	if entry.Status != StatusActive {
		t.Fatalf("expected active after enable, got %v", entry.Status)
	}
	if r.IsDisabled("id1") {
		t.Fatalf("id1 should no longer be disabled")
	}
}

func TestApplyEnableConnectWhileDisabled(t *testing.T) {
	// A service that bounced SERVER_DISABLED at connect never got an id or
	// an entry — enable by name must not require one.
	r := New()
	hadEntry := r.ApplyEnable("a", "id1")
	if hadEntry {
		t.Fatalf("expected no entry for a")
	}
}

func TestApplyRegisteredResurrectsLocallyDisabledOnReconnect(t *testing.T) {
	r := New()
	r.ApplyRegistered([]appproto.RegisteredService{{Name: "a", ID: "id1", URL: "https://a.example/mcp"}}, nil)
	r.ApplyDisable("id1", "quota")

	// A reconnect's registered batch reports the SAME id active again —
	// authoritative, resurrects the locally-disabled entry.
	res := r.ApplyRegistered([]appproto.RegisteredService{{Name: "a", ID: "id1", URL: "https://a.example/mcp"}}, nil)
	if len(res.Resurrected) != 1 || res.Resurrected[0].Name != "a" {
		t.Fatalf("expected a resurrected, got %+v", res)
	}
	entry, _ := r.Get("a")
	if entry.Status != StatusActive {
		t.Fatalf("expected active after resurrection, got %v", entry.Status)
	}
	if r.IsDisabled("id1") {
		t.Fatalf("id1 should no longer be tracked as disabled")
	}
}

func TestResolveByHost(t *testing.T) {
	r := New()
	r.ApplyRegistered([]appproto.RegisteredService{{Name: "a", ID: "id1", URL: "https://Foo.Example.com:443/mcp"}}, nil)

	name, entry, ok := r.ResolveByHost("foo.example.com:8080")
	if !ok || name != "a" || entry.ID != "id1" {
		t.Fatalf("expected to resolve a via canonicalized host, got name=%q ok=%v", name, ok)
	}

	if _, _, ok := r.ResolveByHost("unknown.example.com"); ok {
		t.Fatalf("expected no match for an unregistered host")
	}
}

func TestResolveByHostTrailingDotAndIPv6(t *testing.T) {
	r := New()
	r.ApplyRegistered([]appproto.RegisteredService{
		{Name: "a", ID: "id1", URL: "https://example.com/mcp"},
		{Name: "b", ID: "id2", URL: "http://[::1]:9000/mcp"},
	}, nil)

	if _, _, ok := r.ResolveByHost("example.com."); !ok {
		t.Fatalf("expected trailing-dot host to resolve")
	}
	if name, _, ok := r.ResolveByHost("[::1]:9000"); !ok || name != "b" {
		t.Fatalf("expected bracketed IPv6 host to resolve to b, got name=%q ok=%v", name, ok)
	}
}

// TestResolveByHostDuplicateHostInsertionOrder covers ResolveByHost's own
// doc comment: when two names' public URLs canonicalize to the same
// hostname, order's linear scan must pick the one registered first, not
// whichever Go's randomized map iteration happens to land on.
func TestResolveByHostDuplicateHostInsertionOrder(t *testing.T) {
	r := New()
	r.ApplyRegistered([]appproto.RegisteredService{
		{Name: "first", ID: "id1", URL: "https://shared.example/mcp"},
	}, nil)
	r.ApplyRegistered([]appproto.RegisteredService{
		{Name: "second", ID: "id2", URL: "https://shared.example/mcp"},
	}, nil)

	name, entry, ok := r.ResolveByHost("shared.example")
	if !ok {
		t.Fatalf("expected shared.example to resolve")
	}
	if name != "first" || entry.ID != "id1" {
		t.Fatalf("expected the first-registered entry (first/id1) to win, got name=%q id=%q", name, entry.ID)
	}
}

func TestByID(t *testing.T) {
	r := New()
	r.ApplyRegistered([]appproto.RegisteredService{{Name: "a", ID: "id1", URL: "https://a.example/mcp"}}, nil)
	name, entry, ok := r.ByID("id1")
	if !ok || name != "a" || entry.ID != "id1" {
		t.Fatalf("unexpected result: name=%q entry=%+v ok=%v", name, entry, ok)
	}
	if _, _, ok := r.ByID("missing"); ok {
		t.Fatalf("expected no match")
	}
}

func TestRowsSortedByName(t *testing.T) {
	r := New()
	r.ApplyRegistered([]appproto.RegisteredService{
		{Name: "zeta", ID: "id2", URL: "https://z.example/mcp"},
		{Name: "alpha", ID: "id1", URL: "https://a.example/mcp"},
	}, map[string]string{"zeta": "http", "alpha": "stdio"})
	r.ApplyDisable("id2", "reason")

	rows := r.Rows()
	if len(rows) != 2 || rows[0].Name != "alpha" || rows[1].Name != "zeta" {
		t.Fatalf("unexpected row order: %+v", rows)
	}
	if rows[1].URL != "https://z.example/mcp (disabled)" {
		t.Fatalf("expected disabled marker on zeta's row, got %q", rows[1].URL)
	}
	if rows[0].Disabled {
		t.Fatalf("expected alpha's row not to be Disabled: %+v", rows[0])
	}
	if !rows[1].Disabled {
		t.Fatalf("expected zeta's row to be Disabled: %+v", rows[1])
	}
}
