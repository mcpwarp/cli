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
	if rows[1].URL != "https://z.example/mcp" {
		t.Fatalf("expected zeta's URL to stay bare, got %q", rows[1].URL)
	}
	if !rows[1].Disabled {
		t.Fatalf("expected zeta's row to be Disabled: %+v", rows[1])
	}
	if rows[0].Disabled {
		t.Fatalf("expected alpha's row not to be Disabled: %+v", rows[0])
	}
}

// TestApplyRegisterErrorsUntilNextSuccess: a name rejected on its first
// register has no entry, yet must still appear in Rows (with its kind and
// the error code) so the dashboard can show it as rejected; a later
// successful "registered" for that name clears the rejection. A name that
// had succeeded earlier and is then rejected keeps its entry, flagged.
func TestApplyRegisterErrorsUntilNextSuccess(t *testing.T) {
	r := New()
	kinds := map[string]string{"deepwiki": "http", "fs": "stdio"}
	r.ApplyRegistered([]appproto.RegisteredService{
		{Name: "fs", ID: "id-fs", URL: "https://fs.example/mcp"},
	}, kinds)
	r.ApplyRegisterErrors([]appproto.ServiceError{
		{Name: "deepwiki", Code: "QUOTA_EXCEEDED", Message: "quota"},
		{Name: "fs", Code: "CONFLICT", Message: "conflict"},
	}, kinds)

	rows := r.Rows()
	if len(rows) != 2 {
		t.Fatalf("expected the rejected-only name alongside the entry, got %+v", rows)
	}
	if got := rows[0]; got.Name != "deepwiki" || got.Kind != "http" || got.URL != "" || got.Rejected != "QUOTA_EXCEEDED" {
		t.Fatalf("deepwiki row = %+v, want http, no URL, Rejected QUOTA_EXCEEDED", got)
	}
	if got := rows[1]; got.Name != "fs" || got.URL != "https://fs.example/mcp" || got.Rejected != "CONFLICT" {
		t.Fatalf("fs row = %+v, want its old URL kept and Rejected CONFLICT", got)
	}
	if _, ok := r.Get("deepwiki"); ok {
		t.Fatalf("a rejected-only name must not become a routable entry")
	}

	r.ApplyRegistered([]appproto.RegisteredService{
		{Name: "deepwiki", ID: "id-dw", URL: "https://dw.example/mcp"},
	}, kinds)
	rows = r.Rows()
	if got := rows[0]; got.Name != "deepwiki" || got.Rejected != "" || got.URL != "https://dw.example/mcp" {
		t.Fatalf("deepwiki row after a successful register = %+v, want registered, not rejected", got)
	}
	if got := rows[1]; got.Rejected != "CONFLICT" {
		t.Fatalf("fs's rejection must stand until fs itself registers, got %+v", got)
	}
}

func rowNamed(t *testing.T, r *Registry, name string) Row {
	t.Helper()
	for _, row := range r.Rows() {
		if row.Name == name {
			return row
		}
	}
	t.Fatalf("no row for %q in %+v", name, r.Rows())
	return Row{}
}

// TestLocalDisableOfRejectedNeverRegistered: `d` on a name rejected on its
// first register (no entry) must read disabled and drop the rejection —
// otherwise it stays "rejected" forever, since the `d` also keeps it out of
// every later register batch.
func TestLocalDisableOfRejectedNeverRegistered(t *testing.T) {
	r := New()
	r.ApplyRegisterErrors([]appproto.ServiceError{{Name: "deepwiki", Code: "QUOTA_EXCEEDED"}}, map[string]string{"deepwiki": "http"})
	r.MarkLocallyDisabled("deepwiki", "http")

	got := rowNamed(t, r, "deepwiki")
	if !got.LocallyDisabled || !got.Disabled || got.Rejected != "" || got.Kind != "http" {
		t.Fatalf("deepwiki after d = %+v, want locally disabled, no rejection", got)
	}
}

// TestLocalDisableOfRejectedAfterEarlierSuccess: a name that registered,
// was later rejected on a re-register, then `d`: disabled, not rejected.
func TestLocalDisableOfRejectedAfterEarlierSuccess(t *testing.T) {
	r := New()
	kinds := map[string]string{"deepwiki": "http"}
	r.ApplyRegistered([]appproto.RegisteredService{{Name: "deepwiki", ID: "id-dw", URL: "https://dw.example/mcp"}}, kinds)
	r.ApplyRegisterErrors([]appproto.ServiceError{{Name: "deepwiki", Code: "CONFLICT"}}, kinds)
	// UnregisterService's order: MarkLocallyDisabled, then ApplyDisable.
	r.MarkLocallyDisabled("deepwiki", "http")
	r.ApplyDisable("id-dw", "unregistered locally")

	got := rowNamed(t, r, "deepwiki")
	if !got.LocallyDisabled || !got.Disabled || got.Rejected != "" {
		t.Fatalf("deepwiki after d = %+v, want locally disabled, no rejection", got)
	}
}

// TestLocalDisableBeatsInFlightRegistered: `d` while the first register is
// still unanswered; the reply then lands. The name must stay disabled for
// routing (IsDisabled) and display, not flip to active, and it isn't a
// resurrection. A rejection in such a reply is dropped the same way.
func TestLocalDisableBeatsInFlightRegistered(t *testing.T) {
	r := New()
	kinds := map[string]string{"notes": "http", "deepwiki": "http"}
	r.MarkLocallyDisabled("notes", "http")
	r.MarkLocallyDisabled("deepwiki", "http")
	if got := rowNamed(t, r, "notes"); !got.Disabled || got.URL != "" {
		t.Fatalf("pending notes after d = %+v, want disabled with no URL", got)
	}

	res := r.ApplyRegistered([]appproto.RegisteredService{{Name: "notes", ID: "id-n", URL: "https://n.example/mcp"}}, kinds)
	r.ApplyRegisterErrors([]appproto.ServiceError{{Name: "deepwiki", Code: "QUOTA_EXCEEDED"}}, kinds)
	if len(res.Resurrected) != 0 {
		t.Fatalf("a reply for a locally disabled name must not resurrect it: %+v", res.Resurrected)
	}
	if e, _ := r.Get("notes"); e.Status != StatusDisabled || !r.IsDisabled("id-n") {
		t.Fatalf("notes entry = %+v (IsDisabled=%v), want disabled for routing", e, r.IsDisabled("id-n"))
	}
	if got := rowNamed(t, r, "notes"); !got.LocallyDisabled || !got.Disabled {
		t.Fatalf("notes row = %+v, want disabled", got)
	}
	if got := rowNamed(t, r, "deepwiki"); got.Rejected != "" || !got.Disabled {
		t.Fatalf("deepwiki row = %+v, want disabled with the late rejection dropped", got)
	}
}

// TestLocalDisableEnableRoundTrip: d → e on a registered name reads
// disabled, then re-registering (pending) until the reply, then active —
// and that reply is the usual resurrection of a known id.
func TestLocalDisableEnableRoundTrip(t *testing.T) {
	r := New()
	kinds := map[string]string{"notes": "http"}
	svc := []appproto.RegisteredService{{Name: "notes", ID: "id-n", URL: "https://n.example/mcp"}}
	r.ApplyRegistered(svc, kinds)

	r.MarkLocallyDisabled("notes", "http")
	r.ApplyDisable("id-n", "unregistered locally")
	if got := rowNamed(t, r, "notes"); !got.LocallyDisabled || !got.Disabled {
		t.Fatalf("after d: %+v, want disabled", got)
	}

	r.MarkReregistering("notes", "http")
	if got := rowNamed(t, r, "notes"); got.LocallyDisabled || !got.Reregistering {
		t.Fatalf("after e: %+v, want re-registering and no longer locally disabled", got)
	}

	res := r.ApplyRegistered(svc, kinds)
	if len(res.Resurrected) != 1 || r.IsDisabled("id-n") {
		t.Fatalf("reply after e: resurrected=%+v IsDisabled=%v, want one resurrection and routable", res.Resurrected, r.IsDisabled("id-n"))
	}
	if got := rowNamed(t, r, "notes"); got.Disabled || got.Reregistering || got.LocallyDisabled {
		t.Fatalf("after the reply: %+v, want plain active", got)
	}
}
