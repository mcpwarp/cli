package update

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// releaseServer builds an httptest server returning tag as the release's
// tag_name, or a 404 when tag is "".
func releaseServer(t *testing.T, tag string) (*httptest.Server, *int) {
	t.Helper()
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if tag == "" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Header.Get("Accept") != "application/vnd.github+json" {
			t.Errorf("Accept header = %q", r.Header.Get("Accept"))
		}
		_ = json.NewEncoder(w).Encode(releaseResponse{TagName: tag})
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

// baseOpts returns Options wired to never skip (terminal, no cache hit)
// against a fresh temp home, for a test to layer its own Endpoint/Now on.
// It also clears Check's two opt-out env vars via t.Setenv, so a CI
// runner exporting CI=true (GitHub Actions does) or a developer with
// MCPWARP_NO_UPDATE_NOTIFIER set in their shell can't make Check skip out
// from under a test that isn't exercising that gate itself — Getenv sees
// t.Setenv's "" the same as unset, and t.Cleanup restores the real value
// after the test.
func baseOpts(t *testing.T, endpoint string) Options {
	t.Helper()
	t.Setenv("CI", "")
	t.Setenv("MCPWARP_NO_UPDATE_NOTIFIER", "")
	return Options{
		HomeDir:    t.TempDir(),
		Endpoint:   endpoint,
		IsTerminal: func() bool { return true },
		Now:        time.Now,
	}
}

func TestCheckNewerTagReturnsNotice(t *testing.T) {
	srv, calls := releaseServer(t, "v0.2.0")
	opts := baseOpts(t, srv.URL)

	n := Check(context.Background(), "v0.1.0", opts)
	if n == nil {
		t.Fatal("expected a notice")
	}
	if n.Current != "0.1.0" || n.Latest != "0.2.0" {
		t.Errorf("got Current=%q Latest=%q", n.Current, n.Latest)
	}
	if *calls != 1 {
		t.Errorf("expected 1 HTTP call, got %d", *calls)
	}
}

func TestCheckSameTagReturnsNil(t *testing.T) {
	srv, _ := releaseServer(t, "v0.1.0")
	opts := baseOpts(t, srv.URL)

	if n := Check(context.Background(), "v0.1.0", opts); n != nil {
		t.Errorf("expected nil, got %+v", n)
	}
}

func TestCheckOlderTagReturnsNil(t *testing.T) {
	srv, _ := releaseServer(t, "v0.1.0")
	opts := baseOpts(t, srv.URL)

	if n := Check(context.Background(), "v0.2.0", opts); n != nil {
		t.Errorf("expected nil, got %+v", n)
	}
}

func TestCheck404ReturnsNil(t *testing.T) {
	srv, _ := releaseServer(t, "")
	opts := baseOpts(t, srv.URL)

	if n := Check(context.Background(), "v0.1.0", opts); n != nil {
		t.Errorf("expected nil, got %+v", n)
	}
}

// TestCheckFailedFetchStillWritesCache confirms a failed fetch (404 here;
// a timeout or network error take the same path in Check) still records
// LastCheck=now — an outage or rate limit must debounce for the rest of
// the cacheWindow, not cost every subsequent invocation its own 200ms hit.
func TestCheckFailedFetchStillWritesCache(t *testing.T) {
	srv, _ := releaseServer(t, "")
	home := t.TempDir()
	now := time.Now()

	opts := baseOpts(t, srv.URL)
	opts.HomeDir = home
	opts.Now = func() time.Time { return now }

	if n := Check(context.Background(), "v0.1.0", opts); n != nil {
		t.Fatalf("expected nil, got %+v", n)
	}

	path, err := cachePath(home)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("expected the cache file to have been written, got %v", err)
	}
	var c cacheData
	if err := json.Unmarshal(b, &c); err != nil {
		t.Fatal(err)
	}
	if !c.LastCheck.Equal(now) {
		t.Errorf("cache LastCheck = %v, want %v", c.LastCheck, now)
	}
	if c.LastTag != "" {
		t.Errorf("cache LastTag = %q, want empty (nothing was known before this failed fetch)", c.LastTag)
	}
}

func TestCheckTimeoutReturnsNil(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		_ = json.NewEncoder(w).Encode(releaseResponse{TagName: "v0.2.0"})
	}))
	t.Cleanup(srv.Close)

	opts := baseOpts(t, srv.URL)
	opts.Timeout = 10 * time.Millisecond

	if n := Check(context.Background(), "v0.1.0", opts); n != nil {
		t.Errorf("expected nil, got %+v", n)
	}
}

func TestCheckDevVersionSkipsHTTPEntirely(t *testing.T) {
	srv, calls := releaseServer(t, "v0.2.0")
	opts := baseOpts(t, srv.URL)

	for _, v := range []string{"dev", "df102b3", "v0.1.0-3-gabc-dirty"} {
		if n := Check(context.Background(), v, opts); n != nil {
			t.Errorf("version %q: expected nil, got %+v", v, n)
		}
	}
	if *calls != 0 {
		t.Errorf("expected no HTTP calls for dev builds, got %d", *calls)
	}
}

func TestCheckOptOutEnvVarsSkip(t *testing.T) {
	srv, calls := releaseServer(t, "v0.2.0")

	t.Run("MCPWARP_NO_UPDATE_NOTIFIER", func(t *testing.T) {
		// baseOpts clears both opt-out vars via t.Setenv; set the one
		// under test afterward so it's the value Check actually sees
		// (t.Setenv's last call for a given var wins for the test body,
		// then both unwind in reverse order on cleanup).
		opts := baseOpts(t, srv.URL)
		t.Setenv("MCPWARP_NO_UPDATE_NOTIFIER", "1")
		if n := Check(context.Background(), "v0.1.0", opts); n != nil {
			t.Errorf("expected nil, got %+v", n)
		}
	})

	t.Run("CI", func(t *testing.T) {
		opts := baseOpts(t, srv.URL)
		t.Setenv("CI", "1")
		if n := Check(context.Background(), "v0.1.0", opts); n != nil {
			t.Errorf("expected nil, got %+v", n)
		}
	})

	if *calls != 0 {
		t.Errorf("expected no HTTP calls, got %d", *calls)
	}
}

func TestCheckNonTerminalSkips(t *testing.T) {
	srv, calls := releaseServer(t, "v0.2.0")
	opts := baseOpts(t, srv.URL)
	opts.IsTerminal = func() bool { return false }

	if n := Check(context.Background(), "v0.1.0", opts); n != nil {
		t.Errorf("expected nil, got %+v", n)
	}
	if *calls != 0 {
		t.Errorf("expected no HTTP calls, got %d", *calls)
	}
}

func TestCheckCacheFreshSkipsHTTP(t *testing.T) {
	srv, calls := releaseServer(t, "v0.2.0")
	home := t.TempDir()
	now := time.Now()

	path, err := cachePath(home)
	if err != nil {
		t.Fatal(err)
	}
	if err := saveCache(path, cacheData{LastCheck: now.Add(-time.Hour), LastTag: "v0.2.0"}); err != nil {
		t.Fatal(err)
	}

	opts := baseOpts(t, srv.URL)
	opts.HomeDir = home
	opts.Now = func() time.Time { return now }

	n := Check(context.Background(), "v0.1.0", opts)
	if n == nil || n.Latest != "0.2.0" {
		t.Fatalf("expected notice from cached tag, got %+v", n)
	}
	if *calls != 0 {
		t.Errorf("expected no HTTP calls (cache fresh), got %d", *calls)
	}
}

func TestCheckCacheStaleCallsHTTP(t *testing.T) {
	srv, calls := releaseServer(t, "v0.3.0")
	home := t.TempDir()
	now := time.Now()

	path, err := cachePath(home)
	if err != nil {
		t.Fatal(err)
	}
	if err := saveCache(path, cacheData{LastCheck: now.Add(-25 * time.Hour), LastTag: "v0.2.0"}); err != nil {
		t.Fatal(err)
	}

	opts := baseOpts(t, srv.URL)
	opts.HomeDir = home
	opts.Now = func() time.Time { return now }

	n := Check(context.Background(), "v0.1.0", opts)
	if n == nil || n.Latest != "0.3.0" {
		t.Fatalf("expected notice from freshly-fetched tag, got %+v", n)
	}
	if *calls != 1 {
		t.Errorf("expected 1 HTTP call (cache stale), got %d", *calls)
	}

	// The cache file itself should now reflect the fresh fetch.
	b, err := os.ReadFile(filepath.Join(home, ".mcpwarp", "update-check.json"))
	if err != nil {
		t.Fatal(err)
	}
	var c cacheData
	if err := json.Unmarshal(b, &c); err != nil {
		t.Fatal(err)
	}
	if c.LastTag != "v0.3.0" {
		t.Errorf("cache LastTag = %q, want v0.3.0", c.LastTag)
	}
}

func TestNormalizeVersion(t *testing.T) {
	tests := []struct {
		in      string
		wantOK  bool
		wantOut string
	}{
		{"v1.2.3", true, "v1.2.3"},
		{"1.2.3", true, "v1.2.3"},
		{"v1.2.3-rc.1", true, "v1.2.3-rc.1"},
		{"1.2.3-rc.1", true, "v1.2.3-rc.1"},
		{"dev", false, ""},
		{"", false, ""},
		{"df102b3", false, ""},
		{"v0.1.0-3-gabc-dirty", false, ""},
		{"v1.2", false, ""},
		// .goreleaser.yaml's snapshot.version_template — must read as dev,
		// not as a release older than the tag it was built from.
		{"v0.1.1-next-dev", false, ""},
	}
	for _, tt := range tests {
		out, ok := normalizeVersion(tt.in)
		if ok != tt.wantOK || (ok && out != tt.wantOut) {
			t.Errorf("normalizeVersion(%q) = (%q, %v), want (%q, %v)", tt.in, out, ok, tt.wantOut, tt.wantOK)
		}
	}
}
