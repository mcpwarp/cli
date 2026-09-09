package relay

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestForwardJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/mcp" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	target, _ := url.Parse(srv.URL + "/mcp")
	f := NewForwarder()
	defer f.Close()

	req, _ := http.NewRequest(http.MethodGet, "http://example.com/mcp", nil)
	req.Host = "example.com"
	resp, err := f.Forward(context.Background(), req, target, &PathOptions{PublicPathname: "/mcp"})
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != `{"ok":true}` {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestForwardMergesQueryOnPublicPath(t *testing.T) {
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
	}))
	defer srv.Close()

	target, _ := url.Parse(srv.URL + "/mcp?fixed=1")
	f := NewForwarder()
	defer f.Close()

	req, _ := http.NewRequest(http.MethodGet, "http://example.com/mcp?inbound=2", nil)
	req.Host = "example.com"
	resp, err := f.Forward(context.Background(), req, target, &PathOptions{PublicPathname: "/mcp"})
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	resp.Body.Close()

	q, _ := url.ParseQuery(gotQuery)
	if q.Get("fixed") != "1" || q.Get("inbound") != "2" {
		t.Fatalf("expected merged query, got %q", gotQuery)
	}
}

func TestForwardNonPublicPathPassesThroughVerbatim(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
	}))
	defer srv.Close()

	target, _ := url.Parse(srv.URL + "/mcp")
	f := NewForwarder()
	defer f.Close()

	req, _ := http.NewRequest(http.MethodGet, "http://example.com/other/path", nil)
	req.Host = "example.com"
	resp, err := f.Forward(context.Background(), req, target, &PathOptions{PublicPathname: "/mcp"})
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	resp.Body.Close()
	if gotPath != "/other/path" {
		t.Fatalf("expected inbound path passthrough, got %q", gotPath)
	}
}

func TestForward429PassesThrough(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(429)
	}))
	defer srv.Close()

	target, _ := url.Parse(srv.URL + "/mcp")
	f := NewForwarder()
	defer f.Close()

	req, _ := http.NewRequest(http.MethodGet, "http://example.com/mcp", nil)
	req.Host = "example.com"
	resp, err := f.Forward(context.Background(), req, target, nil)
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 429 {
		t.Fatalf("expected 429 pass-through, got %d", resp.StatusCode)
	}
}

// TestStripHopByHopRemovesConnectionTokensAndTrailers covers headers.ts's
// stripHopByHop: both the fixed RFC 7230 §6.1 set (Trailer/Trailers among
// them) and whatever extra header the request's own Connection value names
// as per-connection must be dropped, while an unrelated header (including
// one merely referenced by a Trailer header's value, which this function
// never inspects) survives untouched.
func TestStripHopByHopRemovesConnectionTokensAndTrailers(t *testing.T) {
	h := http.Header{}
	h.Set("Connection", "X-Custom, Keep-Alive")
	h.Set("X-Custom", "should be stripped: named by Connection")
	h.Set("Trailer", "X-Trailer-Name")
	h.Set("Trailers", "X-Trailer-Name")
	h.Set("X-Trailer-Name", "value") // merely named by Trailer, not itself hop-by-hop
	h.Set("Content-Type", "application/json")

	out := stripHopByHop(h)

	for _, name := range []string{"Connection", "X-Custom", "Trailer", "Trailers", "Keep-Alive"} {
		if out.Get(name) != "" {
			t.Fatalf("expected %q removed, got %q", name, out.Get(name))
		}
	}
	if out.Get("X-Trailer-Name") != "value" {
		t.Fatalf("expected X-Trailer-Name preserved (only named by Trailer, not itself hop-by-hop), got %q", out.Get("X-Trailer-Name"))
	}
	if out.Get("Content-Type") != "application/json" {
		t.Fatalf("expected Content-Type preserved, got %q", out.Get("Content-Type"))
	}
}

func TestForwardUnreachable(t *testing.T) {
	target, _ := url.Parse("http://127.0.0.1:1") // nobody listens on port 1
	f := NewForwarder()
	defer f.Close()

	req, _ := http.NewRequest(http.MethodGet, "http://example.com/mcp", nil)
	req.Host = "example.com"
	_, err := f.Forward(context.Background(), req, target, nil)
	if !errors.Is(err, ErrTargetUnreachable) {
		t.Fatalf("expected ErrTargetUnreachable, got %v", err)
	}
}
