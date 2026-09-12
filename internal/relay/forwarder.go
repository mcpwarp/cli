// Package relay implements the raw-HTTP-over-stream relay from an accepted
// wsmixer.Stream to a service's forward target (DESIGN.md §3, §7): parse
// the request off the stream, resolve the target service by the request's
// Host header via internal/registry, forward it, and stream the response
// back — never throws (a failure is always an HTTP-shaped error response,
// falling back to Stream.Reset only when the stream itself is unusable).
// Ported behaviourally from mcpwarp-cli's src/forward/* and src/http/*.
package relay

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Errors Forwarder.Forward classifies for relay.go's status-code mapping —
// mirrors forward/forwarder.ts's TargetUnreachableError/TargetTimeoutError/
// TargetConnectionResetError.
var (
	// ErrTargetUnreachable: the target's TCP connection could not be
	// established at all (connection refused, DNS failure, ...) -> 502.
	ErrTargetUnreachable = errors.New("relay: local server unreachable")
	// ErrTargetTimeout: the target didn't answer with response headers in
	// time -> 504.
	ErrTargetTimeout = errors.New("relay: local server timed out")
	// ErrTargetConnectionReset: the connection was established, then reset
	// mid-request -> 502.
	ErrTargetConnectionReset = errors.New("relay: connection to local server reset")
)

const (
	headersTimeout = 30 * time.Second
	connectTimeout = 5 * time.Second
)

// Forwarder forwards one parsed request to target and returns the
// response. Pools connections per target origin.
type Forwarder struct {
	mu        sync.Mutex
	transport *http.Transport
}

// NewForwarder builds a Forwarder with a pooled *http.Transport tuned for
// long-lived streaming responses (SSE): no per-response body deadline, only
// a header-arrival timeout and connect timeout, matching undici-forwarder.ts's
// tradeoffs.
func NewForwarder() *Forwarder {
	dialer := &net.Dialer{Timeout: connectTimeout}
	return &Forwarder{
		transport: &http.Transport{
			DialContext:           dialer.DialContext,
			ResponseHeaderTimeout: headersTimeout,
			MaxIdleConnsPerHost:   128,
			IdleConnTimeout:       90 * time.Second,
			// undici (this CLI's Node counterpart) never negotiates
			// compression on the forward leg — a pass-through proxy has no
			// use for it, and it would otherwise silently rewrite the
			// target's own Content-Encoding/Content-Length.
			DisableCompression: true,
		},
	}
}

// Close releases pooled connections.
func (f *Forwarder) Close() {
	f.transport.CloseIdleConnections()
}

// PathOptions carries the registry entry's public URL pathname for this
// service — when the inbound request's path equals this, target's own
// pathname is forwarded instead of the inbound one (DESIGN.md §3's "merging
// the inbound query onto the target's query").
type PathOptions struct {
	PublicPathname string
}

// mergeQueries merges inbound onto base, inbound values winning on a key
// conflict — ports undici-forwarder.ts's mergeQueries.
func mergeQueries(base, inbound string) string {
	if base == "" {
		return inbound
	}
	if inbound == "" {
		return base
	}
	merged, _ := url.ParseQuery(base)
	extra, _ := url.ParseQuery(inbound)
	for k, vs := range extra {
		merged[k] = vs
	}
	return merged.Encode()
}

var hopByHopHeaders = []string{
	"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
	"Te", "Trailer", "Trailers", "Transfer-Encoding", "Upgrade",
}

// stripHopByHop removes RFC 7230 §6.1's hop-by-hop set, plus every token the
// request's own Connection header names as a per-connection header to drop —
// mirrors Node headers.ts's stripHopByHop.
func stripHopByHop(h http.Header) http.Header {
	out := h.Clone()
	for _, line := range h.Values("Connection") {
		for _, token := range strings.Split(line, ",") {
			if token = strings.TrimSpace(token); token != "" {
				out.Del(token)
			}
		}
	}
	for _, name := range hopByHopHeaders {
		out.Del(name)
	}
	return out
}

// Forward sends req to target over a pooled connection, honoring
// PathOptions the way undici-forwarder.ts does: the inbound path is
// forwarded verbatim unless it equals the service's public pathname, in
// which case target's own pathname is used and the queries are merged.
func (f *Forwarder) Forward(ctx context.Context, req *http.Request, target *url.URL, opts *PathOptions) (*http.Response, error) {
	outURL := *target
	isPublicPath := opts != nil && req.URL.Path == opts.PublicPathname
	if isPublicPath {
		outURL.Path = target.Path
		outURL.RawQuery = mergeQueries(target.RawQuery, req.URL.RawQuery)
	} else {
		outURL.Path = req.URL.Path
		outURL.RawQuery = req.URL.RawQuery
	}

	var body = req.Body
	if req.Method == http.MethodGet || req.Method == http.MethodHead {
		body = nil
	}

	outReq, err := http.NewRequestWithContext(ctx, req.Method, outURL.String(), body)
	if err != nil {
		return nil, err
	}
	outReq.Header = stripHopByHop(req.Header)
	outReq.Host = target.Host
	outReq.ContentLength = req.ContentLength

	resp, err := f.transport.RoundTrip(outReq)
	if err != nil {
		return nil, classifyForwardErr(err)
	}
	return resp, nil
}

func classifyForwardErr(err error) error {
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return ErrTargetTimeout
	}
	var dnsErr *net.DNSError
	switch {
	case errors.As(err, &dnsErr), isConnRefused(err), isHostUnreachable(err):
		return ErrTargetUnreachable
	case isConnReset(err), errors.Is(err, net.ErrClosed):
		return ErrTargetConnectionReset
	}
	return err
}
