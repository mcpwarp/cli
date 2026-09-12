// Package registry is the in-memory name/id/url table built from the
// tunnel's "registered" app messages (DESIGN.md §3). Ported behaviourally
// from mcpwarp-cli's src/tunnel/registry.ts.
package registry

import (
	"fmt"
	"log/slog"
	"net/url"
	"sort"
	"strings"
	"sync"

	"github.com/mcpwarp/cli/internal/appproto"
)

// Status is whether an entry is currently routable.
type Status string

const (
	StatusActive   Status = "active"
	StatusDisabled Status = "disabled"
)

// Entry is one registered service.
type Entry struct {
	ID     string
	URL    string
	Kind   string
	Status Status
	// hostname is URL's hostname, canonicalized once at registration time
	// so resolveByHost never re-parses URL on every lookup. Empty if URL
	// failed to parse.
	hostname string
}

// canonicalizeHost lowercases, strips ":port", and drops a trailing FQDN
// dot, leaving a bracketed IPv6 literal ("[::1]"/"[::1]:8080") untouched
// past its closing bracket — mirrors registry.ts's canonicalizeHost.
func canonicalizeHost(raw string) string {
	lower := strings.ToLower(strings.TrimSpace(raw))
	if strings.HasPrefix(lower, "[") {
		if end := strings.Index(lower, "]"); end != -1 {
			return lower[:end+1]
		}
		return lower
	}
	host := lower
	if i := strings.Index(host, ":"); i != -1 {
		host = host[:i]
	}
	return strings.TrimSuffix(host, ".")
}

// AppliedResult reports what one ApplyRegistered call did, mirroring
// registry.ts's ApplyRegisteredResult.
type AppliedResult struct {
	// Created are names newly created this call (services[].created==true).
	Created []string
	// IDChanged are names whose id changed from one already known this run.
	IDChanged []string
	// Resurrected are {name,id} pairs that were locally disabled but showed
	// up active in this "registered" batch anyway — the tunnel's own batch
	// is authoritative, so these are un-disabled the same as a live
	// "enable" would. Fires onEnable for each per DESIGN.md §8.
	Resurrected []Resurrection
}

// Resurrection is one entry ApplyRegistered un-disabled because the
// tunnel's batch reported it active again.
type Resurrection struct {
	Name string
	ID   string
}

// Registry is the name -> Entry table, plus disable/enable bookkeeping that
// can race ahead of the name being known. Safe for concurrent use: mutating
// calls (ApplyRegistered/ApplyDisable/ApplyEnable/ResolvePendingDisables)
// come from internal/tunnel's single dispatcher goroutine, but reads
// (ResolveByHost, IsDisabled, ...) come from concurrent per-stream relay
// goroutines, so a plain mutex guards every method.
type Registry struct {
	mu      sync.RWMutex
	entries map[string]*Entry
	// disabledIds tracks every id currently disabled, independent of
	// entries: a "disable" can arrive before this registry has ever heard
	// a name for that id.
	disabledIds map[string]bool
	// pendingDisables holds a disable reason for an id whose name isn't
	// known yet, keyed by id, resolved once ApplyRegistered learns the
	// name (DESIGN.md §8).
	pendingDisables map[string]string
	// order preserves entries' insertion order — Go map iteration order is
	// randomized, which would otherwise make ResolveByHost's linear scan
	// pick a different entry from one call to the next for two names that
	// happen to share a hostname.
	order []string
	// log receives ApplyRegistered's IDChanged warning; defaults to
	// slog.Default() until SetLogger is called.
	log *slog.Logger
}

// New builds an empty Registry.
func New() *Registry {
	return &Registry{
		entries:         make(map[string]*Entry),
		disabledIds:     make(map[string]bool),
		pendingDisables: make(map[string]string),
	}
}

// SetLogger overrides the logger ApplyRegistered warns an IDChanged through.
func (r *Registry) SetLogger(log *slog.Logger) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.log = log
}

func (r *Registry) logger() *slog.Logger {
	if r.log != nil {
		return r.log
	}
	return slog.Default()
}

// ApplyRegistered upserts every service from one "registered" batch.
// kindByName supplies the kind this CLI registered each name with (the
// tunnel's own reply carries no kind); falls back to whatever kind was
// already on record.
func (r *Registry) ApplyRegistered(services []appproto.RegisteredService, kindByName map[string]string) AppliedResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	var result AppliedResult

	for _, svc := range services {
		existing, hadExisting := r.entries[svc.Name]
		if hadExisting && existing.ID != svc.ID {
			result.IDChanged = append(result.IDChanged, svc.Name)
			r.logger().Warn(fmt.Sprintf(
				"service %q was assigned a different id than it had earlier this run (was %s, now %s) — a second server was minted, not a reconnect",
				svc.Name, existing.ID, svc.ID,
			))
		}

		hostname := ""
		if parsed, err := url.Parse(svc.URL); err == nil && parsed.Hostname() != "" {
			h := parsed.Hostname()
			// net/url.Hostname() strips IPv6 brackets ("::1", not
			// "[::1]"), unlike a raw HTTP Host header — re-bracket so it
			// canonicalizes to the same form ResolveByHost compares
			// against.
			if strings.Contains(h, ":") {
				h = "[" + h + "]"
			}
			hostname = canonicalizeHost(h)
		}

		isReconnectOfKnownID := hadExisting && existing.ID == svc.ID
		var status Status
		if isReconnectOfKnownID {
			if r.disabledIds[svc.ID] {
				delete(r.disabledIds, svc.ID)
				result.Resurrected = append(result.Resurrected, Resurrection{Name: svc.Name, ID: svc.ID})
			}
			status = StatusActive
		} else if r.disabledIds[svc.ID] {
			status = StatusDisabled
		} else {
			status = StatusActive
		}

		kind := kindByName[svc.Name]
		if kind == "" {
			if hadExisting {
				kind = existing.Kind
			} else {
				kind = "unknown"
			}
		}

		r.entries[svc.Name] = &Entry{
			ID:       svc.ID,
			URL:      svc.URL,
			Kind:     kind,
			Status:   status,
			hostname: hostname,
		}
		if !hadExisting {
			r.order = append(r.order, svc.Name)
		}

		if svc.Created {
			result.Created = append(result.Created, svc.Name)
		}
	}

	return result
}

// ResolvedDisable is one deferred "disable" whose id this registry has now
// learned the name for.
type ResolvedDisable struct {
	Name   string
	ID     string
	Reason string
}

// ResolvePendingDisables returns (and clears) every deferred disable whose
// id this registry now has a name for — call after ApplyRegistered so a
// "disable" that arrived before the matching "registered" is finally
// resolved.
func (r *Registry) ResolvePendingDisables() []ResolvedDisable {
	r.mu.Lock()
	defer r.mu.Unlock()
	var resolved []ResolvedDisable
	for name, entry := range r.entries {
		reason, ok := r.pendingDisables[entry.ID]
		if !ok {
			continue
		}
		delete(r.pendingDisables, entry.ID)
		resolved = append(resolved, ResolvedDisable{Name: name, ID: entry.ID, Reason: reason})
	}
	return resolved
}

// ApplyDisable marks the entry with this id disabled, remembering id even
// if no entry has it yet (a disable racing ahead of the matching
// "registered"). Returns the entry's name, or "", false if unknown.
func (r *Registry) ApplyDisable(id, reason string) (name string, known bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.disabledIds[id] = true
	for n, entry := range r.entries {
		if entry.ID == id {
			entry.Status = StatusDisabled
			return n, true
		}
	}
	r.pendingDisables[id] = reason
	return "", false
}

// ApplyEnable un-marks name disabled. id is kept only for disabledIds
// bookkeeping (cleared unconditionally), not for finding the entry — a
// connect-while-disabled service never got an id, so it's resolved by name
// alone. Returns whether name had a known entry.
func (r *Registry) ApplyEnable(name, id string) (hadEntry bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.disabledIds, id)
	delete(r.pendingDisables, id)
	entry, ok := r.entries[name]
	if !ok {
		return false
	}
	entry.Status = StatusActive
	return true
}

// ClearPendingDisable cancels a deferred disable for id without touching
// anything else — for a live "enable" that arrives for an id whose matching
// "registered" (and thus whose name) this registry hasn't seen yet, so a
// later ApplyRegistered doesn't fire a now-stale disable for it (Node
// client.ts's item 4).
func (r *Registry) ClearPendingDisable(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.pendingDisables, id)
}

// Get looks up an entry by its configured name.
func (r *Registry) Get(name string) (Entry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.entries[name]
	if !ok {
		return Entry{}, false
	}
	return *e, true
}

// ByID looks up an entry (and its name) by the tunnel-assigned id.
func (r *Registry) ByID(id string) (name string, entry Entry, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for n, e := range r.entries {
		if e.ID == id {
			return n, *e, true
		}
	}
	return "", Entry{}, false
}

// IsDisabled reports whether id is currently disabled, including one whose
// name isn't known yet.
func (r *Registry) IsDisabled(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.disabledIds[id]
}

// ResolveByHost finds the entry (and its config name) whose public URL's
// hostname matches host — DESIGN.md §3's OPEN-carries-no-metadata routing:
// a stream is matched to a service purely by the inbound request's Host
// header against each registered entry's own URL hostname.
func (r *Registry) ResolveByHost(host string) (name string, entry Entry, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	target := canonicalizeHost(host)
	for _, n := range r.order {
		e, ok := r.entries[n]
		if ok && e.hostname != "" && e.hostname == target {
			return n, *e, true
		}
	}
	return "", Entry{}, false
}

// Row is one line of the display table, sorted by name for stable output.
type Row struct {
	Name string
	Kind string
	URL  string
	// Disabled reports whether this entry is currently disabled — URL is
	// always bare; callers that want a "(disabled)" annotation add it
	// themselves from this field.
	Disabled bool
}

// Rows returns every entry as display rows, sorted by name.
func (r *Registry) Rows() []Row {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.entries))
	for n := range r.entries {
		names = append(names, n)
	}
	sort.Strings(names)

	rows := make([]Row, 0, len(names))
	for _, n := range names {
		e := r.entries[n]
		disabled := e.Status == StatusDisabled
		rows = append(rows, Row{Name: n, Kind: e.Kind, URL: e.URL, Disabled: disabled})
	}
	return rows
}
