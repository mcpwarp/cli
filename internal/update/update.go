// Package update is the gh-CLI-style update notice (DESIGN.md §2): at most
// once per 24h, check GitHub's latest release against the running version
// and hand back a Notice to print — never self-replaces the binary.
package update

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/mcpwarp/cli/internal/output"
	"golang.org/x/mod/semver"
)

// defaultEndpoint is GitHub's "latest release" API, matching the repo this
// binary is released from (DESIGN.md §11).
const defaultEndpoint = "https://api.github.com/repos/mcpwarp/cli/releases/latest"

// cacheWindow is how often Check actually hits the network — a re-run
// within this window reuses the cached tag instead (gh CLI's own policy).
const cacheWindow = 24 * time.Hour

// Notice is a newer release than the one running, ready to print.
// Current/Latest are printed without their leading "v".
type Notice struct {
	Current string
	Latest  string

	// Hint is the upgrade command/message for this install (installHint's
	// result), computed once when the Notice is built rather than on every
	// Lines() call — installHint does an os.Executable/EvalSymlinks round
	// trip that has no reason to repeat. Left zero-value ("") by a caller
	// that builds a Notice directly (e.g. a test) rather than via Check;
	// Lines() prints whatever it holds either way.
	Hint string
}

// Lines is the exact two-line shape Check's caller prints/logs (DESIGN.md
// §2): the notice itself, then how to upgrade for this install method.
func (n *Notice) Lines() []string {
	return []string{
		fmt.Sprintf("A new release of mcpwarp is available: %s → %s", n.Current, n.Latest),
		fmt.Sprintf("To upgrade, run: %s", n.Hint),
	}
}

// Print writes Lines to output.Stderr, plain (no ✓/!/✗ glyph) — this is a
// notice, not a status line.
func (n *Notice) Print() {
	for _, line := range n.Lines() {
		output.Stderrln(line)
	}
}

// Options configures Check. Every field is optional; zero values resolve
// to the real behaviour (see withDefaults) — tests override only the seams
// they need (an httptest endpoint, a fixed clock, a fake HTTP client).
type Options struct {
	// HomeDir is the user's home directory, the way tui.OpenLogFile takes
	// it — a parameter rather than looked up here, so a caller already
	// holding it (the cli Context) doesn't force a second lookup, and
	// tests can point it at a temp dir. Empty resolves via
	// os.UserHomeDir(), same fallback auth.CredentialsPathFor uses.
	HomeDir string

	// HTTPClient makes the GitHub request; defaults to http.DefaultClient.
	HTTPClient *http.Client

	// Endpoint overrides defaultEndpoint — an httptest server URL in tests.
	Endpoint string

	// Timeout bounds the GitHub request; defaults to 2s per DESIGN.md §2.
	Timeout time.Duration

	// Now is the clock Check compares the cache's last-check time against;
	// defaults to time.Now.
	Now func() time.Time

	// Log receives debug-only diagnostics on any failure (network error,
	// bad cache file, ...) — Check itself never returns an error, per
	// DESIGN.md §2 ("on any error: silently do nothing, debug-log only").
	// Defaults to slog.Default().
	Log *slog.Logger

	// IsTerminal reports whether stderr is a terminal; Check skips
	// entirely when it isn't (a notice nobody can see). Defaults to
	// checking the real os.Stderr via output.IsTerminal.
	IsTerminal func() bool
}

func (o Options) withDefaults() Options {
	if o.HTTPClient == nil {
		o.HTTPClient = http.DefaultClient
	}
	if o.Endpoint == "" {
		o.Endpoint = defaultEndpoint
	}
	if o.Timeout <= 0 {
		o.Timeout = 2 * time.Second
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Log == nil {
		o.Log = slog.Default()
	}
	if o.IsTerminal == nil {
		o.IsTerminal = func() bool { return output.IsTerminal(os.Stderr) }
	}
	return o
}

// cacheData is the on-disk shape at ~/.mcpwarp/update-check.json.
type cacheData struct {
	LastCheck time.Time `json:"last_check"`
	LastTag   string    `json:"last_tag"`
}

// Check reports whether a newer mcpwarp release exists, at most hitting
// the network once per 24h (cached at ~/.mcpwarp/update-check.json). It
// returns nil whenever there's nothing to say — no newer release, a dev/
// non-semver build, an opt-out env var set, stderr isn't a terminal, or
// any error along the way (logged at debug level only, per DESIGN.md §2).
func Check(ctx context.Context, current string, opts Options) *Notice {
	opts = opts.withDefaults()

	if os.Getenv("MCPWARP_NO_UPDATE_NOTIFIER") != "" {
		return nil
	}
	if os.Getenv("CI") != "" {
		return nil
	}
	if !opts.IsTerminal() {
		return nil
	}

	normCurrent, ok := normalizeVersion(current)
	if !ok {
		// dev build (git-describe string, or the literal "dev") — nothing
		// meaningful to compare against.
		return nil
	}

	path, err := cachePath(opts.HomeDir)
	if err != nil {
		opts.Log.Debug("update check: resolving cache path", "err", err)
		return nil
	}

	now := opts.Now()
	cache := loadCache(path)

	// !cache.LastCheck.After(now) guards against clock skew (a LastCheck
	// somehow in the future, e.g. the system clock was set back) treating
	// a cache entry as fresh for up to cacheWindow past that bogus time —
	// without it, now.Sub would go negative and satisfy "< cacheWindow" on
	// its own.
	var latestTag string
	if cache != nil && now.Sub(cache.LastCheck) < cacheWindow && !cache.LastCheck.After(now) {
		latestTag = cache.LastTag
	} else {
		tag, err := fetchLatestTag(ctx, current, opts)
		if err != nil {
			opts.Log.Debug("update check: fetching latest release", "err", err)
			// Still record the attempt: an outage or rate limit must not
			// turn into a network call on every single invocation for the
			// rest of the day. Keep whatever tag was last known good, if
			// any, so a later run still has something to compare once the
			// cache goes stale again — this run itself reports nothing.
			prevTag := ""
			if cache != nil {
				prevTag = cache.LastTag
			}
			if err := saveCache(path, cacheData{LastCheck: now, LastTag: prevTag}); err != nil {
				opts.Log.Debug("update check: writing cache after failed fetch", "err", err)
			}
			return nil
		}
		latestTag = tag
		if err := saveCache(path, cacheData{LastCheck: now, LastTag: tag}); err != nil {
			opts.Log.Debug("update check: writing cache", "err", err)
		}
	}

	if latestTag == "" {
		return nil
	}
	normLatest, ok := normalizeVersion(latestTag)
	if !ok {
		return nil
	}
	if semver.Compare(normLatest, normCurrent) <= 0 {
		return nil
	}
	return &Notice{
		Current: strings.TrimPrefix(normCurrent, "v"),
		Latest:  strings.TrimPrefix(normLatest, "v"),
		Hint:    installHint(),
	}
}

// releaseVersionPattern is deliberately stricter than
// golang.org/x/mod/semver.IsValid (which accepts "v1.2", and treats a
// git-describe suffix like "-3-gabc-dirty" as an ordinary, valid
// pre-release identifier): a released tag must be exactly vX.Y.Z with an
// optional dot-separated, alphanumeric-only pre-release. Anything else —
// `df102b3`, `v0.1.0-3-gabc-dirty` (make build's git-describe output),
// `v1.2` — is a dev build, not a comparable release.
var releaseVersionPattern = regexp.MustCompile(`^v\d+\.\d+\.\d+(-[0-9A-Za-z]+(\.[0-9A-Za-z]+)*)?$`)

// normalizeVersion prepends a leading "v" if missing and validates the
// result against releaseVersionPattern; semver.Compare (which does need
// the "v", per its own doc) is used afterward to order two normalized
// versions.
func normalizeVersion(v string) (string, bool) {
	if v == "" || v == "dev" {
		return "", false
	}
	if !strings.HasPrefix(v, "v") {
		v = "v" + v
	}
	if !releaseVersionPattern.MatchString(v) {
		return "", false
	}
	return v, true
}

// cachePath resolves ~/.mcpwarp/update-check.json under homeDir, falling
// back to os.UserHomeDir() when homeDir is empty — the same fallback
// auth.CredentialsPathFor uses for the real (non-test) home directory.
func cachePath(homeDir string) (string, error) {
	home := homeDir
	if home == "" {
		h, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		home = h
	}
	return filepath.Join(home, ".mcpwarp", "update-check.json"), nil
}

// loadCache reads and parses the cache file, returning nil on any error
// (missing file, corrupt JSON) — a cold cache is the expected first-run
// state, not worth logging.
func loadCache(path string) *cacheData {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var c cacheData
	if err := json.Unmarshal(b, &c); err != nil {
		return nil
	}
	return &c
}

// saveCache writes the cache file atomically — temp file in the same dir,
// write, fsync, chmod 0600, rename over path — mirroring auth.Save's
// credentials write (internal/auth/credentials.go), so a crash or a
// concurrent mcpwarp process never observes a half-written cache file.
// Creates the parent directory (0700) if needed, same permissions as
// auth's credentials store and tui.OpenLogFile's log file, both under the
// same ~/.mcpwarp directory.
func saveCache(path string, c cacheData) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}

	content, err := json.Marshal(c)
	if err != nil {
		return err
	}

	tmp := filepath.Join(dir, fmt.Sprintf(".update-check.json.%d.%d.tmp", os.Getpid(), time.Now().UnixMilli()))
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(content); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Chmod(tmp, 0o600); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// releaseResponse is the subset of GitHub's release JSON Check needs.
type releaseResponse struct {
	TagName string `json:"tag_name"`
}

// fetchLatestTag GETs opts.Endpoint (GitHub's "latest release" API by
// default) and returns its tag_name.
func fetchLatestTag(ctx context.Context, current string, opts Options) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, opts.Endpoint, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "mcpwarp/"+current)

	resp, err := opts.HTTPClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("update check: unexpected status %d", resp.StatusCode)
	}

	var body releaseResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", err
	}
	return body.TagName, nil
}
