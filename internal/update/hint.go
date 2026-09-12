package update

import (
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
)

// releaseURL is where to grab a raw archive when no package manager was
// detected.
const releaseURL = "https://github.com/mcpwarp/cli/releases/latest"

// UpgradeHint returns the upgrade command/message to show for the running
// binary, inferred from its resolved executable path — a pure function of
// (goos, exePath) so install-method detection is table-testable without
// touching os.Executable/EvalSymlinks (installHint below supplies those for
// real).
func UpgradeHint(goos, exePath string) string {
	switch goos {
	case "darwin":
		// UpgradeHint is a pure function of (goos, exePath) and the goos
		// branch taken need not match the host OS running it (tests, or a
		// cross-built binary's install-time metadata) — normalise to "/"
		// before matching rather than trusting the host's separator.
		slashed := filepath.ToSlash(exePath)
		if strings.Contains(slashed, "/Cellar/") ||
			strings.Contains(slashed, "/Caskroom/") ||
			strings.HasPrefix(slashed, "/opt/homebrew") ||
			strings.HasPrefix(slashed, "/usr/local/Homebrew") {
			return "brew upgrade --cask mcpwarp"
		}
	case "windows":
		// os.Executable/EvalSymlinks always return "\"-separated paths on
		// Windows, but UpgradeHint is a pure function callers (and tests)
		// can also hand a "/"-separated path — normalise before matching.
		normalized := strings.ReplaceAll(strings.ToLower(exePath), "/", `\`)
		if strings.Contains(normalized, `\scoop\`) {
			return "scoop update mcpwarp"
		}
	case "linux":
		// path.Dir, not filepath.Dir: the goos branch is chosen by the
		// goos argument, not the host running the test, so this must use
		// "/"-only semantics regardless of what filepath.Dir would do on
		// the host (backslashes on a Windows test runner). Not HasPrefix
		// either: "/usr/bin" must be the whole parent directory, not just
		// a string prefix — HasPrefix would also (wrongly) match a path
		// like "/usr/bin-local/mcpwarp".
		if path.Dir(filepath.ToSlash(exePath)) == "/usr/bin" {
			return "use your package manager (apt/dnf/apk) or download from " + releaseURL
		}
	}
	return "download from " + releaseURL
}

// installHint resolves the running binary's real path (following symlinks,
// the way a Homebrew Cellar install is normally reached through a symlink
// in /opt/homebrew/bin) and hands it to UpgradeHint. Falls back to the
// generic "download" hint if the path can't be resolved.
func installHint() string {
	exe, err := os.Executable()
	if err != nil {
		return UpgradeHint(runtime.GOOS, "")
	}
	if resolved, err := filepath.EvalSymlinks(exe); err == nil {
		exe = resolved
	}
	return UpgradeHint(runtime.GOOS, exe)
}
