package update

import (
	"os"
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
		if strings.Contains(exePath, "/Cellar/") ||
			strings.Contains(exePath, "/Caskroom/") ||
			strings.HasPrefix(exePath, "/opt/homebrew") ||
			strings.HasPrefix(exePath, "/usr/local/Homebrew") {
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
		// filepath.Dir, not HasPrefix: "/usr/bin" must be the whole parent
		// directory, not just a string prefix — HasPrefix would also (wrongly)
		// match a path like "/usr/bin-local/mcpwarp".
		if filepath.Dir(exePath) == "/usr/bin" {
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
