package auth

import (
	"log/slog"
	"net/url"
	"os/exec"
	"regexp"
	"runtime"
)

// unsafeWin32URLChars are shell/URL metacharacters refused before handing a
// URL to rundll32/ShellExecute — a defensive check, since that path still
// forwards the string to ShellExecute even though rundll32 itself is never
// invoked through a shell.
var unsafeWin32URLChars = regexp.MustCompile(`["&|<>^%]`)

// execCommand is exec.Command behind a var so tests can assert the exact
// argv OpenBrowser would run without actually spawning a subprocess.
var execCommand = exec.Command

// browserCommand returns the argv OpenBrowser would exec for rawURL on
// goos, or a non-empty reason it won't be opened at all (malformed URL,
// non-http(s) scheme, or — on Windows — a metacharacter injection risk).
// Split out from OpenBrowser so the platform-specific argv can be asserted
// directly, independent of runtime.GOOS.
func browserCommand(goos, rawURL string) (name string, args []string, reason string) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", nil, "URL is not well-formed"
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", nil, "unsupported protocol"
	}

	switch goos {
	case "darwin":
		return "open", []string{rawURL}, ""
	case "windows":
		if unsafeWin32URLChars.MatchString(rawURL) {
			return "", nil, "URL contains characters unsafe for rundll32/ShellExecute"
		}
		return "rundll32", []string{"url.dll,FileProtocolHandler", rawURL}, ""
	default:
		return "xdg-open", []string{rawURL}, ""
	}
}

// OpenBrowser opens url in the platform's default browser via exec.Command
// with a real argv slice — never a shell — so nothing in url is ever
// re-parsed as shell syntax. url is validated first (well-formed, http/https
// only) since it comes from the auth server's device authorization
// response, not from anything the user typed. Failure is silent beyond a
// debug log line: the user can still open the URL manually.
func OpenBrowser(rawURL string, log *slog.Logger) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	name, args, reason := browserCommand(runtime.GOOS, rawURL)
	if reason != "" {
		log.Debug("not opening browser: " + reason)
		return
	}

	cmd := execCommand(name, args...)
	if err := cmd.Start(); err != nil {
		log.Debug("failed to open browser", "err", err)
		return
	}
	go func() { _ = cmd.Wait() }()
}
