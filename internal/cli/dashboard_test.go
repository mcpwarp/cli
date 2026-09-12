package cli

import (
	"log/slog"
	"strings"
	"testing"
)

func TestRunDashboardPrintsURL(t *testing.T) {
	t.Setenv("MCPWARP_WEB_URL", "http://localhost:5173")
	orig := openBrowserFn
	t.Cleanup(func() { openBrowserFn = orig })
	var openedURL string
	openBrowserFn = func(u string, _ *slog.Logger) { openedURL = u }

	stdout := withCapturedStdout(t)
	ctx := &Context{Log: NewLogger(false)}
	if err := runDashboard(ctx); err != nil {
		t.Fatal(err)
	}
	got := strings.TrimSpace(stdout.String())
	if got != "http://localhost:5173" {
		t.Fatalf("got %q", got)
	}
	if openedURL != got {
		t.Fatalf("browser opened %q, want the resolved URL %q", openedURL, got)
	}
}

func TestRunDashboardInvalidWebURLExits2(t *testing.T) {
	t.Setenv("MCPWARP_WEB_URL", "ftp://x")
	stderr := withCapturedStderr(t)

	ctx := &Context{Log: NewLogger(false)}
	err := runDashboard(ctx)
	if err == nil {
		t.Fatal("expected an error")
	}
	if code := err.(ExitCoder).ExitCode(); code != 2 {
		t.Fatalf("got exit code %d, want 2; stderr=%s", code, stderr.String())
	}
}
