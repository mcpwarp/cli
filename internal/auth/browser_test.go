package auth

import (
	"os/exec"
	"reflect"
	"runtime"
	"testing"
)

func TestUnsafeWin32URLChars(t *testing.T) {
	safe := []string{"https://example.com/device?user_code=ABCD-EFGH", "http://x/y"}
	unsafe := []string{`https://x/"`, "https://x/&", "https://x/|", "https://x/<", "https://x/>", "https://x/^y", "https://x/%20"}

	for _, u := range safe {
		if unsafeWin32URLChars.MatchString(u) {
			t.Errorf("%q should be considered safe", u)
		}
	}
	for _, u := range unsafe {
		if !unsafeWin32URLChars.MatchString(u) {
			t.Errorf("%q should be considered unsafe", u)
		}
	}
}

func TestOpenBrowserNeverPanicsOnBadInput(t *testing.T) {
	// Malformed URL and non-http(s) schemes must be silently ignored: never
	// panicked on, and never exec'd — asserted via the execCommand spy
	// rather than just trusting the validation short-circuit runs first.
	orig := execCommand
	t.Cleanup(func() { execCommand = orig })
	called := false
	execCommand = func(name string, args ...string) *exec.Cmd {
		called = true
		return exec.Command("true")
	}

	OpenBrowser("not a url with spaces and \x00 control chars", nil)
	OpenBrowser("ftp://example.com/file", nil)
	OpenBrowser("javascript:alert(1)", nil)

	if called {
		t.Fatal("execCommand should never be called for invalid input")
	}
}

func TestBrowserCommandPerPlatform(t *testing.T) {
	const u = "https://example.com/activate?user_code=ABCD"
	cases := []struct {
		goos     string
		wantName string
		wantArgs []string
	}{
		{"darwin", "open", []string{u}},
		{"windows", "rundll32", []string{"url.dll,FileProtocolHandler", u}},
		{"linux", "xdg-open", []string{u}},
		{"freebsd", "xdg-open", []string{u}}, // anything not darwin/windows falls to the xdg-open default
	}
	for _, tc := range cases {
		t.Run(tc.goos, func(t *testing.T) {
			name, args, reason := browserCommand(tc.goos, u)
			if reason != "" {
				t.Fatalf("unexpected reason %q", reason)
			}
			if name != tc.wantName || !reflect.DeepEqual(args, tc.wantArgs) {
				t.Fatalf("got (%q, %v), want (%q, %v)", name, args, tc.wantName, tc.wantArgs)
			}
		})
	}
}

func TestBrowserCommandRejectsBadInputPerPlatform(t *testing.T) {
	if _, _, reason := browserCommand("darwin", "not a url"); reason == "" {
		t.Fatal("expected a rejection reason for a schemeless URL")
	}
	if _, _, reason := browserCommand("darwin", "ftp://example.com"); reason == "" {
		t.Fatal("expected a rejection reason for a non-http(s) scheme")
	}
	if _, _, reason := browserCommand("windows", "https://x/&"); reason == "" {
		t.Fatal("expected windows to reject an unsafe metacharacter")
	}
}

func TestOpenBrowserInvokesExecCommandWithTheComputedArgv(t *testing.T) {
	orig := execCommand
	t.Cleanup(func() { execCommand = orig })

	var gotName string
	var gotArgs []string
	execCommand = func(name string, args ...string) *exec.Cmd {
		gotName, gotArgs = name, args
		return exec.Command("true") // harmless real command so Start/Wait don't error
	}

	OpenBrowser("https://example.com/activate", nil)

	if gotName == "" {
		t.Fatal("execCommand was never called")
	}
	wantName, wantArgs, _ := browserCommand(runtime.GOOS, "https://example.com/activate")
	if gotName != wantName || !reflect.DeepEqual(gotArgs, wantArgs) {
		t.Fatalf("got (%q, %v), want (%q, %v)", gotName, gotArgs, wantName, wantArgs)
	}
}
