package fakeedge

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/mcpwarp/ws-mixer-go/wsmixer"
)

// TestNotInProductionBinary pins the package doc's promise: nothing the
// shipped binary builds from imports this package, or "testing" at all.
func TestNotInProductionBinary(t *testing.T) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go toolchain not on PATH")
	}
	out, err := exec.Command(goBin, "list", "-deps", "github.com/mcpwarp/cli/cmd/...").CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps: %v\n%s", err, out)
	}
	for _, pkg := range strings.Fields(string(out)) {
		if pkg == "github.com/mcpwarp/cli/internal/fakeedge" || pkg == "testing" {
			t.Errorf("production binary depends on %s", pkg)
		}
	}
}

// TestRefusedHandshakeCloseCodes: Authenticate's error picks the close code
// a wsmixer client sees, and each refusal is recorded.
func TestRefusedHandshakeCloseCodes(t *testing.T) {
	s := New(t, Options{Authenticate: func(attempt int, _ *wsmixer.Hello) error {
		if attempt == 1 {
			return wsmixer.Unauthorized("token rejected")
		}
		return wsmixer.Unsupported("version rejected")
	}})

	for i, want := range []int{4011, 4010} {
		ctx, cancel := context.WithTimeout(context.Background(), DefaultWait)
		c, err := wsmixer.Dial(ctx, s.URL, wsmixer.ClientOptions{Token: "t"})
		cancel()
		if err == nil {
			_ = c.Close(uint32(wsmixer.NoError), "")
			t.Fatalf("dial #%d succeeded, want a refused handshake", i+1)
		}
		var ce *wsmixer.ConnError
		if !errors.As(err, &ce) || ce.CloseCode() != want {
			t.Fatalf("dial #%d error = %v, want a ConnError closing %d", i+1, err, want)
		}
	}
	s.WaitUntil(t, "two refusals", func() bool {
		n := 0
		for _, e := range s.Events() {
			if e.Kind == EventRefused {
				n++
			}
		}
		return n == 2
	})
	if s.Attempts() != 2 || s.ConnCount() != 0 {
		t.Fatalf("Attempts = %d, ConnCount = %d; want 2 refused and none accepted", s.Attempts(), s.ConnCount())
	}
}

// TestCleanupClosesSocketMidHandshake: a socket that upgraded but never
// sent hello must not hold cleanup for the server's hello timeout.
func TestCleanupClosesSocketMidHandshake(t *testing.T) {
	// The client socket stays open until the outer test ends, so only the
	// server's own cleanup (run when the subtest ends) can end the handler.
	start := time.Now()
	t.Run("server", func(st *testing.T) {
		s := New(st, Options{})
		ctx, cancel := context.WithTimeout(context.Background(), DefaultWait)
		defer cancel()
		ws, _, err := websocket.Dial(ctx, s.URL, &websocket.DialOptions{
			Subprotocols: []string{wsmixer.Subprotocol},
			HTTPHeader:   map[string][]string{"Authorization": {"Bearer t"}},
		})
		if err != nil {
			st.Fatalf("dial: %v", err)
		}
		t.Cleanup(func() { _ = ws.CloseNow() })
	})
	if took := time.Since(start); took >= DefaultWait {
		t.Fatalf("cleanup took %v with a socket mid-handshake, want under %v", took, DefaultWait)
	}
}
