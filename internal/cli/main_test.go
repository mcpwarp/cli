package cli

import (
	"os"
	"testing"
)

// TestMain sets MCPWARP_NO_UPDATE_NOTIFIER for this package's whole test
// run. Every test that drives a command through Execute (root_test.go,
// up_e2e_posix_test.go, ...) starts a real background update.Check
// goroutine via ctxFor/startUpdateChecker; without this, any such test
// would reach out to the real GitHub API unless it happens to stub
// updateCheck itself. The gate has to live inside update.Check (checked
// against the real environment there), not in this package's seam,
// precisely so a test that *does* override updateCheck (internal/cli's
// own update_test.go) bypasses Check entirely and is unaffected by this
// env var either way — os.Setenv rather than t.Setenv since TestMain runs
// outside any *testing.T.
func TestMain(m *testing.M) {
	os.Setenv("MCPWARP_NO_UPDATE_NOTIFIER", "1")
	os.Exit(m.Run())
}
