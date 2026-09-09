// Command mcpwarp is the CLI entry point. Exit codes: 0 ok, 1 runtime
// failure, 2 usage/config error (overview.md §8) — --help and --version
// always exit 0.
package main

import (
	"os"

	"github.com/mcpwarp/cli/internal/bridge"
	"github.com/mcpwarp/cli/internal/cli"
)

// version is set via `-ldflags "-X main.version=..."` at release build time.
var version = "dev"

func main() {
	code := cli.Execute(version, os.Args[1:])
	// Last-resort net (DESIGN.md §3): in production, a signal-driven `up`
	// shutdown exits from inside shutdown.Run/FatalExit itself (via
	// os.Exit), so this line is normally unreached for that path — it only
	// matters if Execute returns on its own (no signal fired, e.g. `up`
	// exiting for a reason other than SIGINT/SIGTERM/a fatal disconnect) or
	// if some child somehow survived the servers shutdown handler's own
	// KillAllLiveChildren call.
	bridge.KillAllLiveChildren()
	os.Exit(code)
}
