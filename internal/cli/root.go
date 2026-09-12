// Package cli wires cobra commands to the mcpwarp behaviour ported from the
// Node CLI: global flags, exit-code mapping, and (for M0) the status
// command. main() only calls Execute.
package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/mcpwarp/cli/internal/auth"
	"github.com/mcpwarp/cli/internal/config"
	"github.com/mcpwarp/cli/internal/output"
	"github.com/mcpwarp/cli/internal/shutdown"
	"github.com/spf13/cobra"
)

// rootLong is the root command's --help body: the one-line description,
// the public-URL-is-tied-to-name caveat, then a getting-started walkthrough
// and the environment variables `up`/`dashboard` read defaults from.
var rootLong = fmt.Sprintf(`Expose local MCP servers through the mcpwarp tunnel

A server's public URL is tied to its name; renaming it in config assigns a new URL.

Getting started:
  1. mcpwarp login                      sign in via the device flow
  2. create ~/.mcpwarp/config.json      (or pass --config); example:
%s
  3. mcpwarp up                         registers each server, prints its URL

Environment:
  MCPWARP_AUTH_URL     auth server           (default %s)
  MCPWARP_CONNECT_URL  tunnel WebSocket URL  (default %s)
  MCPWARP_WEB_URL      dashboard URL         (default %s)
  %s        personal access token, skips login
  MCPWARP_NO_UPDATE_NOTIFIER  disable the update check

Docs: https://mcpwarp.io/docs/get-started`,
	indentBlock(config.ExampleJSON, "       "), auth.DefaultAuthURL, defaultConnectURL, defaultWebURL, auth.StaticTokenEnvVar)

// indentBlock prefixes every line of s with prefix, for embedding
// config.ExampleJSON under the getting-started numbered list above.
func indentBlock(s, prefix string) string {
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		lines[i] = prefix + line
	}
	return strings.Join(lines, "\n")
}

// Root builds the mcpwarp root command. version is injected by main via
// ldflags (default "dev").
func Root(version string) *cobra.Command {
	var opts struct {
		verbose    bool
		configPath string
		issuer     string
		connectURL string
	}

	root := &cobra.Command{
		Use:     "mcpwarp",
		Short:   "Expose local MCP servers through the mcpwarp tunnel",
		Long:    rootLong,
		Version: version,
		// We print our own errors (reportConfigError etc.) and want exit-code
		// control ourselves, so cobra's default error/usage printing is off —
		// see Execute below for the exit-code mapping.
		SilenceUsage:  true,
		SilenceErrors: true,
		// Bare `mcpwarp` (no subcommand, --help/--version excepted — those
		// are intercepted before RunE runs) is a usage error in Node too:
		// commander's exitOverride maps "no operand given, but subcommands
		// exist" to exit 2 (program.ts) rather than cobra's default of
		// printing help and exiting 0.
		RunE: func(cmd *cobra.Command, args []string) error {
			// Node writes bare-invocation help to stderr (commander's
			// showHelpAfterError path), not stdout.
			cmd.SetOut(cmd.ErrOrStderr())
			_ = cmd.Help()
			return &exitError{code: 2}
		},
		// An unrecognized subcommand (cobra's default legacyArgs validator,
		// since this command HasSubCommands) is otherwise reported as
		// `unknown command "bogus" for "mcpwarp"` with no usage printed
		// (SilenceUsage/SilenceErrors above suppress cobra's own printing) —
		// overridden here to match Node's commander wording/format instead
		// (program.ts's default "unknown command" handling), the same way
		// SetFlagErrorFunc below matches it for a bad flag.
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return nil
			}
			msg := fmt.Sprintf("unknown command '%s'", args[0])
			// cobra's own Levenshtein-distance suggestion match
			// (SuggestionsMinimumDistance, default 2) — cheap to wire in,
			// but not tuned to reproduce every suggestion commander's own
			// algorithm would find (e.g. "bogus" -> "logout"): a miss here
			// just means no "(Did you mean ...?)" line, not a wrong one.
			if suggestions := cmd.SuggestionsFor(args[0]); len(suggestions) > 0 {
				msg += fmt.Sprintf("\n(Did you mean %s?)", suggestions[0])
			}
			fmt.Fprintf(cmd.ErrOrStderr(), "error: %s\n\n%s", msg, cmd.UsageString())
			return &exitError{code: 2}
		},
	}
	root.SetVersionTemplate("{{.Version}}\n")
	// Node's command surface has no shell-completion command (§2), and
	// cobra adds one by default — turned off since Node has none.
	root.CompletionOptions.DisableDefaultCmd = true
	// Defined ourselves, no shorthand, before cobra's InitDefaultVersionFlag
	// runs: its automatic version flag binds -v too, but Node's -v is unknown.
	root.Flags().Bool("version", false, "print the version and exit")
	root.SetFlagErrorFunc(func(cmd *cobra.Command, err error) error {
		fmt.Fprintf(cmd.ErrOrStderr(), "error: %s\n\n%s", flagErrorMessage(err), cmd.UsageString())
		return &exitError{code: 2}
	})

	root.PersistentFlags().BoolVar(&opts.verbose, "verbose", false, "print debug-level logs to stderr")
	root.PersistentFlags().StringVar(&opts.configPath, "config", "", "path to config.json (default: ~/.mcpwarp/config.json)")
	root.PersistentFlags().StringVar(&opts.issuer, "issuer", "", "auth server URL, overrides MCPWARP_AUTH_URL (default: https://auth.mcpwarp.io)")
	root.PersistentFlags().StringVar(&opts.connectURL, "connect-url", "", "tunnel WebSocket URL, overrides MCPWARP_CONNECT_URL (default: wss://connect.mcpwarp.io)")

	// lastCommandContext (package-level, see its own doc comment) starts
	// every Root() call nil, so a previous invocation's leftover value —
	// this build's own test suite calls Execute repeatedly in one process
	// — never leaks into one where ctxFor is never reached (a bad flag,
	// unknown command).
	lastCommandContext = nil
	ctxFor := func(cmd *cobra.Command) *Context {
		log, logWriter := NewLoggerWithSwap(opts.verbose)
		c := &Context{
			Verbose:            opts.verbose,
			ConfigPath:         opts.configPath,
			IssuerOverride:     opts.issuer,
			ConnectURLOverride: opts.connectURL,
			Log:                log,
			LogWriter:          logWriter,
			Ctx:                cmd.Context(),
			CommandName:        cmd.Name(),
		}
		// Started here — "the beginning of every command" (DESIGN.md §2) —
		// so the goroutine has the whole command's runtime to finish
		// before runRoot's bounded wait for it, below.
		c.UpdateChecker = startUpdateChecker(c.Context(), version, c.HomeDir, log)
		lastCommandContext = c
		return c
	}

	root.AddCommand(newStatusCommand(ctxFor))
	root.AddCommand(newLoginCommand(ctxFor))
	root.AddCommand(newLogoutCommand(ctxFor))
	root.AddCommand(newWhoamiCommand(ctxFor))
	root.AddCommand(newUpCommand(ctxFor))
	root.AddCommand(newDashboardCommand(ctxFor))

	return root
}

// flagErrorMessage turns pflag's own wording for an unrecognized flag —
// "unknown flag: --nope" or "unknown shorthand flag: 'v' in -v" — into
// commander's "unknown option '--nope'" / "unknown option '-v'" text.
func flagErrorMessage(err error) string {
	msg := err.Error()
	if rest, ok := strings.CutPrefix(msg, "unknown flag: "); ok {
		return fmt.Sprintf("unknown option '%s'", rest)
	}
	if idx := strings.LastIndex(msg, " in "); strings.Contains(msg, "unknown shorthand flag: ") && idx != -1 {
		return fmt.Sprintf("unknown option '%s'", msg[idx+len(" in "):])
	}
	return msg
}

// reportConfigError prints a config/auth-settings error's message (already
// terminal-shaped, no glyph beyond output.Error's own ✗) and turns it into
// an exitError carrying the same exit code — config problems are always
// exit 2 (ConfigError, auth.SettingsError); anything else defaults to 1.
func reportConfigError(err error) error {
	output.Error(err.Error())
	code := 1
	if ec, ok := err.(ExitCoder); ok {
		code = ec.ExitCode()
	}
	return &exitError{code: code}
}

func nowMillis() int64 {
	return time.Now().UnixMilli()
}

// runShutdown is shutdown.Run behind a var: in production it runs the
// bounded shutdown sequence and calls os.Exit itself, never returning
// control here. Tests swap it for a stub that returns instead, so the N1
// race fix below (the signal path, not the command's own result, decides
// the exit code) is testable in-process without exiting the test binary.
var runShutdown = shutdown.Run

// executeOSExit is os.Exit behind a var — used only as the last-resort
// fallback below, when runRoot's wait for runShutdown runs past its bound.
// In production shutdown.Run always calls its own os.Exit well inside that
// bound, so this only fires for a genuinely wedged shutdown sequence or a
// test's runShutdown stub.
var executeOSExit = os.Exit

// Execute builds the root command, runs it against args, and maps the
// result to a process exit code: 0 ok, 1 runtime, 2 usage/config
// (overview.md §8) — --help/--version exit 0. Kept separate from main() so
// exit-code mapping is testable in-process, without exec'ing the binary.
func Execute(version string, args []string) int {
	root := Root(version)
	root.SetArgs(args)
	return runRoot(root)
}

// runRoot wires the signal-aware context, runs root, and maps the result to
// an exit code. Split out from Execute so a test can hand it a bare
// *cobra.Command whose RunE self-signals mid-run — reproducing the N1 race
// (a signal arriving just as the command finishes on its own) without
// depending on any real subcommand's timing.
func runRoot(root *cobra.Command) int {
	ctx, stopNotify := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopNotify()

	// A second channel drives the shutdown package's own bounded sequence
	// (DESIGN §3) independently of ctx's cancellation — each signal gets
	// its own goroutine so a second SIGINT/SIGTERM arriving while the first
	// sequence is still running can force-exit immediately rather than
	// waiting behind it.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	var (
		sigMu  sync.Mutex
		gotSig os.Signal
	)
	shutdownDone := make(chan struct{})
	var shutdownDoneOnce sync.Once
	relayDone := make(chan struct{})
	defer close(relayDone)
	go func() {
		for {
			select {
			case sig := <-sigCh:
				sigMu.Lock()
				if gotSig == nil {
					gotSig = sig
				}
				sigMu.Unlock()
				go func() {
					runShutdown(sig)
					// Only meaningful when runShutdown returns instead of
					// exiting the process itself (the test seam, or a real
					// second signal racing a first one already past
					// shutdown's own dedup) — whichever finishes first is
					// as done as runRoot needs to know about.
					shutdownDoneOnce.Do(func() { close(shutdownDone) })
				}()
			case <-relayDone:
				return
			}
		}
	}()

	err := root.ExecuteContext(ctx)

	// Printed here, not a PersistentPostRun, so it runs whether or not the
	// command's RunE returned an error — see lastCommandContext's doc
	// comment. Never touches err/code below.
	printUpdateNotice(lastCommandContext)

	sigMu.Lock()
	sig := gotSig
	sigMu.Unlock()

	if sig != nil {
		// N1: a signal arrived during this invocation, so the signal path
		// owns the exit — regardless of what root.ExecuteContext returned.
		// The command can easily finish successfully (err == nil) in the
		// same instant ctx is cancelled (e.g. logout completing its last
		// write right as SIGINT lands); reporting that result, or a bare
		// context.Canceled error, races shutdown.Run's own os.Exit(128+sig)
		// and — as reproduced — sometimes wins, exiting 0 instead of
		// 130/143. So: never fall through to the result-mapping below. Wait
		// (bounded) for the in-flight runShutdown to finish, then use its
		// exit code ourselves; shutdown.Run always calls os.Exit within
		// shutdown.Deadline in production, so this process never actually
		// reaches the `return` below there — the slack here only matters
		// for the test seam and as a backstop against a wedged sequence.
		select {
		case <-shutdownDone:
			return shutdown.ExitCodeFor(sig)
		case <-time.After(shutdown.Deadline + 2*time.Second):
			code := shutdown.ExitCodeFor(sig)
			executeOSExit(code)
			return code
		}
	}

	if err == nil {
		return 0
	}

	var ec ExitCoder
	if errors.As(err, &ec) {
		return ec.ExitCode()
	}

	// Anything else is cobra's own usage error (unknown flag/command, bad
	// argument count) — usage-shaped, exit 2, matching Node's commander
	// exitOverride (program.ts).
	output.Error(err.Error())
	return 2
}
