package cli

import (
	"fmt"
	"strings"

	"github.com/mcpwarp/cli/internal/auth"
	"github.com/mcpwarp/cli/internal/config"
	"github.com/mcpwarp/cli/internal/output"
	"github.com/spf13/cobra"
)

var statusHeaders = output.TableHeaders{"NAME", "KIND", "TARGET"}

func newStatusCommand(ctxFor func(*cobra.Command) *Context) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show login and config status",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runStatus(ctxFor(cmd))
		},
	}
}

// runStatus mirrors Node's cli/commands/status.ts: resolved config path, the
// NAME/KIND/TARGET server table, then login status — no network call.
func runStatus(ctx *Context) error {
	ctx.Log.Debug("mcpwarp status invoked")

	path, err := config.ResolveConfigPath(ctx.ConfigPath)
	if err != nil {
		output.Error(err.Error())
		return RuntimeError()
	}
	output.Info(fmt.Sprintf("config: %s", path))

	cfg, err := ctx.LoadConfig()
	if err != nil {
		return reportConfigError(err)
	}

	if len(cfg.Servers) == 1 {
		output.Info("1 server configured")
	} else {
		output.Info(fmt.Sprintf("%d servers configured", len(cfg.Servers)))
	}

	rows := make([]output.TableRow, len(cfg.Servers))
	for i, s := range cfg.Servers {
		rows[i] = statusRow(s)
	}
	output.Info(output.FormatTable(rows, statusHeaders))
	output.Info("A server's public URL is tied to its name; renaming it in config assigns a new URL.")
	output.Info("")

	issuer, err := auth.CurrentIssuer(ctx.IssuerOverride)
	if err != nil {
		return reportConfigError(err)
	}
	paths, err := auth.CredentialsPaths(issuer, ctx.HomeDir)
	if err != nil {
		output.Error(err.Error())
		return RuntimeError()
	}

	creds := auth.Load(paths, nil)
	if creds == nil {
		output.Info("login: not logged in")
		return nil
	}

	stale := auth.IsAccessTokenStale(creds, 0, nowMillis())
	who := creds.Email
	if who == "" {
		who = creds.PreferredUsername
	}
	if who == "" {
		who = creds.Sub
	}
	state := "valid"
	if stale {
		state = "expired"
	}
	output.Info(fmt.Sprintf("login: logged in as %s (access token %s)", who, state))
	return nil
}

func statusRow(s config.Server) output.TableRow {
	if s.Kind == config.KindStdio {
		target := strings.Join(append([]string{s.Command}, s.Args...), " ")
		return output.TableRow{Name: s.Name, Kind: s.Kind, URL: target}
	}
	return output.TableRow{Name: s.Name, Kind: s.Kind, URL: s.URL}
}
