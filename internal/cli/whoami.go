package cli

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/mcpwarp/cli/internal/auth"
	"github.com/mcpwarp/cli/internal/output"
	"github.com/spf13/cobra"
)

type whoamiDeps struct {
	paths  *auth.Paths
	issuer string
	client auth.HTTPDoer
}

func newWhoamiCommand(ctxFor func(*cobra.Command) *Context) *cobra.Command {
	var refresh bool
	cmd := &cobra.Command{
		Use:   "whoami",
		Short: "Show who's logged in",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runWhoami(ctxFor(cmd), refresh, whoamiDeps{})
		},
	}
	cmd.Flags().BoolVar(&refresh, "refresh", false, "exercise the token provider (refresh if stale) before printing")
	return cmd
}

// runWhoami mirrors Node's cli/commands/whoami.ts, with one divergence:
// the MCPWARP_TOKEN (static PAT mode, §5/§7) branch below, which Node's
// whoami doesn't have. It short-circuits everything else: no file, no
// refresh.
func runWhoami(ctx *Context, refresh bool, deps whoamiDeps) error {
	ctx.Log.Debug("mcpwarp whoami invoked")

	if token := os.Getenv(auth.StaticTokenEnvVar); token != "" {
		if !strings.HasPrefix(token, auth.PATPrefix) {
			output.Warn("MCPWARP_TOKEN does not look like a mcpwarp personal access token (expected prefix `" + auth.PATPrefix + "`)")
		}
		output.Info("auth: using MCPWARP_TOKEN (personal access token)")
		return nil
	}

	issuer := deps.issuer
	paths := deps.paths
	if issuer == "" || paths == nil {
		var err error
		issuer, err = auth.CurrentIssuer(ctx.IssuerOverride)
		if err != nil {
			return reportConfigError(err)
		}
		p, err := auth.CredentialsPaths(issuer, ctx.HomeDir)
		if err != nil {
			output.Error(err.Error())
			return RuntimeError()
		}
		paths = &p
	}

	if refresh {
		provider := auth.NewTokenProvider(auth.TokenProviderOptions{
			Issuer: issuer,
			Paths:  *paths,
			Client: deps.client,
			Warn:   func(m string) { output.Warn(m) },
		})
		if _, err := provider.Token(ctx.Context()); err != nil {
			switch err.(type) {
			case *auth.NotLoggedInError:
				output.Error("Not logged in. Run `mcpwarp login`.")
			case *auth.SessionExpiredError:
				output.Error("Session expired. Run `mcpwarp login`.")
			default:
				output.Error(err.Error())
			}
			return RuntimeError()
		}
		if provider.LastAction() == "refreshed" {
			output.Info("refreshed")
		} else {
			output.Info("token still fresh")
		}
	}

	creds := auth.Load(*paths, nil)
	if creds == nil {
		output.Error("Not logged in. Run `mcpwarp login`.")
		return RuntimeError()
	}

	who := creds.Email
	if who == "" {
		who = creds.PreferredUsername
	}
	if who == "" {
		who = creds.Sub
	}
	output.Info(fmt.Sprintf("user: %s", who))
	output.Info(fmt.Sprintf("issuer: %s", creds.Issuer))
	stale := auth.IsAccessTokenStale(creds, 0, nowMillis())
	state := "valid"
	if stale {
		state = "expired"
	}
	expiry := time.UnixMilli(creds.ExpiresAt).UTC().Format("2006-01-02T15:04:05.000Z")
	output.Info(fmt.Sprintf("access token expires: %s (%s)", expiry, state))
	return nil
}
