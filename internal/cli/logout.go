package cli

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/mcpwarp/cli/internal/auth"
	"github.com/mcpwarp/cli/internal/output"
	"github.com/spf13/cobra"
)

type logoutDeps struct {
	client auth.HTTPDoer
	paths  *auth.Paths
}

func newLogoutCommand(ctxFor func(*cobra.Command) *Context) *cobra.Command {
	return &cobra.Command{
		Use:   "logout",
		Short: "Log out and clear stored credentials",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runLogout(ctxFor(cmd), logoutDeps{})
		},
	}
}

// runLogout mirrors Node's cli/commands/logout.ts: revocation against
// end_session_endpoint is best-effort (5s budget); discovery or the revoke
// call failing never blocks deleting the local file.
func runLogout(ctx *Context, deps logoutDeps) error {
	ctx.Log.Debug("mcpwarp logout invoked")

	paths := deps.paths
	if paths == nil {
		issuer, err := auth.CurrentIssuer(ctx.IssuerOverride)
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

	creds := auth.Load(*paths, nil)
	if creds == nil {
		output.Info("Not logged in.")
		return nil
	}

	background := ctx.Context()
	if oidc, err := auth.Discover(background, creds.Issuer, deps.client); err == nil {
		if oidc.EndSessionEndpoint != "" {
			revokeCtx, cancel := context.WithTimeout(background, 5*time.Second)
			body := url.Values{"client_id": {creds.ClientID}, "refresh_token": {creds.RefreshToken}}
			req, rerr := http.NewRequestWithContext(revokeCtx, http.MethodPost, oidc.EndSessionEndpoint, strings.NewReader(body.Encode()))
			if rerr == nil {
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				client := deps.client
				if client == nil {
					client = &http.Client{}
				}
				resp, derr := client.Do(req)
				if derr == nil {
					resp.Body.Close()
					if resp.StatusCode < 200 || resp.StatusCode >= 300 {
						ctx.Log.Debug("session revocation endpoint returned a non-ok status", "status", resp.StatusCode)
					}
				} else {
					ctx.Log.Debug("best-effort session revocation failed; clearing local credentials anyway", "err", derr)
				}
			}
			cancel()
		}
	} else {
		ctx.Log.Debug("best-effort session revocation failed; clearing local credentials anyway", "err", err)
	}

	_ = auth.Clear(*paths)
	output.Success("Logged out.")
	return nil
}
