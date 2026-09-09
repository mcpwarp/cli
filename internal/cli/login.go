package cli

import (
	"fmt"
	"os"
	"time"

	"github.com/mcpwarp/cli/internal/auth"
	"github.com/mcpwarp/cli/internal/output"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

func defaultStdoutIsTTY() bool {
	return term.IsTerminal(int(os.Stdout.Fd()))
}

// loginDeps are the seams command-level tests need: a redirectable TTY
// check, HTTP client, browser opener, spinner, clock, and credentials path
// — mirrors Node's LoginDeps (login.ts).
type loginDeps struct {
	isTTY       func() bool
	client      auth.HTTPDoer
	openBrowser func(url string)
	spinner     func(message string) func()
	now         func() int64
	pollDeps    auth.PollForTokenDeps
	paths       *auth.Paths
}

func (d loginDeps) resolve(ctx *Context) loginDeps {
	if d.isTTY == nil {
		d.isTTY = defaultStdoutIsTTY
	}
	if d.spinner == nil {
		d.spinner = output.StartSpinner
	}
	if d.now == nil {
		d.now = nowMillis
	}
	if d.pollDeps.Now == nil {
		// Same clock as savedAt below, unless a test overrides pollDeps.Now
		// on its own to drive the poll deadline independently.
		now := d.now
		d.pollDeps.Now = func() time.Time { return time.UnixMilli(now()) }
	}
	if d.openBrowser == nil {
		d.openBrowser = func(url string) { auth.OpenBrowser(url, ctx.Log) }
	}
	return d
}

func newLoginCommand(ctxFor func(*cobra.Command) *Context) *cobra.Command {
	var noBrowser bool
	cmd := &cobra.Command{
		Use:   "login",
		Short: "Log in via the device authorization flow",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runLogin(ctxFor(cmd), noBrowser, loginDeps{})
		},
	}
	cmd.Flags().BoolVar(&noBrowser, "no-browser", false, "print the login URL instead of opening a browser")
	return cmd
}

// runLogin mirrors Node's cli/commands/login.ts: always overwrites any
// existing credentials without prompting.
func runLogin(ctx *Context, noBrowser bool, deps loginDeps) error {
	ctx.Log.Debug("mcpwarp login invoked")
	deps = deps.resolve(ctx)

	if !deps.isTTY() {
		output.Error("login requires an interactive terminal")
		return RuntimeError()
	}

	settings, err := auth.ResolveSettings(ctx.IssuerOverride)
	if err != nil {
		return reportConfigError(err)
	}
	issuer := auth.BuildIssuer(settings.AuthURL, settings.Realm)

	background := ctx.Context()
	oidc, err := auth.Discover(background, issuer, deps.client)
	if err != nil {
		output.Error(err.Error())
		return RuntimeError()
	}

	pair, err := auth.GeneratePkcePair()
	if err != nil {
		output.Error(err.Error())
		return RuntimeError()
	}

	device, err := auth.RequestDeviceCode(background, auth.RequestDeviceCodeParams{
		DeviceAuthorizationEndpoint: oidc.DeviceAuthorizationEndpoint,
		ClientID:                    settings.ClientID,
		Scope:                       settings.Scope,
		CodeChallenge:               pair.Challenge,
	}, deps.client)
	if err != nil {
		output.Error(fmt.Sprintf("could not start login: %s", err.Error()))
		return RuntimeError()
	}

	output.Info("To log in, open:")
	verificationURI := device.VerificationURI
	if device.VerificationURIComplete != "" {
		verificationURI = device.VerificationURIComplete
	}
	output.Info("  " + verificationURI)
	if device.VerificationURIComplete == "" {
		output.Info("and enter code: " + device.UserCode)
	}

	if !noBrowser {
		deps.openBrowser(verificationURI)
	}

	stopSpinner := deps.spinner("waiting for you to finish logging in in the browser...")
	pollDeps := deps.pollDeps
	if pollDeps.Client == nil {
		pollDeps.Client = deps.client
	}
	tokens, err := auth.PollForToken(background, auth.PollForTokenParams{
		TokenEndpoint: oidc.TokenEndpoint,
		ClientID:      settings.ClientID,
		DeviceCode:    device.DeviceCode,
		CodeVerifier:  pair.Verifier,
		Interval:      device.Interval,
		ExpiresIn:     device.ExpiresIn,
	}, pollDeps)
	stopSpinner()
	if err != nil {
		output.Error(describeLoginError(err))
		return RuntimeError()
	}

	if tokens.RefreshToken == "" {
		output.Error("the auth server returned no refresh token; check that `offline_access` is assigned to the mcpwarp-cli client")
		return RuntimeError()
	}

	savedAt := deps.now()
	payload := auth.DecodeJWTPayload(tokens.AccessToken)
	var sub, email, preferredUsername string
	if payload != nil {
		if s, ok := payload["sub"].(string); ok {
			sub = s
		}
		if e, ok := payload["email"].(string); ok {
			email = e
		}
		if u, ok := payload["preferred_username"].(string); ok {
			preferredUsername = u
		}
	}
	if sub == "" {
		output.Error("the access token has no `sub` claim; check the auth server's client/mapper configuration")
		return RuntimeError()
	}

	scope := settings.Scope
	if tokens.Scope != nil {
		scope = *tokens.Scope
	}
	creds := auth.Credentials{
		AccessToken:       tokens.AccessToken,
		RefreshToken:      tokens.RefreshToken,
		ExpiresAt:         savedAt + tokens.ExpiresIn*1000,
		TokenType:         tokens.TokenType,
		Scope:             scope,
		Sub:               sub,
		Email:             email,
		PreferredUsername: preferredUsername,
		Issuer:            issuer,
		ClientID:          settings.ClientID,
		SavedAt:           savedAt,
	}
	if tokens.RefreshExpiresIn != nil {
		v := savedAt + *tokens.RefreshExpiresIn*1000
		creds.RefreshExpiresAt = &v
	}

	paths := deps.paths
	if paths == nil {
		p, err := auth.CredentialsPaths(issuer, ctx.HomeDir)
		if err != nil {
			output.Error(err.Error())
			return RuntimeError()
		}
		paths = &p
	}
	if err := auth.Save(creds, *paths); err != nil {
		output.Error(fmt.Sprintf("could not write credentials: %s", err.Error()))
		return RuntimeError()
	}

	who := creds.Email
	if who == "" {
		who = creds.PreferredUsername
	}
	if who == "" {
		who = creds.Sub
	}
	output.Success(fmt.Sprintf("Logged in as %s", who))
	return nil
}

func describeLoginError(err error) string {
	dfe, ok := err.(*auth.DeviceFlowError)
	if !ok {
		return fmt.Sprintf("login failed: %s", err.Error())
	}
	switch dfe.Code {
	case auth.CodeExpiredToken:
		return "login code expired before you finished — run `mcpwarp login` again"
	case auth.CodeAccessDenied:
		return "login was declined"
	default:
		return fmt.Sprintf("login failed: %s", dfe.Error())
	}
}
