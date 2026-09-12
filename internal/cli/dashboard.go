package cli

import (
	"github.com/mcpwarp/cli/internal/auth"
	"github.com/mcpwarp/cli/internal/output"
	"github.com/spf13/cobra"
)

// openBrowserFn is auth.OpenBrowser behind a var so a test can stub it and
// never launch a real browser.
var openBrowserFn = auth.OpenBrowser

func newDashboardCommand(ctxFor func(*cobra.Command) *Context) *cobra.Command {
	return &cobra.Command{
		Use:   "dashboard",
		Short: "Open the mcpwarp dashboard in your browser",
		Long:  "Open the mcpwarp dashboard in your browser\n\nThe URL is printed first, so this works on headless machines too.\nMCPWARP_WEB_URL overrides the default.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDashboard(ctxFor(cmd))
		},
	}
}

// runDashboard mirrors `up`'s own resolution of the dashboard URL
// (resolveWebURL), then best-effort opens it — a missing browser must
// never fail the command.
func runDashboard(ctx *Context) error {
	ctx.Log.Debug("mcpwarp dashboard invoked")

	webURL, err := resolveWebURL()
	if err != nil {
		return reportConfigError(err)
	}

	output.Info(webURL)
	openBrowserFn(webURL, ctx.Log)
	return nil
}
