package auth

import (
	"strconv"
	"strings"
)

// StaticTokenEnvVar is the only way to opt into PAT mode — no --token flag,
// since a token in shell history is a worse leak than one in a file.
const StaticTokenEnvVar = "MCPWARP_TOKEN"

// PATPrefix is the expected prefix of a dashboard-issued personal access
// token; its absence only triggers a warning, never a rejection.
const PATPrefix = "mcpwarp_pat_"

// patIDLength is the length of the id segment between PATPrefix and the
// trailing secret, matching the tunnel's own shape gate for a dashboard-
// issued PAT: `mcpwarp_pat_<12-char id>_<secret>`.
const patIDLength = 12

// maxPATLength is the tunnel's own upper bound on a PAT's total length.
const maxPATLength = 200

// StaticTokenProvider returns token verbatim as the Bearer — no file, no
// refresh, no decode. warn (if non-nil) is called once if token doesn't
// look like a well-formed PAT.
type StaticTokenProvider struct {
	token string
}

// staticTokenShapeIssue reports what, if anything, looks wrong about token
// against the tunnel's own PAT shape gate — prefix `mcpwarp_pat_`, a
// 12-character id, `_`, then a non-empty secret, all within 200 bytes total
// — or "" if it looks well-formed. This is a sanity check only: a token
// that passes is still just passed verbatim to the tunnel, which is the
// actual authority on whether it's valid.
func staticTokenShapeIssue(token string) string {
	if len(token) > maxPATLength {
		return "is too long (over " + strconv.Itoa(maxPATLength) + " bytes)"
	}
	if !strings.HasPrefix(token, PATPrefix) {
		return "does not start with `" + PATPrefix + "`"
	}
	rest := token[len(PATPrefix):]
	if len(rest) <= patIDLength || rest[patIDLength] != '_' {
		return "does not have a " + strconv.Itoa(patIDLength) + "-character id followed by `_` after the prefix"
	}
	if rest[patIDLength+1:] == "" {
		return "is missing the secret after the id"
	}
	return ""
}

// NewStaticTokenProvider wraps token, warning once via warn if it doesn't
// look like a well-formed PAT (staticTokenShapeIssue).
func NewStaticTokenProvider(token string, warn func(string)) *StaticTokenProvider {
	if warn != nil {
		if issue := staticTokenShapeIssue(token); issue != "" {
			warn("MCPWARP_TOKEN " + issue + " (expected `" + PATPrefix + "<12-char id>_<secret>`, ≤" + strconv.Itoa(maxPATLength) + " bytes)")
		}
	}
	return &StaticTokenProvider{token: token}
}

// Token returns the static token verbatim.
func (p *StaticTokenProvider) Token() string { return p.token }
