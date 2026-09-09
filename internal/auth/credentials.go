package auth

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Credentials is the per-issuer token set persisted at
// ~/.mcpwarp/credentials/<sha256(issuer)[:16]>.json — same field names as
// Node's CredentialsSchema (auth/credentials.ts). Deliberately no id_token:
// display-only per OIDC, not worth persisting as another secret.
type Credentials struct {
	AccessToken       string `json:"access_token"`
	RefreshToken      string `json:"refresh_token"`
	ExpiresAt         int64  `json:"expires_at"`
	RefreshExpiresAt  *int64 `json:"refresh_expires_at,omitempty"`
	TokenType         string `json:"token_type"`
	Scope             string `json:"scope"`
	Sub               string `json:"sub"`
	Email             string `json:"email,omitempty"`
	PreferredUsername string `json:"preferred_username,omitempty"`
	Issuer            string `json:"issuer"`
	ClientID          string `json:"client_id"`
	SavedAt           int64  `json:"saved_at"`
}

// Paths is the resolved location of one issuer's credentials file.
type Paths struct {
	Dir    string
	File   string
	Issuer string
}

// issuerHash truncates sha256(issuer) to 16 hex chars — short enough for a
// filename, long enough that a collision between two real issuers is not a
// practical concern.
func issuerHash(issuer string) string {
	sum := sha256.Sum256([]byte(issuer))
	return hex.EncodeToString(sum[:])[:16]
}

// CredentialsPathFor computes the credentials file path without creating
// any directory. home defaults to the current user's home directory.
func CredentialsPathFor(issuer, home string) (string, error) {
	if home == "" {
		h, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		home = h
	}
	return filepath.Join(home, ".mcpwarp", "credentials", issuerHash(issuer)+".json"), nil
}

// CredentialsPaths resolves the full Paths for an issuer.
func CredentialsPaths(issuer, home string) (Paths, error) {
	file, err := CredentialsPathFor(issuer, home)
	if err != nil {
		return Paths{}, err
	}
	return Paths{Dir: filepath.Dir(file), File: file, Issuer: issuer}, nil
}

var (
	warnedCorrupt bool
	warnedMu      sync.Mutex
)

// ResetCorruptWarningForTests clears the once-per-process corrupt-file
// warning latch so tests don't leak state into each other.
func ResetCorruptWarningForTests() {
	warnedMu.Lock()
	defer warnedMu.Unlock()
	warnedCorrupt = false
}

func warnCorruptOnce(path, reason string, warn func(string)) {
	warnedMu.Lock()
	defer warnedMu.Unlock()
	if warnedCorrupt {
		return
	}
	warnedCorrupt = true
	warn(fmt.Sprintf("credentials file at %s is corrupt or invalid (%s); treating as not logged in", path, reason))
}

// Load never fails: missing, unreadable, corrupt, schema-invalid, and
// issuer-mismatch files all resolve to (nil, "not logged in") — matching
// Node's auth/credentials.ts load(), which never throws.
func Load(paths Paths, warn func(string)) *Credentials {
	if warn == nil {
		warn = func(message string) { fmt.Fprintf(os.Stderr, "! %s\n", message) }
	}

	raw, err := os.ReadFile(paths.File)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		if os.IsPermission(err) {
			warn(fmt.Sprintf("credentials file at %s could not be read (permission denied): %s", paths.File, err.Error()))
			return nil
		}
		warnCorruptOnce(paths.File, err.Error(), warn)
		return nil
	}

	var shadow credentialsShadow
	if err := json.Unmarshal(raw, &shadow); err != nil {
		warnCorruptOnce(paths.File, err.Error(), warn)
		return nil
	}

	if missing := missingRequiredFields(&shadow); missing != "" {
		warnCorruptOnce(paths.File, missing, warn)
		return nil
	}

	creds := Credentials{
		AccessToken:       *shadow.AccessToken,
		RefreshToken:      *shadow.RefreshToken,
		ExpiresAt:         *shadow.ExpiresAt,
		RefreshExpiresAt:  shadow.RefreshExpiresAt,
		TokenType:         *shadow.TokenType,
		Scope:             *shadow.Scope,
		Sub:               *shadow.Sub,
		Email:             derefOr(shadow.Email, ""),
		PreferredUsername: derefOr(shadow.PreferredUsername, ""),
		Issuer:            *shadow.Issuer,
		ClientID:          *shadow.ClientID,
		SavedAt:           *shadow.SavedAt,
	}

	// The file is keyed by a hash of the issuer, so a mismatch here means
	// either an astronomically unlikely hash collision or a moved/tampered
	// file — either way it does not belong to this issuer's path.
	if creds.Issuer != paths.Issuer {
		return nil
	}

	return &creds
}

// credentialsShadow decodes the same JSON as Credentials but with every
// required field as a pointer, so presence (nil) can be told apart from a
// present-but-zero-value field (`scope: ""`, `saved_at: 0`, ...) — those are
// legal per CredentialsSchema (auth/credentials.ts), which has no min/max on
// them; only refresh_token carries a min(1) length rule of its own.
type credentialsShadow struct {
	AccessToken       *string `json:"access_token"`
	RefreshToken      *string `json:"refresh_token"`
	ExpiresAt         *int64  `json:"expires_at"`
	RefreshExpiresAt  *int64  `json:"refresh_expires_at"`
	TokenType         *string `json:"token_type"`
	Scope             *string `json:"scope"`
	Sub               *string `json:"sub"`
	Email             *string `json:"email"`
	PreferredUsername *string `json:"preferred_username"`
	Issuer            *string `json:"issuer"`
	ClientID          *string `json:"client_id"`
	SavedAt           *int64  `json:"saved_at"`
}

// missingRequiredFields mirrors CredentialsSchema's required (non-optional)
// fields: access_token, refresh_token (min length 1), expires_at, token_type,
// scope, sub, issuer, client_id, saved_at.
func missingRequiredFields(s *credentialsShadow) string {
	switch {
	case s.AccessToken == nil:
		return "access_token: required"
	case s.RefreshToken == nil || *s.RefreshToken == "":
		return "refresh_token: required, min length 1"
	case s.ExpiresAt == nil:
		return "expires_at: required"
	case s.TokenType == nil:
		return "token_type: required"
	case s.Scope == nil:
		return "scope: required"
	case s.Sub == nil:
		return "sub: required"
	case s.Issuer == nil:
		return "issuer: required"
	case s.ClientID == nil:
		return "client_id: required"
	case s.SavedAt == nil:
		return "saved_at: required"
	default:
		return ""
	}
}

func derefOr(p *string, def string) string {
	if p == nil {
		return def
	}
	return *p
}

// DefaultStaleBufferMs is the proactive-refresh buffer used everywhere
// except status's own point-in-time check (which passes 0) — matching
// Node's isAccessTokenStale default (auth/credentials.ts).
const DefaultStaleBufferMs int64 = 60_000

// IsAccessTokenStale reports whether expires_at - bufferMs has passed.
func IsAccessTokenStale(c *Credentials, bufferMs int64, now int64) bool {
	return now >= c.ExpiresAt-bufferMs
}

// Save writes creds atomically: a temp file in the same directory →
// write/fsync/close → chmod 0600 → rename into place. The credentials
// directory is created 0700 if it doesn't exist yet, and chmod 0700 only
// when this call is the one that created it — an existing directory a user
// may have deliberately re-permissioned is left alone on later saves.
func Save(creds Credentials, paths Paths) error {
	shadow := credentialsShadow{
		AccessToken: &creds.AccessToken, RefreshToken: &creds.RefreshToken,
		ExpiresAt: &creds.ExpiresAt, RefreshExpiresAt: creds.RefreshExpiresAt,
		TokenType: &creds.TokenType, Scope: &creds.Scope, Sub: &creds.Sub,
		Issuer: &creds.Issuer, ClientID: &creds.ClientID, SavedAt: &creds.SavedAt,
	}
	if missing := missingRequiredFields(&shadow); missing != "" {
		return fmt.Errorf("refusing to save invalid credentials: %s", missing)
	}

	info, statErr := os.Stat(paths.Dir)
	dirExisted := statErr == nil && info.IsDir()
	if err := os.MkdirAll(paths.Dir, 0o700); err != nil {
		return err
	}
	if !dirExisted {
		if err := os.Chmod(paths.Dir, 0o700); err != nil {
			return err
		}
	}

	tmp := filepath.Join(paths.Dir, fmt.Sprintf(".credentials.json.%d.%d.tmp", os.Getpid(), time.Now().UnixMilli()))
	content, err := json.MarshalIndent(creds, "", "  ")
	if err != nil {
		return err
	}

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(content); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Chmod(tmp, 0o600); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, paths.File); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// Clear removes the credentials file; a missing file is not an error.
func Clear(paths Paths) error {
	if err := os.Remove(paths.File); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// DecodeJWTPayload decodes a JWT's payload with NO signature verification —
// display only, never used for an authorization decision. Returns nil on
// anything malformed. Requires all three dot-separated parts even though
// only the payload is read, so a truncated or non-JWT string is rejected
// up front.
func DecodeJWTPayload(token string) map[string]any {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[1] == "" {
		return nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil
	}
	return payload
}
