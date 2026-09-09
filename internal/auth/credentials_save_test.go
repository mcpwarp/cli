package auth

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"runtime"
	"testing"
)

func TestSave(t *testing.T) {
	t.Run("round-trips and sets 0600/0700 permissions", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("posix permission bits")
		}
		dir := t.TempDir()
		c := sample()
		paths, err := CredentialsPaths(c.Issuer, dir)
		if err != nil {
			t.Fatal(err)
		}
		if err := Save(c, paths); err != nil {
			t.Fatal(err)
		}

		fi, err := os.Stat(paths.File)
		if err != nil {
			t.Fatal(err)
		}
		if fi.Mode().Perm() != 0o600 {
			t.Fatalf("got mode %v", fi.Mode().Perm())
		}
		di, err := os.Stat(paths.Dir)
		if err != nil {
			t.Fatal(err)
		}
		if di.Mode().Perm() != 0o700 {
			t.Fatalf("got dir mode %v", di.Mode().Perm())
		}

		got := Load(paths, nil)
		if got == nil || got.AccessToken != c.AccessToken {
			t.Fatalf("round-trip failed: %#v", got)
		}
	})

	t.Run("leaves an existing directory's permissions alone", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("posix permission bits")
		}
		dir := t.TempDir()
		c := sample()
		paths, err := CredentialsPaths(c.Issuer, dir)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(paths.Dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := Save(c, paths); err != nil {
			t.Fatal(err)
		}
		di, err := os.Stat(paths.Dir)
		if err != nil {
			t.Fatal(err)
		}
		if di.Mode().Perm() != 0o755 {
			t.Fatalf("save() should not have re-chmod'd a pre-existing dir, got %v", di.Mode().Perm())
		}
	})

	t.Run("no leftover temp files after a successful save", func(t *testing.T) {
		dir := t.TempDir()
		c := sample()
		paths, err := CredentialsPaths(c.Issuer, dir)
		if err != nil {
			t.Fatal(err)
		}
		if err := Save(c, paths); err != nil {
			t.Fatal(err)
		}
		entries, err := os.ReadDir(paths.Dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 1 {
			t.Fatalf("expected exactly the credentials file, got %v", entries)
		}
	})
}

func TestClear(t *testing.T) {
	dir := t.TempDir()
	c := sample()
	paths, err := CredentialsPaths(c.Issuer, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := Save(c, paths); err != nil {
		t.Fatal(err)
	}
	if err := Clear(paths); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(paths.File); !os.IsNotExist(err) {
		t.Fatal("file should be gone")
	}
	// Clearing an already-missing file is not an error.
	if err := Clear(paths); err != nil {
		t.Fatalf("clearing a missing file should be a no-op, got %v", err)
	}
}

func b64url(m map[string]any) string {
	raw, _ := json.Marshal(m)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func TestDecodeJWTPayload(t *testing.T) {
	t.Run("decodes a well-formed JWT payload", func(t *testing.T) {
		token := "header." + b64url(map[string]any{"sub": "u1", "email": "u1@example.com"}) + ".sig"
		payload := DecodeJWTPayload(token)
		if payload == nil {
			t.Fatal("expected a payload")
		}
		if payload["sub"] != "u1" {
			t.Fatalf("got %v", payload["sub"])
		}
	})

	cases := []struct {
		name    string
		token   string
		wantNil bool
	}{
		{"empty string", "", true},
		{"only two parts", "a.b", true},
		{"four parts", "a.b.c.d", true},
		{"three parts, empty payload object", "a." + b64url(map[string]any{}) + ".c", false},
		{"unparseable base64 payload", "a.not-base64!.c", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := DecodeJWTPayload(tc.token)
			if tc.wantNil && got != nil {
				t.Fatalf("expected nil, got %#v", got)
			}
			if !tc.wantNil && got == nil {
				t.Fatal("expected a non-nil (possibly empty) payload")
			}
		})
	}

	t.Run("rejects a token that isn't 3 dot-separated parts", func(t *testing.T) {
		if DecodeJWTPayload("only.two") != nil {
			t.Fatal("expected nil")
		}
		if DecodeJWTPayload("") != nil {
			t.Fatal("expected nil")
		}
	})

	t.Run("rejects an unparseable payload segment", func(t *testing.T) {
		if DecodeJWTPayload("a.not-base64!!!.c") != nil {
			t.Fatal("expected nil")
		}
	})
}
