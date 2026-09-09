package auth

import (
	"strings"
	"testing"
)

func TestGenerateVerifier(t *testing.T) {
	v, err := GenerateVerifier()
	if err != nil {
		t.Fatal(err)
	}
	if len(v) < 43 || len(v) > 128 {
		t.Fatalf("verifier length %d out of RFC 7636 range", len(v))
	}
	v2, _ := GenerateVerifier()
	if v == v2 {
		t.Fatal("two verifiers should not collide")
	}
}

func TestChallengeFromVerifier(t *testing.T) {
	// RFC 7636 appendix B worked example.
	verifier := "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
	want := "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
	if got := ChallengeFromVerifier(verifier); got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestGenerateVerifierIsUnpaddedBase64URLCharset(t *testing.T) {
	v, err := GenerateVerifier()
	if err != nil {
		t.Fatal(err)
	}
	if strings.ContainsAny(v, "+/=") {
		t.Fatalf("verifier must be unpadded base64url (no +, /, =), got %q", v)
	}
	for _, r := range v {
		unreserved := (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_'
		if !unreserved {
			t.Fatalf("verifier contains a non-base64url-unreserved character %q in %q", r, v)
		}
	}
}

func TestGeneratePkcePair(t *testing.T) {
	pair, err := GeneratePkcePair()
	if err != nil {
		t.Fatal(err)
	}
	if ChallengeFromVerifier(pair.Verifier) != pair.Challenge {
		t.Fatal("challenge does not match verifier")
	}
}
