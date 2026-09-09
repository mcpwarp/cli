package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
)

// verifierBytes matches Node's pkce.ts: base64url(32 random bytes) = 43
// chars, the RFC 7636 §4.1 minimum.
const verifierBytes = 32

// PkcePair is a freshly generated PKCE verifier/challenge pair (RFC 7636
// §4.1-4.2, S256 only).
type PkcePair struct {
	Verifier  string
	Challenge string
}

// GenerateVerifier returns 43 chars of [A-Za-z0-9-_], base64url(32 random
// bytes), unpadded.
func GenerateVerifier() (string, error) {
	buf := make([]byte, verifierBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

// ChallengeFromVerifier is BASE64URL(SHA256(verifier)), unpadded.
func ChallengeFromVerifier(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// GeneratePkcePair generates a verifier and derives its S256 challenge.
func GeneratePkcePair() (PkcePair, error) {
	verifier, err := GenerateVerifier()
	if err != nil {
		return PkcePair{}, err
	}
	return PkcePair{Verifier: verifier, Challenge: ChallengeFromVerifier(verifier)}, nil
}
