package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/google/uuid"
)

// AccessTokenTTL is the JWT access-token lifetime (ADR-0008: "target 15
// minutes" — short enough to bound a compromised token's blast radius,
// since access tokens can't be revoked mid-lifetime).
const AccessTokenTTL = 15 * time.Minute

// RefreshTokenTTL is how long an unused refresh token remains redeemable.
const RefreshTokenTTL = 30 * 24 * time.Hour

// TokenIssuer issues and verifies JWT access tokens, and generates the
// opaque refresh tokens ADR-0008 requires be stored hashed, never as JWTs
// themselves (a refresh token is only ever looked up by hash against
// Postgres, never decoded client-side).
type TokenIssuer struct {
	signingKey []byte
}

func NewTokenIssuer(signingKey string) (*TokenIssuer, error) {
	if signingKey == "" {
		return nil, errors.New("signing key must not be empty")
	}
	return &TokenIssuer{signingKey: []byte(signingKey)}, nil
}

type accessClaims struct {
	jwt.RegisteredClaims
}

// IssueAccessToken mints a short-lived JWT carrying only the user's
// identity (ADR-0008: authorization is re-checked from Postgres on every
// request, never trusted from the token).
func (i *TokenIssuer) IssueAccessToken(userID uuid.UUID) (token string, expiresAt time.Time, err error) {
	expiresAt = time.Now().Add(AccessTokenTTL)
	claims := accessClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID.String(),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ExpiresAt: jwt.NewNumericDate(expiresAt),
		},
	}
	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(i.signingKey)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("signing access token: %w", err)
	}
	return signed, expiresAt, nil
}

// VerifyAccessToken checks the token's signature and expiry and returns the
// authenticated user's ID.
func (i *TokenIssuer) VerifyAccessToken(token string) (uuid.UUID, error) {
	var claims accessClaims
	parsed, err := jwt.ParseWithClaims(token, &claims, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return i.signingKey, nil
	})
	if err != nil {
		return uuid.Nil, fmt.Errorf("parsing access token: %w", err)
	}
	if !parsed.Valid {
		return uuid.Nil, errors.New("access token is not valid")
	}
	userID, err := uuid.Parse(claims.Subject)
	if err != nil {
		return uuid.Nil, fmt.Errorf("access token subject is not a valid user id: %w", err)
	}
	return userID, nil
}

// GenerateRefreshToken returns a high-entropy opaque token (given to the
// caller) and its SHA-256 hash (stored in Postgres). SHA-256, not bcrypt: a
// refresh token is already a 256-bit random value, not a low-entropy
// human-chosen password, so a fast cryptographic hash is the correct
// primitive here — bcrypt's deliberate slowness defends against guessing a
// password, which doesn't apply to a value nobody could guess in the first
// place.
func GenerateRefreshToken() (token, hash string, err error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", "", fmt.Errorf("generating refresh token: %w", err)
	}
	token = base64.RawURLEncoding.EncodeToString(raw)
	return token, HashRefreshToken(token), nil
}

// HashRefreshToken hashes a caller-supplied refresh token for lookup
// against the stored hash — never compare or store the plaintext.
func HashRefreshToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// apiKeyPrefix marks a bearer credential as an API key rather than a JWT
// access token (api-conventions.md §3) — Authenticate branches on it.
const apiKeyPrefix = "pk_live_"

const base62Alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

// GenerateAPIKey returns a new pk_live_-prefixed API key plaintext (handed
// back to the caller exactly once, at creation — phase-6-multi-tenant-saas.md
// Task 5) and its SHA-256 hash (the only thing ever stored, via
// APIKeyRepository.Create) — same hash-not-plaintext discipline as
// GenerateRefreshToken.
func GenerateAPIKey() (key, hash string, err error) {
	suffix, err := randomBase62(32)
	if err != nil {
		return "", "", fmt.Errorf("generating api key: %w", err)
	}
	key = apiKeyPrefix + suffix
	return key, HashAPIKey(key), nil
}

// HashAPIKey hashes a caller-supplied API key for lookup against the stored
// hash — never compare or store the plaintext (same reasoning as
// HashRefreshToken: a generated key is already high-entropy, so a fast
// cryptographic hash is the right primitive, not bcrypt).
func HashAPIKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// randomBase62 returns a cryptographically random base62 string of length n
// (api-conventions.md §3 / phase-6-multi-tenant-saas.md Task 5's "32 random
// bytes, base62"). Rejection sampling — discarding any byte that would bias
// the result, rather than a plain `randomByte % 62` — avoids the slight
// skew a naive modulo would introduce, since 256 isn't a multiple of 62.
func randomBase62(n int) (string, error) {
	const maxUnbiased = 62 * 4 // largest multiple of 62 that fits in a byte
	out := make([]byte, 0, n)
	buf := make([]byte, n)
	for len(out) < n {
		if _, err := rand.Read(buf); err != nil {
			return "", err
		}
		for _, b := range buf {
			if b >= maxUnbiased {
				continue
			}
			out = append(out, base62Alphabet[b%62])
			if len(out) == n {
				break
			}
		}
	}
	return string(out), nil
}
