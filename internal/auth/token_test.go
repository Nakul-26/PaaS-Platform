package auth

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestAccessToken_IssueAndVerify(t *testing.T) {
	issuer, err := NewTokenIssuer("test-signing-key")
	if err != nil {
		t.Fatalf("NewTokenIssuer: %v", err)
	}
	userID := uuid.New()

	token, expiresAt, err := issuer.IssueAccessToken(userID)
	if err != nil {
		t.Fatalf("IssueAccessToken: %v", err)
	}
	if time.Until(expiresAt) > AccessTokenTTL || time.Until(expiresAt) <= 0 {
		t.Fatalf("expiresAt %v not within (0, %v] of now", expiresAt, AccessTokenTTL)
	}

	got, err := issuer.VerifyAccessToken(token)
	if err != nil {
		t.Fatalf("VerifyAccessToken: %v", err)
	}
	if got != userID {
		t.Fatalf("VerifyAccessToken returned %s, want %s", got, userID)
	}
}

func TestAccessToken_RejectsWrongSigningKey(t *testing.T) {
	issuerA, _ := NewTokenIssuer("key-a")
	issuerB, _ := NewTokenIssuer("key-b")

	token, _, err := issuerA.IssueAccessToken(uuid.New())
	if err != nil {
		t.Fatalf("IssueAccessToken: %v", err)
	}
	if _, err := issuerB.VerifyAccessToken(token); err == nil {
		t.Fatal("expected a token signed by a different key to fail verification")
	}
}

func TestAccessToken_RejectsGarbage(t *testing.T) {
	issuer, _ := NewTokenIssuer("test-signing-key")
	if _, err := issuer.VerifyAccessToken("not-a-jwt"); err == nil {
		t.Fatal("expected a malformed token to fail verification")
	}
}

func TestNewTokenIssuer_RejectsEmptyKey(t *testing.T) {
	if _, err := NewTokenIssuer(""); err == nil {
		t.Fatal("expected an empty signing key to be rejected")
	}
}

func TestGenerateRefreshToken(t *testing.T) {
	tokenA, hashA, err := GenerateRefreshToken()
	if err != nil {
		t.Fatalf("GenerateRefreshToken: %v", err)
	}
	tokenB, hashB, err := GenerateRefreshToken()
	if err != nil {
		t.Fatalf("GenerateRefreshToken: %v", err)
	}

	if tokenA == tokenB {
		t.Fatal("two generated refresh tokens must not collide")
	}
	if hashA != HashRefreshToken(tokenA) {
		t.Fatal("HashRefreshToken(tokenA) must reproduce the hash returned by GenerateRefreshToken")
	}
	if hashA == hashB {
		t.Fatal("hashes of two different tokens must not collide")
	}
}
