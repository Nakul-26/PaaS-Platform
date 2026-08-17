package auth

import (
	"errors"

	"golang.org/x/crypto/bcrypt"
)

// HashPassword hashes a plaintext password for storage (ADR-0008). bcrypt's
// built-in cost/salt handling is sufficient for Phase 1; revisit only if a
// concrete reason to swap (e.g. argon2) shows up (ADR-0011).
func HashPassword(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hash), nil
}

// VerifyPassword reports whether password matches the stored hash. A
// mismatch is returned as a plain (false, nil), not an error — callers
// shouldn't treat "wrong password" as a system failure.
func VerifyPassword(hash, password string) (bool, error) {
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, bcrypt.ErrMismatchedHashAndPassword) {
		return false, nil
	}
	return false, err
}
