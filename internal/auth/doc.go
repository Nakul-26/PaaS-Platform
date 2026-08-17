// Package auth implements password hashing, JWT access-token
// issuance/verification, and rotating refresh tokens (ADR-0008), plus the
// role/permission matrix from docs/rbac-multitenancy.md §2. Per-route
// authorization checks (resolving a caller's role for a specific org and
// applying the matrix) live in the consuming service, since the resolution
// mechanics differ per route shape (docs/api-conventions.md §2) — this
// package provides the primitives, not the wiring.
package auth
