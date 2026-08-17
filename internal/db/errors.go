package db

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

// ErrNotFound is returned by a repository's single-row lookups when no row
// exists — either because it genuinely doesn't, or (deliberately
// indistinguishable, per rbac-multitenancy.md §5) because RLS filtered it
// out for a caller who isn't a member of its org. Handlers map this to
// HTTP 404, never 403, for exactly that reason.
var ErrNotFound = errors.New("db: not found")

// ErrConflict is returned when an insert/update violates a unique
// constraint (e.g. a duplicate project slug within an org, or a duplicate
// email at signup). Handlers map this to HTTP 409.
var ErrConflict = errors.New("db: conflict")

const pgUniqueViolation = "23505"

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation
}
