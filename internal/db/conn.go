package db

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Conn is the minimal query surface repositories need, satisfied by both a
// *pgxpool.Pool and a pgx.Tx — repositories are constructed fresh per call
// with whichever Conn is appropriate (a request-scoped RLS transaction for
// tenant-scoped tables, the bare pool for tables with no RLS to enforce).
type Conn interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Pool wraps a pgx connection pool (ADR-0011: the only place this module
// imports pgx directly).
type Pool struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, connString string) (*Pool, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("opening postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging postgres: %w", err)
	}
	return &Pool{pool: pool}, nil
}

func (p *Pool) Close() {
	p.pool.Close()
}

// Conn returns the bare pool as a Conn, for repository calls against
// tables with no RLS policy to enforce (users, refresh_tokens).
func (p *Pool) Conn() Conn {
	return p.pool
}

// WithTx runs fn inside a transaction with app.current_user_id and/or
// app.current_org_id set as transaction-local session variables
// (ADR-0010, database-schema.md §3) for RLS to key off. Pass uuid.Nil for
// either to leave it unset — e.g. signup, before any user or org exists
// yet, or a deep-by-ID route that hasn't resolved org_id yet (see
// SetCurrentOrg). fn's error aborts the transaction; a nil return commits.
func (p *Pool) WithTx(ctx context.Context, userID, orgID uuid.UUID, fn func(ctx context.Context, conn Conn) error) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if userID != uuid.Nil {
		if err := SetCurrentUser(ctx, tx, userID); err != nil {
			return err
		}
	}
	if orgID != uuid.Nil {
		if err := SetCurrentOrg(ctx, tx, orgID); err != nil {
			return err
		}
	}

	if err := fn(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	return nil
}

// SetCurrentUser sets app.current_user_id on conn's current transaction.
// Exposed separately from WithTx for signup, which must set it mid-tx
// (right after the new user row is created, before the user's own id is
// known to WithTx's caller) so the memberships RLS policy's "own
// membership" branch admits the very first INSERT into memberships.
func SetCurrentUser(ctx context.Context, conn Conn, userID uuid.UUID) error {
	if _, err := conn.Exec(ctx, `SELECT set_config('app.current_user_id', $1, true)`, userID.String()); err != nil {
		return fmt.Errorf("setting app.current_user_id: %w", err)
	}
	return nil
}

// SetCurrentOrg sets app.current_org_id on conn's current transaction. See
// SetCurrentUser for why this is exposed independently of WithTx: a
// deep-by-ID route (api-conventions.md §2) resolves org_id via a query
// scoped only by app.current_user_id (database-schema.md §3), then sets it
// mid-transaction once known, before performing the actual operation.
func SetCurrentOrg(ctx context.Context, conn Conn, orgID uuid.UUID) error {
	if _, err := conn.Exec(ctx, `SELECT set_config('app.current_org_id', $1, true)`, orgID.String()); err != nil {
		return fmt.Errorf("setting app.current_org_id: %w", err)
	}
	return nil
}
