package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type User struct {
	ID           uuid.UUID
	Email        string
	PasswordHash string
	CreatedAt    time.Time
}

// UserRepository is the port apiserver depends on (ADR-0011). users carries
// no RLS (database-schema.md §2 — identity isn't tenant-scoped), so its
// methods work against any Conn.
type UserRepository interface {
	Create(ctx context.Context, email, passwordHash string) (User, error)
	GetByEmail(ctx context.Context, email string) (User, error)
	GetByID(ctx context.Context, id uuid.UUID) (User, error)
}

type userRepository struct{ conn Conn }

func NewUserRepository(conn Conn) UserRepository {
	return &userRepository{conn: conn}
}

func (r *userRepository) Create(ctx context.Context, email, passwordHash string) (User, error) {
	var u User
	err := r.conn.QueryRow(ctx,
		`INSERT INTO users (email, password_hash) VALUES ($1, $2) RETURNING id, email, password_hash, created_at`,
		email, passwordHash,
	).Scan(&u.ID, &u.Email, &u.PasswordHash, &u.CreatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return User{}, ErrConflict
		}
		return User{}, fmt.Errorf("creating user: %w", err)
	}
	return u, nil
}

func (r *userRepository) GetByEmail(ctx context.Context, email string) (User, error) {
	var u User
	err := r.conn.QueryRow(ctx,
		`SELECT id, email, password_hash, created_at FROM users WHERE email = $1`,
		email,
	).Scan(&u.ID, &u.Email, &u.PasswordHash, &u.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return User{}, ErrNotFound
		}
		return User{}, fmt.Errorf("fetching user by email: %w", err)
	}
	return u, nil
}

func (r *userRepository) GetByID(ctx context.Context, id uuid.UUID) (User, error) {
	var u User
	err := r.conn.QueryRow(ctx,
		`SELECT id, email, password_hash, created_at FROM users WHERE id = $1`,
		id,
	).Scan(&u.ID, &u.Email, &u.PasswordHash, &u.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return User{}, ErrNotFound
		}
		return User{}, fmt.Errorf("fetching user by id: %w", err)
	}
	return u, nil
}
