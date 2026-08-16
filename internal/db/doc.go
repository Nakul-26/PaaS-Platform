// Package db holds one repository interface + Postgres (pgx/sqlc) adapter per
// aggregate (ADR-0011), e.g. OrganizationRepository, ApplicationRepository.
// Migrations live in infrastructure/postgres/migrations (Task 2); the
// repository interfaces/adapters land in Task 5.
package db
