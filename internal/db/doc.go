// Package db holds one repository interface + Postgres (pgx, hand-written
// queries) adapter per aggregate (ADR-0011): OrganizationRepository,
// UserRepository, MembershipRepository, ProjectRepository,
// ApplicationRepository, DeploymentRepository, RefreshTokenRepository.
// Migrations live in infrastructure/postgres/migrations. Tenant-scoped
// tables enforce RLS (ADR-0010) via Pool.WithTx, which sets
// app.current_user_id / app.current_org_id as transaction-local session
// variables — repositories themselves never set these directly.
package db
