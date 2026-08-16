# RBAC & Multi-Tenancy Design

Implements ADR-0004 and ADR-0010. This is the detailed enforcement design; `database-schema.md` has the underlying tables.

## 1. Roles

Four roles, fixed set for the MVP-through-Phase-6 timeframe (not user-definable custom roles — that's a real feature, deliberately out of scope until there's evidence it's needed):

| Role | Intended holder |
|---|---|
| `owner` | Org creator / billing-responsible person. Exactly the permissions of `admin`, plus org deletion and ownership transfer. |
| `admin` | Trusted team member managing people and infrastructure. |
| `developer` | Ships code — full application/deployment lifecycle, no member/billing management. |
| `viewer` | Read-only — dashboards, logs, metrics. No mutating actions anywhere. |

## 2. Permission matrix

Permissions are checked server-side against the caller's `memberships.role` for the target resource's `org_id` — never against a client-supplied role claim.

| Permission | owner | admin | developer | viewer |
|---|:---:|:---:|:---:|:---:|
| `organization.manage` (settings, billing) | ✅ | ✅ | ❌ | ❌ |
| `organization.delete` | ✅ | ❌ | ❌ | ❌ |
| `members.invite` / `members.remove` | ✅ | ✅ | ❌ | ❌ |
| `members.change_role` | ✅ | ✅ | ❌ | ❌ |
| `project.create` / `project.delete` | ✅ | ✅ | ✅ | ❌ |
| `application.create` / `application.delete` | ✅ | ✅ | ✅ | ❌ |
| `application.deploy` | ✅ | ✅ | ✅ | ❌ |
| `application.scale` | ✅ | ✅ | ✅ | ❌ |
| `deployment.rollback` | ✅ | ✅ | ✅ | ❌ |
| `env_vars.write` (incl. secrets) | ✅ | ✅ | ✅ | ❌ |
| `domains.manage` | ✅ | ✅ | ✅ | ❌ |
| `logs.view` / `metrics.view` | ✅ | ✅ | ✅ | ✅ |
| `audit_logs.view` | ✅ | ✅ | ❌ | ❌ |
| `api_keys.create` / `api_keys.revoke` | ✅ | ✅ | ❌ | ❌ |
| `billing.view` | ✅ | ✅ | ❌ | ❌ |

This table is the acceptance criteria for the Phase 6 RBAC middleware — every row becomes a test case (see §5).

## 3. Enforcement layers (ADR-0010 applied to authorization specifically)

1. **Middleware layer**: every route declares the permission it requires; a single shared middleware resolves `(user, org_id) → role → permission set` and rejects with `403` before the handler runs. Handlers never independently re-implement role checks — this avoids the "12 endpoints check roles 12 slightly different ways" drift.
2. **RLS layer**: even if a handler had a bug and skipped the middleware, RLS (ADR-0010) still prevents cross-org row access. RLS is *not* a substitute for the permission matrix above (RLS enforces tenant boundary, not fine-grained role permissions within a tenant) — the two layers cover different failure modes.
3. **Test layer**: see §5.

## 4. Tenant isolation vs. role-based permission — two different guarantees

Important distinction, easy to conflate:

- **Tenant isolation**: a member of Org A must never see/affect Org B's data, regardless of role. Enforced primarily by RLS + `org_id` scoping (ADR-0010).
- **Role-based permission**: within Org A, a `viewer` must not be able to do what a `developer` can. Enforced by the middleware layer (§3.1) using the matrix in §2.

A bug that breaks isolation is more severe than a bug that breaks role permission (the former leaks another customer's data; the latter is a privilege-escalation-within-your-own-tenant bug) — both are treated as required-fix-before-merge, but isolation bugs get the cross-tenant test suite specifically called out below.

## 5. Required test coverage

Not optional, not deferred — any PR touching an RBAC-protected route or a tenant-scoped table must include:

- **Cross-tenant denial tests**: for each resource type, a user in Org A attempts to read/write a resource belonging to Org B → must receive `404` (not `403` — existence of another org's resource should not be confirmable via status code) for read attempts, per the principle of not leaking existence.
- **Permission matrix tests**: for each row in §2, a request from each role attempts the action → assert the exact expected allow/deny.
- **Role-change takes effect immediately**: change a user's role mid-session (their JWT is still valid) → next request re-checks role from Postgres, not from token claims (this is why ADR-0008 explicitly does not trust authorization from the JWT).

## 6. Quotas (cross-reference)

Quota enforcement is a related but distinct concern from RBAC — see `ARCHITECTURE.md` §2.9 and `database-schema.md`'s `resource_quotas` table for the three-layer enforcement design (API check, scheduler re-check, DB constraint backstop).
