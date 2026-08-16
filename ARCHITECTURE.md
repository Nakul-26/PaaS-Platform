# Multi-Tenant PaaS & Mini-Kubernetes — Phase 0 Architecture Blueprint

Status: **DRAFT — awaiting approval. No implementation code exists yet.**
Owner: nakul123426@gmail.com
Last updated: 2026-08-16

This file is the entry point. It covers the system overview, component responsibilities, and MVP scope. Anything that would make this file unwieldy has been split out into its own document — see the map below.

## Documentation map

| Document | Covers |
|---|---|
| [`docs/README.md`](docs/README.md) | Full index and reading order for everything below |
| [`docs/adr/`](docs/adr/) | The *why* behind every major, hard-to-reverse decision (10 ADRs) — read these before questioning a choice made in this file |
| [`docs/database-schema.md`](docs/database-schema.md) | Full schema detail, ERD, RLS design, migration ordering |
| [`docs/api-conventions.md`](docs/api-conventions.md) | REST conventions, auth, error format, pagination, streaming |
| [`docs/rbac-multitenancy.md`](docs/rbac-multitenancy.md) | Role/permission matrix, enforcement layers, required test coverage |
| [`docs/modularity-and-extensibility.md`](docs/modularity-and-extensibility.md) | The interface/service boundaries that keep components swappable and the platform microservices-ready long term |
| [`docs/coding-standards.md`](docs/coding-standards.md) | Go/TS conventions, merge checklist |
| [`docs/testing-strategy.md`](docs/testing-strategy.md) | Test tiers, tooling, what's required for what kind of change |
| [`docs/local-development.md`](docs/local-development.md) | How to actually run the stack locally |
| [`docs/phases/phase-1-mvp.md`](docs/phases/phase-1-mvp.md) | Task-by-task implementation plan for what we build first |
| [`ROADMAP.md`](ROADMAP.md) | Live phase-by-phase status tracker |

The §4 (database schema) and §6 (local dev) sections below are kept as summaries with the full detail living in the linked docs — update both together if either changes.

---

## 0. How to use this document

This file is written to survive as a **reusable architecture blueprint**, not just meeting notes for one build. Two kinds of content are mixed together on purpose:

- **Pattern** blocks — decisions that generalize to almost any multi-tenant infrastructure/PaaS project (desired-state reconciliation, tenant isolation layers, control-plane/data-plane split, phase discipline). These are marked **[PATTERN]** and are what you'd copy into a *different* PaaS/SaaS project.
- **Instance** blocks — the specific choices made for *this* project (Go, Postgres, NATS, Next.js, this exact schema). Marked **[INSTANCE]**. These would change if you retargeted the blueprint (e.g., swapping Go for Rust, or Docker for Firecracker).

When reusing this doc for a future project: keep every `[PATTERN]` section, replace every `[INSTANCE]` section with the new stack's equivalent, and re-run the risk register (§9) against the new choices — most risks are pattern-level and will still apply.

---

## 1. High-Level Architecture

### 1.1 System diagram

```text
                              INTERNET
                                  |
                                  v
                    +---------------------------+
                    |   Load Balancer / Ingress  |   <- our own reverse proxy
                    |   (HTTP, WS, TLS term.)    |
                    +---------------------------+
                                  |
                +-----------------+------------------+
                |                                     |
                v                                     v
       +----------------+                   +--------------------+
       |  Dashboard      |                   |  Tenant Applications|
       |  (Next.js)      |                   |  (user containers)  |
       +----------------+                   +--------------------+
                |                                     ^
                v                                     |
       +----------------+                    +-----------------+
       |   API Server    |  <---- CLI ------->|  Service Registry |
       |   (Go, REST)    |                    |  (discovery)      |
       +----------------+                    +-----------------+
          |     |     |                                ^
          |     |     |                                |
          v     |     v                                |
   +--------+   |  +------------------+                 |
   |Postgres|   |  |  NATS (events /  |-----------------+
   |(state) |   |  |  work queues)    |
   +--------+   |  +------------------+
                v            |
       +------------------+  |
       | Controller Manager| |
       | (reconciliation)  | |
       +------------------+  |
                |            |
                v            v
       +------------------------------+
       |         Scheduler            |
       +------------------------------+
                |         |         |
                v         v         v
           +--------+ +--------+ +--------+
           | Worker | | Worker | | Worker |     <- one process per "node"
           | Agent 1| | Agent 2| | Agent 3|
           +--------+ +--------+ +--------+
                |         |         |
                v         v         v
           +--------+ +--------+ +--------+
           | Docker | | Docker | | Docker |
           | Engine | | Engine | | Engine |
           +--------+ +--------+ +--------+
                |         |         |
             containers containers containers
```

### 1.2 Two-plane split **[PATTERN]**

Everything in the system is either **control plane** (decides what *should* be running) or **data plane** (actually runs it / moves traffic). This split is the single most important structural decision in the whole project — almost every other component falls out of it.

| Plane | Owns | Failure mode if it's down |
|---|---|---|
| Control plane (API server, scheduler, controllers, Postgres) | Desired state, scheduling decisions, auth, tenant/billing/RBAC data | New deploys/scaling stop working, but **already-running applications keep serving traffic** |
| Data plane (worker agents, Docker, load balancer) | Actually running containers and routing HTTP traffic | User apps go down |

Design rule: the data plane must be able to keep serving already-scheduled traffic even if the control plane is completely offline for a while. This is why the load balancer talks to the **service registry**, not the API server, for routing decisions.

### 1.3 Control plane internals

```text
API Server ──writes──> Postgres (desired state) <──reads── Controller Manager
                                                                   |
                                                          detects drift, calls
                                                                   |
                                                                   v
                                                              Scheduler
                                                                   |
                                                         assigns work, publishes
                                                                   |
                                                                   v
                                                        NATS subject: node.<id>.assign
```

### 1.4 Modularity & long-term extensibility principle **[PATTERN]**

This platform is meant to be a long-lived, evolving system, not a one-shot build — components will get swapped (Docker → containerd, NATS → something else), extended (new auth providers, new scheduling/load-balancing strategies), and potentially split across separate infrastructure over time. Two rules make that cheap instead of a rewrite each time:

1. **Every external dependency sits behind a small internal interface**, with one adapter until a second is actually needed (ADR-0011).
2. **A `services/` binary only ever talks to another service over its published network contract** (REST, gRPC, or NATS) — never by importing that service's internal package tree (ADR-0012).

Together, these mean swapping a dependency is additive (new adapter, no rewrite of callers), and moving a co-deployed service onto its own infrastructure is a deployment change, not a code change — which is what "supports a microservices architecture" concretely means here, from Phase 1 onward rather than as a Phase-11 aspiration. The full, living registry of every current interface/service boundary — what exists, what it's for, how to swap or extract it — is maintained in [`docs/modularity-and-extensibility.md`](docs/modularity-and-extensibility.md); update it whenever a new seam is added.

This cuts both ways: it's not license to abstract everything speculatively. An interface earns a place in the registry only when there's a concrete, documented reason to expect an alternative (see that doc's §6) — the project's anti-over-engineering rule still applies.

---

## 2. Component Responsibilities

### 2.1 API Server **[INSTANCE: Go, REST]**
- AuthN (password + JWT + refresh tokens), AuthZ (RBAC middleware on every route)
- CRUD for orgs, projects, applications, deployments, domains, env vars, API keys
- Validates and persists **desired state** to Postgres — it does *not* talk to workers directly
- Emits events to NATS on state changes (`deployment.created`, `application.scaled`, …) for controllers/audit log/log streaming to consume
- Enforces tenant quotas at write time (reject if over quota) — see §2.9

### 2.2 Scheduler **[PATTERN + INSTANCE]**
- Consumes "needs placement" work items (new replica, replica lost)
- Picks a node given: available CPU/mem headroom, node health, simple anti-affinity (don't put all replicas of one app on one node), tenant quota
- MVP algorithm: **filter then score** — filter out nodes that fail hard constraints (unhealthy, insufficient resources), score the rest by lowest current load, pick the best. This is deliberately simpler than kube-scheduler's plugin pipeline.
- Writes the placement decision back to Postgres, publishes assignment to the target node's NATS subject
- Open question flagged for Phase 2 design doc: what happens with two scheduler replicas running at once (leader election via Postgres advisory lock, see §9 R3)

### 2.3 Controller Manager (reconciliation loops) **[PATTERN]**
- One loop per resource type (Deployment controller, Node health controller), each independently polling/subscribing and diffing desired vs. actual
- Core loop shape, reused everywhere:
```text
loop:
  desired := readDesiredState()
  actual  := readActualState()
  diff    := desired - actual
  if diff != empty: takeAction(diff)
  sleep(interval) / wait for event
```
- Must be **idempotent and safe to run concurrently** — this is the load-bearing property of the whole reconciliation pattern (see §9 R2)

### 2.4 Worker Agent **[INSTANCE: Go process + Docker Engine SDK]**
- Registers with control plane on startup (node capacity: CPU, memory)
- Sends heartbeat every N seconds; reports current containers + resource usage
- Subscribes to its own NATS subject for assignments
- Pulls image, starts/stops/restarts containers via Docker SDK, runs health checks
- Streams container logs upward (to NATS, fanned out to log storage + live dashboard tail)
- Abstracted behind a `ContainerRuntime` interface from day one — Docker is the first implementation, containerd/OCI can be added later without touching the rest of the agent (see §8 justification)

### 2.5 Load Balancer / Ingress **[INSTANCE: Go reverse proxy]**
- Watches the service registry for healthy backend sets per hostname/path
- Round robin first, pluggable strategy interface for weighted/least-connections later
- Active health checks + passive (ejects backends that error) 
- TLS termination, HTTP + WebSocket upgrade support
- Does **not** query Postgres or the API server on the request hot path — only the in-memory registry snapshot, refreshed via NATS pushes + periodic reconcile pull. This keeps request latency independent of control-plane load.

### 2.6 Service Registry / Discovery **[PATTERN]**
- Source of truth: Postgres `services` + `service_instances` tables (survives restarts)
- Fast path: in-memory cache inside the load balancer, updated via NATS `service.updated` events, with a periodic full-resync fallback in case an event is missed (never trust pub/sub alone for critical state — always pair with reconciliation)
- TTL-based instance expiry: worker heartbeats refresh instance TTL; missed heartbeats age out and get removed from rotation before the node-health controller even declares the node dead — this is a deliberately faster, cheaper failure detector than full node-down handling

### 2.7 Dashboard **[INSTANCE: Next.js/TS/React/Tailwind]**
- Talks only to the API server (never directly to Postgres/NATS/workers)
- Server components for data-heavy views, client components for live log tail / metrics (via SSE or WebSocket proxied through the API server)

### 2.8 CLI **[INSTANCE: Go, cobra]**
- Same REST API as the dashboard — no special backdoor endpoints. This constraint (CLI and dashboard are just two clients of one public API) keeps the API server honest as the single authorization boundary.

### 2.9 Tenant Quota Enforcement **[PATTERN]**
- Enforced in three places, deliberately redundant (defense in depth, see §9 R4):
  1. API server rejects writes that would exceed quota (fast, cheap, best UX)
  2. Scheduler re-checks quota at placement time (closes TOCTOU gap between API write and actual scheduling)
  3. Postgres CHECK constraints / triggers as last-resort backstop

---

## 3. Communication Between Components

| From → To | Protocol | Why |
|---|---|---|
| Dashboard / CLI → API Server | REST over HTTPS, JSON | Broad client compatibility, simple to debug, fine for external-facing, human-triggered traffic |
| API Server → Postgres | SQL (pgx) | Source of truth, ACID guarantees for tenant/billing data |
| API Server → NATS | Publish (fire-and-forget events) | Decouples "state changed" from "who needs to react" — controllers, audit log, and future webhooks all subscribe independently without the API server knowing they exist |
| Scheduler ↔ Worker Agent | NATS (assignment publish + status subscribe), later gRPC for anything needing streaming/backpressure | Lightweight pub/sub fits "push work to whichever node is listening"; avoids the API server maintaining live connections to every node |
| Worker Agent → Control Plane (heartbeat, status) | NATS publish, JetStream for at-least-once where loss is unacceptable (e.g. container-crashed events) | Heartbeats are naturally lossy/periodic (core NATS fine); state-change events need durability (JetStream) |
| Worker Agent ↔ Docker Engine | Docker Engine API via SDK, wrapped in internal `ContainerRuntime` interface | Swappable runtime later without touching agent logic |
| Load Balancer → Service Registry | In-memory cache + NATS subscribe + periodic reconcile pull from Postgres | Hot path must not depend on control-plane request latency |
| Load Balancer → Application container | HTTP/1.1, HTTP/2, WebSocket | Standard reverse proxy behavior |
| Internal control-plane services (future: multi-instance API server, gRPC-based worker protocol) | gRPC + protobuf | Typed contracts, streaming (log tail, exec), better fit than REST once this is service-to-service instead of human-to-service |

**[PATTERN] Rule of thumb applied throughout:** REST for anything a human or external client calls; pub/sub (NATS) for "something happened, N unknown subscribers may care"; direct RPC (gRPC, added later) only for tight request/response or streaming between two known internal services. Don't reach for a message broker because it's fashionable — here it's justified because the platform is explicitly event-driven (deployment lifecycle, log fan-out, service-discovery updates all have the "one producer, multiple independent consumers" shape that pub/sub solves and plain REST doesn't).

---

## 4. Initial Database Schema **[INSTANCE: PostgreSQL]**

Full schema, ERD, RLS design, and migration ordering: **[`docs/database-schema.md`](docs/database-schema.md)**. Decision record: ADR-0003 (Postgres as system of record), ADR-0004 (multi-tenant schema from day one).

Summary — table groups:

| Group | Tables |
|---|---|
| Identity & tenancy | `organizations`, `users`, `memberships`, `api_keys` |
| Projects & applications | `projects`, `applications`, `env_vars` |
| Deployments | `deployments` (one row per revision) |
| Runtime / cluster state | `nodes`, `containers` |
| Service discovery | `services`, `service_instances` |
| Networking | `domains` |
| Governance | `resource_quotas`, `audit_logs`, `resource_usage_samples` |

Multi-tenancy is modeled from day one even though the MVP UI only exercises a single org — retrofitting tenant_id onto every table after the fact is far more painful than carrying an unused column for a few weeks (ADR-0004, see §7). Secrets (`env_vars.value_encrypted`) are envelope-encrypted from the first migration, never plaintext (§9 R5). Tenant isolation is enforced at the DB layer via row-level security, not just application-code filtering (ADR-0010) — see `docs/rbac-multitenancy.md`.

---

## 5. Repository Structure **[INSTANCE, adapted from the proposal]**

```text
platform/
├── apps/
│   ├── dashboard/            # Next.js + TS + Tailwind
│   └── cli/                  # Go, cobra — thin client over the public API
│
├── services/                 # Go, one deployable binary per service
│   ├── apiserver/
│   ├── scheduler/
│   ├── controller-manager/
│   ├── worker/
│   ├── loadbalancer/
│   └── image-builder/        # Phase 7+
│
├── internal/                 # Go-internal shared code, NOT importable outside this module.
│   │                         # Domain-free only (ADR-0012) — config/logging/interfaces, never
│   │                         # one service's business logic reused by another.
│   ├── auth/                 # JWT, RBAC middleware
│   ├── db/                   # sqlc-generated queries + migrations, one repository per aggregate
│   ├── runtime/               # ContainerRuntime interface + Docker implementation (ADR-0006)
│   ├── discovery/            # service registry client/server code
│   ├── eventbus/             # EventBus interface + NATS implementation (ADR-0005, ADR-0011)
│   └── config/
│
├── proto/                    # protobuf contracts once gRPC is introduced (Phase 2+)
│
├── infrastructure/
│   ├── docker-compose.yml    # local dev: postgres, nats, control plane, N workers, LB
│   ├── postgres/             # migrations (goose/atlas)
│   └── local-cluster/        # scripts to spin up N simulated worker nodes locally
│
├── docs/                     # adr/, database-schema.md, api-conventions.md, etc. — see docs/README.md
│
├── ARCHITECTURE.md           # this file (repo root)
├── ROADMAP.md                # phase status tracker (repo root)
│
└── scripts/
```

Deviation from the original proposal: `packages/contracts|auth|logging|config` collapsed into a single Go `internal/` module. This is a monorepo of Go **services**, not Go **libraries meant for external reuse** — an internal package layout is simpler and avoids premature versioning overhead. The TypeScript side (`apps/dashboard`) gets its own `package.json`/`tsconfig` and talks to the Go side purely over HTTP, so there's no need for a shared TS/Go package boundary.

---

## 6. Local Development Architecture **[INSTANCE]**

```text
Developer Machine
 ├── docker-compose:
 │    ├── postgres
 │    ├── nats
 │    ├── apiserver
 │    ├── scheduler
 │    ├── controller-manager
 │    ├── loadbalancer
 │    ├── worker-1, worker-2, worker-3   (see note below)
 │    └── dashboard (next dev server)
 └── CLI run directly on host, pointed at localhost API
```

**Important nuance to design around, not discover later:** in local dev there is only **one real Docker daemon** (the developer's). "3 worker nodes" cannot mean 3 isolated hosts. Two options, and the blueprint picks the first for the MVP:

1. **Logical nodes, shared daemon** — run 3 worker-agent *processes*, each configured with an artificial capacity ceiling (e.g. "pretend I have 2 CPU / 2GB"), all talking to the same host Docker daemon. Scheduling logic is fully exercised (placement, load distribution, node-down simulation by killing a worker process); container-level resource isolation between "nodes" is not real. This is fine for the MVP because the scheduler/reconciliation logic is what we're validating locally — real multi-host resource isolation is a data-plane/ops concern that gets validated in Phase 2's actual multi-VPS deployment, not in local dev.
2. **Docker-in-Docker per worker** — each worker gets its own nested daemon. More realistic isolation, significantly more complex and slower locally. Deferred; revisit only if logical-node simulation proves insufficient for testing a specific scheduler behavior.

This distinction is written down explicitly because it's the kind of thing that causes confusing bug reports later ("why did node 2's container affect node 1's resource graph") if undocumented.

**Target real-world deployment topology (post-MVP, not built yet):** the control/data-plane split (§1.2) maps directly onto multiple AWS EC2 instances acting as one logical cluster — this is not a redesign, it's what Phase 2's multi-node work (§10) is for. Control-plane components (`apiserver`, `scheduler`, `controller-manager`, Postgres, NATS) run on one or more instances; each additional EC2 instance runs a `worker` agent that registers itself as a row in `nodes` and is placed onto by the scheduler exactly as a local logical node is today, just for real. The `loadbalancer` then routes tenant traffic to whichever node actually holds the container. The piece this local simulation (option 1 above) does *not* cover, and that real EC2 deployment adds, is networking: a VPC with private subnets for workers, security groups allowing control-plane↔worker traffic, and a public-facing path for the load balancer — worth its own deployment doc once Phase 2 lands, not something to design prematurely now. Related: R1, R8.

---

## 7. MVP Scope

The MVP **is** Phase 1 from the roadmap (§10), with one addition pulled forward from later phases: the **multi-tenant schema exists from day one**, even though the MVP only exposes a single default org and no RBAC UI. Rationale: schema/tenancy retrofits touch every table and every query; scheduling/networking retrofits don't. Pull forward the cheap-to-add, expensive-to-retrofit piece; defer the rest.

**In scope for MVP:**
- API server: create application, deploy (single image, single replica), stop, delete, view status, view logs (tailed from Docker, not yet through NATS fan-out)
- One worker agent, talking to local Docker Engine directly (scheduler is a pass-through — "the one node" — not yet making real decisions)
- CLI: `login`, `create project`, `deploy`, `logs`, `get deployments`
- Postgres schema from §4 fully created via migrations, even though most tables are lightly used at this stage
- No load balancer, no service discovery, no multi-node scheduling, no RBAC enforcement beyond "is this org_id yours" yet

**Explicitly out of scope until later phases:** everything under §10 Phase 2 onward. Do not pull forward scheduling, load balancing, or RBAC UI — that's how phase discipline erodes.

**Definition of done for MVP:**
```text
platform login
platform create project demo
platform deploy demo --image nginx:latest
platform get deployments          # shows running
curl <worker-host>:<mapped-port>  # serves nginx
platform logs demo
platform delete demo              # container stops and is removed
```

---

## 8. Technology Choices & Justification

| Choice | Alternatives considered | Why this one |
|---|---|---|
| **Go** for all infra services | Rust, Java, Node | Goroutines are a near-perfect fit for the heartbeat/reconciliation-loop-per-resource shape; static binaries simplify worker-agent distribution to nodes; it's what the two closest real systems (Kubernetes, Nomad) are written in, so idioms/prior art transfer directly to what we're learning here |
| **PostgreSQL** for control-plane state | MongoDB, MySQL | Tenant/RBAC/quota data is inherently relational (orgs→projects→apps→deployments) with real foreign-key integrity needs; JSONB columns (labels, ports, metadata) give schema flexibility where genuinely needed without abandoning relational guarantees elsewhere; row-level security is a real, usable second line of defense for tenant isolation (MongoDB has no equivalent) |
| **NATS** (core + JetStream) for eventing | RabbitMQ, Kafka, Redis Streams | Single small Go binary, near-zero ops overhead for a small-team project (vs. Kafka's operational weight); core NATS pub/sub fits the "many independent subscribers to state-change events" shape directly; JetStream gives at-least-once delivery only where it's actually needed (crash events) without paying that cost everywhere. Explicitly not chosen for its own sake — see §3 for the pattern it implements. |
| **Docker Engine** as first container runtime, behind a `ContainerRuntime` interface | containerd directly, Firecracker/gVisor | Docker is the fastest path to a working data plane and has the best-documented Go SDK; wrapping it in an interface from the start means the eventual containerd/OCI move (explicitly called out as a later evaluation in the brief) is a new implementation of one interface, not a rewrite |
| **REST** for the external API server | GraphQL, gRPC-Web | Simplest for CLI + dashboard + future third-party integrations; no schema-stitching complexity for what is, at this scope, a fairly conventional CRUD-shaped API surface |
| **gRPC** for internal service-to-service (introduced Phase 2+, not MVP) | REST internally too, plain NATS RPC | Needed once we have streaming use cases (log tail, exec-into-container) and want compile-time-checked contracts between our own services — protobuf schemas double as documentation of the internal API |
| **Next.js / TypeScript / Tailwind** | Remix, plain React+Vite | Server components suit data-heavy dashboard views (node list, deployment history) without hand-rolled data-fetching boilerplate; strict TS + generated types from the Go API (OpenAPI or similar) keeps the two sides of the REST boundary honest |
| **JWT + refresh tokens** for auth | Server-side sessions | Stateless verification fits a horizontally-scaled API server; refresh-token rotation gives revocation without needing a session store lookup on every request |

---

## 9. Major Architectural Risks

Each risk includes what phase it must be resolved by — most are **[PATTERN]**-level and would reappear in any similar system, not artifacts of this specific stack.

| # | Risk | Phase it must be addressed by | Mitigation |
|---|---|---|---|
| R1 | Local "multi-node" simulation shares one Docker daemon — resource isolation between simulated nodes is not real; a local-only success can mask a real multi-host bug | Phase 2 (real multi-VPS validation) | Explicit design note in §6; treat local multi-node tests as scheduler/logic validation only, not isolation validation |
| R2 | Reconciliation loops must be idempotent and safe under concurrent execution, or you get duplicate replicas / thrashing | Phase 3 | Every controller action keyed by a unique, checkable precondition (e.g. "only create replica if count(actual) < count(desired) at time of write", enforced via a DB transaction, not just an in-memory check) |
| R3 | Two scheduler (or controller) instances running simultaneously could both act on the same drift and double-schedule | Phase 2 (as soon as we consider running >1 control-plane instance) | Postgres advisory lock or a simple lease/leader-election table before allowing schedule-writing actions; single-instance-only is an acceptable MVP constraint as long as it's a *documented* constraint, not an accident |
| R4 | Tenant isolation is only as strong as its weakest enforcement point — one missed `WHERE org_id = ?` leaks cross-tenant data | Phase 6 (multi-tenant layer), but schema supports it from Phase 1 | Defense in depth per §2.9 pattern: app-layer checks + Postgres row-level security policies + integration tests that specifically attempt cross-tenant access and assert denial |
| R5 | Secrets/env vars stored in plaintext in Postgres | Must be true from Phase 1, not deferred | Application-layer envelope encryption from day one (§4 `value_encrypted`); do not ship a "plaintext for now, encrypt later" milestone — encryption-at-rest is far cheaper to build in from the start than to retrofit across existing rows |
| R6 | Running arbitrary tenant-supplied container images is a real security boundary (container breakout, resource abuse, cryptomining) | Phase 1 for baseline limits, hardened through Phase 11 | Non-root containers, no `--privileged`, CPU/memory limits enforced at the Docker level (not just tracked in Postgres), network segmentation between tenants before any public multi-tenant exposure; explicitly do not open this platform to untrusted third parties before Phase 11 hardening is done |
| R7 | NATS becomes a new distributed dependency; if it's down, does the platform degrade or fall over? | Phase 4 (once load balancer depends on it for discovery updates) | Every NATS-dependent read path (service registry, discovery) must have a periodic reconcile-from-Postgres fallback per §2.6 — pub/sub is an optimization, never the sole source of truth |
| R8 | Load balancer is a single point of failure for all tenant traffic | Phase 5 initially single-instance (acceptable, documented); real HA by Phase 11 | Track as a known MVP limitation explicitly rather than silently; HA path is multiple LB instances behind DNS/anycast or a floating IP, deferred by design |
| R9 | Heartbeat-timeout tuning: too aggressive → false node-down under load; too lax → slow failure detection | Phase 2 | Make the timeout configurable and load-test it in Phase 10's failure-injection pass rather than guessing a constant once and never revisiting |
| R10 | 26-section spec is large; the biggest real risk to a small team/solo project is never finishing a working system | Ongoing | Strict phase discipline (§10) — each phase must end in a genuinely working, demoable system before the next begins; this document's own MVP section (§7) is the enforcement mechanism |

---

## 10. Phase-by-Phase Roadmap

Each phase lists: goal, concrete deliverables, and **exit criteria** (what must be true/demoable before moving on — this is the actual enforcement of "don't jump ahead"). Live status per phase: [`ROADMAP.md`](ROADMAP.md) — update it as work lands rather than editing this section's text.

**Phase 0 — Architecture** *(this document + the full `docs/` set)*
Exit criteria: this document and its linked docs reviewed and approved.

**Phase 1 — Single-Node Container Platform (= MVP, §7)**
Deliverables: API server, one worker agent, Docker integration, CLI, full DB schema migrated. Full task breakdown: [`docs/phases/phase-1-mvp.md`](docs/phases/phase-1-mvp.md).
Exit criteria: the §7 demo script runs end-to-end against a locally deployed nginx container.

**Phase 2 — Multi-Node Infrastructure**
Deliverables: node registration, heartbeats, real scheduler (filter+score per §2.2), workload assignment over NATS.
Exit criteria: deploy an application, watch the scheduler place it on one of 3 running worker processes; kill a worker, watch it drop out of the node list.

**Phase 3 — Desired State + Controllers**
Deliverables: Deployment controller enforcing `replicas_desired`, automatic restart of crashed containers, manual scale up/down.
Exit criteria: manually kill a container process; watch the controller notice and start a replacement without any user action, within one reconcile interval.

**Phase 4 — Service Discovery + Load Balancer**
Deliverables: service registry (Postgres + in-memory cache), round-robin LB, health-aware backend removal.
Exit criteria: scale an app to 3 replicas, send repeated requests through the LB, observe distribution across all 3; stop one replica, confirm it's removed from rotation within the health-check interval.

**Phase 5 — Networking + Ingress**
Deliverables: custom domains, host/path-based routing, TLS termination.
Exit criteria: two different applications reachable via two different hostnames through the same load balancer.

**Phase 6 — Multi-Tenant SaaS Layer**
Deliverables: org/project UI, RBAC enforcement on every route, API keys, quotas enforced (§2.9), audit log.
Exit criteria: R4's cross-tenant-access integration tests pass; a `developer`-role user is denied an `organization.manage` action server-side even if the frontend check is bypassed.

**Phase 7 — Deployment Platform**
Deliverables: GitHub repo connection, build pipeline, image build + push to a registry.
Exit criteria: deploy from a Git repo URL without manually building/pushing an image first.

**Phase 8 — Advanced Deployment**
Deliverables: rolling deployments, automatic rollback on failed health checks, deployment history.
Exit criteria: deploy a broken image, watch automatic rollback restore the previous healthy revision without manual intervention.

**Phase 9 — Observability**
Deliverables: metrics (Prometheus-compatible), structured logs pipeline, infra + app dashboards, basic alerting.
Exit criteria: dashboard shows live CPU/memory/replica-count for a running app, sourced from real worker-reported data, not mocked.

**Phase 10 — Reliability (failure injection)**
Deliverables: documented, tested behavior for: worker crash, node disappearance, container crash, Postgres unavailability, network partition, NATS unavailability, scheduler failure.
Exit criteria: each failure scenario in §9's risk register has a corresponding test and a written note on actual observed behavior (not assumed behavior).

**Phase 11 — Production Hardening**
Deliverables: security review pass, rate limiting, secret management audit, backup/recovery, graceful shutdown everywhere, load testing at the target scale from the original brief (§25 of the source spec).
Exit criteria: load test results recorded (throughput, latency, resource usage) with actual numbers, not estimates.

---

## 11. Open Questions for Approval

Flagging these now rather than deciding unilaterally, since they affect Phase 1 scaffolding choices:

1. **Migration tool**: `goose` (simple, SQL-file based) vs `atlas` (schema-as-code, more powerful diffing). **Resolved: goose** — see `docs/database-schema.md` §4 for the actual migration ordering this implies.
2. **API schema contract sharing**: hand-written OpenAPI spec generating TS types, vs. a Go-first codegen (e.g. `oapi-codegen` from Go structs). Recommendation: Go structs → OpenAPI → generated TS client, since Go is the source of truth for the API server anyway. Still open — pick this during Phase 1 Task 5 (`docs/phases/phase-1-mvp.md`).
3. **JWT library / session strategy specifics** — fine to decide inline when Phase 1's auth task comes up rather than blocking this document on it. Overall auth *design* (token lifetimes, rotation) is settled in ADR-0008.
4. **Single vs. multi-module Go monorepo** — recommendation and rationale in `docs/phases/phase-1-mvp.md` Task 1.
5. **Worker agent as a separate process vs. embedded in the API server for Phase 1 only** — recommendation and rationale in `docs/phases/phase-1-mvp.md` Task 4.

Everything else in this document, and everything in the linked `docs/` set, is proposed as final for Phase 0/1 planning purposes.

---

**Next step:** review and approve this document and the linked `docs/` set (or flag changes) before any Phase 1 code is written, per the working agreement in the original brief (§27). Once approved, flip Phase 1 to 🟨 in [`ROADMAP.md`](ROADMAP.md) and start from Task 1 in [`docs/phases/phase-1-mvp.md`](docs/phases/phase-1-mvp.md).
