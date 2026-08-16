# Worker Agent HTTP Contract (Phase 1)

The worker agent (`services/worker`) is a separate process from the API server, driving containers via the `ContainerRuntime` interface (ADR-0006, `internal/runtime`). Per ADR-0012, the API server may only reach it over this published network contract, never by importing its package tree.

This is the **internal** contract between `apiserver` and `worker` — distinct from the platform's public REST API (`api-conventions.md`), which only `apiserver` exposes. It borrows that doc's JSON/error-envelope conventions for consistency, not because external clients ever see it.

Phase 1 has exactly one worker, called directly over HTTP. Per `docs/modularity-and-extensibility.md` §3, Phase 2 introduces NATS-based work assignment as an *additive* transport for the same lifecycle operations — this contract doesn't get retrofitted, it gets a second transport alongside it.

## Endpoints

All bodies are JSON; timestamps are RFC 3339 UTC; errors use the same envelope as `api-conventions.md` (`{"error":{"code":...,"message":...}}`).

```text
POST   /v1/containers            Pull the image, create and start a container
GET    /v1/containers/:id        Report current status
POST   /v1/containers/:id/stop   Stop (graceful, with timeout)
DELETE /v1/containers/:id        Remove
GET    /v1/containers/:id/logs   Stream logs (?follow=true to keep the connection open)
```

### `POST /v1/containers`

```json
{
  "image": "nginx:latest",
  "name": "demo",
  "env": { "PORT": "8080" },
  "ports": [{ "container_port": 80, "host_port": 0, "protocol": "tcp" }],
  "command": ["nginx", "-g", "daemon off;"]
}
```

`image` is required; everything else is optional. `host_port: 0` (or omitted) lets the container engine pick an ephemeral host port. Responds `201` with the container's status (below), or `502 bad_gateway` (`pull_failed` / `create_failed` / `start_failed`) if the container engine rejects the request.

### Status response (`POST /v1/containers`, `GET /v1/containers/:id`, `POST /v1/containers/:id/stop`)

```json
{
  "id": "a1b2c3...",
  "status": "running",
  "exit_code": 0,
  "started_at": "2026-08-16T09:00:00Z"
}
```

`status` is one of `pending`, `running`, `exited`, `unknown` (`internal/runtime.Status`).

### `POST /v1/containers/:id/stop`

Optional body: `{"timeout_seconds": 10}` — defaults to 10s if omitted. Returns the post-stop status.

### `DELETE /v1/containers/:id`

`204 No Content` on success.

### `GET /v1/containers/:id/logs?follow=true|false`

`200` with `Content-Type: text/plain`, streaming raw log bytes (stdout+stderr interleaved). With `follow=true` the connection stays open and new lines are flushed as they arrive; the caller closes the connection to stop following.

## Errors

Unknown container ID: `404 container_not_found` on any of the four container-scoped endpoints. Any other container-engine failure: `502 runtime_error` (or the create-specific codes above). Malformed request body: `400 invalid_body`. Missing `image` on create: `422 missing_field`.
