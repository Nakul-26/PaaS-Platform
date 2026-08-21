# Builds the worker binary for the docker-compose logical-node simulation
# (ADR-0009, phase-2-multi-node.md Task 8). Build context is the repo root
# (infrastructure/docker-compose.yml sets context: ..) since this is a single
# multi-service Go module.
FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/worker ./services/worker

# The worker only needs the Docker Engine API over the mounted host socket
# (internal/runtime/docker.go uses the Go SDK, not the docker CLI), so the
# runtime image needs no docker client — just CA certs for the image pulls
# it performs against registries.
FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /out/worker /usr/local/bin/worker
ENTRYPOINT ["/usr/local/bin/worker"]
