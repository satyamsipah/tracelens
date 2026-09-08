# Shared build for every Go binary in the repo. The target is chosen with
# --build-arg BINARY, so one Dockerfile covers collector, assembler, query,
# loadgen and demo, and they cannot drift on Go version or build flags.
# 1.25 is the floor, not a preference: golang.org/x/sync and
# genproto/googleapis/rpc both declare go >= 1.25, so go.mod inherits it and an
# older image fails at `go mod download` with GOTOOLCHAIN=local.
FROM golang:1.25-alpine AS build

ARG BINARY=collector
ARG TARGET=./cmd/collector

WORKDIR /src

# Copy manifests first so dependency download is cached independently of source.
COPY go.mod go.sum ./

# BuildKit cache mounts persist the module and build caches across builds and
# across the five images built from this file. Beyond speed, they make a
# retried build resume rather than restart, which matters because the module
# proxy fetch is the one step here that depends on the network.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

COPY . .

# CGO off for a static binary on alpine. -trimpath keeps build paths out of
# the binary so images are reproducible across machines.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ${TARGET}

FROM alpine:3.20

RUN apk add --no-cache ca-certificates wget && adduser -D -u 10001 tracelens
USER tracelens

COPY --from=build /out/app /usr/local/bin/app

ENTRYPOINT ["/usr/local/bin/app"]
