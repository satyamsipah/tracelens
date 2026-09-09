# Shared build for every Go binary in the repo. The target is chosen with
# --build-arg TARGET, so one Dockerfile covers collector, assembler, query,
# loadgen and demo, and they cannot drift on Go version or build flags.
# 1.25 is the floor, not a preference: golang.org/x/sync and
# genproto/googleapis/rpc both declare go >= 1.25, so go.mod inherits it and an
# older image fails at `go mod download` with GOTOOLCHAIN=local.
#
# TWO final stages, selected with --target:
#
#   runtime  -- alpine. Has a shell and wget, which Docker Compose
#            healthchecks need, since a Compose healthcheck runs its command
#            INSIDE the container.
#   slim     -- distroless static. ~2MB base instead of ~8MB plus packages,
#            no shell and no package manager (a materially smaller attack
#            surface), and it is what deploy/k8s and deploy/helm use --
#            Kubernetes httpGet probes are executed by the kubelet over the
#            network, so they need no tooling inside the container at all.
#
# NEITHER stage is "the default" in a useful sense: with no --target, BuildKit
# builds the LAST stage in this file, which is `slim`. Every caller therefore
# names its target explicitly -- docker-compose.yml pins `runtime`, and the
# release workflow pins `slim`. An earlier version of this file relied on the
# implicit default and silently handed Compose a shell-less image, which
# failed as an unhealthy container rather than as a build error.
#
# Both run as a non-root user. See deploy/README.md for measured sizes.
#
# --platform=$BUILDPLATFORM pins the build stage to the BUILDER's architecture
# and lets Go cross-compile via GOARCH below. The alternative -- letting
# BuildKit emulate the target arch -- would run the Go compiler itself under
# QEMU, which for a multi-arch build costs minutes per image for no benefit,
# since a static Go binary cross-compiles natively.
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS build

ARG BINARY=collector
ARG TARGET=./cmd/collector
ARG TARGETARCH
ARG TARGETOS

WORKDIR /src

# Copy manifests first so dependency download is cached independently of source.
COPY go.mod go.sum ./

# BuildKit cache mounts persist the module and build caches across builds and
# across the images built from this file. Beyond speed, they make a
# retried build resume rather than restart, which matters because the module
# proxy fetch is the one step here that depends on the network.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

COPY . .

# CGO off for a static binary, which is what makes the distroless/static
# base below viable at all. -trimpath keeps build paths out of the binary so
# images are reproducible across machines.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath -ldflags="-s -w" -o /out/app ${TARGET}

# ---------------------------------------------------------------- runtime --
# Default stage: alpine, for Docker Compose (needs an in-container shell and
# wget for its healthchecks).
FROM alpine:3.20 AS runtime

RUN apk add --no-cache ca-certificates wget && adduser -D -u 10001 tracelens
USER tracelens

COPY --from=build /out/app /usr/local/bin/app

ENTRYPOINT ["/usr/local/bin/app"]

# ------------------------------------------------------------------- slim --
# Production stage: distroless static. No shell, no package manager, no libc
# beyond what a static Go binary needs. The :nonroot tag runs as uid 65532
# and ships the CA bundle, so nothing here needs adding.
#
# Deliberately has NO healthcheck: there is no shell or wget to run one with.
# That is a feature in Kubernetes (kubelet probes the port itself) and the
# reason Compose keeps using the alpine stage above.
FROM gcr.io/distroless/static-debian12:nonroot AS slim

COPY --from=build /out/app /usr/local/bin/app

ENTRYPOINT ["/usr/local/bin/app"]
