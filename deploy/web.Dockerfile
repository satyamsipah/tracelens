# Separate from deploy/Dockerfile deliberately: that one builds a single Go
# binary onto a from-scratch alpine base, which has nothing a Next.js server
# needs (no node runtime, and standalone output still wants a handful of
# native deps resolved by npm rather than cross-compiled by `go build`).
FROM node:20-alpine AS build
WORKDIR /src

COPY web/package.json web/package-lock.json ./
RUN --mount=type=cache,target=/root/.npm npm ci

COPY web/ ./
# TRACELENS_API_BASE only affects server components at request time (not
# baked into the client bundle), so it does not need to be present at build
# time -- unlike NEXT_PUBLIC_* vars, which Next.js inlines at build time and
# therefore DOES need if the browser is ever meant to reach a non-default
# API host.
RUN npm run build

FROM node:20-alpine
RUN adduser -D -u 10001 tracelens
USER tracelens
WORKDIR /app

COPY --from=build /src/.next/standalone ./
COPY --from=build /src/.next/static ./.next/static
COPY --from=build /src/public ./public

ENV NODE_ENV=production
ENV PORT=3001
# Docker auto-sets HOSTNAME to the container's own hostname, and Next.js
# standalone's server.js binds to `process.env.HOSTNAME || '0.0.0.0'` --
# so without this override it binds only to the container's specific
# network IP, not all interfaces, and both `localhost` (the healthcheck)
# and the host's published port mapping get connection refused despite the
# process being "Ready" and genuinely listening.
ENV HOSTNAME=0.0.0.0
EXPOSE 3001

ENTRYPOINT ["node", "server.js"]
