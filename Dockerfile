# syntax=docker/dockerfile:1

# Build stage: statically linked Linux Go binaries
FROM golang:1.23 AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ENV CGO_ENABLED=0
ENV GOOS=linux
ENV GOARCH=amd64

RUN go build -ldflags="-s -w" -o /usr/local/bin/comic-scraper-web ./cmd/web \
    && go build -ldflags="-s -w" -o /usr/local/bin/comic-scraper-cli ./cmd/cli

# Runtime stage: Playwright + Chromium on Ubuntu Jammy.
# Tag must match the playwright-go driver version in go.mod
# (playwright-go v0.5700.1 -> driver 1.57.0).
FROM mcr.microsoft.com/playwright:v1.57.0-jammy

ENV DEBIAN_FRONTEND=noninteractive
# playwright-go looks up its driver (node + package/cli.js) via this env var
# (transformRunOptions). Assembled below from the npm playwright-core tarball
# because the /builds/driver CDN used by playwright-go's `playwright install`
# is deprecated and now 404s for 1.57.0 (see playwright-go issue #593).
ENV PLAYWRIGHT_DRIVER_PATH=/opt/ms-playwright-go

RUN apt-get update && apt-get install -y --no-install-recommends \
    xvfb \
    fonts-liberation \
    fonts-noto-color-emoji \
    curl \
    gosu \
    && rm -rf /var/lib/apt/lists/*

# Assemble the playwright-go driver in the required layout:
#   /opt/ms-playwright-go/node            <- node binary (MCR image ships one at /usr/bin/node)
#   /opt/ms-playwright-go/package/cli.js  + full playwright-core lib
# The npm tarball already contains package/cli.js (the file playwright-go's
# getDriverCliJs expects); node is symlinked from the image into the driver
# dir because playwright-go resolves its node binary relative to the driver
# path. The --version check fails the build if the assembly is broken.
RUN set -eux; \
    mkdir -p /opt/ms-playwright-go/package; \
    curl -fsSL https://registry.npmjs.org/playwright-core/-/playwright-core-1.57.0.tgz \
      -o /tmp/pw.tgz; \
    tar -xzf /tmp/pw.tgz -C /opt/ms-playwright-go package; \
    ln -s /usr/bin/node /opt/ms-playwright-go/node; \
    node /opt/ms-playwright-go/package/cli.js --version | grep -q 1.57.0

COPY --from=builder /usr/local/bin/comic-scraper-web /usr/local/bin/comic-scraper-web
COPY --from=builder /usr/local/bin/comic-scraper-cli /usr/local/bin/comic-scraper-cli
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh /usr/local/bin/comic-scraper-web /usr/local/bin/comic-scraper-cli

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -fsS http://localhost:8080/healthz || exit 1

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["web"]