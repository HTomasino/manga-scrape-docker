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

# Install the playwright-go driver (node + package) into a known path.
# playwright.Run() refuses to start without it ("please install the driver
# (v1.57.0) first"). PLAYWRIGHT_DRIVER_PATH is read by playwright-go's
# transformRunOptions, so the runtime stage gets it verbatim via COPY.
ENV PLAYWRIGHT_DRIVER_PATH=/opt/ms-playwright-go
RUN go run github.com/playwright-community/playwright-go/cmd/playwright@v0.5700.1 install

# Runtime stage: Playwright + Chromium on Ubuntu Jammy.
# Tag must match the playwright-go driver version in go.mod
# (playwright-go v0.5700.1 -> driver 1.57.0).
FROM mcr.microsoft.com/playwright:v1.57.0-jammy

ENV DEBIAN_FRONTEND=noninteractive
# Location of the Go playwright driver copied from the builder stage.
ENV PLAYWRIGHT_DRIVER_PATH=/opt/ms-playwright-go

RUN apt-get update && apt-get install -y --no-install-recommends \
    xvfb \
    fonts-liberation \
    fonts-noto-color-emoji \
    curl \
    gosu \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/ms-playwright-go /opt/ms-playwright-go
COPY --from=builder /usr/local/bin/comic-scraper-web /usr/local/bin/comic-scraper-web
COPY --from=builder /usr/local/bin/comic-scraper-cli /usr/local/bin/comic-scraper-cli
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh /usr/local/bin/comic-scraper-web /usr/local/bin/comic-scraper-cli

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -fsS http://localhost:8080/healthz || exit 1

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["web"]