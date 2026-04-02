FROM node:25-bookworm AS frontend-build
WORKDIR /src

COPY frontend/package.json frontend/package-lock.json ./frontend/
RUN npm --prefix frontend ci

COPY frontend ./frontend
RUN npm --prefix frontend run build

FROM golang:1.25-bookworm AS go-build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
COPY --from=frontend-build /src/internal/approval/dist ./internal/approval/dist

RUN go build -ldflags="-s -w" -o /out/better-airtable-mcp ./cmd/server

FROM debian:bookworm-slim
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates curl \
  && rm -rf /var/lib/apt/lists/*

RUN useradd --uid 10001 --create-home appuser
WORKDIR /app

COPY --from=go-build /out/better-airtable-mcp /app/better-airtable-mcp
COPY --from=go-build /src/README.md /app/README.md
RUN mkdir -p /data/duckdb && chown -R appuser:appuser /app /data

USER appuser
ENV PORT=8080
ENV DUCKDB_DATA_DIR=/data/duckdb
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:${PORT}/healthz || exit 1

CMD ["/app/better-airtable-mcp"]
