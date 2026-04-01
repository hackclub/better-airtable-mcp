# Better Airtable MCP

Hosted MCP server for Airtable with fast DuckDB-backed reads and human-approved writes.

This repository is being implemented from [`SPEC.md`](/Users/zrl/dev/hackclub/better-airtable-mcp/SPEC.md). The current slice sets up the Go module, HTTP server scaffold, MCP/tool plumbing, configuration loading, and the pure logic that is already fully specified:

- Airtable identifier sanitization to DuckDB-friendly `snake_case`
- Airtable field type to DuckDB type mapping
- Read-only SQL validation and limit normalization for the `query` tool
- Initial MCP `initialize`, `tools/list`, and `tools/call` handling

## Toolchain

The repo uses [`mise`](https://mise.jdx.dev/) to pin Go:

```bash
mise install
mise exec -- go test ./...
mise exec -- go build ./...
```

## Local Development

Start Postgres with Docker Compose:

```bash
docker compose up -d postgres
```

Load the local environment file:

```bash
set -a
source .env
set +a
```

Run the test suite and start the server:

```bash
mise exec -- go test ./...
mise exec -- go run ./cmd/server
```

For local Airtable OAuth testing, register this callback URL in your Airtable OAuth app:

```text
http://localhost:8080/oauth/airtable/callback
```

`BASE_URL=http://localhost:8080` is fine for a same-machine browser-based smoke test. If you want to test from a hosted MCP client or anything not running against your local machine directly, switch `BASE_URL` to a public HTTPS tunnel URL and update the Airtable callback to match exactly.

## Environment

The server expects these environment variables:

- `DATABASE_URL`
- `AIRTABLE_CLIENT_ID`
- `AIRTABLE_CLIENT_SECRET`
- `BASE_URL`
- `APP_ENCRYPTION_KEY`

Optional configuration currently matches the defaults described in `SPEC.md`.
