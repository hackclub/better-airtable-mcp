# Better Airtable MCP

1. Go to https://claude.ai/customize/connectors
2. Create a new Connector titled **Airtable** with this URL: https://better-airtable-mcp.hackclub.com/mcp
3. Set permissions to "Always Allow" - `better-airtable-mcp` has an approval flow built-in and will prompt you before creating / modifying / delete records with a web interface

![Google Chrome](https://github.com/user-attachments/assets/5c463e4d-33ad-4f76-973c-a1437c3abfcb)

---

Hosted MCP server for Airtable with fast DuckDB-backed reads and human-approved writes.

This repository is being implemented from `SPEC.md`. The current implementation includes:

- OAuth provider flow for MCP clients chained to Airtable OAuth
- Shared per-base DuckDB caching with continuous sync workers
- Restart-safe sync restoration and startup cleanup of stale DuckDB files
- MCP tools for `list_bases`, `list_schema`, `query`, `sync`, `mutate`, and `check_operation`
- Human approval flow with a bundled React/Vite approval SPA
- Go unit and integration tests plus frontend unit tests

## Toolchain

The repo uses [`mise`](https://mise.jdx.dev/) to pin Go and Node:

```bash
mise install
mise exec -- npm --prefix frontend test
mise exec -- npm --prefix frontend run build
mise exec -- go test ./...
mise exec -- go build ./...
```

Container build:

```bash
docker build -t better-airtable-mcp:dev .
```

## Local Development

Start Postgres only:

```bash
docker compose up -d postgres
```

Or start the full stack from Docker Compose:

```bash
docker compose up --build app
```

Load the local environment file:

```bash
set -a
source .env
set +a
```

Run the full test/build workflow and start the server locally:

```bash
mise exec -- npm --prefix frontend test
mise exec -- npm --prefix frontend run build
mise exec -- go test ./...
mise exec -- go run ./cmd/server
```

Once the server is running locally, open `http://localhost:8080/debug` to run the built-in OAuth flow, mint a bearer token for the local server, inspect the live MCP tool catalog, and execute tool calls manually.

For local Airtable OAuth testing, register this callback URL in your Airtable OAuth app:

```text
http://localhost:8080/oauth/airtable/callback
```

`BASE_URL=http://localhost:8080` is fine for a same-machine browser-based smoke test. If you want to test from a hosted MCP client or anything not running against your local machine directly, switch `BASE_URL` to a public HTTPS tunnel URL and update the Airtable callback to match exactly.

When running the app inside Docker Compose, the app container overrides `DATABASE_URL` to use the internal Postgres hostname, while the checked-in `.env.example` keeps the host-machine URL for local `go run` development.

## Environment

The server expects these environment variables:

- `DATABASE_URL`
- `AIRTABLE_CLIENT_ID`
- `AIRTABLE_CLIENT_SECRET`
- `BASE_URL`
- `APP_ENCRYPTION_KEY`

Optional configuration currently matches the defaults described in `SPEC.md`.
