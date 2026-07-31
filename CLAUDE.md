# CLAUDE.md

Hosted MCP server for Airtable: DuckDB-backed SQL reads, human-approved writes.
`SPEC.md` is the authoritative technical spec. `.impeccable.md` records the
settled frontend design decisions.

## Verify

Run `mise run verify` before committing. It runs: frontend install/test/
typecheck/build, `go vet` (both lanes), `go build`, and `go test -short ./...`.
CI (`.github/workflows/ci.yml`) runs the same checks plus `golangci-lint` and
the integration test lane on every push and PR.

## Toolchain

`mise` pins Go and Node (`.mise.toml`). Prefix ad-hoc commands with
`mise exec --` if the pinned tools aren't on your PATH.

## Gotchas

- `internal/approval/dist` is a **committed build artifact**. After editing
  `frontend/src`, run `npm --prefix frontend run build` and commit the
  regenerated bundle — CI fails if it is stale.
- Tests that start embedded Postgres are tagged `//go:build integration`
  (slow; downloads Postgres on first run). Fast lane: `go test -short ./...`.
  Integration lane: `mise run test:integration`. **Every new test that needs
  Postgres must carry the tag**, or the fast lane breaks.
- `vite build` does **not** typecheck. Run `npm --prefix frontend run typecheck`.
- The design brief is "look like Airtable", not "look distinctive" — follow
  `.impeccable.md`. Do not re-add a base icon tile (settled 2026-06-26).
- Never log or echo secrets. Airtable tokens are encrypted at rest and logs
  are redacted (`internal/logx/sanitize.go`).
- Commit messages: plain lowercase imperative, e.g. "paginate base listing so
  bases past 1000 aren't dropped".

## Layout

- `cmd/server` — entry point, HTTP wiring
- `internal/mcp` — MCP JSON-RPC endpoint and session lifecycle
- `internal/tools` — MCP tool implementations (list_bases, list_schema, query, sync, mutate, manage_schema, check_operation)
- `internal/sync` — continuous Airtable→DuckDB sync workers
- `internal/duckdb` — per-base DuckDB cache store
- `internal/oauth` — OAuth provider for MCP clients, chained to Airtable OAuth
- `internal/approval` — human approval flow + embedded review SPA
- `internal/db` — Postgres store (users, tokens, sync state, pending operations)
- `internal/config`, `internal/cryptoutil`, `internal/logx`, `internal/httpx`, `internal/health`, `internal/landing` — support packages
- `frontend` — React/Vite source for the approval SPA and `/debug` page
