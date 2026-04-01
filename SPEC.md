# Better Airtable MCP — Technical Specification

## Overview

Better Airtable MCP is a hosted MCP (Model Context Protocol) server that gives AI agents fast, SQL-based read access to Airtable bases via DuckDB, and safe record write access through a human-in-the-loop approval flow.

**Core thesis**: Existing Airtable MCPs let agents create and modify records in ways that surprise users. Better Airtable MCP solves this by (1) making reads fast and powerful via SQL, and (2) requiring explicit human approval for all record mutations via a rich preview UI.

- **URL**: `https://better-airtable-mcp.hackclub.com`
- **Language**: Go
- **Transport**: MCP Streamable HTTP (spec 2025-11-25)
- **Hosting**: Coolify (Docker), persistent disk for DuckDB files (wiped on redeploy)
- **Durable storage**: Postgres (OAuth tokens, pending approvals, user records)

---

## 1. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     better-airtable-mcp                         │
│                                                                 │
│  ┌──────────┐  ┌──────────────┐  ┌───────────┐  ┌───────────┐  │
│  │  MCP     │  │  Sync        │  │  Approval  │  │  OAuth    │  │
│  │  Handler │  │  Workers     │  │  Server    │  │  Provider │  │
│  │          │  │              │  │  (React)   │  │           │  │
│  │ Streamable  │ Per-base     │  │            │  │ MCP OAuth │  │
│  │ HTTP     │  │ continuous   │  │ Preview +  │  │ + Airtable│  │
│  │ endpoint │  │ goroutines   │  │ approve/   │  │ OAuth     │  │
│  │          │  │              │  │ reject     │  │ chained   │  │
│  └────┬─────┘  └──────┬───────┘  └─────┬─────┘  └─────┬─────┘  │
│       │               │                │               │        │
│       └───────┬───────┴────────┬───────┘               │        │
│               │                │                       │        │
│          ┌────▼────┐     ┌─────▼──────┐          ┌─────▼─────┐  │
│          │ DuckDB  │     │  Postgres  │          │ Airtable  │  │
│          │ (disk)  │     │            │          │ API       │  │
│          │ per-base│     │ tokens,    │          │           │  │
│          │ shared  │     │ approvals, │          │           │  │
│          │         │     │ users      │          │           │  │
│          └─────────┘     └────────────┘          └───────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### Key subsystems

| Subsystem | Responsibility |
|---|---|
| **MCP Handler** | Streamable HTTP endpoint, JSON-RPC dispatch, session management |
| **Sync Workers** | Per-base goroutines that poll Airtable and write to DuckDB |
| **Approval Server** | HTTP pages for previewing and approving/rejecting mutations |
| **OAuth Provider** | MCP-facing OAuth (we are the auth server) chained to Airtable OAuth (we are the client) |
| **DuckDB Layer** | Per-base `.db` files on disk, read-only connections for queries, write connection for sync |
| **Postgres** | Durable state: OAuth tokens, refresh tokens, pending operations, user-base access mapping |

---

## 2. Authentication

### 2.1 Double OAuth Chain

This service participates in two OAuth flows chained together:

**Flow from the user's perspective:**
1. User adds `https://better-airtable-mcp.hackclub.com/mcp` to their MCP client (e.g., claude.ai)
2. Client discovers our auth server via `GET /.well-known/oauth-protected-resource`
3. Client initiates OAuth with our service (Authorization Code + PKCE)
4. Our authorization page redirects the user to Airtable's OAuth consent screen
5. User authorizes "Better Airtable MCP" on Airtable
6. Airtable redirects back to our callback with an authorization code
7. We exchange the code for Airtable access + refresh tokens, store them in Postgres
8. We complete our own OAuth flow, minting a bearer token for the MCP client
9. The MCP client uses our bearer token on every subsequent request

**Our service acts as:**
- **OAuth Provider** for MCP clients (we issue bearer tokens)
- **OAuth Client** for Airtable (we hold Airtable tokens on behalf of users)

### 2.2 Airtable OAuth Details

- **Grant type**: Authorization Code with PKCE
- **Authorization endpoint**: `https://airtable.com/oauth2/v1/authorize`
- **Token endpoint**: `https://airtable.com/oauth2/v1/token`
- **Access token TTL**: 60 minutes
- **Refresh token TTL**: 60 days (rotated on each use — single-use)
- **Required scopes**: `data.records:read data.records:write schema.bases:read`
- **OAuth app**: Single shared registration ("Better Airtable MCP")
- **Callback URL**: `https://better-airtable-mcp.hackclub.com/oauth/airtable/callback`

### 2.3 Token Refresh Strategy

Airtable refresh tokens are **single-use with rotation**. Using a refresh token twice revokes the entire token chain.

- **Proactive refresh**: A background goroutine refreshes tokens ~10 minutes before expiry (i.e., at the ~50 minute mark)
- **Single-flight locking**: Per-user mutex ensures only one refresh executes at a time. Concurrent requests wait for the in-flight refresh to complete
- **Atomic persistence**: New `(access_token, refresh_token, expires_at)` tuple is written to Postgres in a single transaction
- **Failure handling**: If a refresh fails with `invalid_grant`, mark the user as needing re-authorization. The next MCP request returns an error instructing the agent to tell the user to re-authenticate

### 2.4 MCP OAuth Endpoints

| Endpoint | Purpose |
|---|---|
| `GET /.well-known/oauth-protected-resource` | RFC 9728 Protected Resource Metadata |
| `GET /.well-known/oauth-authorization-server` | RFC 8414 Authorization Server Metadata |
| `POST /oauth/register` | Dynamic Client Registration (RFC 7591) |
| `GET /oauth/authorize` | Authorization endpoint (redirects to Airtable) |
| `POST /oauth/token` | Token endpoint (issues our bearer tokens) |
| `GET /oauth/airtable/callback` | Airtable OAuth callback |

---

## 3. MCP Tool Surface

Six tools, designed to minimize context pollution while giving agents full power.

### 3.1 `search_bases`

Search for Airtable bases the user has access to.

**Input:**
```json
{
  "query": "string (optional) — search term to filter bases by name"
}
```

**Output:**
```json
{
  "bases": [
    {
      "id": "appXXXXXXXXXX",
      "name": "Project Tracker",
      "permission_level": "create"
    }
  ]
}
```

**Behavior:**
- Calls Airtable's List Bases API with the user's token
- If `query` is provided, filters by name (case-insensitive substring match)
- Returns base ID, name, and the user's permission level

---

### 3.2 `list_schema`

List all tables in a base with field metadata and sample data.

**Input:**
```json
{
  "base": "string — base ID or base name"
}
```

**Output:**
```json
{
  "base_id": "appXXXXXXXXXX",
  "base_name": "Project Tracker",
  "last_synced_at": "2026-04-01T12:00:00Z",
  "tables": [
    {
      "airtable_table_id": "tblXXXXXXXXXX",
      "duckdb_table_name": "projects",
      "original_name": "Projects",
      "fields": [
        {
          "airtable_field_id": "fldXXXXXXXXXX",
          "duckdb_column_name": "name",
          "original_name": "Name",
          "type": "VARCHAR",
          "airtable_type": "singleLineText"
        }
      ],
      "sample_rows": [
        {"id": "recXXX", "name": "Website Redesign", "status": "In Progress"},
        {"id": "recYYY", "name": "API Migration", "status": "Done"},
        {"id": "recZZZ", "name": "Mobile App", "status": "Planning"}
      ],
      "total_record_count": 142
    }
  ]
}
```

**Behavior:**
- Resolves base by ID or name (via `search_bases` internally if name is given)
- If the base is not yet synced, triggers a sync and waits for it to complete
- Returns 3 sample rows per table from the DuckDB cache
- Includes the metadata mapping (Airtable IDs ↔ DuckDB names) so the agent can construct mutations later
- The agent should call this before writing queries or mutations to understand the schema

---

### 3.3 `query`

Execute a read-only SQL query against a base's DuckDB cache.

**Input:**
```json
{
  "base": "string — base ID or base name",
  "sql": "string — exactly one SELECT or WITH query",
  "limit": "number (optional, default 100, max 1000)"
}
```

**Output:**
```json
{
  "columns": ["name", "status", "due_date"],
  "rows": [
    ["Website Redesign", "In Progress", "2026-04-15"],
    ...
  ],
  "row_count": 42,
  "truncated": false,
  "last_synced_at": "2026-04-01T12:00:00Z",
  "next_sync_at": "2026-04-01T12:01:00Z"
}
```

**Behavior:**
- Validates that the SQL is exactly one top-level `SELECT` or `WITH` statement; rejects multi-statement SQL and anything containing write/DDL/admin statements
- Opens a **read-only** DuckDB connection (enforced at the connection level)
- Applies a default `LIMIT 100` if the query has no LIMIT clause; the agent can override up to 1000
- Returns freshness metadata so the agent can inform the user how current the data is
- If the base is not yet synced, triggers a sync and waits
- DuckDB is hardened as if SQL is hostile: external file access disabled, extension install/load disabled, and arbitrary `ATTACH`, `COPY`, `INSTALL`, `LOAD`, and `PRAGMA` statements rejected

**SQL examples:**

Simple query:
```sql
SELECT name, status, due_date FROM projects WHERE status = 'In Progress'
```

Join on linked records:
```sql
SELECT p.name, t.name AS task_name
FROM projects p, UNNEST(p.linked_tasks) AS u(task_id)
JOIN tasks t ON t.id = u.task_id
```

---

### 3.4 `mutate`

Request a record mutation. All mutations go through the approval flow.

**Input:**
```json
{
  "base": "string — base ID or base name",
  "operations": [
    {
      "type": "create_records | update_records | delete_records",
      "table": "string — DuckDB table name (snake_case)",
      "records": [
        {
          "id": "recXXX (required for update/delete, omitted for create)",
          "fields": {
            "name": "New Project",
            "status": "Planning",
            "due_date": "2026-05-01"
          }
        }
      ]
    }
  ]
}
```

**Field name resolution:** The agent uses snake_case column names (matching DuckDB). The server resolves these to Airtable field IDs internally using the metadata table. If a snake_case name is ambiguous or unresolved, the server returns an error with suggestions.

**Output:**
```json
{
  "operation_id": "op_XXXXXXXX",
  "status": "pending_approval",
  "approval_url": "https://better-airtable-mcp.hackclub.com/approve/op_XXXXXXXX",
  "expires_at": "2026-04-01T12:10:00Z",
  "summary": "Create 3 records in projects"
}
```

**Behavior:**
- Validates the payload (field names resolve, record IDs exist for updates, etc.)
- For updates, fetches current record values from DuckDB to generate the diff
- Stores the pending operation in Postgres
- Returns immediately with the approval URL and operation ID
- `operation_id` is generated from at least 128 bits of randomness; the approval URL itself is the credential
- The operation expires after **10 minutes** if not approved
- **Approval is all-or-nothing**: the user approves or rejects the entire request as a unit; there is no partial approval UI
- **Execution is not transactional**: Airtable mutations are sent in batches of up to 10 records/request, sequentially, and the server stops on the first failed Airtable request
- If a later batch fails after earlier batches succeeded, the operation status becomes `partially_completed` and the result includes which batches/records succeeded before the failure
- After approval, executes against the Airtable API using the requesting user's Airtable token, never a shared sync token
- After execution, partial completion, or failure, triggers an immediate base sync

---

### 3.5 `sync`

Force a refresh of a base's DuckDB cache.

**Input:**
```json
{
  "base": "string — base ID or base name"
}
```

**Output:**
```json
{
  "operation_id": "sync_XXXXXXXX",
  "status": "syncing",
  "estimated_seconds": 15,
  "last_synced_at": "2026-04-01T11:59:00Z"
}
```

**Behavior:**
- Non-blocking: returns immediately with an operation ID and ETA
- ETA is estimated from the last sync's duration for this base (or a rough estimate based on table/record count)
- If a sync is already in progress, returns the existing operation ID
- The agent can use `check_operation` to poll for completion

---

### 3.6 `check_operation`

Poll the status of a sync or mutate operation.

**Input:**
```json
{
  "operation_id": "string — op_XXX or sync_XXX"
}
```

**Output (sync):**
```json
{
  "operation_id": "sync_XXXXXXXX",
  "type": "sync",
  "status": "completed | syncing | failed",
  "completed_at": "2026-04-01T12:00:15Z",
  "tables_synced": 8,
  "records_synced": 4230
}
```

**Output (mutate):**
```json
{
  "operation_id": "op_XXXXXXXX",
  "type": "mutate",
  "status": "pending_approval | approved | rejected | expired | executing | completed | partially_completed | failed",
  "approval_url": "https://better-airtable-mcp.hackclub.com/approve/op_XXXXXXXX",
  "summary": "Create 3 records in projects",
  "result": {
    "created_record_ids": ["recAAA", "recBBB", "recCCC"],
    "completed_batches": 1,
    "failed_batch": null
  }
}
```

---

## 4. Sync System

### 4.1 Sync Worker Lifecycle

Each active base has a dedicated **sync worker** goroutine.

```
Base becomes active (first query/list_schema)
  → Spawn sync worker goroutine
  → Immediately perform full sync
  → Enter continuous sync loop while active
  → On each tool call touching this base, reset the TTL timer

TTL expires (10 minutes with no tool calls)
  → Stop the sync worker
  → Delete the DuckDB file from disk
  → Clean up in-memory state

Server starts or restarts
  → Load active bases from `sync_state` where `active_until > now()`
  → Restore sync workers for those still-active bases
  → Sweep `DUCKDB_DATA_DIR` for stale `.db` files
  → Delete any DuckDB file whose base is expired or has no corresponding active `sync_state`
```

**Continuous sync scheduling:**
- Only one sync may run for a base at a time
- Let `target_interval = SYNC_INTERVAL_SECONDS`
- After each sync completes, the next sync start time is `last_sync_started_at + target_interval`
- If the completed sync took longer than `target_interval`, the next sync starts immediately
- Manual `sync` requests coalesce onto the in-flight or next-due sync operation instead of starting overlapping syncs

### 4.2 Full Sync Process

1. Fetch the base schema from Airtable (`GET /v0/meta/bases/{baseId}/tables`)
2. For each table:
   a. Paginate through all records (`GET /v0/{baseId}/{tableId}`, 100 records/page)
   b. Respect rate limit: 5 requests/sec/base
3. Open the DuckDB write connection
4. Within a transaction:
   a. Drop and recreate all tables (full refresh)
   b. Create the `_metadata` table (see §4.3)
   c. Insert all records
   d. Update the `_sync_info` table with the current timestamp
5. Close the write transaction (readers see the new data atomically)

### 4.3 DuckDB Schema

**Per-base DuckDB file**: `/data/duckdb/{base_id}.db`

**Metadata table** (`_metadata`):
```sql
CREATE TABLE _metadata (
  duckdb_table_name  VARCHAR,
  original_table_name VARCHAR,
  airtable_table_id  VARCHAR,
  duckdb_column_name VARCHAR,
  original_field_name VARCHAR,
  airtable_field_id  VARCHAR,
  airtable_field_type VARCHAR,
  duckdb_type        VARCHAR
);
```

**Sync info table** (`_sync_info`):
```sql
CREATE TABLE _sync_info (
  last_synced_at TIMESTAMP,
  sync_duration_ms BIGINT,
  total_records BIGINT,
  total_tables INTEGER
);
```

**Data tables**: One table per Airtable table, with sanitized snake_case names.

Every data table has these implicit columns:
- `id` (VARCHAR) — the Airtable record ID (`recXXXXXX`)
- `created_time` (TIMESTAMP) — Airtable's record created time

### 4.4 Field Type Mapping

| Airtable Type | DuckDB Type | Notes |
|---|---|---|
| singleLineText, multilineText, richText, email, url, phoneNumber | VARCHAR | |
| number, percent, currency | DOUBLE | |
| autoNumber | BIGINT | |
| checkbox | BOOLEAN | |
| date, dateTime, createdTime, lastModifiedTime | TIMESTAMP | |
| singleSelect | VARCHAR | Choice name as string |
| multipleSelects | VARCHAR[] | Array of choice names |
| multipleRecordLinks | VARCHAR[] | Array of record IDs |
| lookup | JSON | Heterogeneous results |
| rollup | VARCHAR | Sniff result type; store as string if ambiguous |
| formula | VARCHAR | Same as rollup |
| multipleAttachments | JSON | Array of `{id, url, filename, size, type}` |
| createdBy, lastModifiedBy | VARCHAR | User name |
| barcode | VARCHAR | |
| button | *(omitted)* | Not data |
| rating | BIGINT | |
| duration | DOUBLE | Seconds |

### 4.5 Table Name Sanitization

Airtable table names are converted to snake_case for DuckDB:

1. Strip emoji and non-ASCII characters
2. Replace spaces, hyphens, and other separators with underscores
3. Collapse multiple underscores
4. Lowercase everything
5. If the result is empty or starts with a digit, prefix with `t_`
6. If two tables collide after sanitization, append `_2`, `_3`, etc.

Examples:
- `"Project Tracker 🚀"` → `project_tracker`
- `"Q1 2026 OKRs"` → `q1_2026_okrs`
- `"Tasks"` → `tasks`

Field names follow the same sanitization rules.

### 4.6 Shared Cache Model

DuckDB files are **shared across all users** who have access to the same base for reads.

- One sync worker per base (not per user)
- Sync uses a designated authorized user's token with read access to refresh the shared cache
- Before allowing a user to query a base, verify they have a valid Airtable token with access to that base
- Before allowing a user to mutate a base, verify they still have access to the base, then execute the mutation using that same requesting user's Airtable token so Airtable enforces that user's write permissions
- **Global rate limiter per base ID** ensures that even with many users, we respect Airtable's 5 req/sec/base limit
- DuckDB concurrent access: one write connection (sync worker), multiple read-only connections (query handlers)

---

## 5. Approval System

### 5.1 Approval Flow

```
Agent calls `mutate` tool
  → Server validates payload, resolves field names to IDs
  → Server fetches current values for updates (to build diff)
  → Server stores pending operation in Postgres
  → Server returns approval URL + operation ID to agent
  → Agent tells user: "Please review and approve: <url>"

User visits approval URL
  → Server renders rich preview page (React SPA)
  → User reviews changes
  → User clicks "Approve" or "Reject"

If approved:
  → Server executes mutations against Airtable API using the requesting user's Airtable token
  → Server triggers immediate base sync
  → Operation status → "completed" or "partially_completed"

If rejected:
  → Operation status → "rejected"
  → No changes made

If 10 minutes pass with no action:
  → Operation status → "expired"
  → No changes made
```

### 5.2 Approval Page (React SPA)

The approval page is a small React/Vite app bundled into the Go binary via `embed.FS`. It renders a **mini Airtable-style UI**.

**Page contents:**
- Base name + table name
- Operation type (create / update / delete)
- Timestamp and expiry countdown
- Which MCP session/client requested it
- Explicit notice that the approval URL is the credential: anyone with the link can approve or reject until expiry
- Explicit staleness notice that the preview is generated from the latest synced DuckDB snapshot, not a live Airtable re-read, so Airtable data may have changed since the preview was generated
- The cache timestamp used for the preview (`last_synced_at`)

**For record creates:**
- Table view showing all records that will be created
- Fields rendered with appropriate types (dates formatted, checkboxes shown, etc.)

**For record updates:**
- Side-by-side or inline diff: old value → new value for each changed field
- Unchanged fields shown dimmed for context

**For record deletes:**
- Full current data for each record being deleted, so the user sees what they're losing

**Actions:**
- **Approve** button (green) — executes the operation
- **Reject** button (red) — cancels the operation
- Both require a single click (no second confirmation)

### 5.3 Approval Persistence (Postgres)

```sql
CREATE TABLE pending_operations (
  id                         TEXT PRIMARY KEY,  -- high-entropy opaque secret used directly in the approval URL
  user_id                    TEXT NOT NULL,
  base_id                    TEXT NOT NULL,
  status                     TEXT NOT NULL,     -- pending_approval, approved, rejected, expired, executing, completed, partially_completed, failed
  operation_type             TEXT NOT NULL,     -- record_mutation
  payload_ciphertext         BYTEA NOT NULL,    -- encrypted mutation payload with resolved field IDs
  current_values_ciphertext  BYTEA,             -- encrypted snapshot used for diff display
  result_ciphertext          BYTEA,             -- encrypted execution result
  error                      TEXT,              -- error message if failed
  created_at                 TIMESTAMPTZ NOT NULL,
  expires_at                 TIMESTAMPTZ NOT NULL,
  resolved_at                TIMESTAMPTZ        -- when approved/rejected
);
```

A background goroutine expires stale operations every minute.

---

## 6. Postgres Schema

```sql
-- Users and their Airtable tokens
CREATE TABLE users (
  id                TEXT PRIMARY KEY,
  airtable_user_id  TEXT UNIQUE,
  email             TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE airtable_tokens (
  user_id                  TEXT PRIMARY KEY REFERENCES users(id),
  access_token_ciphertext  BYTEA NOT NULL,  -- app-level encrypted
  refresh_token_ciphertext BYTEA NOT NULL,  -- app-level encrypted
  expires_at               TIMESTAMPTZ NOT NULL,
  scopes                   TEXT NOT NULL,
  updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- MCP session tokens (what we issue to MCP clients)
CREATE TABLE mcp_tokens (
  token_hash        TEXT PRIMARY KEY,     -- SHA-256 of the bearer token
  user_id           TEXT NOT NULL REFERENCES users(id),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at        TIMESTAMPTZ NOT NULL
);

-- Track which users have access to which bases (for shared cache)
CREATE TABLE user_base_access (
  user_id           TEXT NOT NULL REFERENCES users(id),
  base_id           TEXT NOT NULL,
  permission_level  TEXT NOT NULL,
  last_verified_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (user_id, base_id)
);

-- Pending operations (see §5.3)
CREATE TABLE pending_operations (
  id                         TEXT PRIMARY KEY,
  user_id                    TEXT NOT NULL REFERENCES users(id),
  base_id                    TEXT NOT NULL,
  status                     TEXT NOT NULL,
  operation_type             TEXT NOT NULL,
  payload_ciphertext         BYTEA NOT NULL,  -- app-level encrypted
  current_values_ciphertext  BYTEA,           -- app-level encrypted
  result_ciphertext          BYTEA,           -- app-level encrypted
  error                      TEXT,
  created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at                 TIMESTAMPTZ NOT NULL,
  resolved_at                TIMESTAMPTZ
);

-- Sync state tracking
CREATE TABLE sync_state (
  base_id           TEXT PRIMARY KEY,
  last_synced_at    TIMESTAMPTZ,
  last_sync_duration_ms BIGINT,
  total_records     BIGINT,
  total_tables      INTEGER,
  active_until      TIMESTAMPTZ,         -- TTL: when to stop syncing
  sync_token_user_id TEXT REFERENCES users(id)  -- whose token is being used to sync
);

-- OAuth client registrations (Dynamic Client Registration for MCP clients)
CREATE TABLE oauth_clients (
  client_id         TEXT PRIMARY KEY,
  client_secret_hash TEXT,
  redirect_uris     TEXT[] NOT NULL,
  client_name       TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### 6.1 Sensitive Data Handling

- Airtable access tokens, refresh tokens, and pending operation payloads are encrypted at the application layer before writing to Postgres
- The encryption key is supplied via environment variable and never written to disk
- MCP bearer tokens are never stored in plaintext; only a hash is stored
- Server logs must never include Airtable tokens or decrypted mutation payloads
- Approval previews are decrypted only for the specific HTTP request serving the approval page

---

## 7. HTTP Endpoints

### 7.1 MCP

| Method | Path | Purpose |
|---|---|---|
| POST | `/mcp` | MCP Streamable HTTP endpoint (all JSON-RPC) |
| GET | `/mcp` | SSE stream for server-initiated messages |

### 7.2 OAuth (MCP-facing)

| Method | Path | Purpose |
|---|---|---|
| GET | `/.well-known/oauth-protected-resource` | RFC 9728 resource metadata |
| GET | `/.well-known/oauth-authorization-server` | RFC 8414 auth server metadata |
| POST | `/oauth/register` | Dynamic Client Registration |
| GET | `/oauth/authorize` | Authorization endpoint |
| POST | `/oauth/token` | Token exchange |

### 7.3 OAuth (Airtable-facing)

| Method | Path | Purpose |
|---|---|---|
| GET | `/oauth/airtable/callback` | Airtable OAuth callback |

### 7.4 Approval

| Method | Path | Purpose |
|---|---|---|
| GET | `/approve/{operation_id}` | Serve approval page (React SPA) |
| GET | `/api/operations/{operation_id}` | API: get operation details for the SPA |
| POST | `/api/operations/{operation_id}/approve` | API: approve operation |
| POST | `/api/operations/{operation_id}/reject` | API: reject operation |

### 7.5 Landing

| Method | Path | Purpose |
|---|---|---|
| GET | `/` | Render the README as the landing page |

---

## 8. Project Structure

```
better-airtable-mcp/
├── cmd/
│   └── server/
│       └── main.go              # Entrypoint
├── internal/
│   ├── mcp/
│   │   ├── handler.go           # Streamable HTTP handler, JSON-RPC dispatch
│   │   ├── session.go           # Session management (Mcp-Session-Id)
│   │   └── tools.go             # Tool definitions and dispatch
│   ├── tools/
│   │   ├── query.go             # query tool implementation
│   │   ├── list_schema.go       # list_schema tool implementation
│   │   ├── mutate.go            # mutate tool implementation
│   │   ├── sync.go              # sync tool implementation
│   │   ├── search_bases.go      # search_bases tool implementation
│   │   └── check_operation.go   # check_operation tool implementation
│   ├── sync/
│   │   ├── worker.go            # Per-base sync worker goroutine
│   │   ├── manager.go           # Manages worker lifecycle and TTLs
│   │   ├── airtable.go          # Airtable API client (list records, schema)
│   │   └── duckdb.go            # DuckDB write operations (schema creation, inserts)
│   ├── duckdb/
│   │   ├── pool.go              # Read-only connection pool per base
│   │   ├── schema.go            # Table name sanitization, type mapping
│   │   └── metadata.go          # _metadata and _sync_info table operations
│   ├── approval/
│   │   ├── handler.go           # HTTP handlers for approval API
│   │   ├── executor.go          # Executes approved mutations against Airtable
│   │   └── expiry.go            # Background goroutine to expire stale operations
│   ├── oauth/
│   │   ├── provider.go          # MCP-facing OAuth provider (authorize, token, register)
│   │   ├── airtable.go          # Airtable OAuth client (auth, callback, refresh)
│   │   ├── middleware.go         # Bearer token validation middleware
│   │   └── wellknown.go         # .well-known endpoint handlers
│   ├── db/
│   │   ├── postgres.go          # Postgres connection and migrations
│   │   └── queries.go           # SQL queries (consider sqlc)
│   └── config/
│       └── config.go            # Configuration (env vars, defaults)
├── frontend/
│   ├── src/
│   │   ├── App.tsx              # Approval page SPA
│   │   ├── components/
│   │   │   ├── RecordTable.tsx   # Table view for creates
│   │   │   ├── DiffView.tsx      # Old → new diff for updates
│   │   │   └── DeletePreview.tsx  # Records being deleted
│   │   └── ...
│   ├── package.json
│   └── vite.config.ts
├── Dockerfile
├── docker-compose.yml           # Local dev (Postgres + app)
├── README.md                    # Also serves as the landing page
├── go.mod
├── go.sum
└── SPEC.md                      # This file
```

---

## 9. Configuration

All configuration via environment variables:

| Variable | Description | Default |
|---|---|---|
| `PORT` | HTTP server port | `8080` |
| `DATABASE_URL` | Postgres connection string | required |
| `DUCKDB_DATA_DIR` | Directory for DuckDB files | `/data/duckdb` |
| `AIRTABLE_CLIENT_ID` | Airtable OAuth app client ID | required |
| `AIRTABLE_CLIENT_SECRET` | Airtable OAuth app client secret | required |
| `BASE_URL` | Public URL of the service | required |
| `APP_ENCRYPTION_KEY` | 32-byte key for app-level encryption of Airtable tokens and pending operation payloads | required |
| `SYNC_INTERVAL_SECONDS` | Target seconds between sync starts while a base is active | `60` |
| `SYNC_TTL_MINUTES` | Minutes of inactivity before cache eviction | `10` |
| `APPROVAL_TTL_MINUTES` | Minutes before pending approvals expire | `10` |
| `QUERY_DEFAULT_LIMIT` | Default row limit for queries | `100` |
| `QUERY_MAX_LIMIT` | Maximum row limit for queries | `1000` |

---

## 10. Key Design Decisions

| Decision | Rationale |
|---|---|
| **Full sync, not incremental** | Simpler implementation; Airtable's API doesn't support efficient deltas without webhooks |
| **Polling, not webhooks** | User reports webhooks are unreliable |
| **Shared DuckDB per base** | Avoids redundant syncs; Airtable permissions are base-level for reads |
| **Read-only DuckDB + SQL allowlist** | Prevents agents from corrupting the cache or escaping to the filesystem; enforced by connection config and SQL validation |
| **Approval for all record writes** | Core product thesis — no surprises |
| **Approval is atomic; execution is not** | Users make a single approve/reject decision, but Airtable's API does not offer cross-request transactions |
| **Snake_case table/field names** | SQL ergonomics for LLMs; metadata table preserves the mapping |
| **10-minute approval expiry** | Long enough to review, short enough to avoid stale operations |
| **On-disk DuckDB (not in-memory)** | Survives sync worker restarts; shared across connections |
| **DuckDB files wiped on redeploy** | Acceptable since they're caches; Postgres is the source of truth for everything durable |
| **Startup sweep for stale DuckDB files** | TTL-based cleanup is lazy during normal operation, so boot should remove expired or orphaned cache files left behind by crashes/restarts |
| **Single Airtable OAuth app** | Simpler onboarding; users authorize once, not per-base |
| **Writes execute as the requesting user** | Shared caches are fine for reads, but Airtable must enforce write permissions for the specific approving/requesting user |
| **Continuous resync while active** | Avoids drift from a fixed poll loop when sync duration exceeds the nominal interval |
| **Records-only mutations in V1** | Keeps the approval flow and executor focused on the core use case |
| **Encrypt sensitive state at rest** | Protects third-party tokens and pending mutation payloads stored in Postgres |

---

## 11. Rate Limiting & Performance

### Airtable API
- **5 requests/sec per base** — enforced by a global rate limiter keyed on base ID
- **50 requests/sec per user token** — enforced per-user
- **10 records per batch** for create/update/delete
- **429 responses**: back off for 30 seconds

### Sync Performance (estimates)
| Base size | Tables | Records | Estimated sync time |
|---|---|---|---|
| Small | 5 | 500 | ~2s |
| Medium | 15 | 5,000 | ~15s |
| Large | 30 | 50,000 | ~120s |

- Active bases run a single continuous sync loop; if a sync takes longer than `SYNC_INTERVAL_SECONDS`, the next sync starts immediately after completion

### Query Performance
- DuckDB queries against local files are sub-second for most analytical queries
- The bottleneck is sync, never query

---

## 12. Error Handling

| Scenario | Behavior |
|---|---|
| Airtable token expired, refresh succeeds | Transparent to user |
| Airtable token expired, refresh fails | Return error; agent tells user to re-authenticate |
| Sync fails mid-way | Retain previous DuckDB state; log error; retry on next interval |
| Query on unsynced base | Trigger sync, wait for completion, then execute query |
| Query is not exactly one `SELECT` or `WITH` statement | Return validation error; do not execute |
| Mutate with invalid field names | Return error with suggestions (closest matching field names) |
| Mutate approved but Airtable API fails before any batch succeeds | Operation status → `failed` with error details |
| Mutate approved but Airtable API fails after one or more batches succeed | Operation status → `partially_completed`; include partial result details and stop on first failed Airtable request |
| DuckDB file missing (post-redeploy) | Triggers fresh sync transparently |

---

## 13. Future Considerations (Out of Scope for V1)

- **Incremental sync** using Airtable's `lastModifiedTime` to only fetch changed records
- **Webhook-based invalidation** if Airtable webhook reliability improves
- **Cross-base joins** via server-controlled multi-base query plumbing (not raw SQL `ATTACH`)
- **Push notifications** (email/Slack) for approval requests
- **Audit log** of all approved/rejected operations
- **View-scoped sync** (sync only records visible in a specific Airtable view)
- **Streaming large query results** instead of returning all rows at once
- **Schema mutations** (`create_table`, `add_field`, `update_field`) after the record-mutation flow is solid
