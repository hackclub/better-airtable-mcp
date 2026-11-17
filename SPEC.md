# Better Airtable MCP — Technical Specification

## Overview

Better Airtable MCP is a hosted MCP (Model Context Protocol) server that gives AI agents fast, SQL-based read access to Airtable bases via DuckDB, and safe record write access through a human-in-the-loop approval flow.

**Core thesis**: Existing Airtable MCPs let agents create and modify records in ways that surprise users. Better Airtable MCP solves this by (1) making reads fast and powerful via SQL, and (2) requiring explicit human approval for all record mutations via a rich preview UI.

- **URL**: `https://better-airtable-mcp.hackclub.com`
- **Language**: Go
- **Transport**: MCP Streamable HTTP (spec 2025-11-25)
- **Hosting**: Coolify (Docker), persistent disk for DuckDB files (wiped on redeploy)
- **Durable storage**: Postgres (OAuth tokens, MCP refresh tokens, pending approvals, user records)

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
8. We complete our own OAuth flow, minting an MCP bearer token plus an MCP refresh token for the MCP client
9. The MCP client uses our bearer token on every subsequent request and can refresh it via `POST /oauth/token`

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

### 2.4 MCP OAuth Token Issuance and Refresh

The MCP-facing OAuth provider issues tokens that are separate from the Airtable tokens we hold for users.

- **Access token TTL**: 24 hours
- **Refresh token TTL**: 30 days
- **Access token storage**: only the SHA-256 hash is stored in `mcp_tokens`
- **Refresh token storage**: only the SHA-256 hash is stored in `mcp_refresh_tokens`; raw refresh tokens are returned once to the client and never logged
- **Refresh semantics**: MCP refresh tokens are single-use. A successful `grant_type=refresh_token` exchange atomically deletes the submitted refresh token row and issues a new access token plus a rotated refresh token.
- **Restart/redeploy behavior**: MCP refresh grants must be durable in Postgres, not process memory, so clients can refresh across Coolify deploys and app restarts.
- **Client binding**: If the refresh request supplies `client_id`, it must match the `client_id` stored with the refresh token.
- **Failure handling**: Missing, expired, already-consumed, or client-mismatched refresh tokens return OAuth `invalid_grant`; the MCP client must reconnect or re-authorize.
- **Cleanup**: The OAuth cleanup loop prunes expired MCP refresh token rows.

### 2.5 MCP OAuth Endpoints

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

### 3.1 `list_bases`

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
  "last_synced_at": "",
  "sync": {
    "operation_id": "sync_appXXXXXXXXXX",
    "status": "syncing",
    "read_snapshot": "partial",
    "sync_started_at": "2026-04-01T12:00:00Z",
    "tables_total": 8,
    "tables_started": 8,
    "tables_completed": 1,
    "pages_fetched": 8,
    "records_visible": 430,
    "records_synced_this_run": 430
  },
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
      "visible_record_count": 37,
      "total_record_count": null,
      "table_complete": false,
      "sync_status": "syncing"
    }
  ]
}
```

**MCP tool result text format:**
```text
sync_status
operation_id,status,read_snapshot,sync_started_at,last_synced_at,tables_total,tables_started,tables_completed,pages_fetched,records_visible,records_synced_this_run,error
sync_appXXXXXXXXXX,syncing,partial,2026-04-01T12:00:00Z,,8,8,1,8,430,430,

base
base_id,base_name
appXXXXXXXXXX,Project Tracker

tables

# projects
id,created_time,name,status
recXXX,2026-04-01T12:00:00Z,Website Redesign,In Progress
recYYY,2026-04-01T12:05:00Z,API Migration,Done
recZZZ,2026-04-01T12:10:00Z,Mobile App,Planning

# tasks
id,created_time,name
recTask1,2026-04-01T13:00:00Z,Design new homepage
recTask2,2026-04-01T13:15:00Z,QA revised layout
```

- In the text output only, each sample cell is capped at 100 characters; truncated values end with ` [truncated]`

**Behavior:**
- Resolves base by ID or name (via `list_bases` internally if name is given)
- If the base has no local snapshot yet, initializes schema immediately, creates empty/queryable DuckDB tables, starts a background sync, and waits until the first page for every table has been ingested (or an empty first page proves that table currently has no records) before returning
- Returns up to 3 sample rows per table from the DuckDB cache
- Includes the metadata mapping (Airtable IDs ↔ DuckDB names) so the agent can construct mutations later
- Includes top-level sync progress plus per-table sync state
- `last_synced_at` is blank until the first full sync completes
- `visible_record_count` is the number of rows currently queryable from that table
- `total_record_count` is only authoritative after `table_complete=true`
- `content[0].text` is organized as `sync_status`, `base`, `tables`, then one `# {duckdb_table_name}` CSV section per table using the queryable column names as headers (`id`, `created_time`, and sanitized Airtable field names)
- The agent should call this before writing queries or mutations to understand the schema

---

### 3.3 `query`

Execute one or more read-only SQL queries against a base's DuckDB cache.

**Input:**
```json
{
  "base": "string — base ID or base name",
  "sql": [
    "string — exactly one SELECT or WITH query",
    "string — exactly one SELECT or WITH query"
  ]
}
```

**Output:**
```json
{
  "freshness": {
    "state": "fresh",
    "as_of": "2026-04-01T12:00:00Z",
    "reconciled_at": "2026-04-01T08:00:00Z",
    "tables_catching_up": []
  },
  "sync": {
    "operation_id": "sync_appXXXXXXXXXX",
    "status": "syncing",
    "read_snapshot": "partial",
    "sync_started_at": "2026-04-01T12:00:00Z",
    "tables_total": 8,
    "tables_started": 3,
    "tables_completed": 1,
    "pages_fetched": 12,
    "records_visible": 430,
    "records_synced_this_run": 430
  },
  "results": [
    {
      "sql": "SELECT name, status, due_date FROM projects WHERE status = 'In Progress' LIMIT 100",
      "columns": ["name", "status", "due_date"],
      "rows": [
        ["Website Redesign", "In Progress", "2026-04-15"],
        "..."
      ],
      "row_count": 42,
      "truncated": false,
      "last_synced_at": "",
      "next_sync_at": ""
    }
  ]
}
```

**MCP tool result format:**
- `structuredContent` contains the JSON payload above
- `content[0].text` contains a CSV-style text summary for agents that prefer plain text

Single-query text format:
```text
sync_status
operation_id,status,read_snapshot,sync_started_at,last_synced_at,tables_total,tables_started,tables_completed,pages_fetched,records_visible,records_synced_this_run,error
sync_appXXXXXXXXXX,syncing,partial,2026-04-01T12:00:00Z,,8,3,1,12,430,430,

query_metadata
row_count,truncated,last_synced_at,next_sync_at
42,false,,

query_rows
name,status,due_date
Website Redesign,In Progress,2026-04-15
```

Multi-query text format:
```text
sync_status
operation_id,status,read_snapshot,sync_started_at,last_synced_at,tables_total,tables_started,tables_completed,pages_fetched,records_visible,records_synced_this_run,error
sync_appXXXXXXXXXX,syncing,partial,2026-04-01T12:00:00Z,,8,3,1,12,430,430,

query_1_metadata
sql,row_count,truncated,last_synced_at,next_sync_at
"SELECT name, status FROM projects ORDER BY name LIMIT 100",42,false,,

query_1_rows
name,status
Website Redesign,In Progress

query_2_metadata
sql,row_count,truncated,last_synced_at,next_sync_at
"SELECT name FROM tasks ORDER BY name LIMIT 100",12,false,,

query_2_rows
name
Design new homepage
```

**Behavior:**
- Requires `sql` to be an array of SQL strings, even when executing only one query; results are returned in the same order as the input batch
- Validates that each SQL string is exactly one top-level `SELECT` or `WITH` statement; rejects multi-statement SQL and anything containing write/DDL/admin statements
- Opens a **read-only** DuckDB connection (enforced at the connection level)
- Applies a default `LIMIT 100` independently to each query only if that query's SQL text contains no `LIMIT` token anywhere
- If a query's SQL text contains `LIMIT` anywhere, the server assumes the caller is intentionally controlling row count and does not inject its own top-level limit for that query
- Returns freshness metadata for each query result so the agent can inform the user how current the data is
- Returns top-level sync progress so the agent can warn the user when they are looking at a partial initial snapshot
- If the base has no local snapshot yet, initializes schema immediately, starts a background sync, and executes against whatever subset is already visible instead of waiting for a full sync
- During the very first sync, queries may legitimately return partial or zero-row results even though more data is still loading
- DuckDB is hardened as if SQL is hostile: external file access disabled, extension install/load disabled, and arbitrary `ATTACH`, `COPY`, `INSTALL`, `LOAD`, and `PRAGMA` statements rejected

**SQL examples:**

Single query:
```json
{
  "base": "Project Tracker",
  "sql": [
    "SELECT name, status, due_date FROM projects WHERE status = 'In Progress'"
  ]
}
```

Multiple queries:
```json
{
  "base": "Project Tracker",
  "sql": [
    "SELECT name, status FROM projects ORDER BY name",
    "SELECT name FROM tasks ORDER BY name"
  ]
}
```

Join on linked records:
```sql
SELECT p.name, t.name AS task_name
FROM projects p, UNNEST(p.linked_tasks) AS u(task_id)
JOIN tasks t ON t.id = u.task_id
```

---

**Freshness.** Every `query` and `list_schema` result starts with a `freshness` block, in both the structured payload and the text the model reads:

| Field | Meaning |
|---|---|
| `state` | `fresh`: the cache is current. `catching_up`: a complete cache that is known to lag (a delta is still running or failing, or `tables_catching_up` is not empty). `first_sync`: the base is being downloaded for the first time and results cover only the records loaded so far |
| `as_of` | Edits and new records made in Airtable up to this time are in the cache |
| `reconciled_at` | Deletions made in Airtable up to this time are reflected (the last full fetch) |
| `tables_catching_up` | Tables whose formula, lookup and rollup columns may lag until the next full fetch |

In the `first_sync` state the text result leads with an explicit warning that counts are too low and that zero rows may be wrong, and names the `check_operation` ID to wait on. A partial cache used to be reported only through sync counters, and agents read empty results from it as real.

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

For `delete_records`, `records` may be either objects containing `id` or plain Airtable record ID strings.

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

**Alternative output when an update/delete target has not synced yet:**
```json
{
  "reason": "records_not_synced_yet",
  "base_id": "appXXXXXXXXXX",
  "base_name": "Project Tracker",
  "table": "projects",
  "record_ids": ["recXXX"],
  "sync": {
    "operation_id": "sync_appXXXXXXXXXX",
    "status": "syncing",
    "read_snapshot": "partial",
    "sync_started_at": "2026-04-01T12:00:00Z",
    "tables_total": 8,
    "tables_started": 3,
    "tables_completed": 1,
    "pages_fetched": 12,
    "records_visible": 430,
    "records_synced_this_run": 430
  }
}
```

**Behavior:**
- Validates the payload (field names resolve, record IDs exist for updates, etc.)
- `create_records` only requires schema readiness; it does not wait for a full base snapshot
- For updates/deletes, fetches current record values from DuckDB to generate the diff
- If an update/delete target ID is absent and that table is still incomplete, returns `records_not_synced_yet` instead of guessing whether the record exists
- If an update/delete target ID is absent and that table is complete, returns the normal `record not found` validation error
- Mixed requests are still all-or-nothing at approval-preparation time: if any update/delete target is not yet synced, the whole mutate request is rejected
- Stores the pending operation in Postgres
- Returns immediately with the approval URL and operation ID
- `operation_id` is generated from at least 128 bits of randomness; the approval URL itself is the credential
- The operation expires after **24 hours** by default if not approved (configurable via `APPROVAL_TTL_MINUTES`)
- **Approval is all-or-nothing**: the user approves or rejects the entire request as a unit; there is no partial approval UI
- **Execution is not transactional**: Airtable mutations are sent in batches of up to 10 records/request, sequentially, and the server stops on the first failed Airtable request
- If a later batch fails after earlier batches succeeded, the operation status becomes `partially_completed` and the result includes which batches/records succeeded before the failure
- After approval, executes against the Airtable API using the requesting user's Airtable token, never a shared sync token
- After execution, partial completion, or failure, triggers an immediate base sync

---

### 3.5 `sync`

Refresh a base's DuckDB cache now. Reads already pick up recent Airtable edits on their own (section 4.1), so this is rarely needed.

**Input:**
```json
{
  "base": "string — base ID or base name",
  "mode": "\"delta\" (default) | \"full\""
}
```

- `delta` fetches only what changed since the last sync and usually takes seconds
- `full` downloads every record again. It is the way to clear a record that was deleted in Airtable but still shows up, or a formula, lookup or rollup value that looks out of date (section 4.2)

**Output:**
```json
{
  "operation_id": "sync_appXXXXXXXXXX",
  "status": "syncing",
  "read_snapshot": "partial",
  "sync_started_at": "2026-04-01T12:00:00Z",
  "estimated_seconds": 15,
  "last_synced_at": "2026-04-01T11:59:00Z",
  "tables_total": 8,
  "tables_started": 3,
  "tables_completed": 1,
  "pages_fetched": 12,
  "records_visible": 430,
  "records_synced_this_run": 430,
  "error": ""
}
```

**Behavior:**
- Non-blocking: returns immediately with an operation ID and ETA
- ETA is estimated from the last completed full sync's duration for this base (or a rough default if no history exists)
- If a sync is already in progress, returns the existing operation ID
- Includes live progress counters for the current run
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
  "operation_id": "sync_appXXXXXXXXXX",
  "type": "sync",
  "status": "completed | syncing | failed",
  "read_snapshot": "partial | complete",
  "sync_started_at": "2026-04-01T12:00:00Z",
  "completed_at": "2026-04-01T12:00:15Z",
  "last_synced_at": "2026-04-01T12:00:15Z",
  "tables_total": 8,
  "tables_started": 8,
  "tables_completed": 8,
  "pages_fetched": 42,
  "records_visible": 4230,
  "records_synced_this_run": 4230,
  "error": ""
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

### 3.7 `manage_schema`

Request a schema change (create or rename tables and fields), subject to the same
human-approval flow as `mutate`. Requires the `schema.bases:write` Airtable scope.

**Supported operations only.** Airtable's meta API can create tables and fields and
rename/redescribe tables and fields. It **cannot** delete a table or field, and it
**cannot** change an existing field's type. This tool exposes only what the API supports
and refuses the rest with guidance rather than faking it (it never quietly renames a
field to "deprecated" and calls it a deletion).

**Input:**
```json
{
  "base": "string — base ID or base name",
  "operations": [
    {
      "type": "create_table | create_field | update_table | update_field",
      "table": "existing table name/ID (create_field, update_table, update_field)",
      "field": "existing field name/ID to rename (update_field)",
      "name": "new table name (create_table) / new field name (create_field) / rename target (update_*)",
      "description": "optional table or field description",
      "field_type": "Airtable field type (create_field)",
      "options": { "…": "Airtable field-options object (create_field)" },
      "fields": [
        { "name": "Name", "type": "singleLineText", "description": "", "options": {} }
      ]
    }
  ]
}
```

**Behavior:**
- Refuses deletion (`delete_*`/`remove`/`drop`) and type-change (a `change_field_type`-style
  op, or a `field_type` on `update_field`) with a plain explanation of the alternatives
  (create a new field of the desired type and migrate values; remove the old field by hand).
- Resolves table/field references to Airtable IDs from the synced schema; rejects
  unknown tables/fields and name collisions for create operations.
- Stores a pending operation with `operation_type = "schema_mutation"`; returns the
  approval URL and default 24-hour expiry, exactly like `mutate`.
- On approval, executes one meta API call per operation, sequentially, using the
  requesting user's Airtable token. A mid-sequence failure yields `partially_completed`.
- Schema changes restructure the base, so execution triggers a full base re-sync.

**Output:**
```json
{
  "operation_id": "op_XXXXXXXX",
  "status": "pending_approval",
  "approval_url": "https://better-airtable-mcp.hackclub.com/approve/op_XXXXXXXX",
  "expires_at": "2026-04-01T12:10:00Z",
  "summary": "Add field Status to Projects"
}
```

---

## 4. Sync System

### 4.1 Sync Worker Lifecycle

A base's cache is a DuckDB file that is **kept on disk** for as long as there is room. It is not deleted when the base goes idle or when the server restarts. A base that has been downloaded once answers reads from its cache immediately from then on, and only fetches what changed.

Each base in use has a dedicated **sync worker** goroutine.

```
Base is used (first query/list_schema/mutate after being idle)
  → Spawn sync worker goroutine
  → Cache file exists and is complete:
      reads are served from it straight away, and a delta sync starts
  → No cache file yet:
      fetch schema, create an empty/queryable snapshot, mark the base readable,
      and download every record in the background (first sync)
  → While the base stays in use, run a delta sync every SYNC_INTERVAL_SECONDS
  → On each tool call touching this base, reset the activity timer

Activity window expires (SYNC_TTL_MINUTES with no tool calls)
  → If the base's last full fetch is older than RECONCILE_INTERVAL_HOURS,
    run a reconcile now, while nobody is waiting on it
  → Stop the sync worker and clean up in-memory state
  → Keep the DuckDB file

Server starts or restarts
  → Load bases from `sync_state` where `active_until > now()` and restore their workers
  → Remove staging files left behind by a refresh that was interrupted
  → If the cache directory is over CACHE_MAX_MEGABYTES, delete the least
    recently used cache files (never one with a live worker) until it fits
```

**Sync modes.** A worker run is one of:

| Mode | When | What it does |
|---|---|---|
| Delta | The default, whenever the cache can be patched | Fetches only records created or edited since each table's watermark (section 4.2) |
| Full | No cache file, a cache written by an older layout, a changed Airtable schema, or a `sync` call with `mode: "full"` | Downloads every record (section 4.2.1) |
| Reconcile | The last full fetch is older than `RECONCILE_INTERVAL_HOURS` and the base just went idle, or the base has been in constant use for a second full interval | A full download throttled to 2 requests per second, so it leaves most of the base's shared API budget to everything else |

**Scheduling:**
- Only one run may be in progress for a base at a time
- After each run, the next delta starts at `last_sync_started_at + SYNC_INTERVAL_SECONDS`. When a delta is expensive (a bulk job is rewriting thousands of rows), the gap stretches to three times the delta's duration, up to 10 minutes, so refreshes do not run back to back against the base's shared API budget
- Manual `sync` requests coalesce onto the in-flight or next-due run instead of starting overlapping runs
- A read on a complete cache whose last successful sync started more than `SYNC_INTERVAL_SECONDS` ago asks for a delta and waits up to 5 seconds for it. If the delta is not done by then, the read is served from the cache as it is and reports `catching_up` (section 3.3). A read never fails because a refresh is slow or broken.

### 4.2 Delta Sync Process

A delta brings the cache up to date with a few requests instead of a full download. For a base where nothing changed it costs one schema request plus one record request per table.

1. Fetch the base schema and compare its fingerprint with the one stored in the cache (`_sync_info.schema_hash`). The fingerprint covers table and field IDs, names, types and options; it leaves out views, descriptions and select choices, which change often and do not invalidate stored rows. A different fingerprint means the cache cannot be patched, and a full sync runs instead.
2. For each table, list the records created or edited after `delta_watermark - 2 minutes` with
   `IS_AFTER(IF(LAST_MODIFIED_TIME(), LAST_MODIFIED_TIME(), CREATED_TIME()), <time>)`.
   `LAST_MODIFIED_TIME()` is blank for a record no user has edited, and a blank operand makes `IS_AFTER` an error that drops the record, so those fall back to their creation time. The two minute overlap covers clock skew and computed values that settle a moment after an edit; upserts are idempotent, so re-reading it is harmless.
3. Upsert the returned records by record ID in one transaction per batch, and advance the table's watermark to the time the listing started. The watermark only moves if it still equals the value the delta was planned against, so a full fetch that swapped a new file in underneath is never skipped past.
4. **Follow computed dependencies.** Airtable's modified time ignores lookups, rollups and formulas, so a change in one record does not mark the records that compute from it. The schema says which fields those are (`recordLinkFieldId`, `fieldIdInLinkedTable`, and formulas that reference a link field). A conditional lookup or rollup also depends on the fields its filter and sort conditions name; those are watched too, and only when one of them is not a cached column does any change to the source row count. When an upsert changes a value that another table looks up or rolls up, the rows linking to that record are refetched by record ID. This repeats until no watched value changes.
5. **Follow links.** Airtable mirrors a link edit on the record at the other end, so both ends normally arrive in step 2. A record that was *deleted* cannot arrive, but it disappears from every link that pointed at it. Record IDs that drop out of (or newly appear in) a link column, and that the delta did not already fetch, are fetched by ID. One Airtable no longer returns is deleted from the cache.
6. If following steps 4 and 5 would refetch more than 5,000 rows in one delta, the affected tables are flagged `computed_stale` instead and wait for the next full fetch. A flagged table makes the base's reconcile due at its next idle moment, whatever its age.

**The follow-up queue.** Steps 4 and 5 are driven by a queue stored in the cache file (`_delta_pending`). The write that upserts a row records, in the same transaction, which follow-up fetches it calls for. A delta that fails or is killed partway therefore cannot forget them: once a changed row is in the cache, the next listing returns it equal to the cache and a comparison would find nothing to follow. The next run, or the next write-through, drains the queue first.

**The first delta after a full fetch** treats every row its listings return as changed, without comparing. A full fetch reads its tables one after another, so a row edited while it ran can be cached with its new value while a row computed from it was read before the edit.

**Comparing values.** Attachment URLs are signed and differ on every read, so URLs are blanked before JSON columns are compared; otherwise every refetch of a row with an attachment would look like a change.

**What a delta cannot see** (a reconcile or a `sync` with `mode: "full"` fixes these):
- A deleted record that nothing linked to
- Formulas that change with the clock (`NOW()`, `TODAY()`)
- A renamed select choice

**Write-through.** After an approved mutation executes, the records it created or updated are refetched by ID and the ones it deleted are removed, followed by steps 4 and 5. The agent's next read sees its own write without waiting for a sync. `mutate` uses the same fetch-by-ID when an update or delete targets a record the cache does not have yet.

### 4.2.1 Full Sync Process

1. Fetch the base schema from Airtable (`GET /v0/meta/bases/{baseId}/tables`)
2. Create empty DuckDB tables plus `_metadata`, `_sync_info`, and `_table_sync` immediately
3. For each table, determine a best-effort server-side sort:
   a. If the table has a real Airtable field of type `createdTime`, request descending sort on that field
   b. Otherwise, use Airtable's default order
4. Fetch record pages in a fair round-robin schedule:
   a. At most one outstanding page request per table at a time
   b. First page of every table is fetched before the second page of any table, subject to response timing
   c. Requests are started as aggressively as allowed by the Airtable rate limiter (a reconcile is additionally capped at 2 requests per second)
5. Apply each fetched page to DuckDB as it arrives and update `_sync_info` / `_table_sync`
6. First sync of a base:
   a. Pages are written directly into the active DuckDB file
   b. `query` and `list_schema` can read partial data as soon as schema initialization completes, and say so loudly (`first_sync`)
7. Full sync of a base that already has a complete snapshot:
   a. Sync into `{base_id}.staging.db`
   b. Keep serving the active snapshot during the refresh; deltas keep applying to it
   c. Atomically swap the staging DB into place on success
   d. Discard the staging DB on failure
8. On completion, record the schema fingerprint and set every table's `delta_watermark` to the time the download started, so the next delta picks up anything edited while it ran

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
  sync_started_at TIMESTAMP,
  last_synced_at TIMESTAMP,
  sync_duration_ms BIGINT,
  total_records BIGINT,
  total_tables INTEGER,
  status VARCHAR,
  tables_started INTEGER,
  tables_completed INTEGER,
  pages_fetched BIGINT,
  records_synced_this_run BIGINT,
  error VARCHAR,
  schema_hash VARCHAR,   -- fingerprint of the Airtable schema the tables were built from
  cache_format INTEGER   -- layout version; an older file is rebuilt by a full sync
);
```

**Per-table sync table** (`_table_sync`):
```sql
CREATE TABLE _table_sync (
  duckdb_table_name VARCHAR PRIMARY KEY,
  sync_status VARCHAR,
  visible_record_count BIGINT,
  pages_fetched BIGINT,
  has_more BOOLEAN,
  delta_watermark TIMESTAMP,   -- edits and new records are applied up to here
  delta_checked_at TIMESTAMP,  -- last successful delta check
  reconciled_at TIMESTAMP,     -- last full fetch (deletions verified up to here)
  computed_stale BOOLEAN       -- rows may lag until the next full fetch
);
```

**Follow-up queue** (`_delta_pending`, section 4.2):
```sql
CREATE TABLE _delta_pending (
  generation INTEGER,     -- which pass of which run queued it
  source_table VARCHAR,   -- table whose write caused the follow-up
  kind VARCHAR,           -- 'changed' | 'changed_any' | 'link_end'
  column_name VARCHAR,    -- the changed column, or the link column
  record_id VARCHAR
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
| multipleLookupValues (lookup) | JSON | Heterogeneous results |
| count | BIGINT | Number of linked records |
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

Reserved field-name handling:
- Every synced table already has implicit DuckDB columns named `id` and `created_time`
- If an Airtable field would sanitize to `id` or `created_time`, the server remaps it to `_airtable_id` or `_airtable_created_time`
- If those remapped names also collide, numeric suffixes are appended (`_airtable_id_2`, `_airtable_created_time_2`, etc.)

### 4.6 Shared Cache Model

DuckDB files are **shared across all users** who have access to the same base for reads.

- One sync worker per base (not per user)
- Sync uses a designated authorized user's token with read access to refresh the shared cache
- Before allowing a user to query a base, verify they have a valid Airtable token with access to that base
- Before allowing a user to mutate a base, verify they still have access to the base, then execute the mutation using that same requesting user's Airtable token so Airtable enforces that user's write permissions
- **Global rate limiter per base ID** ensures that even with many users, we respect Airtable's 5 req/sec/base limit
- DuckDB concurrent access: one write connection (sync worker), multiple read-only connections (query handlers)
- During the first sync of a base, reads use the active partial snapshot
- During a refresh of an already-complete base, reads stay on the last complete snapshot until the staging refresh finishes successfully

---

## 5. Approval System

### 5.1 Approval Flow

```
Agent calls `mutate` tool
  → Server validates payload, resolves field names to IDs
  → Server fetches current values for updates/deletes (to build diff)
  → If an update/delete target row has not synced yet, return `records_not_synced_yet` with sync progress instead of creating an approval
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

If the approval TTL passes with no action (24 hours by default):
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

### 5.3 MCP Debug Page

The same embedded React/Vite bundle also exposes a lightweight MCP debugging page at `/debug`.

**Purpose:**
- Show the live MCP tool catalog exposed by the running server
- Let a developer complete the normal OAuth flow directly from the browser and obtain a bearer token for this server
- Let a developer initialize an MCP session directly against `/mcp`
- Let a developer execute individual tools with editable JSON arguments
- Show the full JSON-RPC request and response payloads for each tool call

**Behavior:**
- The page is intended for development/debugging and is not part of the normal end-user approval flow
- The page is OAuth-only; it does not accept a manually pasted bearer token
- Clicking `Connect with OAuth` uses the same browser-facing auth flow as any normal MCP client:
  - register or reuse a public OAuth client via `POST /oauth/register`
  - generate a browser PKCE verifier/challenge pair
  - redirect to `GET /oauth/authorize`
  - return to `/debug?code=...&state=...`
  - exchange the authorization code at `POST /oauth/token`
- The page stores the issued bearer token plus debug OAuth client metadata in browser local storage for local debugging convenience
- After OAuth succeeds, the page can initialize a session, call `tools/list`, and render one interactive card per tool
- Each tool card shows the tool name, description, input schema, editable JSON arguments, and the full raw response from `tools/call`
- MCP sessions are in-memory, so a server restart or hot reload invalidates them; if a tool call returns `session was not found`, the page should reinitialize the MCP session once and retry that tool call

### 5.4 Approval Persistence (Postgres)

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
  execution_started_at       TIMESTAMPTZ,       -- when execution begins
  resolved_at                TIMESTAMPTZ        -- terminal resolution time only
);
```

A background goroutine expires stale operations every minute.
It also marks operations stuck in `executing` for more than 15 minutes as failed,
independent of approval expiry. The atomic transition to `executing` records its
start in `execution_started_at`; `resolved_at` remains NULL until terminal resolution.
Execution itself is bounded to 5 minutes. On startup, legacy executing rows
without a start timestamp receive the startup time, giving them a full 15-minute
recovery window. Repeated migrations preserve existing timestamps.

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
  updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  reauth_required_at       TIMESTAMPTZ
);

-- MCP access tokens (what MCP clients send as bearer tokens)
CREATE TABLE mcp_tokens (
  token_hash        TEXT PRIMARY KEY,     -- SHA-256 of the bearer token
  user_id           TEXT NOT NULL REFERENCES users(id),
  client_id         TEXT,
  client_name       TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at        TIMESTAMPTZ NOT NULL
);

-- MCP refresh tokens (what MCP clients exchange for rotated bearer tokens)
CREATE TABLE mcp_refresh_tokens (
  token_hash        TEXT PRIMARY KEY,     -- SHA-256 of the refresh token
  user_id           TEXT NOT NULL REFERENCES users(id),
  client_id         TEXT NOT NULL,
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
  execution_started_at       TIMESTAMPTZ,     -- when execution begins
  resolved_at                TIMESTAMPTZ      -- terminal resolution time only
);

-- Sync state tracking
CREATE TABLE sync_state (
  base_id           TEXT PRIMARY KEY,
  last_synced_at    TIMESTAMPTZ,
  last_sync_duration_ms BIGINT,
  total_records     BIGINT,
  total_tables      INTEGER,
  active_until      TIMESTAMPTZ,         -- activity window: when the sync worker goes idle
  sync_token_user_id TEXT REFERENCES users(id), -- whose token is being used to sync
  last_used_at      TIMESTAMPTZ,         -- last tool call; orders cache eviction
  last_reconciled_at TIMESTAMPTZ,        -- last full fetch of the base
  cache_bytes       BIGINT               -- size of the base's cache file
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
- MCP bearer and refresh tokens are never stored in plaintext; only SHA-256 hashes are stored
- Server logs must never include Airtable tokens or decrypted mutation payloads
- Server logs must never include OAuth authorization codes, OAuth state values, PKCE verifiers, client secrets, raw request bodies, raw query strings, Airtable request bodies, raw approval URLs, or raw approval `op_...` identifiers
- Structured logging for mutation requests must log only IDs, counts, hashes, statuses, and sanitized previews; it must never log decrypted field values or approval payload contents
- Approval previews are decrypted only for the specific HTTP request serving the approval page

### 6.2 Observability and Logging

- The server emits structured JSON logs to stdout
- The server uses one fixed production-debuggable verbosity; it does not expose multiple runtime log levels
- Every HTTP request log includes a request ID, route template, method, status, duration, bytes written, and cancellation state
- Logs for MCP, OAuth, sync, Airtable, and approval flows must include stable event names plus safe correlation fields when available, including request IDs, user IDs, token hashes, client IDs, MCP session IDs, base IDs, table IDs, and sync operation IDs
- Approval operation IDs must be logged only as hashes
- MCP tool-call logs must include sanitized argument summaries rather than raw tool payloads
- SQL logging must use sanitized previews plus hashes rather than raw literal-bearing SQL text
- Airtable HTTP logging must record retries, failures, and slow calls without logging tokens or request bodies
- Approval logging must record prepare, approve, reject, batch execution, completion, and expiry events without logging approval URLs or mutation field values

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

### 7.5 Debug

| Method | Path | Purpose |
|---|---|---|
| GET | `/debug` | Serve embedded MCP debugging page |

### 7.6 Landing

| Method | Path | Purpose |
|---|---|---|
| GET | `/` | Render the README as the landing page |

### 7.7 Health

| Method | Path | Purpose |
|---|---|---|
| GET | `/healthz` | Health check endpoint for database-backed readiness |

---

## 8. Project Structure

```
better-airtable-mcp/
├── cmd/
│   └── server/
│       └── main.go              # Entrypoint
├── internal/
│   ├── approval/
│   │   ├── assets.go            # Embedded approval/debug frontend assets
│   │   ├── handler.go           # Approval page and approval API handlers
│   │   └── service.go           # Approval lifecycle, execution, and expiry
│   ├── config/
│   │   └── config.go            # Environment-driven server configuration
│   ├── cryptoutil/
│   │   └── cryptoutil.go        # Application-layer encryption helpers
│   ├── db/
│   │   └── postgres.go          # Postgres connection, migrations, and store methods
│   ├── duckdb/
│   │   ├── schema.go            # Table name sanitization and type mapping
│   │   └── store.go             # DuckDB snapshot storage operations
│   ├── health/
│   │   └── handler.go           # Health check endpoint
│   ├── httpx/
│   │   └── json.go              # Shared JSON HTTP helpers
│   ├── landing/
│   │   └── handler.go           # README-backed landing page handler
│   ├── logx/
│   │   ├── http.go              # Request logging middleware and route templates
│   │   ├── logx.go              # Structured logger and context helpers
│   │   └── sanitize.go          # Privacy-safe log sanitizers and summarizers
│   ├── mcp/
│   │   ├── handler.go           # Streamable HTTP handler and JSON-RPC dispatch
│   │   ├── session.go           # MCP session management and expiry
│   │   └── tools.go             # Tool catalog exposure and invocation plumbing
│   ├── oauth/
│   │   ├── airtable.go          # Airtable OAuth client and token exchange
│   │   ├── handler.go           # MCP-facing OAuth endpoints and callback handling
│   │   ├── middleware.go        # Bearer token validation middleware
│   │   ├── ratelimit.go         # OAuth endpoint rate limiting
│   │   └── token_manager.go     # Airtable token refresh and cached token access
│   ├── sync/
│   │   ├── airtable.go          # Airtable API client for sync reads and mutations
│   │   ├── manager.go           # Per-base sync worker lifecycle and coordination
│   │   └── service.go           # Snapshot sync execution and DuckDB writes
│   └── tools/
│       ├── auth.go              # Tool auth gating and capability checks
│       ├── catalog.go           # MCP tool catalog definitions
│       ├── check_operation.go   # check_operation tool
│       ├── common.go            # Shared tool helpers and logging
│       ├── instructions.go      # Tool instruction text
│       ├── list_schema.go       # list_schema tool
│       ├── mutate.go            # mutate tool
│       ├── query.go             # query tool
│       ├── runtime.go           # Shared runtime dependencies for tools
│       ├── search_bases.go      # search_bases tool
│       ├── sql.go               # SQL validation and sanitization helpers
│       ├── sync.go              # sync tool
│       └── text_result.go       # Text result rendering helpers
├── frontend/
│   ├── src/
│   │   ├── App.tsx              # Approval page SPA
│   │   ├── McpDebugPage.tsx     # Embedded MCP debugging page
│   │   ├── components/
│   │   │   ├── RecordTable.tsx  # Table view for creates
│   │   │   ├── DiffView.tsx     # Old -> new diff for updates
│   │   │   └── DeletePreview.tsx # Records being deleted
│   │   ├── formatters.tsx       # Shared approval/debug formatting helpers
│   │   ├── main.tsx             # Vite entrypoint
│   │   ├── styles.css           # Frontend styling
│   │   └── types.ts             # Shared frontend types
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
| `SYNC_INTERVAL_SECONDS` | Seconds between delta syncs while a base is in use; also how old the last sync may be before a read asks for one | `60` |
| `SYNC_TTL_MINUTES` | Minutes without a tool call before a base's sync worker goes idle. The cache file is kept | `10` |
| `RECONCILE_INTERVAL_HOURS` | Hours a base's last full fetch may age before a reconcile is scheduled. `0` disables scheduled reconciles | `6` |
| `CACHE_MAX_MEGABYTES` | Disk budget for cache files; over it, the least recently used are deleted. `0` means no limit | `8192` |
| `APPROVAL_TTL_MINUTES` | Minutes before pending approvals expire | `1440` (24 hours) |
| `QUERY_DEFAULT_LIMIT` | Default row limit for queries | `100` |
| `QUERY_MAX_LIMIT` | Maximum row limit for queries | `1000` |
| `SYNC_MAX_CONCURRENT` | Maximum base syncs running at once across the process; others wait for a slot | `4` |

---

## 10. Key Design Decisions

| Decision | Rationale |
|---|---|
| **Delta sync by modified time, full fetch as the backstop** | A full download of a large base costs thousands of requests against a 5 request per second limit shared with every other integration, and made every idle base start from zero. `filterByFormula` on `LAST_MODIFIED_TIME()` answers in well under a second per table even on a 60,000 row table, so only changes are fetched. A periodic full fetch covers what modified time cannot see |
| **Schema-first partial reads on initial sync** | Agents can start querying immediately while still seeing explicit sync progress |
| **Polling, not webhooks** | Webhook notifications are reported unreliable, and creating a webhook needs a base creator to grant an extra OAuth scope. The webhook payload log could later replace the reconcile for deletions |
| **Shared DuckDB per base** | Avoids redundant syncs; Airtable permissions are base-level for reads |
| **Read-only DuckDB + SQL allowlist** | Prevents agents from corrupting the cache or escaping to the filesystem; enforced by connection config and SQL validation |
| **Approval for all record writes** | Core product thesis — no surprises |
| **Approval is atomic; execution is not** | Users make a single approve/reject decision, but Airtable's API does not offer cross-request transactions |
| **Snake_case table/field names** | SQL ergonomics for LLMs; metadata table preserves the mapping |
| **24-hour default approval expiry** | Allows a full day to review; previews show a snapshot staleness notice |
| **On-disk DuckDB (not in-memory)** | Survives sync worker restarts; shared across connections |
| **DuckDB files kept across idle periods and restarts** | A kept cache answers the next read immediately and is patched by a delta. The files are still only caches: any of them can be deleted and is rebuilt from Airtable |
| **Disk budget instead of a time limit** | Cache files are evicted least recently used first when the directory outgrows `CACHE_MAX_MEGABYTES`, never because time passed |
| **Single Airtable OAuth app** | Simpler onboarding; users authorize once, not per-base |
| **Writes execute as the requesting user** | Shared caches are fine for reads, but Airtable must enforce write permissions for the specific approving/requesting user |
| **Reconcile when a base goes idle** | The full fetch is the expensive run, so it happens when nobody is waiting on a delta; a base in constant use gets one after a second overdue interval |
| **Reads never wait long for freshness** | A read waits at most 5 seconds for a delta, then answers from the cache and labels it `catching_up`; a broken refresh degrades freshness, not availability |
| **Round-robin page scheduling across tables** | Prevents one large table from starving every other table during the first sync |
| **Staging refreshes after the first complete snapshot** | Keeps existing readers on a full snapshot while a refresh is in flight |
| **Records-only mutations in V1** | Keeps the approval flow and executor focused on the core use case |
| **Encrypt sensitive state at rest** | Protects third-party tokens and pending mutation payloads stored in Postgres |

---

## 11. Rate Limiting & Performance

### Airtable API
- **5 requests/sec per base** — enforced by a global rate limiter keyed on base ID
- **50 requests/sec per user token** — enforced per-user
- **10 records per batch** for create/update/delete
- **429 responses**: back off for 30 seconds
- Record sync requests are scheduled round-robin across tables and pushed as hard as the limiter allows

### Sync Performance (estimates)
| Base size | Tables | Records | Estimated sync time |
|---|---|---|---|
| Small | 5 | 500 | ~2s |
| Medium | 15 | 5,000 | ~15s |
| Large | 30 | 50,000 | ~120s |

- These are first-sync and reconcile times. A delta sync costs one schema request and one record request per table when nothing changed, plus one request per 100 changed records

### Query Performance
- DuckDB queries against local files are sub-second for most analytical queries
- The bottleneck is sync, never query

---

## 12. Error Handling

| Scenario | Behavior |
|---|---|
| Airtable token expired, refresh succeeds | Transparent to user |
| Airtable token expired, refresh fails | Return error; agent tells user to re-authenticate |
| MCP access token expired, refresh token valid | Rotate MCP refresh token and return a new MCP access token transparently to the client |
| MCP refresh token missing, expired, already consumed, or client-mismatched | Return OAuth `invalid_grant`; client must reconnect or re-authorize |
| Initial sync fails mid-way | Keep the partial snapshot and mark sync status as failed; retry on next interval or manual sync |
| Refresh fails mid-way after a base already has a complete snapshot | Retain the previous complete DuckDB state; discard staging DB; emit a structured sanitized log event; retry on next interval |
| Query on a base with no local snapshot yet | Initialize schema immediately, start a background sync, and execute against the currently visible subset |
| Query is not exactly one `SELECT` or `WITH` statement | Return validation error; do not execute |
| Mutate with invalid field names | Return error with suggestions (closest matching field names) |
| Mutate update/delete target not yet synced | Return `records_not_synced_yet` plus sync progress; do not create an approval |
| Mutate approved but Airtable API fails before any batch succeeds | Operation status → `failed` with error details |
| Mutate approved but Airtable API fails after one or more batches succeed | Operation status → `partially_completed`; include partial result details and stop on first failed Airtable request |
| DuckDB file missing (first use, evicted, or deleted) | Triggers a first sync transparently |
| Delta sync fails | The cache keeps serving its last state; the worker retries with backoff and reads report `catching_up`. Follow-up fetches the failed run still owed stay queued in the cache file |
| Scheduled reconcile fails | Deltas continue; another reconcile is not attempted for 30 minutes |
| Cache file unreadable | The next sync is a full one that rebuilds it |
| Airtable schema changed since the cache was built | The next sync is a full one into a staging file; reads stay on the existing snapshot until it swaps in |
| Mutate update/delete target missing from the cache | Fetched from Airtable by record ID before the approval is prepared; only a record Airtable does not have is reported missing |
| DuckDB file unreadable by this build (newer storage version after a rollback, or not a DuckDB file) | The next sync deletes it and rebuilds it from Airtable |

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
- **Automation management** (list/create/update Airtable automations and their triggers/actions via the meta API) once schema mutations are solid
