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
- ETA is estimated from the last completed sync's duration for this base (or a rough default if no history exists)
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

## 4. Sync System

### 4.1 Sync Worker Lifecycle

Each active base has a dedicated **sync worker** goroutine.

```
Base becomes active (first query/list_schema)
  → Spawn sync worker goroutine
  → Fetch schema and create an empty/queryable DuckDB snapshot immediately
  → Mark the base readable as soon as schema initialization succeeds
  → Continue syncing records in the background
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
2. Create empty DuckDB tables plus `_metadata`, `_sync_info`, and `_table_sync` immediately
3. For each table, determine a best-effort server-side sort:
   a. If the table has a real Airtable field of type `createdTime`, request descending sort on that field
   b. Otherwise, use Airtable's default order
4. Fetch record pages in a fair round-robin schedule:
   a. At most one outstanding page request per table at a time
   b. First page of every table is fetched before the second page of any table, subject to response timing
   c. Requests are started as aggressively as allowed by the Airtable rate limiter
5. Apply each fetched page to DuckDB as it arrives and update `_sync_info` / `_table_sync`
6. Initial sync behavior:
   a. Pages are written directly into the active DuckDB file
   b. `query` and `list_schema` can read partial data as soon as schema initialization completes
7. Refresh behavior after a base already has a complete snapshot:
   a. Sync into `{base_id}.staging.db`
   b. Keep serving the last complete active snapshot during the refresh
   c. Atomically swap the staging DB into place on success
   d. Discard the staging DB on failure

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
  error VARCHAR
);
```

**Per-table sync table** (`_table_sync`):
```sql
CREATE TABLE _table_sync (
  duckdb_table_name VARCHAR PRIMARY KEY,
  sync_status VARCHAR,
  visible_record_count BIGINT,
  pages_fetched BIGINT,
  has_more BOOLEAN
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

### 7.5 Debug

| Method | Path | Purpose |
|---|---|---|
| GET | `/debug` | Serve embedded MCP debugging page |

### 7.6 Landing

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
│   │   ├── search_bases.go      # list_bases tool implementation
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
| **Schema-first partial reads on initial sync** | Agents can start querying immediately while still seeing explicit sync progress |
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
| Initial sync fails mid-way | Keep the partial snapshot and mark sync status as failed; retry on next interval or manual sync |
| Refresh fails mid-way after a base already has a complete snapshot | Retain the previous complete DuckDB state; discard staging DB; log error; retry on next interval |
| Query on a base with no local snapshot yet | Initialize schema immediately, start a background sync, and execute against the currently visible subset |
| Query is not exactly one `SELECT` or `WITH` statement | Return validation error; do not execute |
| Mutate with invalid field names | Return error with suggestions (closest matching field names) |
| Mutate update/delete target not yet synced | Return `records_not_synced_yet` plus sync progress; do not create an approval |
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
