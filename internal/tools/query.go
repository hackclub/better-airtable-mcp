package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/hackclub/better-airtable-mcp/internal/duckdb"
	"github.com/hackclub/better-airtable-mcp/internal/mcp"
)

type QueryInput struct {
	Base  string `json:"base"`
	SQL   string `json:"sql"`
	Limit int    `json:"limit,omitempty"`
}

type QueryTool struct {
	defaultLimit int
	maxLimit     int
	runtime      *Runtime
}

func NewQueryTool(defaultLimit, maxLimit int, runtime *Runtime) mcp.Tool {
	return QueryTool{
		defaultLimit: defaultLimit,
		maxLimit:     maxLimit,
		runtime:      runtime,
	}
}

func (QueryTool) Definition() mcp.ToolDefinition {
	return mcp.ToolDefinition{
		Name:        "query",
		Description: "Execute a read-only SQL query against a base's DuckDB cache.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"base": map[string]any{
					"type":        "string",
					"description": "Airtable base ID or base name.",
				},
				"sql": map[string]any{
					"type":        "string",
					"description": "Exactly one top-level SELECT or WITH query.",
				},
				"limit": map[string]any{
					"type":        "integer",
					"description": "Optional row limit override.",
					"minimum":     1,
				},
			},
			"required":             []string{"base", "sql"},
			"additionalProperties": false,
		},
	}
}

func (t QueryTool) Call(ctx context.Context, raw json.RawMessage) (mcp.ToolCallResult, error) {
	var input QueryInput
	if err := decodeArgs(raw, &input); err != nil {
		return mcp.ToolCallResult{}, err
	}

	input.Base = strings.TrimSpace(input.Base)
	if input.Base == "" {
		return mcp.ToolCallResult{}, fmt.Errorf("base is required")
	}

	normalized, err := NormalizeQuery(input.SQL, input.Limit, t.defaultLimit, t.maxLimit)
	if err != nil {
		return mcp.ToolCallResult{}, err
	}

	if t.runtime == nil || t.runtime.Syncer == nil {
		return mcp.ErrorResult("query execution is not implemented yet; SQL validation passed", map[string]any{
			"base":            input.Base,
			"normalized_sql":  normalized.SQL,
			"effective_limit": normalized.EffectiveLimit,
		}), nil
	}

	userID, ok := authenticatedUserID(ctx)
	if !ok {
		return mcp.ToolCallResult{}, fmt.Errorf("missing authenticated user")
	}

	if t.runtime.SyncManager != nil {
		if _, err := t.runtime.SyncManager.EnsureBaseReady(ctx, userID, input.Base); err != nil {
			return mcp.ToolCallResult{}, err
		}
	}

	accessToken, err := t.runtime.AirtableAccessToken(ctx, userID)
	if err != nil {
		return mcp.ToolCallResult{}, err
	}

	result, err := t.runtime.Syncer.QueryBase(ctx, accessToken, input.Base, normalized.ExecutionSQL)
	if err != nil {
		return mcp.ToolCallResult{}, err
	}
	result, truncated := applyQueryResultLimit(result, normalized)

	nextSyncAt := t.runtime.NextSyncTime(result.LastSyncedAt, result.LastSyncDuration)
	return mcp.TextResult(fmt.Sprintf("Query returned %d row(s).", result.RowCount), map[string]any{
		"columns":        result.Columns,
		"rows":           result.Rows,
		"row_count":      result.RowCount,
		"truncated":      truncated,
		"last_synced_at": result.LastSyncedAt.Format(time.RFC3339),
		"next_sync_at":   nextSyncAt.Format(time.RFC3339),
	}), nil
}

func applyQueryResultLimit(result duckdb.QueryResult, normalized NormalizedQuery) (duckdb.QueryResult, bool) {
	if !normalized.ServerAppliedLimit || result.RowCount <= normalized.EffectiveLimit {
		return result, false
	}

	result.Rows = append([][]any(nil), result.Rows[:normalized.EffectiveLimit]...)
	result.RowCount = normalized.EffectiveLimit
	return result, true
}
