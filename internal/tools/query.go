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
	Base string   `json:"base"`
	SQL  []string `json:"sql"`
}

func normalizeQueryBatch(queries []string) ([]string, error) {
	if len(queries) == 0 {
		return nil, fmt.Errorf("sql is required")
	}

	normalized := make([]string, 0, len(queries))
	for index, raw := range queries {
		sql := strings.TrimSpace(raw)
		if sql == "" {
			return nil, fmt.Errorf("sql[%d] is required", index)
		}
		normalized = append(normalized, sql)
	}

	return normalized, nil
}

type normalizedQueryCall struct {
	Normalized NormalizedQuery
}

type formattedQueryResult struct {
	SQL          string
	Columns      []string
	Rows         [][]any
	RowCount     int
	Truncated    bool
	LastSyncedAt string
	NextSyncAt   string
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
		Description: "Execute one or more read-only DuckDB SQL queries against a base's cache. Pass sql as an array of SQL strings, even for a single query. If a query contains LIMIT anywhere, the server assumes you are intentionally controlling row count and does not inject its default top-level limit for that query.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"base": map[string]any{
					"type":        "string",
					"description": "Airtable base ID or base name.",
				},
				"sql": map[string]any{
					"type":        "array",
					"description": "One or more exactly one top-level DuckDB SELECT or WITH queries. Results are returned in the same order. If LIMIT appears anywhere in a query's SQL text, the server will not add its own top-level default limit for that query.",
					"minItems":    1,
					"items": map[string]any{
						"type":      "string",
						"minLength": 1,
					},
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

	queries, err := normalizeQueryBatch(input.SQL)
	if err != nil {
		return mcp.ToolCallResult{}, err
	}

	normalizedQueries := make([]normalizedQueryCall, 0, len(queries))
	for index, query := range queries {
		normalized, err := NormalizeQuery(query, t.defaultLimit, t.maxLimit)
		if err != nil {
			return mcp.ToolCallResult{}, wrapQueryError(index, len(queries), err)
		}
		normalizedQueries = append(normalizedQueries, normalizedQueryCall{
			Normalized: normalized,
		})
	}

	if t.runtime == nil || t.runtime.Syncer == nil {
		previewResults := make([]map[string]any, 0, len(normalizedQueries))
		for _, query := range normalizedQueries {
			previewResults = append(previewResults, map[string]any{
				"sql":             query.Normalized.SQL,
				"effective_limit": query.Normalized.EffectiveLimit,
			})
		}
		return mcp.ErrorResult(
			"query execution is not implemented yet; SQL validation passed",
			map[string]any{
				"base":    input.Base,
				"results": previewResults,
			},
		), nil
	}

	userID, ok := authenticatedUserID(ctx)
	if !ok {
		err := fmt.Errorf("missing authenticated user")
		logToolFailed(ctx, "query", err)
		return mcp.ToolCallResult{}, err
	}

	var baseID string
	var syncPayload map[string]any
	var syncStatus *formattedSyncStatus
	if t.runtime.SyncManager != nil {
		base, err := t.runtime.SyncManager.EnsureBaseReadable(ctx, userID, input.Base)
		if err != nil {
			logToolFailed(ctx, "query", err, "user_id", userID)
			return mcp.ToolCallResult{}, err
		}
		baseID = base.ID
		if status, found := t.runtime.SyncManager.BaseStatus(base.ID); found {
			syncPayload = syncStatusPayload(status)
			formatted := formattedSyncStatusFromOperation(status)
			syncStatus = &formatted
		}
	}
	if baseID == "" {
		baseID = input.Base
	}

	accessToken, err := t.runtime.AirtableAccessToken(ctx, userID)
	if err != nil {
		logToolFailed(ctx, "query", err, "user_id", userID, "base_id", baseID)
		return mcp.ToolCallResult{}, err
	}

	payloadResults := make([]map[string]any, 0, len(normalizedQueries))
	formattedResults := make([]formattedQueryResult, 0, len(normalizedQueries))

	for index, query := range normalizedQueries {
		result, err := t.runtime.Syncer.QueryBase(ctx, accessToken, baseID, query.Normalized.ExecutionSQL)
		if err != nil {
			wrapped := wrapQueryError(index, len(normalizedQueries), err)
			logToolFailed(ctx, "query", wrapped, "user_id", userID, "base_id", baseID, "query_index", index)
			return mcp.ToolCallResult{}, wrapped
		}
		result, truncated := applyQueryResultLimit(result, query.Normalized)

		lastSyncedAt := formatTimeOrBlank(result.LastSyncedAt)
		nextSyncAtText := ""
		if !result.LastSyncedAt.IsZero() {
			nextSyncAtText = t.runtime.NextSyncTime(result.LastSyncedAt, result.LastSyncDuration).Format(time.RFC3339)
		}

		payloadResults = append(payloadResults, map[string]any{
			"sql":             query.Normalized.SQL,
			"columns":         result.Columns,
			"rows":            result.Rows,
			"row_count":       result.RowCount,
			"truncated":       truncated,
			"last_synced_at":  lastSyncedAt,
			"next_sync_at":    nextSyncAtText,
			"effective_limit": query.Normalized.EffectiveLimit,
		})
		formattedResults = append(formattedResults, formattedQueryResult{
			SQL:          query.Normalized.SQL,
			Columns:      result.Columns,
			Rows:         result.Rows,
			RowCount:     result.RowCount,
			Truncated:    truncated,
			LastSyncedAt: lastSyncedAt,
			NextSyncAt:   nextSyncAtText,
		})
	}

	payload := map[string]any{
		"results": payloadResults,
	}
	if syncPayload != nil {
		payload["sync"] = syncPayload
	}
	logToolCompleted(ctx, "query",
		"user_id", userID,
		"base_id", baseID,
		"query_count", len(normalizedQueries),
	)
	return textOnlyResult(formatBatchQueryCSV(formattedResults, syncStatus), payload), nil
}

func applyQueryResultLimit(result duckdb.QueryResult, normalized NormalizedQuery) (duckdb.QueryResult, bool) {
	if !normalized.ServerAppliedLimit || result.RowCount <= normalized.EffectiveLimit {
		return result, false
	}

	result.Rows = append([][]any(nil), result.Rows[:normalized.EffectiveLimit]...)
	result.RowCount = normalized.EffectiveLimit
	return result, true
}

func wrapQueryError(index, total int, err error) error {
	if total <= 1 {
		return err
	}
	return fmt.Errorf("sql[%d]: %w", index, err)
}
