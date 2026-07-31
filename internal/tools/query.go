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
	SQL            string
	Columns        []string
	Rows           [][]any
	RowCount       int
	Truncated      bool
	LastSyncedAt   string
	NextSyncAt     string
	EffectiveLimit int
}

// queryResultPayload maps a per-statement result to the structured-content
// shape. The CSV text and this payload are derived from the same slice so
// each statement's rows are assembled exactly once.
func queryResultPayload(result formattedQueryResult) map[string]any {
	return map[string]any{
		"sql":             result.SQL,
		"columns":         result.Columns,
		"rows":            result.Rows,
		"row_count":       result.RowCount,
		"truncated":       result.Truncated,
		"last_synced_at":  result.LastSyncedAt,
		"next_sync_at":    result.NextSyncAt,
		"effective_limit": result.EffectiveLimit,
	}
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

	userID, ok := authenticatedUserID(ctx)
	if !ok {
		err := fmt.Errorf("missing authenticated user")
		logToolFailed(ctx, "query", err)
		return mcp.ToolCallResult{}, err
	}

	var baseID string
	var baseResolved bool
	var syncPayload map[string]any
	var syncStatus *formattedSyncStatus
	if t.runtime.SyncManager != nil {
		base, err := t.runtime.SyncManager.EnsureBaseReadable(ctx, userID, input.Base)
		if err != nil {
			logToolFailed(ctx, "query", err, "user_id", userID)
			return mcp.ToolCallResult{}, err
		}
		baseID = base.ID
		baseResolved = true
		if status, found := t.runtime.SyncManager.BaseStatus(base.ID); found {
			syncPayload = syncStatusPayload(status)
			formatted := formattedSyncStatusFromOperation(status)
			syncStatus = &formatted
		}
	}
	if baseID == "" {
		baseID = input.Base
	}

	var accessToken string
	if !baseResolved {
		// Without a sync manager the statement loop resolves (and thereby
		// authorizes) the base itself, which needs the Airtable token. When
		// the sync manager already resolved the base, the loop reads the
		// local DuckDB file directly and no token is required.
		accessToken, err = t.runtime.AirtableAccessToken(ctx, userID)
		if err != nil {
			logToolFailed(ctx, "query", err, "user_id", userID, "base_id", baseID)
			return mcp.ToolCallResult{}, err
		}
	}

	formattedResults := make([]formattedQueryResult, 0, len(normalizedQueries))

	for index, query := range normalizedQueries {
		var result duckdb.QueryResult
		if baseResolved {
			// The base was resolved, authorized, and synced once for this
			// request — do not re-resolve it per statement.
			result, err = t.runtime.Syncer.QueryResolvedBase(ctx, baseID, query.Normalized.ExecutionSQL)
		} else {
			result, err = t.runtime.Syncer.QueryBase(ctx, accessToken, baseID, query.Normalized.ExecutionSQL)
		}
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

		formattedResults = append(formattedResults, formattedQueryResult{
			SQL:            query.Normalized.SQL,
			Columns:        result.Columns,
			Rows:           result.Rows,
			RowCount:       result.RowCount,
			Truncated:      truncated,
			LastSyncedAt:   lastSyncedAt,
			NextSyncAt:     nextSyncAtText,
			EffectiveLimit: query.Normalized.EffectiveLimit,
		})
	}

	payloadResults := make([]map[string]any, 0, len(formattedResults))
	for _, result := range formattedResults {
		payloadResults = append(payloadResults, queryResultPayload(result))
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
