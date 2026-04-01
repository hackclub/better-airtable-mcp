package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/hackclub/better-airtable-mcp/internal/mcp"
)

type SyncInput struct {
	Base string `json:"base"`
}

type SyncTool struct {
	runtime *Runtime
}

func NewSyncTool(runtime *Runtime) mcp.Tool {
	return SyncTool{runtime: runtime}
}

func (SyncTool) Definition() mcp.ToolDefinition {
	return mcp.ToolDefinition{
		Name:        "sync",
		Description: "Force a refresh of a base's DuckDB cache.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"base": map[string]any{
					"type":        "string",
					"description": "Airtable base ID or base name.",
				},
			},
			"required":             []string{"base"},
			"additionalProperties": false,
		},
	}
}

func (t SyncTool) Call(ctx context.Context, raw json.RawMessage) (mcp.ToolCallResult, error) {
	var input SyncInput
	if err := decodeArgs(raw, &input); err != nil {
		return mcp.ToolCallResult{}, err
	}

	input.Base = strings.TrimSpace(input.Base)
	if input.Base == "" {
		return mcp.ToolCallResult{}, fmt.Errorf("base is required")
	}

	if t.runtime == nil || t.runtime.Syncer == nil {
		return mcp.ErrorResult("sync orchestration is not implemented yet", map[string]any{
			"base": input.Base,
		}), nil
	}

	userID, ok := authenticatedUserID(ctx)
	if !ok {
		return mcp.ToolCallResult{}, fmt.Errorf("missing authenticated user")
	}

	if t.runtime.SyncManager == nil {
		accessToken, err := t.runtime.AirtableAccessToken(ctx, userID)
		if err != nil {
			return mcp.ToolCallResult{}, err
		}

		result, err := t.runtime.Syncer.SyncBase(ctx, accessToken, input.Base)
		if err != nil {
			return mcp.ToolCallResult{}, err
		}

		payload := map[string]any{
			"operation_id":      "sync_" + result.BaseID,
			"status":            "completed",
			"estimated_seconds": 0,
			"last_synced_at":    result.LastSyncedAt.Format(time.RFC3339),
			"tables_synced":     result.TablesSynced,
			"records_synced":    result.RecordsSynced,
		}
		return textOnlyResult(formatSingleRowCSV([]string{
			"operation_id", "status", "estimated_seconds", "last_synced_at", "tables_synced", "records_synced",
		}, payload), payload), nil
	}

	status, err := t.runtime.SyncManager.RequestSync(ctx, userID, input.Base)
	if err != nil {
		return mcp.ToolCallResult{}, err
	}

	payload := map[string]any{
		"operation_id":      status.OperationID,
		"status":            status.Status,
		"estimated_seconds": status.EstimatedSeconds,
	}
	if status.LastSyncedAt != nil {
		payload["last_synced_at"] = status.LastSyncedAt.Format(time.RFC3339)
	}

	return textOnlyResult(formatSingleRowCSV([]string{
		"operation_id", "status", "estimated_seconds", "last_synced_at",
	}, payload), payload), nil
}
