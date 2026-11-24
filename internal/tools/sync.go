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
	Mode string `json:"mode,omitempty"`
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
		Description: "Refresh a base's cache now. Reads already pick up recent Airtable edits on their own, so this is rarely needed. The default mode \"delta\" fetches only what changed and takes seconds. Mode \"full\" downloads every record again, which can take minutes on a large base; use it only when a record deleted in Airtable still shows up, or a formula, lookup or rollup value looks out of date.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"base": map[string]any{
					"type":        "string",
					"description": "Airtable base ID or base name.",
				},
				"mode": map[string]any{
					"type":        "string",
					"enum":        []string{"delta", "full"},
					"description": "\"delta\" (default) fetches only changes. \"full\" downloads everything again.",
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
	input.Mode = strings.ToLower(strings.TrimSpace(input.Mode))
	if input.Mode == "" {
		input.Mode = "delta"
	}
	if input.Mode != "delta" && input.Mode != "full" {
		return mcp.ToolCallResult{}, fmt.Errorf("mode must be \"delta\" or \"full\"")
	}

	userID, ok := authenticatedUserID(ctx)
	if !ok {
		err := fmt.Errorf("missing authenticated user")
		logToolFailed(ctx, "sync", err)
		return mcp.ToolCallResult{}, err
	}

	if t.runtime.SyncManager == nil {
		accessToken, err := t.runtime.AirtableAccessToken(ctx, userID)
		if err != nil {
			logToolFailed(ctx, "sync", err, "user_id", userID)
			return mcp.ToolCallResult{}, err
		}

		result, err := t.runtime.Syncer.SyncBase(ctx, accessToken, input.Base)
		if err != nil {
			logToolFailed(ctx, "sync", err, "user_id", userID, "base_id", result.BaseID)
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
		logToolCompleted(ctx, "sync",
			"user_id", userID,
			"base_id", result.BaseID,
			"status", "completed",
			"records_synced", result.RecordsSynced,
		)
		return textOnlyResult(formatSingleRowCSV([]string{
			"operation_id", "status", "estimated_seconds", "last_synced_at", "tables_synced", "records_synced",
		}, payload), payload), nil
	}

	request := t.runtime.SyncManager.RequestSync
	if input.Mode == "full" {
		request = t.runtime.SyncManager.RequestFullSync
	}
	status, err := request(ctx, userID, input.Base)
	if err != nil {
		logToolFailed(ctx, "sync", err, "user_id", userID)
		return mcp.ToolCallResult{}, err
	}

	payload := syncStatusPayload(status)
	payload["estimated_seconds"] = status.EstimatedSeconds
	if status.LastSyncedAt != nil {
		payload["last_synced_at"] = status.LastSyncedAt.Format(time.RFC3339)
	}
	logToolCompleted(ctx, "sync",
		"user_id", userID,
		"base_id", status.BaseID,
		"status", status.Status,
		"read_snapshot", status.ReadSnapshot,
	)

	return textOnlyResult(formatSingleRowCSV([]string{
		"operation_id", "status", "read_snapshot", "sync_started_at", "estimated_seconds", "last_synced_at", "tables_total", "tables_started", "tables_completed", "pages_fetched", "records_visible", "records_synced_this_run", "error",
	}, payload), payload), nil
}
