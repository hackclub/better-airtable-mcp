package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/hackclub/better-airtable-mcp/internal/mcp"
)

type CheckOperationInput struct {
	OperationID string `json:"operation_id"`
}

type CheckOperationTool struct {
	runtime *Runtime
}

func NewCheckOperationTool(runtime *Runtime) mcp.Tool {
	return CheckOperationTool{runtime: runtime}
}

func (CheckOperationTool) Definition() mcp.ToolDefinition {
	return mcp.ToolDefinition{
		Name:        "check_operation",
		Description: "Check the status of a pending sync or mutation operation.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"operation_id": map[string]any{
					"type":        "string",
					"description": "Operation identifier returned by sync or mutate.",
				},
			},
			"required":             []string{"operation_id"},
			"additionalProperties": false,
		},
	}
}

func (t CheckOperationTool) Call(ctx context.Context, raw json.RawMessage) (mcp.ToolCallResult, error) {
	var input CheckOperationInput
	if err := decodeArgs(raw, &input); err != nil {
		return mcp.ToolCallResult{}, err
	}

	input.OperationID = strings.TrimSpace(input.OperationID)
	if input.OperationID == "" {
		return mcp.ToolCallResult{}, fmt.Errorf("operation_id is required")
	}
	if !strings.HasPrefix(input.OperationID, "op_") && !strings.HasPrefix(input.OperationID, "sync_") {
		return mcp.ToolCallResult{}, fmt.Errorf("operation_id must start with op_ or sync_")
	}

	if strings.HasPrefix(input.OperationID, "sync_") && t.runtime != nil && t.runtime.SyncManager != nil {
		status, found, err := t.runtime.SyncManager.CheckOperation(ctx, input.OperationID)
		if err != nil {
			return mcp.ToolCallResult{}, err
		}
		if found {
			payload := map[string]any{
				"operation_id":   status.OperationID,
				"type":           status.Type,
				"status":         status.Status,
				"tables_synced":  status.TablesSynced,
				"records_synced": status.RecordsSynced,
			}
			if status.CompletedAt != nil {
				payload["completed_at"] = status.CompletedAt.Format(time.RFC3339)
			}
			if status.LastSyncedAt != nil {
				payload["last_synced_at"] = status.LastSyncedAt.Format(time.RFC3339)
			}
			if status.Error != "" {
				payload["error"] = status.Error
			}
			return mcp.TextResult(fmt.Sprintf("Operation %s is %s.", status.OperationID, status.Status), payload), nil
		}
	}

	if strings.HasPrefix(input.OperationID, "op_") && t.runtime != nil && t.runtime.Approval != nil {
		operation, err := t.runtime.Approval.GetOperation(ctx, input.OperationID)
		if err != nil {
			return mcp.ToolCallResult{}, err
		}
		payload := map[string]any{
			"operation_id": operation.OperationID,
			"type":         "mutate",
			"status":       operation.Status,
			"approval_url": operation.ApprovalURL,
			"summary":      operation.Summary,
		}
		if operation.Result != nil {
			payload["result"] = operation.Result
		}
		if operation.Error != "" {
			payload["error"] = operation.Error
		}
		return mcp.TextResult(fmt.Sprintf("Operation %s is %s.", operation.OperationID, operation.Status), payload), nil
	}

	return mcp.ErrorResult("operation tracking is not implemented yet", map[string]any{
		"operation_id": input.OperationID,
	}), nil
}
