package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hackclub/better-airtable-mcp/internal/logx"
	"github.com/hackclub/better-airtable-mcp/internal/mcp"
	"github.com/jackc/pgx/v5"
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

	userID, ok := authenticatedUserID(ctx)
	if !ok {
		err := fmt.Errorf("missing authenticated user")
		logToolFailed(ctx, "check_operation", err)
		return mcp.ToolCallResult{}, err
	}

	if strings.HasPrefix(input.OperationID, "sync_") && t.runtime != nil && t.runtime.SyncManager != nil {
		baseID := strings.TrimPrefix(input.OperationID, "sync_")
		if t.runtime.Syncer == nil {
			return mcp.ErrorResult("operation tracking is not implemented yet", map[string]any{
				"operation_id": input.OperationID,
			}), nil
		}
		accessToken, err := t.runtime.AirtableAccessToken(ctx, userID)
		if err != nil {
			logToolFailed(ctx, "check_operation", err, "user_id", userID, "sync_operation_id", input.OperationID)
			return mcp.ToolCallResult{}, err
		}
		bases, err := t.runtime.Syncer.SearchBases(ctx, accessToken, "")
		if err != nil {
			logToolFailed(ctx, "check_operation", err, "user_id", userID, "sync_operation_id", input.OperationID)
			return mcp.ToolCallResult{}, err
		}
		allowed := false
		for _, base := range bases {
			if base.ID == baseID {
				allowed = true
				break
			}
		}
		if !allowed {
			return mcp.ErrorResult("operation was not found", map[string]any{
				"operation_id": input.OperationID,
			}), nil
		}

		status, found, err := t.runtime.SyncManager.CheckOperation(ctx, input.OperationID)
		if err != nil {
			logToolFailed(ctx, "check_operation", err, "user_id", userID, "sync_operation_id", input.OperationID)
			return mcp.ToolCallResult{}, err
		}
		if found {
			payload := syncStatusPayload(status)
			payload["type"] = status.Type
			if status.CompletedAt != nil {
				payload["completed_at"] = status.CompletedAt.Format(time.RFC3339)
			}
			logToolCompleted(ctx, "check_operation",
				"user_id", userID,
				"sync_operation_id", input.OperationID,
				"status", status.Status,
			)
			return textOnlyResult(formatSingleRowCSV([]string{
				"operation_id", "type", "status", "read_snapshot", "sync_started_at", "completed_at", "last_synced_at", "tables_total", "tables_started", "tables_completed", "pages_fetched", "records_visible", "records_synced_this_run", "error",
			}, payload), payload), nil
		}
	}

	if strings.HasPrefix(input.OperationID, "op_") && t.runtime != nil && t.runtime.Approval != nil {
		if t.runtime.Store == nil {
			return mcp.ErrorResult("operation tracking is not implemented yet", map[string]any{
				"operation_id": input.OperationID,
			}), nil
		}
		record, err := t.runtime.Store.GetPendingOperation(ctx, input.OperationID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return mcp.ErrorResult("operation was not found", map[string]any{
					"operation_id": input.OperationID,
				}), nil
			}
			logToolFailed(ctx, "check_operation", err, "user_id", userID, "approval_operation_id_hash", logx.ApprovalOperationIDHash(input.OperationID))
			return mcp.ToolCallResult{}, err
		}
		if record.UserID != userID {
			return mcp.ErrorResult("operation was not found", map[string]any{
				"operation_id": input.OperationID,
			}), nil
		}

		operation, err := t.runtime.Approval.GetOperation(ctx, input.OperationID)
		if err != nil {
			logToolFailed(ctx, "check_operation", err, "user_id", userID, "approval_operation_id_hash", logx.ApprovalOperationIDHash(input.OperationID))
			return mcp.ToolCallResult{}, err
		}
		payload := map[string]any{
			"operation_id": operation.OperationID,
			"type":         "mutate",
			"status":       operation.Status,
			"approval_url": operation.ApprovalURL,
			"summary":      operation.Summary,
		}
		if operation.Status == "pending_approval" {
			payload["assistant_instruction"] = approvalURLAssistantInstruction
		}
		if operation.Result != nil {
			payload["result"] = operation.Result
		}
		if operation.Error != "" {
			payload["error"] = operation.Error
		}
		logToolCompleted(ctx, "check_operation",
			"user_id", userID,
			"approval_operation_id_hash", logx.ApprovalOperationIDHash(input.OperationID),
			"status", operation.Status,
		)
		return textOnlyResult(formatSingleRowCSV([]string{
			"operation_id", "type", "status", "approval_url", "summary", "assistant_instruction", "result", "error",
		}, payload), payload), nil
	}

	return mcp.ErrorResult("operation tracking is not implemented yet", map[string]any{
		"operation_id": input.OperationID,
	}), nil
}
