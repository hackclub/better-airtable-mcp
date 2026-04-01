package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/hackclub/better-airtable-mcp/internal/approval"
	"github.com/hackclub/better-airtable-mcp/internal/mcp"
)

type MutateInput struct {
	Base       string              `json:"base"`
	Operations []MutationOperation `json:"operations"`
}

type MutationOperation struct {
	Type    string           `json:"type"`
	Table   string           `json:"table"`
	Records []MutationRecord `json:"records"`
}

type MutationRecord struct {
	ID     string         `json:"id,omitempty"`
	Fields map[string]any `json:"fields,omitempty"`
}

type MutateTool struct {
	runtime *Runtime
}

func NewMutateTool(runtime *Runtime) mcp.Tool {
	return MutateTool{runtime: runtime}
}

func (MutateTool) Definition() mcp.ToolDefinition {
	return mcp.ToolDefinition{
		Name:        "mutate",
		Description: "Request a record mutation, subject to human approval.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"base": map[string]any{
					"type":        "string",
					"description": "Airtable base ID or base name.",
				},
				"operations": map[string]any{
					"type":        "array",
					"description": "Mutation operations to submit as a single approval request.",
					"minItems":    1,
					"items": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"type": map[string]any{
								"type": "string",
								"enum": []string{"create_records", "update_records", "delete_records"},
							},
							"table": map[string]any{
								"type": "string",
							},
							"records": map[string]any{
								"type":     "array",
								"minItems": 1,
							},
						},
						"required":             []string{"type", "table", "records"},
						"additionalProperties": false,
					},
				},
			},
			"required":             []string{"base", "operations"},
			"additionalProperties": false,
		},
	}
}

func (t MutateTool) Call(ctx context.Context, raw json.RawMessage) (mcp.ToolCallResult, error) {
	var input MutateInput
	if err := decodeArgs(raw, &input); err != nil {
		return mcp.ToolCallResult{}, err
	}
	if err := validateMutateInput(input); err != nil {
		return mcp.ToolCallResult{}, err
	}

	if t.runtime == nil || t.runtime.Approval == nil {
		return mcp.ErrorResult("mutate execution and approval flow are not implemented yet; payload validation passed", map[string]any{
			"base":            strings.TrimSpace(input.Base),
			"operation_count": len(input.Operations),
			"record_count":    countMutationRecords(input.Operations),
			"operation_types": collectOperationTypes(input.Operations),
		}), nil
	}

	userID, ok := authenticatedUserID(ctx)
	if !ok {
		return mcp.ToolCallResult{}, fmt.Errorf("missing authenticated user")
	}

	prepared, err := t.runtime.Approval.PrepareMutation(ctx, userID, toApprovalRequest(ctx, input))
	if err != nil {
		return mcp.ToolCallResult{}, err
	}

	return mcp.TextResult(prepared.Summary, map[string]any{
		"operation_id": prepared.OperationID,
		"status":       prepared.Status,
		"approval_url": prepared.ApprovalURL,
		"expires_at":   prepared.ExpiresAt.Format(time.RFC3339),
		"summary":      prepared.Summary,
	}), nil
}

func toApprovalRequest(ctx context.Context, input MutateInput) approval.MutationRequest {
	request := approval.MutationRequest{
		Base:       strings.TrimSpace(input.Base),
		Operations: make([]approval.MutationOperation, 0, len(input.Operations)),
	}
	if sessionID, ok := authenticatedSessionID(ctx); ok {
		request.SessionID = sessionID
	}
	if clientID, ok := authenticatedClientID(ctx); ok {
		request.ClientID = clientID
	}
	if clientName, ok := authenticatedClientName(ctx); ok {
		request.ClientName = clientName
	}
	for _, operation := range input.Operations {
		mapped := approval.MutationOperation{
			Type:    operation.Type,
			Table:   strings.TrimSpace(operation.Table),
			Records: make([]approval.MutationRecord, 0, len(operation.Records)),
		}
		for _, record := range operation.Records {
			mapped.Records = append(mapped.Records, approval.MutationRecord{
				ID:     strings.TrimSpace(record.ID),
				Fields: record.Fields,
			})
		}
		request.Operations = append(request.Operations, mapped)
	}
	return request
}

func validateMutateInput(input MutateInput) error {
	input.Base = strings.TrimSpace(input.Base)
	if input.Base == "" {
		return fmt.Errorf("base is required")
	}
	if len(input.Operations) == 0 {
		return fmt.Errorf("operations must contain at least one operation")
	}

	for operationIndex, operation := range input.Operations {
		operation.Type = strings.TrimSpace(operation.Type)
		operation.Table = strings.TrimSpace(operation.Table)

		if operation.Type != "create_records" && operation.Type != "update_records" && operation.Type != "delete_records" {
			return fmt.Errorf("operations[%d].type must be one of create_records, update_records, or delete_records", operationIndex)
		}
		if operation.Table == "" {
			return fmt.Errorf("operations[%d].table is required", operationIndex)
		}
		if len(operation.Records) == 0 {
			return fmt.Errorf("operations[%d].records must contain at least one record", operationIndex)
		}

		for recordIndex, record := range operation.Records {
			record.ID = strings.TrimSpace(record.ID)
			switch operation.Type {
			case "create_records":
				if record.ID != "" {
					return fmt.Errorf("operations[%d].records[%d].id must be omitted for create_records", operationIndex, recordIndex)
				}
				if len(record.Fields) == 0 {
					return fmt.Errorf("operations[%d].records[%d].fields must contain at least one field for create_records", operationIndex, recordIndex)
				}
			case "update_records":
				if record.ID == "" {
					return fmt.Errorf("operations[%d].records[%d].id is required for update_records", operationIndex, recordIndex)
				}
				if len(record.Fields) == 0 {
					return fmt.Errorf("operations[%d].records[%d].fields must contain at least one field for update_records", operationIndex, recordIndex)
				}
			case "delete_records":
				if record.ID == "" {
					return fmt.Errorf("operations[%d].records[%d].id is required for delete_records", operationIndex, recordIndex)
				}
				if len(record.Fields) > 0 {
					return fmt.Errorf("operations[%d].records[%d].fields must be omitted for delete_records", operationIndex, recordIndex)
				}
			}
		}
	}

	return nil
}

func collectOperationTypes(operations []MutationOperation) []string {
	types := make([]string, 0, len(operations))
	for _, operation := range operations {
		types = append(types, operation.Type)
	}
	return types
}

func countMutationRecords(operations []MutationOperation) int {
	total := 0
	for _, operation := range operations {
		total += len(operation.Records)
	}
	return total
}
