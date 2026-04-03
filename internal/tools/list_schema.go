package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/hackclub/better-airtable-mcp/internal/mcp"
)

type ListSchemaInput struct {
	Base string `json:"base"`
}

type ListSchemaTool struct {
	runtime *Runtime
}

func NewListSchemaTool(runtime *Runtime) mcp.Tool {
	return ListSchemaTool{runtime: runtime}
}

func (ListSchemaTool) Definition() mcp.ToolDefinition {
	return mcp.ToolDefinition{
		Name:        "list_schema",
		Description: "List tables, fields, and sample data for a base.",
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

func (t ListSchemaTool) Call(ctx context.Context, raw json.RawMessage) (mcp.ToolCallResult, error) {
	var input ListSchemaInput
	if err := decodeArgs(raw, &input); err != nil {
		return mcp.ToolCallResult{}, err
	}

	input.Base = strings.TrimSpace(input.Base)
	if input.Base == "" {
		return mcp.ToolCallResult{}, fmt.Errorf("base is required")
	}

	if t.runtime == nil || t.runtime.Syncer == nil {
		return mcp.ErrorResult("list_schema is not implemented yet", map[string]any{
			"base": input.Base,
		}), nil
	}

	userID, ok := authenticatedUserID(ctx)
	if !ok {
		err := fmt.Errorf("missing authenticated user")
		logToolFailed(ctx, "list_schema", err)
		return mcp.ToolCallResult{}, err
	}

	var baseID string
	var syncPayload map[string]any
	var syncStatus *formattedSyncStatus
	if t.runtime.SyncManager != nil {
		base, err := t.runtime.SyncManager.EnsureBaseSchemaSampled(ctx, userID, input.Base)
		if err != nil {
			logToolFailed(ctx, "list_schema", err, "user_id", userID)
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
		logToolFailed(ctx, "list_schema", err, "user_id", userID, "base_id", baseID)
		return mcp.ToolCallResult{}, err
	}

	schema, err := t.runtime.Syncer.ListSchema(ctx, accessToken, baseID)
	if err != nil {
		logToolFailed(ctx, "list_schema", err, "user_id", userID, "base_id", baseID)
		return mcp.ToolCallResult{}, err
	}

	tables := make([]map[string]any, 0, len(schema.Tables))
	for _, table := range schema.Tables {
		fields := make([]map[string]any, 0, len(table.Fields))
		for _, field := range table.Fields {
			fields = append(fields, map[string]any{
				"airtable_field_id":  field.AirtableFieldID,
				"duckdb_column_name": field.DuckDBColumnName,
				"original_name":      field.OriginalName,
				"type":               field.Type,
				"airtable_type":      field.AirtableType,
			})
		}
		totalRecordCount := any(table.TotalRecordCount)
		if !table.TableComplete {
			totalRecordCount = nil
		}
		tables = append(tables, map[string]any{
			"airtable_table_id":    table.AirtableTableID,
			"duckdb_table_name":    table.DuckDBTableName,
			"original_name":        table.OriginalName,
			"fields":               fields,
			"sample_rows":          table.SampleRows,
			"visible_record_count": table.VisibleRecordCount,
			"total_record_count":   totalRecordCount,
			"table_complete":       table.TableComplete,
			"sync_status":          table.SyncStatus,
		})
	}

	payload := map[string]any{
		"base_id":        schema.BaseID,
		"base_name":      schema.BaseName,
		"last_synced_at": formatTimeOrBlank(schema.LastSyncedAt),
		"tables":         tables,
	}
	if syncPayload != nil {
		payload["sync"] = syncPayload
	}
	logToolCompleted(ctx, "list_schema",
		"user_id", userID,
		"base_id", schema.BaseID,
		"table_count", len(schema.Tables),
	)
	return textOnlyResult(
		formatSchemaCSV(schema.BaseID, schema.BaseName, tables, syncStatus),
		payload,
	), nil
}
