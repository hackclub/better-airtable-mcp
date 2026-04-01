package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/hackclub/better-airtable-mcp/internal/db"
	"github.com/hackclub/better-airtable-mcp/internal/mcp"
)

type SearchBasesInput struct {
	Query string `json:"query,omitempty"`
}

type ListBasesTool struct {
	runtime *Runtime
}

func NewListBasesTool(runtime *Runtime) mcp.Tool {
	return ListBasesTool{runtime: runtime}
}

func (ListBasesTool) Definition() mcp.ToolDefinition {
	return mcp.ToolDefinition{
		Name:        "list_bases",
		Description: "List Airtable bases the authenticated user can access, optionally filtered by a query string.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"query": map[string]any{
					"type":        "string",
					"description": "Optional case-insensitive substring to filter base names.",
				},
			},
			"additionalProperties": false,
		},
	}
}

func (t ListBasesTool) Call(ctx context.Context, raw json.RawMessage) (mcp.ToolCallResult, error) {
	var input SearchBasesInput
	if err := decodeArgs(raw, &input); err != nil {
		return mcp.ToolCallResult{}, err
	}

	input.Query = strings.TrimSpace(input.Query)
	if t.runtime == nil || t.runtime.Syncer == nil {
		return mcp.ErrorResult("list_bases is not implemented yet", map[string]any{
			"query": input.Query,
		}), nil
	}

	userID, ok := authenticatedUserID(ctx)
	if !ok {
		return mcp.ToolCallResult{}, fmt.Errorf("missing authenticated user")
	}

	accessToken, err := t.runtime.AirtableAccessToken(ctx, userID)
	if err != nil {
		return mcp.ToolCallResult{}, err
	}

	bases, err := t.runtime.Syncer.SearchBases(ctx, accessToken, input.Query)
	if err != nil {
		return mcp.ToolCallResult{}, err
	}

	items := make([]map[string]any, 0, len(bases))
	for _, base := range bases {
		if t.runtime != nil && t.runtime.Store != nil {
			_ = t.runtime.Store.UpsertUserBaseAccess(ctx, db.UserBaseAccess{
				UserID:          userID,
				BaseID:          base.ID,
				PermissionLevel: base.PermissionLevel,
				LastVerifiedAt:  time.Now().UTC(),
			})
		}
		items = append(items, map[string]any{
			"id":               base.ID,
			"name":             base.Name,
			"permission_level": base.PermissionLevel,
		})
	}

	payload := map[string]any{
		"bases": items,
	}
	return textOnlyResult(formatListBasesCSV(items), payload), nil
}
