// Package tools implements the MCP tool surface: list_bases, list_schema,
// query, sync, mutate, manage_schema, and check_operation.
package tools

import (
	"github.com/hackclub/better-airtable-mcp/internal/config"
	"github.com/hackclub/better-airtable-mcp/internal/mcp"
)

func NewCatalog(cfg config.Config, runtime *Runtime) []mcp.Tool {
	return []mcp.Tool{
		NewListBasesTool(runtime),
		NewListSchemaTool(runtime),
		NewQueryTool(cfg.QueryDefaultLimit, cfg.QueryMaxLimit, runtime),
		NewMutateTool(runtime),
		NewSchemaMutateTool(runtime),
		NewSyncTool(runtime),
		NewCheckOperationTool(runtime),
	}
}
