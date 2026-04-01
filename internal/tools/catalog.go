package tools

import (
	"github.com/hackclub/better-airtable-mcp/internal/config"
	"github.com/hackclub/better-airtable-mcp/internal/mcp"
)

func NewCatalog(cfg config.Config, runtime *Runtime) []mcp.Tool {
	return []mcp.Tool{
		NewSearchBasesTool(runtime),
		NewListSchemaTool(runtime),
		NewQueryTool(cfg.QueryDefaultLimit, cfg.QueryMaxLimit, runtime),
		NewMutateTool(runtime),
		NewSyncTool(runtime),
		NewCheckOperationTool(runtime),
	}
}
