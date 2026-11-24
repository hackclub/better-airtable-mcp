package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/hackclub/better-airtable-mcp/internal/duckdb"
	"github.com/hackclub/better-airtable-mcp/internal/mcp"
)

type ListSchemaInput struct {
	Base   string   `json:"base"`
	Tables []string `json:"tables"`
	Search string   `json:"search"`
}

type ListSchemaTool struct {
	runtime *Runtime
}

func NewListSchemaTool(runtime *Runtime) mcp.Tool {
	return ListSchemaTool{runtime: runtime}
}

func (ListSchemaTool) Definition() mcp.ToolDefinition {
	return mcp.ToolDefinition{
		Name: "list_schema",
		Description: "Describe a base's tables and columns, for writing SQL against it. " +
			"A small base returns every column. A large base returns a table overview instead; call again with tables for the columns of the ones you need. " +
			"Each column row gives the DuckDB column name and type, the Airtable field kind (select choices, the table a link points to, the link a lookup or rollup reads through, read-only for computed fields), the share of rows that have a value, and an example value. " +
			"Use search to find columns by name across the whole base.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"base": map[string]any{
					"type":        "string",
					"description": "Airtable base ID or base name.",
				},
				"tables": map[string]any{
					"type":        "array",
					"items":       map[string]any{"type": "string"},
					"description": "Tables to show every column of. Accepts the DuckDB table name, the Airtable table name or the Airtable table ID.",
				},
				"search": map[string]any{
					"type":        "string",
					"description": "One or more words separated by spaces. Returns the columns whose name or description contains any of them, across all tables (or only the ones in tables).",
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

	userID, ok := authenticatedUserID(ctx)
	if !ok {
		err := fmt.Errorf("missing authenticated user")
		logToolFailed(ctx, "list_schema", err)
		return mcp.ToolCallResult{}, err
	}

	baseID := input.Base
	var schema duckdb.BaseSchema
	var err error
	if t.runtime.SyncManager != nil {
		base, ensureErr := t.runtime.SyncManager.EnsureBaseSchemaSampled(ctx, userID, input.Base)
		if ensureErr != nil {
			logToolFailed(ctx, "list_schema", ensureErr, "user_id", userID)
			return mcp.ToolCallResult{}, ensureErr
		}
		baseID = base.ID
		// The base was resolved, authorized, and synced above: read the
		// local schema directly instead of re-resolving it.
		schema, err = t.runtime.Syncer.ListResolvedSchema(ctx, base.ID, base.Name)
	} else {
		accessToken, tokenErr := t.runtime.AirtableAccessToken(ctx, userID)
		if tokenErr != nil {
			logToolFailed(ctx, "list_schema", tokenErr, "user_id", userID, "base_id", baseID)
			return mcp.ToolCallResult{}, tokenErr
		}
		schema, err = t.runtime.Syncer.ListSchema(ctx, accessToken, baseID)
		if err == nil {
			baseID = schema.BaseID
		}
	}
	if err != nil {
		logToolFailed(ctx, "list_schema", err, "user_id", userID, "base_id", baseID)
		return mcp.ToolCallResult{}, err
	}

	text, view, err := t.render(ctx, schema, input)
	if err != nil {
		logToolFailed(ctx, "list_schema", err, "user_id", userID, "base_id", baseID)
		return mcp.ToolCallResult{}, err
	}
	if !schema.DetailsLoaded {
		text += "\nnote: select choices, link targets and descriptions are not stored for this base yet. They appear after its next sync, usually within seconds; call list_schema again if you need them.\n"
	}
	if t.runtime.SyncManager != nil {
		if freshness, ok := t.runtime.SyncManager.Freshness(ctx, baseID); ok {
			text = withFreshness(text, baseID, freshness)
		}
	}
	logToolCompleted(ctx, "list_schema",
		"user_id", userID,
		"base_id", schema.BaseID,
		"table_count", len(schema.Tables),
		"view", view,
		"result_chars", len(text),
	)
	// Text only: clients that are given structured content show it to the
	// model in place of the text, and a JSON copy of a schema is several
	// times the size of the table above.
	return textOnlyResult(text, nil), nil
}

// render picks the view of the schema to return: columns matching a search,
// every column of the requested tables, every column of a small base, or the
// table overview of a large one.
func (t ListSchemaTool) render(ctx context.Context, schema duckdb.BaseSchema, input ListSchemaInput) (text, view string, err error) {
	selected, err := selectSchemaTables(schema.Tables, input.Tables)
	if err != nil {
		return "", "", err
	}

	if terms := schemaSearchTerms(input.Search); len(terms) > 0 {
		matches := searchSchemaColumns(selected, terms)
		matchedTables := make([]duckdb.TableSchema, 0)
		seen := map[string]bool{}
		for index, match := range matches {
			if index >= schemaSearchMaxMatches {
				break
			}
			if !seen[match.Table.DuckDBTableName] {
				seen[match.Table.DuckDBTableName] = true
				matchedTables = append(matchedTables, match.Table)
			}
		}
		stats, err := t.columnStats(ctx, schema.BaseID, matchedTables)
		if err != nil {
			return "", "", err
		}
		return formatSchemaSearch(schema, selected, terms, matches, stats), "search", nil
	}

	if len(input.Tables) > 0 {
		stats, err := t.columnStats(ctx, schema.BaseID, selected)
		if err != nil {
			return "", "", err
		}
		return formatSchemaDetail(schema, selected, stats, schemaDetailBudget), "tables", nil
	}

	if schemaColumnCount(schema.Tables) <= schemaAutoDetailMaxColumns {
		stats, err := t.columnStats(ctx, schema.BaseID, schema.Tables)
		if err != nil {
			return "", "", err
		}
		if detail := formatSchemaDetail(schema, schema.Tables, stats, 0); len(detail) <= schemaAutoDetailBudget {
			return detail, "all", nil
		}
	}
	return formatSchemaOverview(schema), "overview", nil
}

func (t ListSchemaTool) columnStats(ctx context.Context, baseID string, tables []duckdb.TableSchema) (map[string]duckdb.TableStats, error) {
	if len(tables) == 0 {
		return nil, nil
	}
	return t.runtime.Syncer.ColumnStats(ctx, baseID, tables)
}

// selectSchemaTables resolves the requested table references, in the order
// asked for. No references selects every table.
func selectSchemaTables(tables []duckdb.TableSchema, requested []string) ([]duckdb.TableSchema, error) {
	if len(requested) == 0 {
		return tables, nil
	}

	lookup := make(map[string]int, len(tables)*3)
	available := make([]string, 0, len(tables))
	for index, table := range tables {
		available = append(available, table.DuckDBTableName)
		for _, alias := range []string{table.DuckDBTableName, table.OriginalName, table.AirtableTableID} {
			key := strings.ToLower(strings.TrimSpace(alias))
			if _, exists := lookup[key]; key != "" && !exists {
				lookup[key] = index
			}
		}
	}

	selected := make([]duckdb.TableSchema, 0, len(requested))
	seen := map[int]bool{}
	for _, reference := range requested {
		index, ok := lookup[strings.ToLower(strings.TrimSpace(reference))]
		if !ok {
			return nil, fmt.Errorf("unknown table %q; tables in this base: %s", reference, strings.Join(available, ", "))
		}
		if !seen[index] {
			seen[index] = true
			selected = append(selected, tables[index])
		}
	}
	return selected, nil
}
