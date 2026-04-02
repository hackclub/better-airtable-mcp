package tools

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/hackclub/better-airtable-mcp/internal/mcp"
)

type formattedSyncStatus struct {
	OperationID          string
	Status               string
	ReadSnapshot         string
	SyncStartedAt        string
	LastSyncedAt         string
	TablesTotal          int
	TablesStarted        int
	TablesCompleted      int
	PagesFetched         int64
	RecordsVisible       int64
	RecordsSyncedThisRun int64
	Error                string
}

func textOnlyResult(text string, structured any) mcp.ToolCallResult {
	return mcp.ToolCallResult{
		Content:           []mcp.ToolContent{{Type: "text", Text: text}},
		StructuredContent: structured,
	}
}

func formatListBasesCSV(rows []map[string]any) string {
	items := make([][]string, 0, len(rows))
	for _, row := range rows {
		items = append(items, []string{
			fmt.Sprint(row["id"]),
			fmt.Sprint(row["name"]),
			fmt.Sprint(row["permission_level"]),
		})
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i][1] == items[j][1] {
			return items[i][0] < items[j][0]
		}
		return items[i][1] < items[j][1]
	})
	return csvSection([]string{"id", "name", "permission_level"}, items)
}

func formatQueryCSV(columns []string, rows [][]any, rowCount int, truncated bool, lastSyncedAt, nextSyncAt string, syncStatus *formattedSyncStatus) string {
	sections := make([]string, 0, 6)
	if syncStatus != nil {
		sections = append(sections, "sync_status", formatSyncStatusCSV(*syncStatus))
	}
	sections = append(
		sections,
		"query_metadata",
		csvSection(
			[]string{"row_count", "truncated", "last_synced_at", "next_sync_at"},
			[][]string{{fmt.Sprint(rowCount), fmt.Sprint(truncated), lastSyncedAt, nextSyncAt}},
		),
		"query_rows",
		csvSection(columns, csvRows(rows)),
	)
	return joinSections(sections...)
}

func formatBatchQueryCSV(results []formattedQueryResult, syncStatus *formattedSyncStatus) string {
	if len(results) == 1 {
		result := results[0]
		return formatQueryCSV(result.Columns, result.Rows, result.RowCount, result.Truncated, result.LastSyncedAt, result.NextSyncAt, syncStatus)
	}

	sections := make([]string, 0, len(results)*4+2)
	if syncStatus != nil {
		sections = append(sections, "sync_status", formatSyncStatusCSV(*syncStatus))
	}
	for index, result := range results {
		sections = append(
			sections,
			fmt.Sprintf("query_%d_metadata", index+1),
			csvSection(
				[]string{"sql", "row_count", "truncated", "last_synced_at", "next_sync_at"},
				[][]string{{
					result.SQL,
					fmt.Sprint(result.RowCount),
					fmt.Sprint(result.Truncated),
					result.LastSyncedAt,
					result.NextSyncAt,
				}},
			),
			fmt.Sprintf("query_%d_rows", index+1),
			csvSection(result.Columns, csvRows(result.Rows)),
		)
	}

	return joinSections(sections...)
}

func formatSchemaCSV(baseID, baseName string, tables []map[string]any, syncStatus *formattedSyncStatus) string {
	sort.SliceStable(tables, func(i, j int) bool {
		return fmt.Sprint(tables[i]["duckdb_table_name"]) < fmt.Sprint(tables[j]["duckdb_table_name"])
	})

	sections := make([]string, 0, len(tables)*2+4)
	if syncStatus != nil {
		sections = append(sections, "sync_status", formatSyncStatusCSV(*syncStatus))
	}
	sections = append(
		sections,
		"base",
		csvSection(
			[]string{"base_id", "base_name"},
			[][]string{{baseID, baseName}},
		),
		"tables",
	)

	for _, table := range tables {
		tableName := fmt.Sprint(table["duckdb_table_name"])
		headers := schemaTableHeaders(table)
		rows := schemaSampleRows(table, headers)
		sections = append(sections, "# "+tableName, csvSection(headers, rows))
	}

	return joinSections(sections...)
}

func formatSyncStatusCSV(syncStatus formattedSyncStatus) string {
	return csvSection(
		[]string{"operation_id", "status", "read_snapshot", "sync_started_at", "last_synced_at", "tables_total", "tables_started", "tables_completed", "pages_fetched", "records_visible", "records_synced_this_run", "error"},
		[][]string{{
			syncStatus.OperationID,
			syncStatus.Status,
			syncStatus.ReadSnapshot,
			syncStatus.SyncStartedAt,
			syncStatus.LastSyncedAt,
			fmt.Sprint(syncStatus.TablesTotal),
			fmt.Sprint(syncStatus.TablesStarted),
			fmt.Sprint(syncStatus.TablesCompleted),
			fmt.Sprint(syncStatus.PagesFetched),
			fmt.Sprint(syncStatus.RecordsVisible),
			fmt.Sprint(syncStatus.RecordsSyncedThisRun),
			syncStatus.Error,
		}},
	)
}

func formatSingleRowCSV(headers []string, row map[string]any) string {
	values := make([]string, 0, len(headers))
	for _, header := range headers {
		values = append(values, csvCell(row[header]))
	}
	return csvSection(headers, [][]string{values})
}

func csvRows(rows [][]any) [][]string {
	formatted := make([][]string, 0, len(rows))
	for _, row := range rows {
		formattedRow := make([]string, 0, len(row))
		for _, value := range row {
			formattedRow = append(formattedRow, csvCell(value))
		}
		formatted = append(formatted, formattedRow)
	}
	return formatted
}

func csvCell(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return fmt.Sprint(typed)
	default:
		return jsonString(typed)
	}
}

func jsonString(value any) string {
	normalized := normalizeJSONValue(value)
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return fmt.Sprint(normalized)
	}
	return string(encoded)
}

func normalizeJSONValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		normalized := make(map[string]any, len(typed))
		for _, key := range keys {
			normalized[key] = normalizeJSONValue(typed[key])
		}
		return normalized
	case []any:
		normalized := make([]any, 0, len(typed))
		for _, item := range typed {
			normalized = append(normalized, normalizeJSONValue(item))
		}
		return normalized
	default:
		return typed
	}
}

func csvSection(headers []string, rows [][]string) string {
	var buffer bytes.Buffer
	writer := csv.NewWriter(&buffer)
	if len(headers) > 0 {
		_ = writer.Write(headers)
	}
	for _, row := range rows {
		_ = writer.Write(row)
	}
	writer.Flush()
	return buffer.String()
}

func joinSections(sections ...string) string {
	var buffer bytes.Buffer
	for index, section := range sections {
		if index > 0 {
			buffer.WriteString("\n")
		}
		buffer.WriteString(section)
		if len(section) == 0 || section[len(section)-1] != '\n' {
			buffer.WriteByte('\n')
		}
	}
	return buffer.String()
}

func schemaTableHeaders(table map[string]any) []string {
	headers := []string{"id", "created_time"}
	if fields, ok := table["fields"].([]map[string]any); ok {
		for _, field := range fields {
			headers = append(headers, fmt.Sprint(field["duckdb_column_name"]))
		}
	}
	return headers
}

func schemaSampleRows(table map[string]any, headers []string) [][]string {
	samples, _ := table["sample_rows"].([]map[string]any)
	rows := make([][]string, 0, len(samples))
	for _, sample := range samples {
		row := make([]string, 0, len(headers))
		for _, header := range headers {
			row = append(row, csvCell(sample[header]))
		}
		rows = append(rows, row)
	}
	return rows
}
