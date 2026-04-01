package tools

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/hackclub/better-airtable-mcp/internal/mcp"
)

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

func formatQueryCSV(columns []string, rows [][]any, rowCount int, truncated bool, lastSyncedAt, nextSyncAt string) string {
	sections := []string{
		"query_metadata",
		csvSection(
			[]string{"row_count", "truncated", "last_synced_at", "next_sync_at"},
			[][]string{{fmt.Sprint(rowCount), fmt.Sprint(truncated), lastSyncedAt, nextSyncAt}},
		),
		"query_rows",
		csvSection(columns, csvRows(rows)),
	}
	return joinSections(sections...)
}

func formatSchemaCSV(baseID, baseName, lastSyncedAt string, tables []map[string]any) string {
	tableRows := make([][]string, 0, len(tables))
	fieldRows := make([][]string, 0)
	sampleRows := make([][]string, 0)

	for _, table := range tables {
		tableName := fmt.Sprint(table["duckdb_table_name"])
		tableRows = append(tableRows, []string{
			fmt.Sprint(table["airtable_table_id"]),
			tableName,
			fmt.Sprint(table["original_name"]),
			fmt.Sprint(table["total_record_count"]),
		})

		if fields, ok := table["fields"].([]map[string]any); ok {
			for _, field := range fields {
				fieldRows = append(fieldRows, []string{
					tableName,
					fmt.Sprint(field["airtable_field_id"]),
					fmt.Sprint(field["duckdb_column_name"]),
					fmt.Sprint(field["original_name"]),
					fmt.Sprint(field["type"]),
					fmt.Sprint(field["airtable_type"]),
				})
			}
		}

		if samples, ok := table["sample_rows"].([]map[string]any); ok {
			for index, sample := range samples {
				sampleRows = append(sampleRows, []string{
					tableName,
					fmt.Sprint(index + 1),
					jsonString(sample),
				})
			}
		}
	}

	sort.SliceStable(tableRows, func(i, j int) bool { return tableRows[i][1] < tableRows[j][1] })
	sort.SliceStable(fieldRows, func(i, j int) bool {
		if fieldRows[i][0] == fieldRows[j][0] {
			return fieldRows[i][2] < fieldRows[j][2]
		}
		return fieldRows[i][0] < fieldRows[j][0]
	})
	sort.SliceStable(sampleRows, func(i, j int) bool {
		if sampleRows[i][0] == sampleRows[j][0] {
			return sampleRows[i][1] < sampleRows[j][1]
		}
		return sampleRows[i][0] < sampleRows[j][0]
	})

	sections := []string{
		"base",
		csvSection(
			[]string{"base_id", "base_name", "last_synced_at"},
			[][]string{{baseID, baseName, lastSyncedAt}},
		),
		"tables",
		csvSection(
			[]string{"airtable_table_id", "duckdb_table_name", "original_name", "total_record_count"},
			tableRows,
		),
		"fields",
		csvSection(
			[]string{"table", "airtable_field_id", "duckdb_column_name", "original_name", "type", "airtable_type"},
			fieldRows,
		),
		"sample_rows",
		csvSection(
			[]string{"table", "row_index", "row_json"},
			sampleRows,
		),
	}
	return joinSections(sections...)
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
