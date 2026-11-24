package tools

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/hackclub/better-airtable-mcp/internal/duckdb"
)

// list_schema text is tab-separated: one header row, then one row per table
// (overview) or per column (detail). Cells never contain tabs or newlines.

const (
	// schemaAutoDetailBudget is how large the every-column view may be for
	// list_schema to return it without being asked for specific tables.
	// Larger bases get the table overview instead.
	schemaAutoDetailBudget = 12000
	// schemaAutoDetailMaxColumns skips computing the every-column view for
	// bases that cannot fit the budget anyway.
	schemaAutoDetailMaxColumns = 250
	// schemaDetailBudget caps the output of an explicit tables request.
	schemaDetailBudget = 40000
	// schemaSearchMaxMatches caps how many matching columns a search lists.
	schemaSearchMaxMatches = 150

	schemaExampleMaxChars     = 60
	schemaDescriptionMaxChars = 120
	schemaMaxChoicesShown     = 20
	schemaChoicesMaxChars     = 300

	schemaBuiltinColumnsNote = "Every table also has id (VARCHAR, the Airtable record ID) and created_time (TIMESTAMP). filled is the share of rows that hold a value; example is one stored value."
)

// readOnlyAirtableTypes are field types Airtable computes itself, which
// mutate cannot write.
var readOnlyAirtableTypes = map[string]bool{
	"formula":              true,
	"rollup":               true,
	"multipleLookupValues": true,
	"count":                true,
	"createdTime":          true,
	"lastModifiedTime":     true,
	"createdBy":            true,
	"lastModifiedBy":       true,
	"autoNumber":           true,
	"button":               true,
	"aiText":               true,
	"externalSyncSource":   true,
}

// airtableFieldOptions is the part of Airtable's field options list_schema
// describes.
type airtableFieldOptions struct {
	Choices []struct {
		Name string `json:"name"`
	} `json:"choices"`
	LinkedTableID     string `json:"linkedTableId"`
	RecordLinkFieldID string `json:"recordLinkFieldId"`
	Result            *struct {
		Type string `json:"type"`
	} `json:"result"`
}

// schemaNames resolves Airtable IDs in field options to the DuckDB names an
// agent writes in SQL.
type schemaNames struct {
	tableByID map[string]string
}

func newSchemaNames(tables []duckdb.TableSchema) schemaNames {
	names := schemaNames{tableByID: make(map[string]string, len(tables))}
	for _, table := range tables {
		names.tableByID[table.AirtableTableID] = table.DuckDBTableName
	}
	return names
}

func schemaColumnCount(tables []duckdb.TableSchema) int {
	total := 0
	for _, table := range tables {
		total += len(table.Fields)
	}
	return total
}

func schemaBaseLine(schema duckdb.BaseSchema) string {
	return fmt.Sprintf("base: %s (%s), %s, %s",
		cleanCell(schema.BaseName),
		schema.BaseID,
		plural(len(schema.Tables), "table"),
		plural(schemaColumnCount(schema.Tables), "column"),
	)
}

// formatSchemaOverview lists the tables of a base that is too large to show
// column by column.
func formatSchemaOverview(schema duckdb.BaseSchema) string {
	names := newSchemaNames(schema.Tables)
	withDescriptions := false
	for _, table := range schema.Tables {
		if table.Description != "" {
			withDescriptions = true
		}
	}

	header := []string{"table", "rows", "columns", "primary", "links_to"}
	if withDescriptions {
		header = append(header, "description")
	}
	lines := []string{
		schemaBaseLine(schema),
		"This base has too many columns to list at once. Call list_schema again with tables set to the ones you need, or with search to find columns by name across every table.",
		"",
		tsvRow(header...),
	}
	for _, table := range schema.Tables {
		rows := fmt.Sprint(table.TotalRecordCount)
		if !table.TableComplete {
			rows += "+"
		}
		primary := ""
		linked := make([]string, 0)
		seen := map[string]bool{}
		for _, field := range table.Fields {
			if field.IsPrimary {
				primary = field.DuckDBColumnName
			}
			if field.AirtableType != "multipleRecordLinks" {
				continue
			}
			target := names.tableByID[parseFieldOptions(field.Options).LinkedTableID]
			if target != "" && !seen[target] {
				seen[target] = true
				linked = append(linked, target)
			}
		}
		cells := []string{table.DuckDBTableName, rows, fmt.Sprint(len(table.Fields)), primary, strings.Join(linked, ", ")}
		if withDescriptions {
			cells = append(cells, truncateCell(table.Description, schemaDescriptionMaxChars))
		}
		lines = append(lines, tsvRow(cells...))
	}
	if schemaHasIncompleteTable(schema.Tables) {
		lines = append(lines, "", "A row count ending in + is still downloading.")
	}
	return strings.Join(lines, "\n") + "\n"
}

// formatSchemaDetail shows every column of the given tables. With a positive
// budget it stops adding tables (and, for a first table that alone exceeds
// it, columns) once the output would pass that many characters, and says
// what was left out.
func formatSchemaDetail(schema duckdb.BaseSchema, tables []duckdb.TableSchema, stats map[string]duckdb.TableStats, budget int) string {
	names := newSchemaNames(schema.Tables)
	var out strings.Builder
	out.WriteString(schemaBaseLine(schema))
	out.WriteString("\n")
	out.WriteString(schemaBuiltinColumnsNote)
	out.WriteString("\n")

	omitted := make([]string, 0)
	for index, table := range tables {
		section, shown := formatTableDetail(table, stats[table.DuckDBTableName], names, 0)
		if budget > 0 && out.Len()+len(section) > budget {
			if index > 0 {
				omitted = append(omitted, table.DuckDBTableName)
				continue
			}
			section, shown = formatTableDetail(table, stats[table.DuckDBTableName], names, budget-out.Len())
		}
		out.WriteString("\n")
		out.WriteString(section)
		if shown < len(table.Fields) {
			fmt.Fprintf(&out, "Showing the first %d of %d columns of %s. Use search to find the others.\n", shown, len(table.Fields), table.DuckDBTableName)
		}
	}
	if len(omitted) > 0 {
		fmt.Fprintf(&out, "\nNot shown, to keep this result small: %s. Ask for them in another call.\n", strings.Join(omitted, ", "))
	}
	return out.String()
}

// formatTableDetail renders one table's heading and column rows. A positive
// budget stops the rows once the section would pass that many characters;
// shown is how many columns made it in.
func formatTableDetail(table duckdb.TableSchema, stats duckdb.TableStats, names schemaNames, budget int) (section string, shown int) {
	rowCount := table.TotalRecordCount
	if stats.Columns != nil {
		rowCount = stats.Rows
	}
	rows := plural64(rowCount, "row")
	if !table.TableComplete {
		rows += " so far (still downloading)"
	}

	var out strings.Builder
	fmt.Fprintf(&out, "# %s: %s, %s (Airtable table %q, %s)\n",
		table.DuckDBTableName,
		rows,
		plural(len(table.Fields), "column"),
		cleanCell(table.OriginalName),
		table.AirtableTableID,
	)
	if table.Description != "" {
		out.WriteString(truncateCell(table.Description, schemaDescriptionMaxChars*2))
		out.WriteString("\n")
	}

	withDescriptions := false
	for _, field := range table.Fields {
		if field.Description != "" {
			withDescriptions = true
		}
	}
	header := []string{"column", "type", "airtable", "filled", "example"}
	if withDescriptions {
		header = append(header, "description")
	}
	out.WriteString(tsvRow(header...))
	out.WriteString("\n")

	columnByFieldID := make(map[string]string, len(table.Fields))
	for _, field := range table.Fields {
		columnByFieldID[field.AirtableFieldID] = field.DuckDBColumnName
	}
	for _, field := range table.Fields {
		cells := schemaColumnCells(field, stats, rowCount, names, columnByFieldID)
		if withDescriptions {
			cells = append(cells, truncateCell(field.Description, schemaDescriptionMaxChars))
		}
		line := tsvRow(cells...) + "\n"
		if budget > 0 && shown > 0 && out.Len()+len(line) > budget {
			break
		}
		out.WriteString(line)
		shown++
	}
	return out.String(), shown
}

// schemaMatch is one column found by a list_schema search.
type schemaMatch struct {
	Table duckdb.TableSchema
	Field duckdb.FieldSchema
}

// searchSchemaColumns returns the columns whose DuckDB name, Airtable name or
// description contains any of the search terms, ignoring case.
func searchSchemaColumns(tables []duckdb.TableSchema, terms []string) []schemaMatch {
	matches := make([]schemaMatch, 0)
	for _, table := range tables {
		for _, field := range table.Fields {
			haystacks := []string{
				strings.ToLower(field.DuckDBColumnName),
				strings.ToLower(field.OriginalName),
				strings.ToLower(field.Description),
			}
			if containsAnyTerm(haystacks, terms) {
				matches = append(matches, schemaMatch{Table: table, Field: field})
			}
		}
	}
	return matches
}

func containsAnyTerm(haystacks, terms []string) bool {
	for _, term := range terms {
		for _, haystack := range haystacks {
			if strings.Contains(haystack, term) {
				return true
			}
		}
	}
	return false
}

// schemaSearchTerms splits a search string into lower-cased terms. Spaces and
// commas both separate terms, and a column matching any term is returned.
func schemaSearchTerms(search string) []string {
	fields := strings.FieldsFunc(strings.ToLower(search), func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n' || r == '\r'
	})
	terms := make([]string, 0, len(fields))
	seen := map[string]bool{}
	for _, field := range fields {
		if field != "" && !seen[field] {
			seen[field] = true
			terms = append(terms, field)
		}
	}
	return terms
}

func formatSchemaSearch(schema duckdb.BaseSchema, searched []duckdb.TableSchema, terms []string, matches []schemaMatch, stats map[string]duckdb.TableStats) string {
	names := newSchemaNames(schema.Tables)
	lines := []string{schemaBaseLine(schema)}
	quoted := strings.Join(terms, ", ")

	if len(matches) == 0 {
		tableNames := make([]string, 0, len(searched))
		for _, table := range searched {
			tableNames = append(tableNames, table.DuckDBTableName)
		}
		lines = append(lines,
			fmt.Sprintf("No column name or description contains any of: %s.", quoted),
			"Tables searched: "+strings.Join(tableNames, ", "),
		)
		return strings.Join(lines, "\n") + "\n"
	}

	shown := matches
	if len(shown) > schemaSearchMaxMatches {
		shown = shown[:schemaSearchMaxMatches]
	}
	verb := "match"
	if len(matches) == 1 {
		verb = "matches"
	}
	summary := fmt.Sprintf("%s %s any of: %s.", plural(len(matches), "column"), verb, quoted)
	if len(shown) < len(matches) {
		summary += fmt.Sprintf(" Showing the first %d; use a narrower search or set tables to see the rest.", len(shown))
	}
	lines = append(lines, summary, schemaBuiltinColumnsNote, "")

	withDescriptions := false
	for _, match := range shown {
		if match.Field.Description != "" {
			withDescriptions = true
		}
	}
	header := []string{"table", "column", "type", "airtable", "filled", "example"}
	if withDescriptions {
		header = append(header, "description")
	}
	lines = append(lines, tsvRow(header...))

	columnsByTable := map[string]map[string]string{}
	for _, match := range shown {
		tableName := match.Table.DuckDBTableName
		columnByFieldID, ok := columnsByTable[tableName]
		if !ok {
			columnByFieldID = make(map[string]string, len(match.Table.Fields))
			for _, field := range match.Table.Fields {
				columnByFieldID[field.AirtableFieldID] = field.DuckDBColumnName
			}
			columnsByTable[tableName] = columnByFieldID
		}
		tableStats := stats[tableName]
		rowCount := match.Table.TotalRecordCount
		if tableStats.Columns != nil {
			rowCount = tableStats.Rows
		}
		cells := append([]string{tableName}, schemaColumnCells(match.Field, tableStats, rowCount, names, columnByFieldID)...)
		if withDescriptions {
			cells = append(cells, truncateCell(match.Field.Description, schemaDescriptionMaxChars))
		}
		lines = append(lines, tsvRow(cells...))
	}
	return strings.Join(lines, "\n") + "\n"
}

// schemaColumnCells is the column, type, airtable, filled and example cells
// of one column row.
func schemaColumnCells(field duckdb.FieldSchema, stats duckdb.TableStats, rowCount int64, names schemaNames, columnByFieldID map[string]string) []string {
	filled, example := "", ""
	if columnStats, ok := stats.Columns[field.DuckDBColumnName]; ok {
		filled = fillShare(columnStats.Filled, rowCount)
		example = truncateCell(columnStats.Example, schemaExampleMaxChars)
	}
	return []string{
		field.DuckDBColumnName,
		field.Type,
		describeAirtableField(field, names, columnByFieldID),
		filled,
		example,
	}
}

// describeAirtableField says what kind of Airtable field a column is, with
// the detail an agent needs to query or write it: the choices of a select,
// the table a link points at, the link a lookup or rollup reads through, and
// whether Airtable computes the value.
func describeAirtableField(field duckdb.FieldSchema, names schemaNames, columnByFieldID map[string]string) string {
	options := parseFieldOptions(field.Options)
	kind := field.AirtableType

	switch field.AirtableType {
	case "singleSelect", "multipleSelects":
		if choices := describeChoices(options); choices != "" {
			kind += ": " + choices
		}
	case "multipleRecordLinks":
		kind = "link"
		if target := names.tableByID[options.LinkedTableID]; target != "" {
			kind = "link to " + target
		}
	case "multipleLookupValues":
		kind = "lookup"
		if via := columnByFieldID[options.RecordLinkFieldID]; via != "" {
			kind = "lookup via " + via
		}
	case "rollup":
		if via := columnByFieldID[options.RecordLinkFieldID]; via != "" {
			kind = "rollup via " + via
		}
	case "count":
		if via := columnByFieldID[options.RecordLinkFieldID]; via != "" {
			kind = "count of " + via
		}
	}
	if options.Result != nil && options.Result.Type != "" {
		switch field.AirtableType {
		case "formula", "rollup", "multipleLookupValues":
			kind += " (" + options.Result.Type + ")"
		}
	}
	if field.IsPrimary {
		kind += ", primary"
	}
	if readOnlyAirtableTypes[field.AirtableType] {
		kind += ", read-only"
	}
	return cleanCell(kind)
}

func describeChoices(options airtableFieldOptions) string {
	if len(options.Choices) == 0 {
		return ""
	}
	var out strings.Builder
	shown := 0
	for _, choice := range options.Choices {
		name := cleanCell(choice.Name)
		if shown >= schemaMaxChoicesShown || (shown > 0 && out.Len()+len(name) > schemaChoicesMaxChars) {
			break
		}
		if shown > 0 {
			out.WriteString(" | ")
		}
		out.WriteString(name)
		shown++
	}
	if rest := len(options.Choices) - shown; rest > 0 {
		fmt.Fprintf(&out, " (+%d more)", rest)
	}
	return out.String()
}

func parseFieldOptions(options string) airtableFieldOptions {
	var parsed airtableFieldOptions
	if options != "" {
		_ = json.Unmarshal([]byte(options), &parsed)
	}
	return parsed
}

// fillShare is the share of rows holding a value, as a whole percentage.
// 0% and 100% are exact: anything in between never rounds to them.
func fillShare(filled, rows int64) string {
	if rows <= 0 {
		return ""
	}
	if filled <= 0 {
		return "0%"
	}
	if filled >= rows {
		return "100%"
	}
	percent := (filled*100 + rows/2) / rows
	percent = max(1, min(99, percent))
	return fmt.Sprintf("%d%%", percent)
}

func schemaHasIncompleteTable(tables []duckdb.TableSchema) bool {
	for _, table := range tables {
		if !table.TableComplete {
			return true
		}
	}
	return false
}

func tsvRow(cells ...string) string {
	cleaned := make([]string, len(cells))
	for index, cell := range cells {
		cleaned[index] = cleanCell(cell)
	}
	return strings.Join(cleaned, "\t")
}

// cleanCell folds tabs and line breaks into single spaces so a value cannot
// break the row it sits in.
func cleanCell(value string) string {
	if !strings.ContainsAny(value, "\t\n\r") {
		return strings.TrimSpace(value)
	}
	return strings.Join(strings.Fields(value), " ")
}

func truncateCell(value string, maxChars int) string {
	cleaned := cleanCell(value)
	runes := []rune(cleaned)
	if len(runes) <= maxChars {
		return cleaned
	}
	return string(runes[:maxChars-3]) + "..."
}

func plural(count int, noun string) string {
	return plural64(int64(count), noun)
}

func plural64(count int64, noun string) string {
	if count == 1 {
		return fmt.Sprintf("1 %s", noun)
	}
	return fmt.Sprintf("%d %ss", count, noun)
}
