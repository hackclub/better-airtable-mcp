package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// Field details (descriptions, options, primary flags) live in _metadata next
// to the column mapping. They never decide the cache's layout or its stored
// values, so they are refreshed in place instead of forcing a full sync: a
// renamed select choice or an edited description is a cheap metadata write.

var metadataDetailColumns = []string{
	`field_description VARCHAR`,
	`field_options JSON`,
	`is_primary_field BOOLEAN`,
	`table_description VARCHAR`,
}

// FieldDetailsCurrent reports whether the details stored in the cache file
// already match tables, so the caller can skip opening the file for writing.
func FieldDetailsCurrent(ctx context.Context, path string, tables []TableSnapshot) (bool, error) {
	db, err := openDatabase(path, "READ_ONLY")
	if err != nil {
		return false, err
	}
	defer db.Close()

	hasDetails, err := columnExists(ctx, db, "_metadata", "field_options")
	if err != nil || !hasDetails {
		return false, err
	}

	rows, err := db.QueryContext(ctx, `
		SELECT
			airtable_table_id,
			airtable_field_id,
			COALESCE(field_description, ''),
			COALESCE(CAST(field_options AS VARCHAR), ''),
			COALESCE(is_primary_field, FALSE),
			COALESCE(table_description, '')
		FROM _metadata
	`)
	if err != nil {
		return false, fmt.Errorf("read field details: %w", err)
	}
	defer rows.Close()

	type detail struct {
		description      string
		options          string
		isPrimary        bool
		tableDescription string
	}
	stored := map[string]detail{}
	for rows.Next() {
		var (
			tableID, fieldID string
			entry            detail
		)
		if err := rows.Scan(&tableID, &fieldID, &entry.description, &entry.options, &entry.isPrimary, &entry.tableDescription); err != nil {
			return false, fmt.Errorf("scan field details: %w", err)
		}
		stored[tableID+"/"+fieldID] = entry
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate field details: %w", err)
	}

	for _, table := range tables {
		for _, field := range table.Fields {
			entry, ok := stored[table.AirtableTableID+"/"+field.AirtableFieldID]
			if !ok {
				// A field the file has no row for means the schema itself
				// changed; the full sync that follows writes fresh details.
				continue
			}
			if entry.description != field.Description ||
				entry.options != field.Options ||
				entry.isPrimary != field.IsPrimary ||
				entry.tableDescription != table.Description {
				return false, nil
			}
		}
	}
	return true, nil
}

// WriteFieldDetails stores each field's details in the cache file, adding the
// detail columns first when the file predates them.
func WriteFieldDetails(ctx context.Context, path string, tables []TableSnapshot) error {
	db, err := openDatabase(path, "READ_WRITE")
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin duckdb transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if err := ensureMetadataDetailColumns(ctx, tx); err != nil {
		return err
	}
	for _, table := range tables {
		for _, field := range table.Fields {
			if _, err := tx.ExecContext(ctx, `
				UPDATE _metadata SET
					field_description = ?,
					field_options = ?,
					is_primary_field = ?,
					table_description = ?
				WHERE airtable_table_id = ? AND airtable_field_id = ?`,
				nullableString(field.Description),
				nullableString(field.Options),
				field.IsPrimary,
				nullableString(table.Description),
				table.AirtableTableID,
				field.AirtableFieldID,
			); err != nil {
				return fmt.Errorf("write field details for %s.%s: %w", table.DuckDBTableName, field.DuckDBColumnName, err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit field details: %w", err)
	}
	return nil
}

func ensureMetadataDetailColumns(ctx context.Context, tx *sql.Tx) error {
	for _, column := range metadataDetailColumns {
		if _, err := tx.ExecContext(ctx, `ALTER TABLE _metadata ADD COLUMN IF NOT EXISTS `+column); err != nil {
			return fmt.Errorf("add metadata column %q: %w", column, err)
		}
	}
	return nil
}

// ColumnStats describes how one column is actually used in the cached rows.
type ColumnStats struct {
	// Filled is how many rows hold a value.
	Filled int64
	// Example is one stored value as text, cut to columnExampleMaxChars, or
	// "" when no row holds one.
	Example string
}

// TableStats is the row count of a table and the stats of its columns, keyed
// by DuckDB column name.
type TableStats struct {
	Rows    int64
	Columns map[string]ColumnStats
}

// columnExampleMaxChars bounds the example fetched per column. Callers trim
// further for display; this only keeps a huge cell out of memory.
const columnExampleMaxChars = 200

// ReadColumnStats computes fill counts and one example value for every column
// of the given tables, with one aggregate query per table. The result is
// keyed by DuckDB table name.
func ReadColumnStats(ctx context.Context, path string, tables []TableSchema) (map[string]TableStats, error) {
	db, err := openDatabase(path, "READ_ONLY")
	if err != nil {
		return nil, err
	}
	defer db.Close()

	stats := make(map[string]TableStats, len(tables))
	for _, table := range tables {
		tableStats, err := readTableStats(ctx, db, table)
		if err != nil {
			return nil, err
		}
		stats[table.DuckDBTableName] = tableStats
	}
	return stats, nil
}

func readTableStats(ctx context.Context, db *sql.DB, table TableSchema) (TableStats, error) {
	selects := make([]string, 0, 1+2*len(table.Fields))
	selects = append(selects, `count(*)`)
	for _, field := range table.Fields {
		column := quoteIdent(field.DuckDBColumnName)
		example := fmt.Sprintf(`any_value(%s)`, column)
		if field.Type == "VARCHAR" {
			// Skip blank strings so the example shows a real value.
			example = fmt.Sprintf(`any_value(%s) FILTER (WHERE %s <> '')`, column, column)
		}
		selects = append(selects,
			fmt.Sprintf(`count(%s)`, column),
			fmt.Sprintf(`left(CAST(%s AS VARCHAR), %d)`, example, columnExampleMaxChars),
		)
	}

	query := fmt.Sprintf(`SELECT %s FROM %s`, strings.Join(selects, ", "), quoteIdent(table.DuckDBTableName))
	var rowCount int64
	filled := make([]int64, len(table.Fields))
	examples := make([]sql.NullString, len(table.Fields))
	targets := make([]any, 0, len(selects))
	targets = append(targets, &rowCount)
	for index := range table.Fields {
		targets = append(targets, &filled[index], &examples[index])
	}
	if err := db.QueryRowContext(ctx, query).Scan(targets...); err != nil {
		return TableStats{}, fmt.Errorf("read column stats for %s: %w", table.DuckDBTableName, err)
	}

	result := TableStats{Rows: rowCount, Columns: make(map[string]ColumnStats, len(table.Fields))}
	for index, field := range table.Fields {
		result.Columns[field.DuckDBColumnName] = ColumnStats{
			Filled:  filled[index],
			Example: examples[index].String,
		}
	}
	return result, nil
}
