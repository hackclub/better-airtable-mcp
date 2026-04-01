package duckdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

type BaseSnapshot struct {
	BaseID       string
	BaseName     string
	Tables       []TableSnapshot
	SyncedAt     time.Time
	SyncDuration time.Duration
}

type TableSnapshot struct {
	AirtableTableID string
	OriginalName    string
	DuckDBTableName string
	Fields          []FieldSnapshot
	Records         []RecordSnapshot
}

type FieldSnapshot struct {
	AirtableFieldID   string
	OriginalFieldName string
	DuckDBColumnName  string
	AirtableFieldType string
	DuckDBType        string
}

type RecordSnapshot struct {
	ID          string
	CreatedTime time.Time
	Fields      map[string]any
}

type SyncInfo struct {
	LastSyncedAt   time.Time
	SyncDurationMS int64
	TotalRecords   int64
	TotalTables    int
}

type BaseSchema struct {
	BaseID       string
	BaseName     string
	LastSyncedAt time.Time
	Tables       []TableSchema
}

type TableSchema struct {
	AirtableTableID  string
	DuckDBTableName  string
	OriginalName     string
	Fields           []FieldSchema
	SampleRows       []map[string]any
	TotalRecordCount int64
}

type FieldSchema struct {
	AirtableFieldID  string
	DuckDBColumnName string
	OriginalName     string
	Type             string
	AirtableType     string
}

type QueryResult struct {
	Columns          []string
	Rows             [][]any
	RowCount         int
	LastSyncedAt     time.Time
	LastSyncDuration time.Duration
}

func DatabaseFileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func WriteSnapshot(ctx context.Context, path string, snapshot BaseSnapshot) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create duckdb data dir: %w", err)
	}

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

	tableNames, err := listUserTables(ctx, tx)
	if err != nil {
		return err
	}
	for _, tableName := range tableNames {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, quoteIdent(tableName))); err != nil {
			return fmt.Errorf("drop table %s: %w", tableName, err)
		}
	}

	if err := createMetadataTables(ctx, tx); err != nil {
		return err
	}

	var totalRecords int64
	for _, table := range snapshot.Tables {
		if err := createDataTable(ctx, tx, table); err != nil {
			return err
		}
		if err := insertMetadataRows(ctx, tx, table); err != nil {
			return err
		}
		if err := insertRecords(ctx, tx, table); err != nil {
			return err
		}
		totalRecords += int64(len(table.Records))
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_info`); err != nil {
		return fmt.Errorf("clear sync info: %w", err)
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO _sync_info (last_synced_at, sync_duration_ms, total_records, total_tables) VALUES (?, ?, ?, ?)`,
		snapshot.SyncedAt.UTC(),
		snapshot.SyncDuration.Milliseconds(),
		totalRecords,
		len(snapshot.Tables),
	); err != nil {
		return fmt.Errorf("insert sync info: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit duckdb snapshot: %w", err)
	}

	return nil
}

func ReadSchema(ctx context.Context, path, baseID, baseName string) (BaseSchema, error) {
	db, err := openDatabase(path, "READ_ONLY")
	if err != nil {
		return BaseSchema{}, err
	}
	defer db.Close()

	syncInfo, err := readSyncInfo(ctx, db)
	if err != nil {
		return BaseSchema{}, err
	}

	rows, err := db.QueryContext(ctx, `
		SELECT
			duckdb_table_name,
			original_table_name,
			airtable_table_id,
			duckdb_column_name,
			original_field_name,
			airtable_field_id,
			airtable_field_type,
			duckdb_type
		FROM _metadata
		ORDER BY duckdb_table_name, rowid
	`)
	if err != nil {
		return BaseSchema{}, fmt.Errorf("read metadata rows: %w", err)
	}
	defer rows.Close()

	tablesByName := map[string]*TableSchema{}
	order := make([]string, 0)

	for rows.Next() {
		var (
			duckTableName     string
			originalTableName string
			airtableTableID   string
			duckColumnName    string
			originalFieldName string
			airtableFieldID   string
			airtableFieldType string
			duckType          string
		)

		if err := rows.Scan(
			&duckTableName,
			&originalTableName,
			&airtableTableID,
			&duckColumnName,
			&originalFieldName,
			&airtableFieldID,
			&airtableFieldType,
			&duckType,
		); err != nil {
			return BaseSchema{}, fmt.Errorf("scan metadata row: %w", err)
		}

		table, ok := tablesByName[duckTableName]
		if !ok {
			table = &TableSchema{
				AirtableTableID: airtableTableID,
				DuckDBTableName: duckTableName,
				OriginalName:    originalTableName,
			}
			tablesByName[duckTableName] = table
			order = append(order, duckTableName)
		}

		table.Fields = append(table.Fields, FieldSchema{
			AirtableFieldID:  airtableFieldID,
			DuckDBColumnName: duckColumnName,
			OriginalName:     originalFieldName,
			Type:             duckType,
			AirtableType:     airtableFieldType,
		})
	}
	if err := rows.Err(); err != nil {
		return BaseSchema{}, fmt.Errorf("iterate metadata rows: %w", err)
	}

	tables := make([]TableSchema, 0, len(order))
	for _, tableName := range order {
		table := tablesByName[tableName]

		sampleRows, err := queryRowsAsMaps(ctx, db, fmt.Sprintf(`SELECT * FROM %s LIMIT 3`, quoteIdent(table.DuckDBTableName)))
		if err != nil {
			return BaseSchema{}, fmt.Errorf("load sample rows for %s: %w", table.DuckDBTableName, err)
		}
		table.SampleRows = sampleRows

		if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s`, quoteIdent(table.DuckDBTableName))).Scan(&table.TotalRecordCount); err != nil {
			return BaseSchema{}, fmt.Errorf("count rows for %s: %w", table.DuckDBTableName, err)
		}

		tables = append(tables, *table)
	}

	return BaseSchema{
		BaseID:       baseID,
		BaseName:     baseName,
		LastSyncedAt: syncInfo.LastSyncedAt,
		Tables:       tables,
	}, nil
}

func Query(ctx context.Context, path, query string) (QueryResult, error) {
	db, err := openDatabase(path, "READ_ONLY")
	if err != nil {
		return QueryResult{}, err
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return QueryResult{}, fmt.Errorf("execute query: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return QueryResult{}, fmt.Errorf("read query columns: %w", err)
	}

	resultRows := make([][]any, 0)
	for rows.Next() {
		values, err := scanRow(rows, len(columns))
		if err != nil {
			return QueryResult{}, fmt.Errorf("scan query row: %w", err)
		}
		resultRows = append(resultRows, values)
	}
	if err := rows.Err(); err != nil {
		return QueryResult{}, fmt.Errorf("iterate query rows: %w", err)
	}

	syncInfo, err := readSyncInfo(ctx, db)
	if err != nil {
		return QueryResult{}, err
	}

	return QueryResult{
		Columns:          columns,
		Rows:             resultRows,
		RowCount:         len(resultRows),
		LastSyncedAt:     syncInfo.LastSyncedAt,
		LastSyncDuration: time.Duration(syncInfo.SyncDurationMS) * time.Millisecond,
	}, nil
}

func ReadSyncInfo(ctx context.Context, path string) (SyncInfo, error) {
	db, err := openDatabase(path, "READ_ONLY")
	if err != nil {
		return SyncInfo{}, err
	}
	defer db.Close()

	return readSyncInfo(ctx, db)
}

func ReadTableRowsByIDs(ctx context.Context, path, tableName string, ids []string) ([]map[string]any, error) {
	db, err := openDatabase(path, "READ_ONLY")
	if err != nil {
		return nil, err
	}
	defer db.Close()

	if len(ids) == 0 {
		return []map[string]any{}, nil
	}

	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for index, id := range ids {
		placeholders[index] = "?"
		args[index] = id
	}

	query := fmt.Sprintf(
		`SELECT * FROM %s WHERE id IN (%s)`,
		quoteIdent(tableName),
		strings.Join(placeholders, ", "),
	)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("read rows by id from %s: %w", tableName, err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("read row columns: %w", err)
	}

	found := make(map[string]map[string]any, len(ids))
	for rows.Next() {
		values, err := scanRow(rows, len(columns))
		if err != nil {
			return nil, fmt.Errorf("scan row by id: %w", err)
		}

		row := make(map[string]any, len(columns))
		for index, column := range columns {
			row[column] = values[index]
		}
		id, _ := row["id"].(string)
		found[id] = row
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rows by id: %w", err)
	}

	result := make([]map[string]any, 0, len(ids))
	for _, id := range ids {
		if row, ok := found[id]; ok {
			result = append(result, row)
		}
	}

	return result, nil
}

func createMetadataTables(ctx context.Context, tx *sql.Tx) error {
	statements := []string{
		`CREATE TABLE _metadata (
			duckdb_table_name VARCHAR,
			original_table_name VARCHAR,
			airtable_table_id VARCHAR,
			duckdb_column_name VARCHAR,
			original_field_name VARCHAR,
			airtable_field_id VARCHAR,
			airtable_field_type VARCHAR,
			duckdb_type VARCHAR
		)`,
		`CREATE TABLE _sync_info (
			last_synced_at TIMESTAMP,
			sync_duration_ms BIGINT,
			total_records BIGINT,
			total_tables INTEGER
		)`,
	}

	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("create metadata table: %w", err)
		}
	}

	return nil
}

func createDataTable(ctx context.Context, tx *sql.Tx, table TableSnapshot) error {
	columnDefs := []string{
		`"id" VARCHAR`,
		`"created_time" TIMESTAMP`,
	}
	for _, field := range table.Fields {
		columnDefs = append(columnDefs, fmt.Sprintf(`%s %s`, quoteIdent(field.DuckDBColumnName), field.DuckDBType))
	}

	statement := fmt.Sprintf(`CREATE TABLE %s (%s)`, quoteIdent(table.DuckDBTableName), strings.Join(columnDefs, ", "))
	if _, err := tx.ExecContext(ctx, statement); err != nil {
		return fmt.Errorf("create table %s: %w", table.DuckDBTableName, err)
	}

	return nil
}

func insertMetadataRows(ctx context.Context, tx *sql.Tx, table TableSnapshot) error {
	for _, field := range table.Fields {
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO _metadata (
				duckdb_table_name,
				original_table_name,
				airtable_table_id,
				duckdb_column_name,
				original_field_name,
				airtable_field_id,
				airtable_field_type,
				duckdb_type
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			table.DuckDBTableName,
			table.OriginalName,
			table.AirtableTableID,
			field.DuckDBColumnName,
			field.OriginalFieldName,
			field.AirtableFieldID,
			field.AirtableFieldType,
			field.DuckDBType,
		); err != nil {
			return fmt.Errorf("insert metadata row for %s.%s: %w", table.DuckDBTableName, field.DuckDBColumnName, err)
		}
	}

	return nil
}

func insertRecords(ctx context.Context, tx *sql.Tx, table TableSnapshot) error {
	if len(table.Records) == 0 {
		return nil
	}

	columnNames := []string{quoteIdent("id"), quoteIdent("created_time")}
	valueExprs := []string{"?", "?"}
	for _, field := range table.Fields {
		columnNames = append(columnNames, quoteIdent(field.DuckDBColumnName))
		if field.DuckDBType == "JSON" {
			valueExprs = append(valueExprs, "CAST(? AS JSON)")
		} else {
			valueExprs = append(valueExprs, "?")
		}
	}

	statement := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s)`,
		quoteIdent(table.DuckDBTableName),
		strings.Join(columnNames, ", "),
		strings.Join(valueExprs, ", "),
	)

	stmt, err := tx.PrepareContext(ctx, statement)
	if err != nil {
		return fmt.Errorf("prepare insert for %s: %w", table.DuckDBTableName, err)
	}
	defer stmt.Close()

	for _, record := range table.Records {
		args := make([]any, 0, len(table.Fields)+2)
		args = append(args, record.ID, record.CreatedTime.UTC())
		for _, field := range table.Fields {
			args = append(args, normalizeWriteValue(record.Fields[field.OriginalFieldName], field.AirtableFieldType))
		}

		if _, err := stmt.ExecContext(ctx, args...); err != nil {
			return fmt.Errorf("insert record into %s: %w", table.DuckDBTableName, err)
		}
	}

	return nil
}

func listUserTables(ctx context.Context, tx *sql.Tx) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'main'
	`)
	if err != nil {
		return nil, fmt.Errorf("list existing tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("scan existing table name: %w", err)
		}
		tables = append(tables, tableName)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate existing tables: %w", err)
	}

	sort.Strings(tables)
	return tables, nil
}

func openDatabase(path, accessMode string) (*sql.DB, error) {
	dsn := buildDSN(path, accessMode)

	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, fmt.Errorf("open duckdb database: %w", err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping duckdb database: %w", err)
	}

	return db, nil
}

func buildDSN(path, accessMode string) string {
	values := url.Values{}
	if accessMode != "" {
		values.Set("access_mode", accessMode)
	}
	values.Set("enable_external_access", "false")
	values.Set("autoload_known_extensions", "false")
	values.Set("autoinstall_known_extensions", "false")

	separator := "?"
	if strings.Contains(path, "?") {
		separator = "&"
	}
	return path + separator + values.Encode()
}

func readSyncInfo(ctx context.Context, db *sql.DB) (SyncInfo, error) {
	var info SyncInfo
	if err := db.QueryRowContext(ctx, `
		SELECT last_synced_at, sync_duration_ms, total_records, total_tables
		FROM _sync_info
		LIMIT 1
	`).Scan(&info.LastSyncedAt, &info.SyncDurationMS, &info.TotalRecords, &info.TotalTables); err != nil {
		return SyncInfo{}, fmt.Errorf("read sync info: %w", err)
	}

	return info, nil
}

func queryRowsAsMaps(ctx context.Context, db *sql.DB, query string) ([]map[string]any, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	results := make([]map[string]any, 0)
	for rows.Next() {
		values, err := scanRow(rows, len(columns))
		if err != nil {
			return nil, err
		}

		row := make(map[string]any, len(columns))
		for index, column := range columns {
			row[column] = values[index]
		}
		results = append(results, row)
	}

	return results, rows.Err()
}

func scanRow(rows *sql.Rows, columnCount int) ([]any, error) {
	targets := make([]any, columnCount)
	values := make([]any, columnCount)
	for index := range targets {
		targets[index] = &values[index]
	}

	if err := rows.Scan(targets...); err != nil {
		return nil, err
	}

	for index := range values {
		values[index] = normalizeReadValue(values[index])
	}

	return values, nil
}

func normalizeWriteValue(value any, airtableType string) any {
	if value == nil {
		return nil
	}

	switch airtableType {
	case "singleLineText", "multilineText", "richText", "email", "url", "phoneNumber", "singleSelect":
		return fmt.Sprint(value)
	case "number", "percent", "currency", "duration":
		if numeric, ok := value.(float64); ok {
			return numeric
		}
		return value
	case "autoNumber", "rating":
		if numeric, ok := value.(float64); ok {
			return int64(numeric)
		}
		return value
	case "checkbox":
		return value
	case "date", "dateTime", "createdTime", "lastModifiedTime":
		return parseTimestamp(value)
	case "multipleSelects", "multipleRecordLinks":
		return toStringSlice(value)
	case "lookup", "multipleAttachments":
		return mustMarshalJSON(value)
	case "rollup", "formula":
		return stringifyValue(value)
	case "createdBy", "lastModifiedBy":
		if record, ok := value.(map[string]any); ok {
			if name, ok := record["name"].(string); ok {
				return name
			}
		}
		return stringifyValue(value)
	case "barcode":
		if record, ok := value.(map[string]any); ok {
			if text, ok := record["text"].(string); ok {
				return text
			}
		}
		return stringifyValue(value)
	default:
		return value
	}
}

func normalizeReadValue(value any) any {
	switch typed := value.(type) {
	case time.Time:
		return typed.UTC().Format(time.RFC3339Nano)
	case []byte:
		return string(typed)
	case []any:
		result := make([]any, len(typed))
		for index, item := range typed {
			result[index] = normalizeReadValue(item)
		}
		return result
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, item := range typed {
			result[key] = normalizeReadValue(item)
		}
		return result
	case map[any]any:
		result := make(map[string]any, len(typed))
		for key, item := range typed {
			result[fmt.Sprint(key)] = normalizeReadValue(item)
		}
		return result
	default:
		return typed
	}
}

func parseTimestamp(value any) any {
	raw, ok := value.(string)
	if !ok {
		return value
	}

	if parsed, err := time.Parse(time.RFC3339, raw); err == nil {
		return parsed.UTC()
	}
	if parsed, err := time.Parse("2006-01-02", raw); err == nil {
		return parsed.UTC()
	}

	return raw
}

func toStringSlice(value any) any {
	switch typed := value.(type) {
	case []string:
		return typed
	case []any:
		result := make([]string, 0, len(typed))
		for _, item := range typed {
			if item == nil {
				continue
			}
			result = append(result, fmt.Sprint(item))
		}
		return result
	default:
		return []string{fmt.Sprint(value)}
	}
}

func stringifyValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case json.Number:
		return typed.String()
	default:
		return fmt.Sprint(value)
	}
}

func mustMarshalJSON(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(data)
}

func quoteIdent(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}
