package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	duckdbdriver "github.com/duckdb/duckdb-go/v2"
)

// sqlExecutor is what the write helpers need from a *sql.Tx or *sql.Conn.
type sqlExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

var errNotAppendable = errors.New("value has no exact appender type")

// writePages applies pages and sync state in one transaction on a dedicated
// connection, writing records through DuckDB's appender.
//
// A prepared INSERT executes a full query per row: profiling a Unified YSWS DB
// sync (174k records, 85 columns on the largest table) put 75% of the run in
// per-row statement execution. The appender writes rows in bulk instead.
// database/sql transactions do not expose the driver connection the appender
// needs, so the transaction is opened with explicit BEGIN/COMMIT, and appends
// on the connection join it.
func writePages(ctx context.Context, db *sql.DB, pages []TablePage, info SyncInfo) (err error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("open duckdb connection: %w", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	if _, err := conn.ExecContext(ctx, `BEGIN TRANSACTION`); err != nil {
		return fmt.Errorf("begin duckdb transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			// The caller's ctx may be the reason we are rolling back.
			_, _ = conn.ExecContext(context.WithoutCancel(ctx), `ROLLBACK`)
		}
	}()

	for _, page := range pages {
		table := TableSnapshot{
			DuckDBTableName: page.Table.DuckDBTableName,
			Fields:          page.Table.Fields,
			Records:         page.Records,
		}
		rows, ok := appenderRows(table)
		if ok {
			err = appendRows(conn, table.DuckDBTableName, rows)
		} else {
			// A value the appender would reject is left to the INSERT path,
			// whose implicit casts keep the original behaviour for it.
			err = insertRecords(ctx, conn, table)
		}
		if err != nil {
			return err
		}
		if err := upsertTableSyncState(ctx, conn, page.State); err != nil {
			return err
		}
	}
	if err := upsertSyncInfo(ctx, conn, info); err != nil {
		return err
	}

	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return fmt.Errorf("commit duckdb pages: %w", err)
	}
	committed = true
	return nil
}

func appendRows(conn *sql.Conn, tableName string, rows [][]driver.Value) error {
	if len(rows) == 0 {
		return nil
	}
	return conn.Raw(func(driverConn any) error {
		dc, ok := driverConn.(driver.Conn)
		if !ok {
			return fmt.Errorf("duckdb driver connection has unexpected type %T", driverConn)
		}
		appender, err := duckdbdriver.NewAppenderFromConn(dc, "", tableName)
		if err != nil {
			return fmt.Errorf("open appender for %s: %w", tableName, err)
		}
		for _, row := range rows {
			if err := appender.AppendRow(row...); err != nil {
				_ = appender.Close()
				return fmt.Errorf("append record into %s: %w", tableName, err)
			}
		}
		if err := appender.Close(); err != nil {
			return fmt.Errorf("flush appender for %s: %w", tableName, err)
		}
		return nil
	})
}

// appenderRows converts a page into rows of the exact Go types the appender
// requires for each column, in table column order (id, created_time, then
// fields as createDataTable declares them). It reports false if any value
// does not convert exactly, so the caller can fall back to INSERT.
func appenderRows(table TableSnapshot) ([][]driver.Value, bool) {
	rows := make([][]driver.Value, 0, len(table.Records))
	for _, record := range table.Records {
		row := make([]driver.Value, 0, len(table.Fields)+2)
		row = append(row, record.ID, record.CreatedTime.UTC())
		for _, field := range table.Fields {
			value, err := appenderValue(normalizeWriteValue(record.Fields[field.OriginalFieldName], field.AirtableFieldType), field.DuckDBType)
			if err != nil {
				return nil, false
			}
			row = append(row, value)
		}
		rows = append(rows, row)
	}
	return rows, true
}

func appenderValue(value any, duckDBType string) (driver.Value, error) {
	if value == nil {
		return nil, nil
	}
	switch duckDBType {
	case "VARCHAR":
		if s, ok := value.(string); ok {
			return s, nil
		}
	case "JSON":
		// The driver json.Marshals every value bound for a JSON column, so a
		// plain string would be stored as a quoted JSON string. The value is
		// already encoded; RawMessage passes it through as is.
		if s, ok := value.(string); ok {
			return json.RawMessage(s), nil
		}
	case "DOUBLE":
		switch v := value.(type) {
		case float64:
			return v, nil
		case int64:
			return float64(v), nil
		}
	case "BIGINT":
		switch v := value.(type) {
		case int64:
			return v, nil
		case float64:
			if v == math.Trunc(v) && math.Abs(v) < 1<<53 {
				return int64(v), nil
			}
		}
	case "BOOLEAN":
		if b, ok := value.(bool); ok {
			return b, nil
		}
	case "TIMESTAMP":
		if t, ok := value.(time.Time); ok {
			return t, nil
		}
	case "VARCHAR[]":
		if list, ok := value.([]string); ok {
			out := make([]any, len(list))
			for i, s := range list {
				out[i] = s
			}
			return out, nil
		}
	}
	return nil, fmt.Errorf("%w: %T for %s", errNotAppendable, value, duckDBType)
}
