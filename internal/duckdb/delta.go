package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// CacheFormat is the layout version of a cache file. A file written by an
// older layout is rebuilt by a full sync instead of being delta-synced.
const CacheFormat = 3

// ReconcileMeta is what a completed full fetch records about the cache: the
// schema it was built from and the point in time each table is known to match
// Airtable exactly.
type ReconcileMeta struct {
	SchemaHash   string
	Watermark    time.Time
	ReconciledAt time.Time
}

// pendingTableDDL creates the follow-up queue. A delta write records here, in
// the same transaction as the rows it wrote, which records now need a
// follow-up fetch: rows computed from a value that changed, and records at
// the other end of a link that changed. Keeping the queue in the file means a
// delta that fails or is killed halfway cannot forget the work it caused.
const pendingTableDDL = `CREATE TABLE IF NOT EXISTS _delta_pending (
	generation INTEGER,
	source_table VARCHAR,
	kind VARCHAR,
	column_name VARCHAR,
	record_id VARCHAR
)`

// Kinds of follow-up work in _delta_pending.
const (
	// PendingChanged: record_id in source_table changed value in column_name.
	PendingChanged = "changed"
	// PendingChangedAny: record_id in source_table changed in some column.
	PendingChangedAny = "changed_any"
	// PendingLinkEnd: record_id is at the other end of link column_name of
	// source_table, and that link changed.
	PendingLinkEnd = "link_end"
)

// PendingChange is one queued follow-up.
type PendingChange struct {
	Generation  int
	SourceTable string
	Kind        string
	Column      string
	RecordID    string
}

// TableDeltaState is one table's sync position inside a cache file.
type TableDeltaState struct {
	TableName     string
	SyncStatus    string
	RecordCount   int64
	Watermark     time.Time
	CheckedAt     time.Time
	ReconciledAt  time.Time
	ComputedStale bool
}

// CacheState is the sync position of a whole cache file.
type CacheState struct {
	SchemaHash  string
	CacheFormat int
	Info        SyncInfo
	Tables      []TableDeltaState
}

// DeltaTable is one table's share of a delta write.
type DeltaTable struct {
	// Table carries the DuckDB name and column layout; Table.Records are the
	// rows to upsert by record ID.
	Table     TableSnapshot
	DeleteIDs []string

	// WatchColumns asks which upserted rows changed value in each of these
	// columns compared to the row they replaced. WatchAny asks the same for
	// the row as a whole. Rows that did not exist before are not reported.
	WatchColumns []string
	WatchAny     bool
	// LinkColumns are VARCHAR[] columns of record IDs. For each one the write
	// reports which record IDs stopped being referenced (by a replaced or
	// deleted row) and which started being referenced (by a new or changed
	// row). Airtable mirrors every link edit on the record at the other end,
	// so those are the rows whose own link column just changed.
	LinkColumns []string

	// PendingGeneration, when positive, queues what this write changed in
	// _delta_pending under that generation.
	PendingGeneration int
	// ForceChanged reports every upserted row as changed in every watched
	// column, without comparing. It is for rows whose dependents may have
	// been fetched before the row's latest edit, where comparing against the
	// cache proves nothing.
	ForceChanged bool

	// AdvanceWatermark moves the table's delta watermark to NewWatermark, but
	// only while the stored watermark still equals BasedOnWatermark. A full
	// fetch that swapped a fresh file in underneath this delta rewinds the
	// watermark, and a delta planned against the old file must not skip it
	// forward again.
	AdvanceWatermark bool
	BasedOnWatermark time.Time
	NewWatermark     time.Time
	CheckedAt        time.Time
	ComputedStale    *bool
}

// DeltaTableResult reports what a delta write changed in one table.
type DeltaTableResult struct {
	TableName         string
	Upserted          int
	Deleted           int
	RecordCount       int64
	ChangedByColumn   map[string][]string
	ChangedAny        []string
	DroppedLinks      map[string][]string
	AddedLinks        map[string][]string
	WatermarkAdvanced bool
}

var recordIDPattern = regexp.MustCompile(`^rec[A-Za-z0-9]{1,40}$`)

// recordIDList renders record IDs as a SQL list literal. IDs are validated
// against Airtable's record ID shape, so nothing else can reach the statement.
func recordIDList(ids []string) (string, error) {
	quoted := make([]string, 0, len(ids))
	for _, id := range ids {
		if !recordIDPattern.MatchString(id) {
			return "", fmt.Errorf("invalid airtable record id %q", id)
		}
		quoted = append(quoted, "'"+id+"'")
	}
	return "[" + strings.Join(quoted, ", ") + "]", nil
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func uniqueSorted(values []string) []string {
	out := uniqueStrings(values)
	sort.Strings(out)
	return out
}

func microTime(value time.Time) time.Time {
	return value.UTC().Truncate(time.Microsecond)
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return microTime(value)
}

// ApplyDelta upserts and deletes rows in the active cache file and advances
// the per-table sync positions, all in one transaction.
func ApplyDelta(ctx context.Context, path string, tables []DeltaTable, syncedAt time.Time) ([]DeltaTableResult, error) {
	// Opening read-write would create a missing file as an empty database,
	// which every later read would then trip over.
	if !DatabaseFileExists(path) {
		return nil, fmt.Errorf("cache file %s is gone", filepath.Base(path))
	}
	db, err := openDatabase(path, "READ_WRITE")
	if err != nil {
		return nil, err
	}
	defer db.Close()

	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("open duckdb connection: %w", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	if _, err := conn.ExecContext(ctx, `BEGIN TRANSACTION`); err != nil {
		return nil, fmt.Errorf("begin duckdb transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.WithoutCancel(ctx), `ROLLBACK`)
		}
	}()
	if _, err := conn.ExecContext(ctx, pendingTableDDL); err != nil {
		return nil, fmt.Errorf("create follow-up queue: %w", err)
	}

	results := make([]DeltaTableResult, 0, len(tables))
	for _, table := range tables {
		result, err := applyDeltaTable(ctx, conn, table)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	if !syncedAt.IsZero() {
		if _, err := conn.ExecContext(ctx, `UPDATE _sync_info SET last_synced_at = ?, status = 'completed', error = NULL`, microTime(syncedAt)); err != nil {
			return nil, fmt.Errorf("update sync info after delta: %w", err)
		}
	}

	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return nil, fmt.Errorf("commit duckdb delta: %w", err)
	}
	committed = true
	return results, nil
}

func applyDeltaTable(ctx context.Context, conn *sql.Conn, table DeltaTable) (DeltaTableResult, error) {
	name := table.Table.DuckDBTableName
	quotedTable := quoteIdent(name)
	result := DeltaTableResult{
		TableName:       name,
		ChangedByColumn: map[string][]string{},
		DroppedLinks:    map[string][]string{},
		AddedLinks:      map[string][]string{},
	}

	upsertIDs := make([]string, 0, len(table.Table.Records))
	for _, record := range table.Table.Records {
		upsertIDs = append(upsertIDs, record.ID)
	}
	upsertIDs = uniqueStrings(upsertIDs)
	deleteIDs := uniqueStrings(table.DeleteIDs)
	if len(upsertIDs) != len(table.Table.Records) {
		return result, fmt.Errorf("delta for %s repeats a record id", name)
	}

	watching := len(upsertIDs) > 0 && (len(table.WatchColumns) > 0 || table.WatchAny || len(table.LinkColumns) > 0)
	if watching {
		list, err := recordIDList(upsertIDs)
		if err != nil {
			return result, err
		}
		if _, err := conn.ExecContext(ctx, `DROP TABLE IF EXISTS _delta_old`); err != nil {
			return result, fmt.Errorf("reset delta scratch table: %w", err)
		}
		if _, err := conn.ExecContext(ctx, fmt.Sprintf(`CREATE TEMP TABLE _delta_old AS SELECT * FROM %s WHERE id IN (SELECT unnest(%s))`, quotedTable, list)); err != nil {
			return result, fmt.Errorf("capture replaced rows of %s: %w", name, err)
		}
	}

	if removeIDs := uniqueStrings(append(append([]string{}, upsertIDs...), deleteIDs...)); len(removeIDs) > 0 {
		list, err := recordIDList(removeIDs)
		if err != nil {
			return result, err
		}
		if len(deleteIDs) > 0 {
			deleteList, err := recordIDList(deleteIDs)
			if err != nil {
				return result, err
			}
			var deleted int64
			if err := conn.QueryRowContext(ctx, fmt.Sprintf(`SELECT count(*) FROM %s WHERE id IN (SELECT unnest(%s))`, quotedTable, deleteList)).Scan(&deleted); err != nil {
				return result, fmt.Errorf("count deleted rows of %s: %w", name, err)
			}
			result.Deleted = int(deleted)
			// Whatever a deleted row linked to has lost that link.
			for _, column := range uniqueStrings(table.LinkColumns) {
				ids, err := queryIDs(ctx, conn, fmt.Sprintf(
					`SELECT DISTINCT unnest(%s) FROM %s WHERE id IN (SELECT unnest(%s))`,
					quoteIdent(column), quotedTable, deleteList,
				))
				if err != nil {
					return result, fmt.Errorf("read links %s of deleted rows of %s: %w", column, name, err)
				}
				if len(ids) > 0 {
					result.DroppedLinks[column] = ids
				}
			}
		}
		if _, err := conn.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE id IN (SELECT unnest(%s))`, quotedTable, list)); err != nil {
			return result, fmt.Errorf("remove replaced rows of %s: %w", name, err)
		}
	}

	if len(table.Table.Records) > 0 {
		rows, ok := appenderRows(table.Table)
		var err error
		if ok {
			err = appendRows(conn, name, rows)
		} else {
			err = insertRecords(ctx, conn, table.Table)
		}
		if err != nil {
			return result, err
		}
		result.Upserted = len(table.Table.Records)
	}

	if watching {
		columnTypes := make(map[string]string, len(table.Table.Fields))
		for _, field := range table.Table.Fields {
			columnTypes[field.DuckDBColumnName] = field.DuckDBType
		}
		for _, column := range uniqueStrings(table.WatchColumns) {
			if table.ForceChanged {
				result.ChangedByColumn[column] = uniqueSorted(upsertIDs)
				continue
			}
			ids, err := queryIDs(ctx, conn, fmt.Sprintf(
				`SELECT n.id FROM %s n JOIN _delta_old o ON n.id = o.id WHERE %s`,
				quotedTable, distinctCondition(column, columnTypes[column]),
			))
			if err != nil {
				return result, fmt.Errorf("compare column %s of %s: %w", column, name, err)
			}
			if len(ids) > 0 {
				result.ChangedByColumn[column] = ids
			}
		}
		if table.WatchAny && table.ForceChanged {
			result.ChangedAny = uniqueSorted(upsertIDs)
		} else if table.WatchAny {
			conditions := make([]string, 0, len(table.Table.Fields))
			for _, field := range table.Table.Fields {
				conditions = append(conditions, distinctCondition(field.DuckDBColumnName, field.DuckDBType))
			}
			if len(conditions) > 0 {
				ids, err := queryIDs(ctx, conn, fmt.Sprintf(
					`SELECT n.id FROM %s n JOIN _delta_old o ON n.id = o.id WHERE %s`,
					quotedTable, strings.Join(conditions, " OR "),
				))
				if err != nil {
					return result, fmt.Errorf("compare rows of %s: %w", name, err)
				}
				result.ChangedAny = ids
			}
		}
		for _, column := range uniqueStrings(table.LinkColumns) {
			quotedColumn := quoteIdent(column)
			// Both comparisons are set differences over (row, linked id)
			// pairs. Joining each link against its row's whole list instead
			// copies that list once per link: a few rows with thousands of
			// links each need hundreds of MiB at once and run the write out
			// of memory.
			ids, err := queryIDs(ctx, conn, fmt.Sprintf(
				`SELECT DISTINCT target FROM (
					SELECT id, unnest(%s) AS target FROM _delta_old
					EXCEPT
					SELECT id, unnest(%s) AS target FROM %s WHERE id IN (SELECT id FROM _delta_old)
				 ) WHERE target IS NOT NULL ORDER BY target`,
				quotedColumn, quotedColumn, quotedTable,
			))
			if err != nil {
				return result, fmt.Errorf("compare links %s of %s: %w", column, name, err)
			}
			if len(ids) > 0 {
				result.DroppedLinks[column] = uniqueSorted(append(result.DroppedLinks[column], ids...))
			}

			upsertList, err := recordIDList(upsertIDs)
			if err != nil {
				return result, err
			}
			added, err := queryIDs(ctx, conn, fmt.Sprintf(
				`SELECT DISTINCT target FROM (
					SELECT id, unnest(%s) AS target FROM %s WHERE id IN (SELECT unnest(%s))
					EXCEPT
					SELECT id, unnest(%s) AS target FROM _delta_old
				 ) WHERE target IS NOT NULL ORDER BY target`,
				quotedColumn, quotedTable, upsertList, quotedColumn,
			))
			if err != nil {
				return result, fmt.Errorf("compare new links %s of %s: %w", column, name, err)
			}
			if len(added) > 0 {
				result.AddedLinks[column] = added
			}
		}
		if _, err := conn.ExecContext(ctx, `DROP TABLE IF EXISTS _delta_old`); err != nil {
			return result, fmt.Errorf("drop delta scratch table: %w", err)
		}
	}

	if table.PendingGeneration > 0 {
		queue := func(kind, column string, ids []string) error {
			valid := make([]string, 0, len(ids))
			for _, id := range ids {
				if recordIDPattern.MatchString(id) {
					valid = append(valid, id)
				}
			}
			if len(valid) == 0 {
				return nil
			}
			list, err := recordIDList(valid)
			if err != nil {
				return err
			}
			_, err = conn.ExecContext(ctx,
				fmt.Sprintf(`INSERT INTO _delta_pending SELECT ?, ?, ?, ?, unnest(%s)`, list),
				table.PendingGeneration, name, kind, column,
			)
			if err != nil {
				return fmt.Errorf("queue follow-up for %s: %w", name, err)
			}
			return nil
		}
		for column, ids := range result.ChangedByColumn {
			if err := queue(PendingChanged, column, ids); err != nil {
				return result, err
			}
		}
		if err := queue(PendingChangedAny, "", result.ChangedAny); err != nil {
			return result, err
		}
		for _, links := range []map[string][]string{result.DroppedLinks, result.AddedLinks} {
			for column, ids := range links {
				if err := queue(PendingLinkEnd, column, ids); err != nil {
					return result, err
				}
			}
		}
	}

	if err := conn.QueryRowContext(ctx, fmt.Sprintf(`SELECT count(*) FROM %s`, quotedTable)).Scan(&result.RecordCount); err != nil {
		return result, fmt.Errorf("count rows of %s: %w", name, err)
	}

	if table.AdvanceWatermark {
		var stored sql.NullTime
		if err := conn.QueryRowContext(ctx, `SELECT delta_watermark FROM _table_sync WHERE duckdb_table_name = ?`, name).Scan(&stored); err != nil {
			return result, fmt.Errorf("read delta watermark of %s: %w", name, err)
		}
		if stored.Valid && microTime(stored.Time).Equal(microTime(table.BasedOnWatermark)) {
			if _, err := conn.ExecContext(ctx, `UPDATE _table_sync SET delta_watermark = ? WHERE duckdb_table_name = ?`, microTime(table.NewWatermark), name); err != nil {
				return result, fmt.Errorf("advance delta watermark of %s: %w", name, err)
			}
			result.WatermarkAdvanced = true
		}
	}
	if _, err := conn.ExecContext(ctx,
		`UPDATE _table_sync
		 SET visible_record_count = ?,
		     delta_checked_at = COALESCE(?, delta_checked_at),
		     computed_stale = COALESCE(?, computed_stale)
		 WHERE duckdb_table_name = ?`,
		result.RecordCount, nullableTime(table.CheckedAt), nullableBool(table.ComputedStale), name,
	); err != nil {
		return result, fmt.Errorf("update sync position of %s: %w", name, err)
	}

	return result, nil
}

// distinctCondition compares one column between the new row (n) and the row
// it replaced (o). Attachment URLs are signed and differ on every read, and
// attachments reach JSON columns both directly and through lookups, so URLs
// are blanked before JSON values are compared; otherwise every refetch of a
// row with an attachment would look like a change.
func distinctCondition(column, duckDBType string) string {
	quoted := quoteIdent(column)
	if duckDBType != "JSON" {
		return fmt.Sprintf(`n.%s IS DISTINCT FROM o.%s`, quoted, quoted)
	}
	const blankURLs = `regexp_replace(CAST(%s.%s AS VARCHAR), '"url":\s*"[^"]*"', '"url":""', 'g')`
	return fmt.Sprintf(blankURLs+` IS DISTINCT FROM `+blankURLs, "n", quoted, "o", quoted)
}

func nullableBool(value *bool) any {
	if value == nil {
		return nil
	}
	return *value
}

type rowQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func queryIDs(ctx context.Context, db rowQuerier, query string) ([]string, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id sql.NullString
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		if id.Valid {
			ids = append(ids, id.String)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Strings(ids)
	return ids, nil
}

// markReconciled records that every table in the file matches Airtable as of
// the start of the full fetch that just completed.
func markReconciled(ctx context.Context, db *sql.DB, meta ReconcileMeta) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin duckdb transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if _, err := tx.ExecContext(ctx, `UPDATE _sync_info SET schema_hash = ?, cache_format = ?`, meta.SchemaHash, CacheFormat); err != nil {
		return fmt.Errorf("record cache schema: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE _table_sync SET delta_watermark = ?, delta_checked_at = ?, reconciled_at = ?, computed_stale = FALSE`,
		microTime(meta.Watermark), microTime(meta.ReconciledAt), microTime(meta.ReconciledAt),
	); err != nil {
		return fmt.Errorf("record table sync positions: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit reconcile metadata: %w", err)
	}
	return nil
}

// MarkReconciled is markReconciled for a file opened just for this write.
func MarkReconciled(ctx context.Context, path string, meta ReconcileMeta) error {
	db, err := openDatabase(path, "READ_WRITE")
	if err != nil {
		return err
	}
	defer db.Close()
	return markReconciled(ctx, db, meta)
}

// MarkReconciled records the reconcile metadata through the session's handle.
func (s *WriteSession) MarkReconciled(ctx context.Context, meta ReconcileMeta) error {
	return markReconciled(ctx, s.db, meta)
}

// ReadCacheState reads the sync position of a cache file. A file written
// before delta sync existed reports CacheFormat 0 and no table positions.
func ReadCacheState(ctx context.Context, path string) (CacheState, error) {
	db, err := openDatabase(path, "READ_ONLY")
	if err != nil {
		return CacheState{}, err
	}
	defer db.Close()

	info, err := readSyncInfo(ctx, db)
	if err != nil {
		return CacheState{}, err
	}
	state := CacheState{Info: info}

	hasFormat, err := columnExists(ctx, db, "_sync_info", "cache_format")
	if err != nil {
		return CacheState{}, err
	}
	hasPositions, err := columnExists(ctx, db, "_table_sync", "delta_watermark")
	if err != nil {
		return CacheState{}, err
	}
	if !hasFormat || !hasPositions {
		return state, nil
	}

	var schemaHash sql.NullString
	var cacheFormat sql.NullInt64
	err = db.QueryRowContext(ctx, `SELECT schema_hash, cache_format FROM _sync_info LIMIT 1`).Scan(&schemaHash, &cacheFormat)
	if err != nil && err != sql.ErrNoRows {
		return CacheState{}, fmt.Errorf("read cache schema: %w", err)
	}
	state.SchemaHash = schemaHash.String
	state.CacheFormat = int(cacheFormat.Int64)

	rows, err := db.QueryContext(ctx, `
		SELECT duckdb_table_name, sync_status, visible_record_count, delta_watermark, delta_checked_at, reconciled_at, computed_stale
		FROM _table_sync
		ORDER BY duckdb_table_name`)
	if err != nil {
		return CacheState{}, fmt.Errorf("read table sync positions: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			table         TableDeltaState
			status        sql.NullString
			count         sql.NullInt64
			watermark     sql.NullTime
			checkedAt     sql.NullTime
			reconciledAt  sql.NullTime
			computedStale sql.NullBool
		)
		if err := rows.Scan(&table.TableName, &status, &count, &watermark, &checkedAt, &reconciledAt, &computedStale); err != nil {
			return CacheState{}, fmt.Errorf("scan table sync position: %w", err)
		}
		table.SyncStatus = status.String
		table.RecordCount = count.Int64
		if watermark.Valid {
			table.Watermark = watermark.Time.UTC()
		}
		if checkedAt.Valid {
			table.CheckedAt = checkedAt.Time.UTC()
		}
		if reconciledAt.Valid {
			table.ReconciledAt = reconciledAt.Time.UTC()
		}
		table.ComputedStale = computedStale.Bool
		state.Tables = append(state.Tables, table)
	}
	if err := rows.Err(); err != nil {
		return CacheState{}, fmt.Errorf("iterate table sync positions: %w", err)
	}
	return state, nil
}

// FindRowsLinking returns the IDs of rows whose link column references any
// of the target record IDs.
func FindRowsLinking(ctx context.Context, path, tableName, linkColumn string, targetIDs []string) ([]string, error) {
	if len(targetIDs) == 0 {
		return nil, nil
	}
	list, err := recordIDList(uniqueStrings(targetIDs))
	if err != nil {
		return nil, err
	}
	db, err := openDatabase(path, "READ_ONLY")
	if err != nil {
		return nil, err
	}
	defer db.Close()

	ids, err := queryIDs(ctx, db, fmt.Sprintf(
		`SELECT id FROM %s WHERE list_has_any(%s, %s)`,
		quoteIdent(tableName), quoteIdent(linkColumn), list,
	))
	if err != nil {
		return nil, fmt.Errorf("find rows of %s linking through %s: %w", tableName, linkColumn, err)
	}
	return ids, nil
}

// ExistingRecordIDs returns which of the given IDs are present in a table.
func ExistingRecordIDs(ctx context.Context, path, tableName string, ids []string) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	list, err := recordIDList(uniqueStrings(ids))
	if err != nil {
		return nil, err
	}
	db, err := openDatabase(path, "READ_ONLY")
	if err != nil {
		return nil, err
	}
	defer db.Close()

	found, err := queryIDs(ctx, db, fmt.Sprintf(`SELECT id FROM %s WHERE id IN (SELECT unnest(%s))`, quoteIdent(tableName), list))
	if err != nil {
		return nil, fmt.Errorf("look up records of %s: %w", tableName, err)
	}
	return found, nil
}

// ReadPending returns the queued follow-ups up to and including a generation,
// and the highest generation present in the queue.
func ReadPending(ctx context.Context, path string, throughGeneration int) (pending []PendingChange, highest int, err error) {
	db, err := openDatabase(path, "READ_ONLY")
	if err != nil {
		return nil, 0, err
	}
	defer db.Close()

	exists, err := tableExists(ctx, db, "_delta_pending")
	if err != nil || !exists {
		return nil, 0, err
	}
	var highestGeneration sql.NullInt64
	if err := db.QueryRowContext(ctx, `SELECT max(generation) FROM _delta_pending`).Scan(&highestGeneration); err != nil {
		return nil, 0, fmt.Errorf("read follow-up queue: %w", err)
	}
	rows, err := db.QueryContext(ctx,
		`SELECT DISTINCT generation, source_table, kind, column_name, record_id FROM _delta_pending WHERE generation <= ? ORDER BY ALL`,
		throughGeneration,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("read follow-up queue: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var change PendingChange
		var column sql.NullString
		if err := rows.Scan(&change.Generation, &change.SourceTable, &change.Kind, &column, &change.RecordID); err != nil {
			return nil, 0, fmt.Errorf("scan follow-up: %w", err)
		}
		change.Column = column.String
		pending = append(pending, change)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate follow-up queue: %w", err)
	}
	return pending, int(highestGeneration.Int64), nil
}

// ClearPending removes queued follow-ups up to and including a generation.
func ClearPending(ctx context.Context, path string, throughGeneration int) error {
	if !DatabaseFileExists(path) {
		return fmt.Errorf("cache file %s is gone", filepath.Base(path))
	}
	db, err := openDatabase(path, "READ_WRITE")
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, pendingTableDDL); err != nil {
		return fmt.Errorf("create follow-up queue: %w", err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM _delta_pending WHERE generation <= ?`, throughGeneration); err != nil {
		return fmt.Errorf("clear follow-up queue: %w", err)
	}
	return nil
}
