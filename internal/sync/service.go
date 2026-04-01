package syncer

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/hackclub/better-airtable-mcp/internal/duckdb"
)

type Service struct {
	client  Client
	dataDir string

	mu        sync.Mutex
	syncLocks map[string]*sync.Mutex
	duckLocks map[string]*sync.RWMutex
}

type SyncResult struct {
	BaseID        string
	BaseName      string
	LastSyncedAt  time.Time
	TablesSynced  int
	RecordsSynced int
	SyncDuration  time.Duration
}

func NewService(client Client, dataDir string) *Service {
	return &Service{
		client:    client,
		dataDir:   dataDir,
		syncLocks: make(map[string]*sync.Mutex),
		duckLocks: make(map[string]*sync.RWMutex),
	}
}

func (s *Service) SearchBases(ctx context.Context, accessToken, query string) ([]Base, error) {
	bases, err := s.client.ListBases(ctx, accessToken)
	if err != nil {
		return nil, err
	}

	query = strings.TrimSpace(strings.ToLower(query))
	if query == "" {
		return bases, nil
	}

	filtered := make([]Base, 0, len(bases))
	for _, base := range bases {
		if strings.Contains(strings.ToLower(base.Name), query) {
			filtered = append(filtered, base)
		}
	}

	return filtered, nil
}

func (s *Service) SyncBase(ctx context.Context, accessToken, baseRef string) (SyncResult, error) {
	base, err := s.resolveBase(ctx, accessToken, baseRef)
	if err != nil {
		return SyncResult{}, err
	}

	unlockSync := s.lockSync(base.ID)
	defer unlockSync()

	startedAt := time.Now()
	tables, err := s.client.GetBaseSchema(ctx, accessToken, base.ID)
	if err != nil {
		return SyncResult{}, err
	}

	tableNames := make([]string, 0, len(tables))
	for _, table := range tables {
		tableNames = append(tableNames, table.Name)
	}
	sanitizedTableNames := duckdb.SanitizeIdentifiers(tableNames)

	snapshotTables := make([]duckdb.TableSnapshot, 0, len(tables))
	totalRecords := 0

	for tableIndex, table := range tables {
		fieldNames := make([]string, 0, len(table.Fields))
		for _, field := range table.Fields {
			mapping, ok := duckdb.AirtableTypeToDuckDBType(field.Type)
			if !ok || mapping.Omitted {
				continue
			}
			fieldNames = append(fieldNames, field.Name)
		}

		sanitizedFieldNames := duckdb.SanitizeIdentifiers(fieldNames)
		fieldNameMap := make(map[string]string, len(sanitizedFieldNames))
		for index, sanitized := range sanitizedFieldNames {
			fieldNameMap[fieldNames[index]] = sanitized.Sanitized
		}

		records, err := s.client.ListRecords(ctx, accessToken, base.ID, table.ID)
		if err != nil {
			return SyncResult{}, err
		}
		totalRecords += len(records)

		fields := make([]duckdb.FieldSnapshot, 0, len(fieldNames))
		for _, field := range table.Fields {
			mapping, ok := duckdb.AirtableTypeToDuckDBType(field.Type)
			if !ok || mapping.Omitted {
				continue
			}

			fields = append(fields, duckdb.FieldSnapshot{
				AirtableFieldID:   field.ID,
				OriginalFieldName: field.Name,
				DuckDBColumnName:  fieldNameMap[field.Name],
				AirtableFieldType: field.Type,
				DuckDBType:        mapping.DuckDBType,
			})
		}

		tableSnapshot := duckdb.TableSnapshot{
			AirtableTableID: table.ID,
			OriginalName:    table.Name,
			DuckDBTableName: sanitizedTableNames[tableIndex].Sanitized,
			Fields:          fields,
			Records:         make([]duckdb.RecordSnapshot, 0, len(records)),
		}

		for _, record := range records {
			tableSnapshot.Records = append(tableSnapshot.Records, duckdb.RecordSnapshot{
				ID:          record.ID,
				CreatedTime: record.CreatedTime,
				Fields:      record.Fields,
			})
		}

		snapshotTables = append(snapshotTables, tableSnapshot)
	}

	snapshot := duckdb.BaseSnapshot{
		BaseID:       base.ID,
		BaseName:     base.Name,
		Tables:       snapshotTables,
		SyncedAt:     time.Now().UTC(),
		SyncDuration: time.Since(startedAt),
	}

	unlockDuck := s.lockDuckWrite(base.ID)
	defer unlockDuck()
	if err := duckdb.WriteSnapshot(ctx, s.basePath(base.ID), snapshot); err != nil {
		return SyncResult{}, err
	}

	return SyncResult{
		BaseID:        base.ID,
		BaseName:      base.Name,
		LastSyncedAt:  snapshot.SyncedAt,
		TablesSynced:  len(snapshotTables),
		RecordsSynced: totalRecords,
		SyncDuration:  snapshot.SyncDuration,
	}, nil
}

func (s *Service) ListSchema(ctx context.Context, accessToken, baseRef string) (duckdb.BaseSchema, error) {
	base, err := s.ensureSynced(ctx, accessToken, baseRef)
	if err != nil {
		return duckdb.BaseSchema{}, err
	}

	unlock := s.lockDuckRead(base.ID)
	defer unlock()
	return duckdb.ReadSchema(ctx, s.basePath(base.ID), base.ID, base.Name)
}

func (s *Service) QueryBase(ctx context.Context, accessToken, baseRef, query string) (duckdb.QueryResult, error) {
	base, err := s.ensureSynced(ctx, accessToken, baseRef)
	if err != nil {
		return duckdb.QueryResult{}, err
	}

	unlock := s.lockDuckRead(base.ID)
	defer unlock()
	return duckdb.Query(ctx, s.basePath(base.ID), query)
}

func (s *Service) ReadTableRowsByIDs(ctx context.Context, baseID, tableName string, ids []string) ([]map[string]any, error) {
	unlock := s.lockDuckRead(baseID)
	defer unlock()
	return duckdb.ReadTableRowsByIDs(ctx, s.basePath(baseID), tableName, ids)
}

func (s *Service) ensureSynced(ctx context.Context, accessToken, baseRef string) (Base, error) {
	base, err := s.resolveBase(ctx, accessToken, baseRef)
	if err != nil {
		return Base{}, err
	}

	if duckdb.DatabaseFileExists(s.basePath(base.ID)) {
		return base, nil
	}

	if _, err := s.SyncBase(ctx, accessToken, base.ID); err != nil {
		return Base{}, err
	}

	return base, nil
}

func (s *Service) resolveBase(ctx context.Context, accessToken, baseRef string) (Base, error) {
	baseRef = strings.TrimSpace(baseRef)
	if baseRef == "" {
		return Base{}, fmt.Errorf("base reference is required")
	}

	bases, err := s.client.ListBases(ctx, accessToken)
	if err != nil {
		return Base{}, err
	}

	for _, base := range bases {
		if base.ID == baseRef {
			return base, nil
		}
	}

	var exactMatches []Base
	for _, base := range bases {
		if strings.EqualFold(base.Name, baseRef) {
			exactMatches = append(exactMatches, base)
		}
	}
	if len(exactMatches) == 1 {
		return exactMatches[0], nil
	}
	if len(exactMatches) > 1 {
		return Base{}, fmt.Errorf("base name %q is ambiguous", baseRef)
	}

	var partialMatches []Base
	for _, base := range bases {
		if strings.Contains(strings.ToLower(base.Name), strings.ToLower(baseRef)) {
			partialMatches = append(partialMatches, base)
		}
	}
	if len(partialMatches) == 1 {
		return partialMatches[0], nil
	}
	if len(partialMatches) > 1 {
		return Base{}, fmt.Errorf("base reference %q matched multiple bases", baseRef)
	}

	return Base{}, fmt.Errorf("base %q was not found", baseRef)
}

func (s *Service) lockSync(baseID string) func() {
	s.mu.Lock()
	lock, ok := s.syncLocks[baseID]
	if !ok {
		lock = &sync.Mutex{}
		s.syncLocks[baseID] = lock
	}
	s.mu.Unlock()

	lock.Lock()
	return lock.Unlock
}

func (s *Service) lockDuckRead(baseID string) func() {
	s.mu.Lock()
	lock, ok := s.duckLocks[baseID]
	if !ok {
		lock = &sync.RWMutex{}
		s.duckLocks[baseID] = lock
	}
	s.mu.Unlock()

	lock.RLock()
	return lock.RUnlock
}

func (s *Service) lockDuckWrite(baseID string) func() {
	s.mu.Lock()
	lock, ok := s.duckLocks[baseID]
	if !ok {
		lock = &sync.RWMutex{}
		s.duckLocks[baseID] = lock
	}
	s.mu.Unlock()

	lock.Lock()
	return lock.Unlock
}

func (s *Service) basePath(baseID string) string {
	return filepath.Join(s.dataDir, baseID+".db")
}

func (s *Service) DatabasePath(baseID string) string {
	return s.basePath(baseID)
}
