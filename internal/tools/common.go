package tools

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	syncer "github.com/hackclub/better-airtable-mcp/internal/sync"
)

func decodeArgs(raw json.RawMessage, dst any) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = []byte("{}")
	}

	if err := json.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("invalid arguments: %w", err)
	}

	return nil
}

func formatTimeOrBlank(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.Format(time.RFC3339)
}

func syncStatusPayload(status syncer.SyncOperationStatus) map[string]any {
	payload := map[string]any{
		"operation_id":            status.OperationID,
		"status":                  status.Status,
		"read_snapshot":           status.ReadSnapshot,
		"tables_total":            status.TablesTotal,
		"tables_started":          status.TablesStarted,
		"tables_completed":        status.TablesSynced,
		"pages_fetched":           status.PagesFetched,
		"records_visible":         status.RecordsVisible,
		"records_synced_this_run": status.RecordsSynced,
	}
	if status.SyncStartedAt != nil {
		payload["sync_started_at"] = status.SyncStartedAt.Format(time.RFC3339)
	}
	if status.LastSyncedAt != nil {
		payload["last_synced_at"] = status.LastSyncedAt.Format(time.RFC3339)
	}
	if status.Error != "" {
		payload["error"] = status.Error
	}
	return payload
}

func formattedSyncStatusFromOperation(status syncer.SyncOperationStatus) formattedSyncStatus {
	formatted := formattedSyncStatus{
		OperationID:          status.OperationID,
		Status:               status.Status,
		ReadSnapshot:         status.ReadSnapshot,
		TablesTotal:          status.TablesTotal,
		TablesStarted:        status.TablesStarted,
		TablesCompleted:      status.TablesSynced,
		PagesFetched:         status.PagesFetched,
		RecordsVisible:       status.RecordsVisible,
		RecordsSyncedThisRun: int64(status.RecordsSynced),
		Error:                status.Error,
	}
	if status.SyncStartedAt != nil {
		formatted.SyncStartedAt = status.SyncStartedAt.Format(time.RFC3339)
	}
	if status.LastSyncedAt != nil {
		formatted.LastSyncedAt = status.LastSyncedAt.Format(time.RFC3339)
	}
	return formatted
}
