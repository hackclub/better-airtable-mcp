package approval

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/hackclub/better-airtable-mcp/internal/cryptoutil"
	"github.com/hackclub/better-airtable-mcp/internal/db"
	"github.com/hackclub/better-airtable-mcp/internal/duckdb"
	"github.com/hackclub/better-airtable-mcp/internal/logx"
	syncer "github.com/hackclub/better-airtable-mcp/internal/sync"
)

type AirtableWriter interface {
	CreateRecords(ctx context.Context, accessToken, baseID, tableID string, records []syncer.MutationRecord) ([]syncer.Record, error)
	UpdateRecords(ctx context.Context, accessToken, baseID, tableID string, records []syncer.MutationRecord) ([]syncer.Record, error)
	DeleteRecords(ctx context.Context, accessToken, baseID, tableID string, recordIDs []string) ([]string, error)
}

type Service struct {
	store       *db.Store
	cipher      *cryptoutil.Cipher
	syncer      *syncer.Service
	syncManager *syncer.Manager
	tokens      syncer.TokenSource
	writer      AirtableWriter
	baseURL     string
	ttl         time.Duration
	now         func() time.Time
}

type MutationRequest struct {
	Base       string
	SessionID  string
	ClientID   string
	ClientName string
	Operations []MutationOperation
}

type MutationOperation struct {
	Type    string
	Table   string
	Records []MutationRecord
}

type MutationRecord struct {
	ID     string
	Fields map[string]any
}

type PreparedMutation struct {
	OperationID string
	Status      string
	ApprovalURL string
	ExpiresAt   time.Time
	Summary     string
}

type RecordsNotSyncedError struct {
	BaseID    string
	BaseName  string
	Table     string
	RecordIDs []string
	Sync      syncer.SyncOperationStatus
}

func (e RecordsNotSyncedError) Error() string {
	return fmt.Sprintf("records are not synced yet for table %q: %s", e.Table, strings.Join(e.RecordIDs, ", "))
}

type OperationView struct {
	OperationID             string             `json:"operation_id"`
	Status                  string             `json:"status"`
	ApprovalURL             string             `json:"approval_url"`
	BaseID                  string             `json:"base_id"`
	BaseName                string             `json:"base_name"`
	MCPSessionID            string             `json:"mcp_session_id,omitempty"`
	MCPClientID             string             `json:"mcp_client_id,omitempty"`
	MCPClientName           string             `json:"mcp_client_name,omitempty"`
	Summary                 string             `json:"summary"`
	CreatedAt               time.Time          `json:"created_at"`
	ExpiresAt               time.Time          `json:"expires_at"`
	ResolvedAt              *time.Time         `json:"resolved_at,omitempty"`
	LastSyncedAt            time.Time          `json:"last_synced_at"`
	Operations              []OperationPreview `json:"operations"`
	Result                  *ExecutionResult   `json:"result,omitempty"`
	Error                   string             `json:"error,omitempty"`
	ApprovalURLIsCredential bool               `json:"approval_url_is_credential"`
	PreviewIsSnapshot       bool               `json:"preview_is_snapshot"`
	CanApprove              bool               `json:"can_approve"`
	CanReject               bool               `json:"can_reject"`
}

type OperationPreview struct {
	Type              string                   `json:"type"`
	Table             string                   `json:"table"`
	OriginalTableName string                   `json:"original_table_name"`
	Records           []OperationPreviewRecord `json:"records"`
}

type OperationPreviewRecord struct {
	ID            string         `json:"id,omitempty"`
	Fields        map[string]any `json:"fields,omitempty"`
	CurrentFields map[string]any `json:"current_fields,omitempty"`
}

type ExecutionResult struct {
	CreatedRecordIDs []string `json:"created_record_ids,omitempty"`
	UpdatedRecordIDs []string `json:"updated_record_ids,omitempty"`
	DeletedRecordIDs []string `json:"deleted_record_ids,omitempty"`
	CompletedBatches int      `json:"completed_batches"`
	FailedBatch      *int     `json:"failed_batch,omitempty"`
}

type pendingPayload struct {
	BaseID        string                    `json:"base_id"`
	BaseName      string                    `json:"base_name"`
	MCPSessionID  string                    `json:"mcp_session_id,omitempty"`
	MCPClientID   string                    `json:"mcp_client_id,omitempty"`
	MCPClientName string                    `json:"mcp_client_name,omitempty"`
	LastSyncedAt  time.Time                 `json:"last_synced_at"`
	Summary       string                    `json:"summary"`
	Operations    []pendingPayloadOperation `json:"operations"`
}

type pendingPayloadOperation struct {
	Type              string                 `json:"type"`
	Table             string                 `json:"table"`
	OriginalTableName string                 `json:"original_table_name"`
	AirtableTableID   string                 `json:"airtable_table_id"`
	Records           []pendingPayloadRecord `json:"records"`
}

type pendingPayloadRecord struct {
	ID             string         `json:"id,omitempty"`
	Fields         map[string]any `json:"fields,omitempty"`
	AirtableFields map[string]any `json:"airtable_fields,omitempty"`
}

type currentValuesSnapshot map[string]map[string]map[string]any

func NewService(store *db.Store, cipher *cryptoutil.Cipher, syncService *syncer.Service, syncManager *syncer.Manager, tokens syncer.TokenSource, writer AirtableWriter, baseURL string, ttl time.Duration) *Service {
	if writer == nil {
		writer = syncer.NewHTTPClient("", nil)
	}
	return &Service{
		store:       store,
		cipher:      cipher,
		syncer:      syncService,
		syncManager: syncManager,
		tokens:      tokens,
		writer:      writer,
		baseURL:     strings.TrimRight(baseURL, "/"),
		ttl:         ttl,
		now:         time.Now,
	}
}

func (s *Service) PrepareMutation(ctx context.Context, userID string, request MutationRequest) (PreparedMutation, error) {
	if s == nil || s.store == nil || s.cipher == nil || s.syncer == nil || s.tokens == nil {
		return PreparedMutation{}, fmt.Errorf("approval service is not configured")
	}

	logx.Event(ctx, "approval", "approval.prepare_started",
		"user_id", userID,
		"base_ref_hash", logx.HashString(strings.TrimSpace(request.Base)),
		"operation_count", len(request.Operations),
	)
	fail := func(err error) (PreparedMutation, error) {
		if err != nil {
			attrs := []any{
				"user_id", userID,
				"base_ref_hash", logx.HashString(strings.TrimSpace(request.Base)),
				"operation_count", len(request.Operations),
				"error_kind", logx.ErrorKind(err),
				"error_message", logx.ErrorPreview(err),
			}
			var notReady RecordsNotSyncedError
			if errors.As(err, &notReady) {
				attrs = append(attrs,
					"base_id", notReady.BaseID,
					"table_hash", logx.HashString(notReady.Table),
					"record_count", len(notReady.RecordIDs),
				)
			}
			logx.Event(ctx, "approval", "approval.prepare_failed", attrs...)
		}
		return PreparedMutation{}, err
	}

	accessToken, err := s.tokens.AirtableAccessToken(ctx, userID)
	if err != nil {
		return fail(err)
	}

	var base syncer.Base
	if s.syncManager != nil {
		base, err = s.syncManager.EnsureBaseReadable(ctx, userID, request.Base)
		if err != nil {
			return fail(err)
		}
	} else {
		return fail(fmt.Errorf("sync manager is not configured"))
	}

	schema, err := s.syncer.ListSchema(ctx, accessToken, base.ID)
	if err != nil {
		return fail(err)
	}
	syncStatus, _ := s.syncManager.BaseStatus(base.ID)

	payload := pendingPayload{
		BaseID:        schema.BaseID,
		BaseName:      schema.BaseName,
		MCPSessionID:  strings.TrimSpace(request.SessionID),
		MCPClientID:   strings.TrimSpace(request.ClientID),
		MCPClientName: strings.TrimSpace(request.ClientName),
		LastSyncedAt:  schema.LastSyncedAt.UTC(),
	}
	currentValues := currentValuesSnapshot{}

	for _, operation := range request.Operations {
		table, tableNames, ok := resolveTableSchema(schema.Tables, operation.Table)
		if !ok {
			return fail(fmt.Errorf("unknown table %q; available tables: %s", operation.Table, strings.Join(suggestions(operation.Table, tableNames), ", ")))
		}

		fieldNames := collectFieldAliases(table.Fields)

		resolved := pendingPayloadOperation{
			Type:              operation.Type,
			Table:             table.DuckDBTableName,
			OriginalTableName: table.OriginalName,
			AirtableTableID:   table.AirtableTableID,
			Records:           make([]pendingPayloadRecord, 0, len(operation.Records)),
		}

		recordIDs := make([]string, 0, len(operation.Records))
		for _, record := range operation.Records {
			if record.ID != "" {
				recordIDs = append(recordIDs, record.ID)
			}
		}

		currentRows := map[string]map[string]any{}
		if len(recordIDs) > 0 {
			rows, err := s.syncer.ReadTableRowsByIDs(ctx, base.ID, table.DuckDBTableName, recordIDs)
			if err != nil {
				return fail(err)
			}
			for _, row := range rows {
				id, _ := row["id"].(string)
				currentRows[id] = row
			}
			missingRecordIDs := make([]string, 0)
			for _, recordID := range recordIDs {
				if _, ok := currentRows[recordID]; !ok {
					missingRecordIDs = append(missingRecordIDs, recordID)
				}
			}
			if len(missingRecordIDs) > 0 {
				if !table.TableComplete {
					return fail(RecordsNotSyncedError{
						BaseID:    base.ID,
						BaseName:  base.Name,
						Table:     table.DuckDBTableName,
						RecordIDs: missingRecordIDs,
						Sync:      syncStatus,
					})
				}
				return fail(fmt.Errorf("record %q was not found in table %q", missingRecordIDs[0], table.DuckDBTableName))
			}
			if currentValues[table.DuckDBTableName] == nil {
				currentValues[table.DuckDBTableName] = map[string]map[string]any{}
			}
			for recordID, row := range currentRows {
				currentValues[table.DuckDBTableName][recordID] = row
			}
		}

		for _, record := range operation.Records {
			resolvedRecord := pendingPayloadRecord{
				ID:             record.ID,
				Fields:         cloneMap(record.Fields),
				AirtableFields: map[string]any{},
			}
			resolvedFieldKeys := make(map[string]string, len(record.Fields))

			for fieldName, value := range record.Fields {
				field, ok := resolveFieldSchema(table.Fields, fieldName)
				if !ok {
					return fail(fmt.Errorf("unknown field %q on table %q; available fields: %s", fieldName, table.DuckDBTableName, strings.Join(suggestions(fieldName, fieldNames), ", ")))
				}
				fieldKey := resolvedFieldIdentity(field)
				if priorName, exists := resolvedFieldKeys[fieldKey]; exists {
					return fail(fmt.Errorf("field %q duplicates %q on table %q; use one reference per Airtable field", fieldName, priorName, table.DuckDBTableName))
				}
				resolvedFieldKeys[fieldKey] = fieldName
				resolvedRecord.AirtableFields[field.OriginalName] = value
			}

			resolved.Records = append(resolved.Records, resolvedRecord)
		}

		payload.Operations = append(payload.Operations, resolved)
	}

	payload.Summary = summarizeOperations(payload.Operations)
	payloadCiphertext, err := s.encryptJSON(payload)
	if err != nil {
		return fail(err)
	}
	currentValuesCiphertext, err := s.encryptJSON(currentValues)
	if err != nil {
		return fail(err)
	}

	operationID, err := newOperationID()
	if err != nil {
		return fail(err)
	}
	expiresAt := s.now().Add(s.ttl).UTC()
	createdAt := s.now().UTC()

	if err := s.store.PutPendingOperation(ctx, db.PendingOperation{
		ID:                      operationID,
		UserID:                  userID,
		BaseID:                  payload.BaseID,
		Status:                  "pending_approval",
		OperationType:           "record_mutation",
		PayloadCiphertext:       payloadCiphertext,
		CurrentValuesCiphertext: currentValuesCiphertext,
		CreatedAt:               createdAt,
		ExpiresAt:               expiresAt,
	}); err != nil {
		return fail(err)
	}

	prepared := PreparedMutation{
		OperationID: operationID,
		Status:      "pending_approval",
		ApprovalURL: s.approvalURL(operationID),
		ExpiresAt:   expiresAt,
		Summary:     payload.Summary,
	}
	logx.Event(ctx, "approval", "approval.prepare_completed",
		"user_id", userID,
		"approval_operation_id_hash", logx.ApprovalOperationIDHash(operationID),
		"base_id", payload.BaseID,
		"operation_count", len(payload.Operations),
		"status", prepared.Status,
	)
	return prepared, nil
}

func (s *Service) GetOperation(ctx context.Context, operationID string) (OperationView, error) {
	operation, err := s.store.GetPendingOperation(ctx, operationID)
	if err != nil {
		return OperationView{}, err
	}
	if operation.Status == "pending_approval" && s.now().After(operation.ExpiresAt) {
		if err := s.expireOperation(ctx, operation.ID); err != nil {
			return OperationView{}, err
		}
		operation, err = s.store.GetPendingOperation(ctx, operationID)
		if err != nil {
			return OperationView{}, err
		}
	}

	payload, err := decryptJSON[pendingPayload](s.cipher, operation.PayloadCiphertext)
	if err != nil {
		return OperationView{}, err
	}
	currentValues, err := decryptJSON[currentValuesSnapshot](s.cipher, operation.CurrentValuesCiphertext)
	if err != nil {
		return OperationView{}, err
	}

	view := OperationView{
		OperationID:             operation.ID,
		Status:                  operation.Status,
		ApprovalURL:             s.approvalURL(operation.ID),
		BaseID:                  payload.BaseID,
		BaseName:                payload.BaseName,
		MCPSessionID:            payload.MCPSessionID,
		MCPClientID:             payload.MCPClientID,
		MCPClientName:           payload.MCPClientName,
		Summary:                 payload.Summary,
		CreatedAt:               operation.CreatedAt.UTC(),
		ExpiresAt:               operation.ExpiresAt.UTC(),
		ResolvedAt:              operation.ResolvedAt,
		LastSyncedAt:            payload.LastSyncedAt.UTC(),
		ApprovalURLIsCredential: true,
		PreviewIsSnapshot:       true,
		CanApprove:              operation.Status == "pending_approval",
		CanReject:               operation.Status == "pending_approval",
	}
	if operation.Error != nil {
		view.Error = *operation.Error
	}

	if len(operation.ResultCiphertext) > 0 {
		result, err := decryptJSON[ExecutionResult](s.cipher, operation.ResultCiphertext)
		if err != nil {
			return OperationView{}, err
		}
		view.Result = &result
	}

	for _, payloadOperation := range payload.Operations {
		preview := OperationPreview{
			Type:              payloadOperation.Type,
			Table:             payloadOperation.Table,
			OriginalTableName: payloadOperation.OriginalTableName,
			Records:           make([]OperationPreviewRecord, 0, len(payloadOperation.Records)),
		}
		for _, record := range payloadOperation.Records {
			currentFields := currentValues[payloadOperation.Table][record.ID]
			preview.Records = append(preview.Records, OperationPreviewRecord{
				ID:            record.ID,
				Fields:        cloneMap(record.Fields),
				CurrentFields: cloneMap(currentFields),
			})
		}
		view.Operations = append(view.Operations, preview)
	}

	logx.Event(ctx, "approval", "approval.view_loaded",
		"approval_operation_id_hash", logx.ApprovalOperationIDHash(operationID),
		"base_id", payload.BaseID,
		"status", view.Status,
		"has_result", view.Result != nil,
		"has_error", view.Error != "",
	)
	return view, nil
}

func (s *Service) Approve(ctx context.Context, operationID string) (OperationView, error) {
	operation, err := s.store.GetPendingOperation(ctx, operationID)
	if err != nil {
		return OperationView{}, err
	}
	if operation.Status == "pending_approval" && s.now().After(operation.ExpiresAt) {
		if err := s.expireOperation(ctx, operation.ID); err != nil {
			return OperationView{}, err
		}
		return s.GetOperation(ctx, operationID)
	}
	if operation.Status != "pending_approval" {
		return s.GetOperation(ctx, operationID)
	}

	logx.Event(ctx, "approval", "approval.approved",
		"approval_operation_id_hash", logx.ApprovalOperationIDHash(operationID),
		"status", operation.Status,
	)
	if err := s.store.UpdatePendingOperationStatus(ctx, operation.ID, "executing", nil, nil, nil); err != nil {
		return OperationView{}, err
	}

	payload, err := decryptJSON[pendingPayload](s.cipher, operation.PayloadCiphertext)
	if err != nil {
		return OperationView{}, err
	}

	result, status, errText := s.execute(ctx, operation.UserID, operation.ID, payload)
	resultCiphertext, err := s.encryptJSON(result)
	if err != nil {
		return OperationView{}, err
	}
	resolvedAt := s.now().UTC()

	var errorPtr *string
	if errText != "" {
		errorPtr = &errText
	}
	if err := s.store.UpdatePendingOperationStatus(ctx, operation.ID, status, resultCiphertext, errorPtr, &resolvedAt); err != nil {
		return OperationView{}, err
	}
	attrs := []any{
		"approval_operation_id_hash", logx.ApprovalOperationIDHash(operation.ID),
		"base_id", payload.BaseID,
		"status", status,
		"completed_batches", result.CompletedBatches,
		"created_record_count", len(result.CreatedRecordIDs),
		"updated_record_count", len(result.UpdatedRecordIDs),
		"deleted_record_count", len(result.DeletedRecordIDs),
		"failed_batch", valueOrZero(result.FailedBatch),
		"has_error", errText != "",
	}
	if errText != "" {
		attrs = append(attrs,
			"error_kind", logx.ErrorKind(errors.New(errText)),
			"error_message", logx.Truncate(logx.RedactString(errText), logx.MaxErrorPreviewLength),
		)
	}
	logx.Event(ctx, "approval", "approval.execute_completed", attrs...)

	if s.syncManager != nil {
		_ = s.syncManager.TriggerSync(ctx, operation.UserID, payload.BaseID)
	}

	return s.GetOperation(ctx, operationID)
}

func (s *Service) Reject(ctx context.Context, operationID string) (OperationView, error) {
	operation, err := s.store.GetPendingOperation(ctx, operationID)
	if err != nil {
		return OperationView{}, err
	}
	if operation.Status == "pending_approval" && s.now().After(operation.ExpiresAt) {
		if err := s.expireOperation(ctx, operation.ID); err != nil {
			return OperationView{}, err
		}
		return s.GetOperation(ctx, operationID)
	}
	if operation.Status != "pending_approval" {
		return s.GetOperation(ctx, operationID)
	}

	resolvedAt := s.now().UTC()
	if err := s.store.UpdatePendingOperationStatus(ctx, operationID, "rejected", nil, nil, &resolvedAt); err != nil {
		return OperationView{}, err
	}
	logx.Event(ctx, "approval", "approval.rejected",
		"approval_operation_id_hash", logx.ApprovalOperationIDHash(operationID),
	)
	return s.GetOperation(ctx, operationID)
}

func (s *Service) RunExpiryLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			expired, err := s.store.ExpirePendingOperations(ctx, s.now().UTC())
			if err != nil {
				logx.Event(ctx, "approval", "approval.expiry_loop_failed",
					"error_kind", logx.ErrorKind(err),
					"error_message", logx.ErrorPreview(err),
				)
				continue
			}
			if expired > 0 {
				logx.Event(ctx, "approval", "approval.expiry_loop",
					"expired_operations", expired,
				)
			}
		}
	}
}

func (s *Service) execute(ctx context.Context, userID, operationID string, payload pendingPayload) (ExecutionResult, string, string) {
	result := ExecutionResult{}
	logx.Event(ctx, "approval", "approval.execute_started",
		"approval_operation_id_hash", logx.ApprovalOperationIDHash(operationID),
		"user_id", userID,
		"base_id", payload.BaseID,
		"operation_count", len(payload.Operations),
	)
	accessToken, err := s.tokens.AirtableAccessToken(ctx, userID)
	if err != nil {
		logx.Event(ctx, "approval", "approval.execute_failed",
			"approval_operation_id_hash", logx.ApprovalOperationIDHash(operationID),
			"base_id", payload.BaseID,
			"error_kind", logx.ErrorKind(err),
			"error_message", logx.ErrorPreview(err),
		)
		return result, "failed", err.Error()
	}

	globalBatch := 0
	for _, operation := range payload.Operations {
		for batchStart := 0; batchStart < len(operation.Records); batchStart += 10 {
			globalBatch++
			batchEnd := batchStart + 10
			if batchEnd > len(operation.Records) {
				batchEnd = len(operation.Records)
			}
			batch := operation.Records[batchStart:batchEnd]

			switch operation.Type {
			case "create_records":
				records := make([]syncer.MutationRecord, 0, len(batch))
				for _, record := range batch {
					records = append(records, syncer.MutationRecord{Fields: record.AirtableFields})
				}
				created, err := s.writer.CreateRecords(ctx, accessToken, payload.BaseID, operation.AirtableTableID, records)
				if err != nil {
					logx.Event(ctx, "approval", "approval.execute_batch",
						"approval_operation_id_hash", logx.ApprovalOperationIDHash(operationID),
						"base_id", payload.BaseID,
						"table_id", operation.AirtableTableID,
						"mutation_type", operation.Type,
						"batch_index", globalBatch,
						"batch_size", len(batch),
						"outcome", "failed",
						"error_kind", logx.ErrorKind(err),
						"error_message", logx.ErrorPreview(err),
					)
					result.FailedBatch = intPtr(globalBatch)
					if result.CompletedBatches == 0 {
						return result, "failed", err.Error()
					}
					return result, "partially_completed", err.Error()
				}
				for _, record := range created {
					result.CreatedRecordIDs = append(result.CreatedRecordIDs, record.ID)
				}
			case "update_records":
				records := make([]syncer.MutationRecord, 0, len(batch))
				for _, record := range batch {
					records = append(records, syncer.MutationRecord{ID: record.ID, Fields: record.AirtableFields})
				}
				updated, err := s.writer.UpdateRecords(ctx, accessToken, payload.BaseID, operation.AirtableTableID, records)
				if err != nil {
					logx.Event(ctx, "approval", "approval.execute_batch",
						"approval_operation_id_hash", logx.ApprovalOperationIDHash(operationID),
						"base_id", payload.BaseID,
						"table_id", operation.AirtableTableID,
						"mutation_type", operation.Type,
						"batch_index", globalBatch,
						"batch_size", len(batch),
						"outcome", "failed",
						"error_kind", logx.ErrorKind(err),
						"error_message", logx.ErrorPreview(err),
					)
					result.FailedBatch = intPtr(globalBatch)
					if result.CompletedBatches == 0 {
						return result, "failed", err.Error()
					}
					return result, "partially_completed", err.Error()
				}
				for _, record := range updated {
					result.UpdatedRecordIDs = append(result.UpdatedRecordIDs, record.ID)
				}
			case "delete_records":
				recordIDs := make([]string, 0, len(batch))
				for _, record := range batch {
					recordIDs = append(recordIDs, record.ID)
				}
				deleted, err := s.writer.DeleteRecords(ctx, accessToken, payload.BaseID, operation.AirtableTableID, recordIDs)
				if err != nil {
					logx.Event(ctx, "approval", "approval.execute_batch",
						"approval_operation_id_hash", logx.ApprovalOperationIDHash(operationID),
						"base_id", payload.BaseID,
						"table_id", operation.AirtableTableID,
						"mutation_type", operation.Type,
						"batch_index", globalBatch,
						"batch_size", len(batch),
						"outcome", "failed",
						"error_kind", logx.ErrorKind(err),
						"error_message", logx.ErrorPreview(err),
					)
					result.FailedBatch = intPtr(globalBatch)
					if result.CompletedBatches == 0 {
						return result, "failed", err.Error()
					}
					return result, "partially_completed", err.Error()
				}
				result.DeletedRecordIDs = append(result.DeletedRecordIDs, deleted...)
			default:
				return result, "failed", fmt.Sprintf("unsupported mutation type %q", operation.Type)
			}

			result.CompletedBatches++
			logx.Event(ctx, "approval", "approval.execute_batch",
				"approval_operation_id_hash", logx.ApprovalOperationIDHash(operationID),
				"base_id", payload.BaseID,
				"table_id", operation.AirtableTableID,
				"mutation_type", operation.Type,
				"batch_index", globalBatch,
				"batch_size", len(batch),
				"completed_batches", result.CompletedBatches,
				"outcome", "completed",
			)
		}
	}

	return result, "completed", ""
}

func (s *Service) approvalURL(operationID string) string {
	return s.baseURL + "/approve/" + operationID
}

func (s *Service) expireOperation(ctx context.Context, operationID string) error {
	resolvedAt := s.now().UTC()
	if err := s.store.UpdatePendingOperationStatus(ctx, operationID, "expired", nil, nil, &resolvedAt); err != nil {
		return err
	}
	logx.Event(ctx, "approval", "approval.expired",
		"approval_operation_id_hash", logx.ApprovalOperationIDHash(operationID),
	)
	return nil
}

func (s *Service) encryptJSON(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal encrypted payload: %w", err)
	}
	ciphertext, err := s.cipher.Encrypt(data)
	if err != nil {
		return nil, fmt.Errorf("encrypt payload: %w", err)
	}
	return ciphertext, nil
}

func decryptJSON[T any](cipher *cryptoutil.Cipher, ciphertext []byte) (T, error) {
	var zero T
	if len(ciphertext) == 0 {
		return zero, nil
	}
	plaintext, err := cipher.Decrypt(ciphertext)
	if err != nil {
		return zero, fmt.Errorf("decrypt payload: %w", err)
	}
	var value T
	if err := json.Unmarshal(plaintext, &value); err != nil {
		return zero, fmt.Errorf("decode payload json: %w", err)
	}
	return value, nil
}

func summarizeOperations(operations []pendingPayloadOperation) string {
	if len(operations) == 0 {
		return "No operations"
	}
	if len(operations) == 1 {
		operation := operations[0]
		return fmt.Sprintf("%s %d record(s) in %s", summarizeVerb(operation.Type), len(operation.Records), operation.Table)
	}

	parts := make([]string, 0, len(operations))
	for _, operation := range operations {
		parts = append(parts, fmt.Sprintf("%s %d", strings.ToLower(summarizeVerb(operation.Type)), len(operation.Records)))
	}
	return capitalizeFirst(strings.Join(parts, ", ")) + fmt.Sprintf(" across %d table(s)", distinctTableCount(operations))
}

func summarizeVerb(operationType string) string {
	switch operationType {
	case "create_records":
		return "Create"
	case "update_records":
		return "Update"
	case "delete_records":
		return "Delete"
	default:
		return "Mutate"
	}
}

func distinctTableCount(operations []pendingPayloadOperation) int {
	seen := map[string]struct{}{}
	for _, operation := range operations {
		seen[operation.Table] = struct{}{}
	}
	return len(seen)
}

func resolveTableSchema(tables []duckdb.TableSchema, requested string) (duckdb.TableSchema, []string, bool) {
	lookup := make(map[string]duckdb.TableSchema, len(tables)*3)
	aliases := make([]string, 0, len(tables)*3)
	for _, table := range tables {
		for _, alias := range tableAliases(table) {
			aliases = append(aliases, alias)
			key := normalizedAlias(alias)
			if key == "" {
				continue
			}
			if _, exists := lookup[key]; !exists {
				lookup[key] = table
			}
		}
	}

	table, ok := lookup[normalizedAlias(requested)]
	return table, uniqueNonEmptyStrings(aliases...), ok
}

func resolveFieldSchema(fields []duckdb.FieldSchema, requested string) (duckdb.FieldSchema, bool) {
	lookup := make(map[string]duckdb.FieldSchema, len(fields)*3)
	for _, field := range fields {
		for _, alias := range fieldAliases(field) {
			key := normalizedAlias(alias)
			if key == "" {
				continue
			}
			if _, exists := lookup[key]; !exists {
				lookup[key] = field
			}
		}
	}

	field, ok := lookup[normalizedAlias(requested)]
	return field, ok
}

func collectFieldAliases(fields []duckdb.FieldSchema) []string {
	aliases := make([]string, 0, len(fields)*3)
	for _, field := range fields {
		aliases = append(aliases, fieldAliases(field)...)
	}
	return uniqueNonEmptyStrings(aliases...)
}

func tableAliases(table duckdb.TableSchema) []string {
	return uniqueNonEmptyStrings(table.DuckDBTableName, table.OriginalName, table.AirtableTableID)
}

func fieldAliases(field duckdb.FieldSchema) []string {
	return uniqueNonEmptyStrings(field.DuckDBColumnName, field.OriginalName, field.AirtableFieldID)
}

func resolvedFieldIdentity(field duckdb.FieldSchema) string {
	switch {
	case strings.TrimSpace(field.AirtableFieldID) != "":
		return "id:" + normalizedAlias(field.AirtableFieldID)
	case strings.TrimSpace(field.OriginalName) != "":
		return "name:" + normalizedAlias(field.OriginalName)
	default:
		return "duck:" + normalizedAlias(field.DuckDBColumnName)
	}
}

func normalizedAlias(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func uniqueNonEmptyStrings(values ...string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		key := normalizedAlias(value)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, value)
	}
	return result
}

func suggestions(target string, candidates []string) []string {
	if len(candidates) == 0 {
		return nil
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		return suggestionScore(target, candidates[i]) < suggestionScore(target, candidates[j])
	})
	if len(candidates) > 5 {
		candidates = candidates[:5]
	}
	return candidates
}

func suggestionScore(target, candidate string) int {
	target = strings.ToLower(target)
	candidate = strings.ToLower(candidate)
	if target == candidate {
		return 0
	}
	if strings.HasPrefix(candidate, target) || strings.HasPrefix(target, candidate) {
		return 1
	}
	if strings.Contains(candidate, target) || strings.Contains(target, candidate) {
		return 2
	}
	return levenshtein(target, candidate) + 3
}

func levenshtein(a, b string) int {
	if a == b {
		return 0
	}
	if a == "" {
		return len(b)
	}
	if b == "" {
		return len(a)
	}

	prev := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}

	for i := 1; i <= len(a); i++ {
		current := make([]int, len(b)+1)
		current[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 0
			if a[i-1] != b[j-1] {
				cost = 1
			}
			current[j] = minInt(
				current[j-1]+1,
				prev[j]+1,
				prev[j-1]+cost,
			)
		}
		prev = current
	}

	return prev[len(b)]
}

func cloneMap(input map[string]any) map[string]any {
	if len(input) == 0 {
		return nil
	}
	cloned := make(map[string]any, len(input))
	for key, value := range input {
		cloned[key] = value
	}
	return cloned
}

func newOperationID() (string, error) {
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("generate operation id: %w", err)
	}
	return "op_" + hex.EncodeToString(random), nil
}

func valueOrZero(value *int) int {
	if value == nil {
		return 0
	}
	return *value
}

func intPtr(value int) *int {
	return &value
}

func minInt(values ...int) int {
	best := values[0]
	for _, value := range values[1:] {
		if value < best {
			best = value
		}
	}
	return best
}

func capitalizeFirst(value string) string {
	if value == "" {
		return value
	}
	return strings.ToUpper(value[:1]) + value[1:]
}
