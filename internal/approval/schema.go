package approval

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/hackclub/better-airtable-mcp/internal/db"
	"github.com/hackclub/better-airtable-mcp/internal/logx"
	syncer "github.com/hackclub/better-airtable-mcp/internal/sync"
)

// schemaMetaFetchTimeout bounds the best-effort current-description lookup so a
// rate-limited base (whose client backs off for many seconds) can never hang the
// whole prepare. On timeout the old description is simply left blank.
const schemaMetaFetchTimeout = 4 * time.Second

const (
	operationTypeRecord = "record_mutation"
	operationTypeSchema = "schema_mutation"

	schemaCreateTable = "create_table"
	schemaCreateField = "create_field"
	schemaUpdateTable = "update_table"
	schemaUpdateField = "update_field"
)

// SchemaMutationRequest is the input to PrepareSchemaMutation. It only carries
// operations Airtable's meta API supports; deletion and type-change are rejected
// upstream in the manage_schema tool, never disguised as a rename here.
type SchemaMutationRequest struct {
	Base       string
	SessionID  string
	ClientID   string
	ClientName string
	Operations []SchemaMutationOp
}

type SchemaMutationOp struct {
	Type        string
	Table       string // existing table ref (create_field, update_table, update_field)
	Field       string // existing field ref (update_field)
	Name        string // new table name (create_table), new field name (create_field), rename target (update_*)
	Description string
	FieldType   string // create_field
	Options     map[string]any
	Fields      []SchemaFieldDefinition // create_table
}

type SchemaFieldDefinition struct {
	Name        string
	Type        string
	Description string
	Options     map[string]any
}

// schemaPayload is the encrypted, stored form of a pending schema mutation.
type schemaPayload struct {
	BaseID        string            `json:"base_id"`
	BaseName      string            `json:"base_name"`
	MCPSessionID  string            `json:"mcp_session_id,omitempty"`
	MCPClientID   string            `json:"mcp_client_id,omitempty"`
	MCPClientName string            `json:"mcp_client_name,omitempty"`
	Summary       string            `json:"summary"`
	Operations    []schemaPayloadOp `json:"operations"`
}

type schemaPayloadOp struct {
	Type           string                  `json:"type"`
	TableID        string                  `json:"table_id,omitempty"`
	TableName      string                  `json:"table_name,omitempty"`
	FieldID        string                  `json:"field_id,omitempty"`
	FieldName      string                  `json:"field_name,omitempty"`
	NewName        string                  `json:"new_name,omitempty"`
	Description    string                  `json:"description,omitempty"`
	OldDescription string                  `json:"old_description,omitempty"`
	FieldType      string                  `json:"field_type,omitempty"`
	Options        map[string]any          `json:"options,omitempty"`
	Fields         []schemaPayloadFieldDef `json:"fields,omitempty"`
}

type schemaPayloadFieldDef struct {
	Name        string         `json:"name"`
	Type        string         `json:"type"`
	Description string         `json:"description,omitempty"`
	Options     map[string]any `json:"options,omitempty"`
}

// SchemaOperationPreview is the per-operation view the approval UI renders.
type SchemaOperationPreview struct {
	Type           string               `json:"type"`
	TableID        string               `json:"table_id,omitempty"`
	TableName      string               `json:"table_name,omitempty"`
	FieldID        string               `json:"field_id,omitempty"`
	FieldName      string               `json:"field_name,omitempty"`
	NewName        string               `json:"new_name,omitempty"`
	Description    string               `json:"description,omitempty"`
	OldDescription string               `json:"old_description,omitempty"`
	FieldType      string               `json:"field_type,omitempty"`
	Choices        []string             `json:"choices,omitempty"`
	Fields         []SchemaFieldPreview `json:"fields,omitempty"`
}

type SchemaFieldPreview struct {
	Name        string   `json:"name"`
	Type        string   `json:"type"`
	Description string   `json:"description,omitempty"`
	Choices     []string `json:"choices,omitempty"`
}

// SchemaExecutionResult records what a schema mutation actually changed.
type SchemaExecutionResult struct {
	CompletedOperations int      `json:"completed_operations"`
	FailedOperation     *int     `json:"failed_operation,omitempty"`
	CreatedTableIDs     []string `json:"created_table_ids,omitempty"`
	CreatedFieldIDs     []string `json:"created_field_ids,omitempty"`
	UpdatedTableIDs     []string `json:"updated_table_ids,omitempty"`
	UpdatedFieldIDs     []string `json:"updated_field_ids,omitempty"`
}

// PrepareSchemaMutation validates and stores a pending schema change, resolving
// table/field references against the synced schema so execution has concrete
// Airtable IDs. It does not require a full base snapshot (schema ops carry no
// record current-values).
func (s *Service) PrepareSchemaMutation(ctx context.Context, userID string, request SchemaMutationRequest) (PreparedMutation, error) {
	if s == nil || s.store == nil || s.cipher == nil || s.syncer == nil || s.tokens == nil {
		return PreparedMutation{}, fmt.Errorf("approval service is not configured")
	}

	logx.Event(ctx, "approval", "approval.schema_prepare_started",
		"user_id", userID,
		"base_ref_hash", logx.HashString(strings.TrimSpace(request.Base)),
		"operation_count", len(request.Operations),
	)
	fail := func(err error) (PreparedMutation, error) {
		if err != nil {
			logx.Event(ctx, "approval", "approval.schema_prepare_failed",
				"user_id", userID,
				"base_ref_hash", logx.HashString(strings.TrimSpace(request.Base)),
				"error_kind", logx.ErrorKind(err),
				"error_message", logx.ErrorPreview(err),
			)
		}
		return PreparedMutation{}, err
	}

	if len(request.Operations) == 0 {
		return fail(fmt.Errorf("at least one schema operation is required"))
	}

	accessToken, err := s.tokens.AirtableAccessToken(ctx, userID)
	if err != nil {
		return fail(err)
	}
	if s.syncManager == nil {
		return fail(fmt.Errorf("sync manager is not configured"))
	}
	base, err := s.syncManager.EnsureBaseReadable(ctx, userID, request.Base)
	if err != nil {
		return fail(err)
	}
	schema, err := s.syncer.ListSchema(ctx, accessToken, base.ID)
	if err != nil {
		return fail(err)
	}

	payload := schemaPayload{
		BaseID:        schema.BaseID,
		BaseName:      schema.BaseName,
		MCPSessionID:  strings.TrimSpace(request.SessionID),
		MCPClientID:   strings.TrimSpace(request.ClientID),
		MCPClientName: strings.TrimSpace(request.ClientName),
	}

	for i, op := range request.Operations {
		switch strings.TrimSpace(op.Type) {
		case schemaCreateTable:
			if _, _, ok := resolveTableSchema(schema.Tables, op.Name); ok {
				return fail(fmt.Errorf("operations[%d]: a table named %q already exists in this base", i, op.Name))
			}
			fields := make([]schemaPayloadFieldDef, 0, len(op.Fields))
			for _, field := range op.Fields {
				fields = append(fields, schemaPayloadFieldDef{
					Name:        strings.TrimSpace(field.Name),
					Type:        strings.TrimSpace(field.Type),
					Description: field.Description,
					Options:     normalizeChoiceColors(field.Options),
				})
			}
			payload.Operations = append(payload.Operations, schemaPayloadOp{
				Type:        schemaCreateTable,
				TableName:   strings.TrimSpace(op.Name),
				Description: op.Description,
				Fields:      fields,
			})

		case schemaCreateField:
			table, names, ok := resolveTableSchema(schema.Tables, op.Table)
			if !ok {
				return fail(fmt.Errorf("operations[%d]: unknown table %q; available tables: %s", i, op.Table, strings.Join(suggestions(op.Table, names), ", ")))
			}
			if _, exists := resolveFieldSchema(table.Fields, op.Name); exists {
				return fail(fmt.Errorf("operations[%d]: a field named %q already exists on table %q", i, op.Name, table.OriginalName))
			}
			payload.Operations = append(payload.Operations, schemaPayloadOp{
				Type:        schemaCreateField,
				TableID:     table.AirtableTableID,
				TableName:   table.OriginalName,
				FieldName:   strings.TrimSpace(op.Name),
				FieldType:   strings.TrimSpace(op.FieldType),
				Options:     normalizeChoiceColors(op.Options),
				Description: op.Description,
			})

		case schemaUpdateTable:
			table, names, ok := resolveTableSchema(schema.Tables, op.Table)
			if !ok {
				return fail(fmt.Errorf("operations[%d]: unknown table %q; available tables: %s", i, op.Table, strings.Join(suggestions(op.Table, names), ", ")))
			}
			payload.Operations = append(payload.Operations, schemaPayloadOp{
				Type:        schemaUpdateTable,
				TableID:     table.AirtableTableID,
				TableName:   table.OriginalName,
				NewName:     strings.TrimSpace(op.Name),
				Description: op.Description,
			})

		case schemaUpdateField:
			table, names, ok := resolveTableSchema(schema.Tables, op.Table)
			if !ok {
				return fail(fmt.Errorf("operations[%d]: unknown table %q; available tables: %s", i, op.Table, strings.Join(suggestions(op.Table, names), ", ")))
			}
			field, ok := resolveFieldSchema(table.Fields, op.Field)
			if !ok {
				return fail(fmt.Errorf("operations[%d]: unknown field %q on table %q; available fields: %s", i, op.Field, table.OriginalName, strings.Join(suggestions(op.Field, collectFieldAliases(table.Fields)), ", ")))
			}
			payload.Operations = append(payload.Operations, schemaPayloadOp{
				Type:        schemaUpdateField,
				TableID:     table.AirtableTableID,
				TableName:   table.OriginalName,
				FieldID:     field.AirtableFieldID,
				FieldName:   field.OriginalName,
				NewName:     strings.TrimSpace(op.Name),
				Description: op.Description,
			})

		default:
			return fail(fmt.Errorf("operations[%d]: unsupported schema operation %q", i, op.Type))
		}
	}

	// For description edits, capture the current description so the preview can
	// show old -> new (and "empty" when there was none). Best-effort: a meta
	// fetch failure just leaves the old value blank.
	if schemaHasDescriptionEdit(payload.Operations) {
		metaCtx, cancel := context.WithTimeout(ctx, schemaMetaFetchTimeout)
		metaTables, metaErr := s.writer.GetBaseSchema(metaCtx, accessToken, base.ID)
		cancel()
		if metaErr == nil {
			tableDesc, fieldDesc := schemaDescriptionIndex(metaTables)
			for i := range payload.Operations {
				op := &payload.Operations[i]
				if op.Description == "" {
					continue
				}
				switch op.Type {
				case schemaUpdateTable:
					op.OldDescription = tableDesc[op.TableID]
				case schemaUpdateField:
					op.OldDescription = fieldDesc[op.FieldID]
				}
			}
		}
	}

	payload.Summary = summarizeSchemaOperations(payload.Operations)
	payloadCiphertext, err := s.encryptJSON(payload)
	if err != nil {
		return fail(err)
	}

	operationID, err := newOperationID()
	if err != nil {
		return fail(err)
	}
	createdAt := s.now().UTC()
	expiresAt := createdAt.Add(s.ttl).UTC()

	if err := s.store.PutPendingOperation(ctx, db.PendingOperation{
		ID:                operationID,
		UserID:            userID,
		BaseID:            payload.BaseID,
		Status:            "pending_approval",
		OperationType:     operationTypeSchema,
		PayloadCiphertext: payloadCiphertext,
		CreatedAt:         createdAt,
		ExpiresAt:         expiresAt,
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
	logx.Event(ctx, "approval", "approval.schema_prepare_completed",
		"user_id", userID,
		"approval_operation_id_hash", logx.ApprovalOperationIDHash(operationID),
		"base_id", payload.BaseID,
		"operation_count", len(payload.Operations),
	)
	return prepared, nil
}

func (s *Service) getSchemaOperationView(ctx context.Context, operation db.PendingOperation) (OperationView, error) {
	payload, err := decryptJSON[schemaPayload](s.cipher, operation.PayloadCiphertext)
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
		OperationType:           operation.OperationType,
		CreatedAt:               operation.CreatedAt.UTC(),
		ExpiresAt:               operation.ExpiresAt.UTC(),
		ResolvedAt:              operation.ResolvedAt,
		ApprovalURLIsCredential: true,
		PreviewIsSnapshot:       false,
		CanApprove:              operation.Status == "pending_approval",
		CanReject:               operation.Status == "pending_approval",
	}
	if operation.Error != nil {
		view.Error = *operation.Error
	}

	if len(operation.ResultCiphertext) > 0 {
		result, err := decryptJSON[SchemaExecutionResult](s.cipher, operation.ResultCiphertext)
		if err != nil {
			return OperationView{}, err
		}
		view.SchemaResult = &result
	}

	for _, op := range payload.Operations {
		preview := SchemaOperationPreview{
			Type:           op.Type,
			TableID:        op.TableID,
			TableName:      op.TableName,
			FieldID:        op.FieldID,
			FieldName:      op.FieldName,
			NewName:        op.NewName,
			Description:    op.Description,
			OldDescription: op.OldDescription,
			FieldType:      op.FieldType,
			Choices:        selectChoiceNames(op.Options),
		}
		for _, field := range op.Fields {
			preview.Fields = append(preview.Fields, SchemaFieldPreview{
				Name:        field.Name,
				Type:        field.Type,
				Description: field.Description,
				Choices:     selectChoiceNames(field.Options),
			})
		}
		view.SchemaOperations = append(view.SchemaOperations, preview)
	}

	logx.Event(ctx, "approval", "approval.schema_view_loaded",
		"approval_operation_id_hash", logx.ApprovalOperationIDHash(operation.ID),
		"base_id", payload.BaseID,
		"status", view.Status,
		"has_result", view.SchemaResult != nil,
		"has_error", view.Error != "",
	)
	return view, nil
}

func (s *Service) approveSchemaMutation(ctx context.Context, operation db.PendingOperation) (OperationView, error) {
	won, err := s.store.TransitionPendingOperationStatus(ctx, operation.ID, "pending_approval", "executing")
	if err != nil {
		return OperationView{}, err
	}
	if !won {
		// A concurrent approval already claimed this operation; do not execute again.
		return s.GetOperation(ctx, operation.ID)
	}

	payload, err := decryptJSON[schemaPayload](s.cipher, operation.PayloadCiphertext)
	if err != nil {
		return OperationView{}, err
	}

	result, status, errText := s.executeSchema(ctx, operation.UserID, operation.ID, payload)
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

	logx.Event(ctx, "approval", "approval.schema_execute_completed",
		"approval_operation_id_hash", logx.ApprovalOperationIDHash(operation.ID),
		"base_id", payload.BaseID,
		"status", status,
		"completed_operations", result.CompletedOperations,
		"failed_operation", valueOrZero(result.FailedOperation),
		"has_error", errText != "",
	)

	// A schema change rebuilds the base's structure, so re-sync the whole base
	// (a new table needs a new DuckDB table, renamed fields need new columns).
	if s.syncManager != nil {
		_ = s.syncManager.TriggerSync(ctx, operation.UserID, payload.BaseID)
	}

	return s.GetOperation(ctx, operation.ID)
}

func (s *Service) executeSchema(ctx context.Context, userID, operationID string, payload schemaPayload) (SchemaExecutionResult, string, string) {
	result := SchemaExecutionResult{}
	logx.Event(ctx, "approval", "approval.schema_execute_started",
		"approval_operation_id_hash", logx.ApprovalOperationIDHash(operationID),
		"user_id", userID,
		"base_id", payload.BaseID,
		"operation_count", len(payload.Operations),
	)

	accessToken, err := s.tokens.AirtableAccessToken(ctx, userID)
	if err != nil {
		return result, "failed", err.Error()
	}

	for i, op := range payload.Operations {
		stepErr := func(err error) (SchemaExecutionResult, string, string) {
			result.FailedOperation = intPtr(i)
			logx.Event(ctx, "approval", "approval.schema_execute_step",
				"approval_operation_id_hash", logx.ApprovalOperationIDHash(operationID),
				"base_id", payload.BaseID,
				"operation_index", i,
				"operation_type", op.Type,
				"outcome", "failed",
				"error_kind", logx.ErrorKind(err),
				"error_message", logx.ErrorPreview(err),
			)
			if result.CompletedOperations == 0 {
				return result, "failed", err.Error()
			}
			return result, "partially_completed", err.Error()
		}

		switch op.Type {
		case schemaCreateTable:
			fields := make([]syncer.FieldDefinition, 0, len(op.Fields))
			for _, field := range op.Fields {
				fields = append(fields, syncer.FieldDefinition{
					Name:        field.Name,
					Type:        field.Type,
					Description: field.Description,
					Options:     field.Options,
				})
			}
			table, err := s.writer.CreateTable(ctx, accessToken, payload.BaseID, op.TableName, op.Description, fields)
			if err != nil {
				return stepErr(err)
			}
			result.CreatedTableIDs = append(result.CreatedTableIDs, table.ID)

		case schemaCreateField:
			field, err := s.writer.CreateField(ctx, accessToken, payload.BaseID, op.TableID, syncer.FieldDefinition{
				Name:        op.FieldName,
				Type:        op.FieldType,
				Description: op.Description,
				Options:     op.Options,
			})
			if err != nil {
				return stepErr(err)
			}
			result.CreatedFieldIDs = append(result.CreatedFieldIDs, field.ID)

		case schemaUpdateTable:
			table, err := s.writer.UpdateTable(ctx, accessToken, payload.BaseID, op.TableID, op.NewName, op.Description)
			if err != nil {
				return stepErr(err)
			}
			result.UpdatedTableIDs = append(result.UpdatedTableIDs, table.ID)

		case schemaUpdateField:
			field, err := s.writer.UpdateField(ctx, accessToken, payload.BaseID, op.TableID, op.FieldID, op.NewName, op.Description)
			if err != nil {
				return stepErr(err)
			}
			result.UpdatedFieldIDs = append(result.UpdatedFieldIDs, field.ID)

		default:
			return stepErr(fmt.Errorf("unsupported schema operation %q", op.Type))
		}

		result.CompletedOperations++
		logx.Event(ctx, "approval", "approval.schema_execute_step",
			"approval_operation_id_hash", logx.ApprovalOperationIDHash(operationID),
			"base_id", payload.BaseID,
			"operation_index", i,
			"operation_type", op.Type,
			"outcome", "completed",
		)
	}

	return result, "completed", ""
}

// summarizeSchemaOperations builds the count-based headline shown at the top of
// the approval page, mirroring the record-mutation summary ("Create 3 records
// in projects"): a coarse "Add 1 field, create 1 table" with the specifics left
// to the per-operation cards below. Counts aggregate by operation kind in the
// order each kind first appears.
func summarizeSchemaOperations(operations []schemaPayloadOp) string {
	if len(operations) == 0 {
		return "No schema changes"
	}

	type tally struct {
		verb  string
		noun  string
		count int
	}
	order := make([]string, 0, 4)
	byKind := map[string]*tally{}
	kinds := map[string]struct{ verb, noun string }{
		schemaCreateTable: {"create", "table"},
		schemaCreateField: {"add", "field"},
		schemaUpdateTable: {"update", "table"},
		schemaUpdateField: {"update", "field"},
	}

	for _, op := range operations {
		kind, ok := kinds[op.Type]
		if !ok {
			continue
		}
		if _, seen := byKind[op.Type]; !seen {
			order = append(order, op.Type)
			byKind[op.Type] = &tally{verb: kind.verb, noun: kind.noun}
		}
		byKind[op.Type].count++
	}

	parts := make([]string, 0, len(order))
	for _, key := range order {
		t := byKind[key]
		parts = append(parts, fmt.Sprintf("%s %s", t.verb, pluralize(t.count, t.noun)))
	}
	return capitalizeFirst(strings.Join(parts, ", "))
}

// defaultChoiceColors is Airtable's default select-choice palette, in the order
// its own UI cycles through when auto-coloring new options. The frontend preview
// maps these same tokens to their hex values by index, so a field created here
// looks identical to its preview.
var defaultChoiceColors = []string{
	"blueLight2", "cyanLight2", "tealLight2", "greenLight2", "yellowLight2",
	"orangeLight2", "redLight2", "pinkLight2", "purpleLight2", "grayLight2",
}

// normalizeChoiceColors assigns a color to any select choice that doesn't already
// have one. The Airtable API leaves API-created options gray unless a color is
// given, unlike its UI which auto-colors them; this fills that gap so created
// fields match the preview (and look like normal Airtable fields).
func normalizeChoiceColors(options map[string]any) map[string]any {
	if len(options) == 0 {
		return options
	}
	choices, ok := options["choices"].([]any)
	if !ok {
		return options
	}
	for i, item := range choices {
		choice, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if color, ok := choice["color"].(string); ok && strings.TrimSpace(color) != "" {
			continue
		}
		choice["color"] = defaultChoiceColors[i%len(defaultChoiceColors)]
	}
	return options
}

// schemaHasDescriptionEdit reports whether any table/field rename-or-describe op
// actually sets a description (the only case that needs the current value).
func schemaHasDescriptionEdit(ops []schemaPayloadOp) bool {
	for _, op := range ops {
		if op.Description != "" && (op.Type == schemaUpdateTable || op.Type == schemaUpdateField) {
			return true
		}
	}
	return false
}

// schemaDescriptionIndex maps table and field IDs to their current descriptions,
// from a freshly fetched base schema.
func schemaDescriptionIndex(tables []syncer.Table) (map[string]string, map[string]string) {
	tableDesc := make(map[string]string)
	fieldDesc := make(map[string]string)
	for _, table := range tables {
		tableDesc[table.ID] = table.Description
		for _, field := range table.Fields {
			fieldDesc[field.ID] = field.Description
		}
	}
	return tableDesc, fieldDesc
}

// selectChoiceNames pulls the choice names out of an Airtable select field's
// options object, so the preview can show what the options will be.
func selectChoiceNames(options map[string]any) []string {
	if len(options) == 0 {
		return nil
	}
	raw, ok := options["choices"].([]any)
	if !ok {
		return nil
	}
	names := make([]string, 0, len(raw))
	for _, choice := range raw {
		choiceMap, ok := choice.(map[string]any)
		if !ok {
			continue
		}
		if name, ok := choiceMap["name"].(string); ok && strings.TrimSpace(name) != "" {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return nil
	}
	return names
}
