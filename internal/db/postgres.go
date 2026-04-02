package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

type User struct {
	ID             string
	AirtableUserID *string
	Email          *string
	CreatedAt      time.Time
}

type AirtableTokenRecord struct {
	UserID                 string
	AccessTokenCiphertext  []byte
	RefreshTokenCiphertext []byte
	ExpiresAt              time.Time
	Scopes                 string
	UpdatedAt              time.Time
	ReauthRequiredAt       *time.Time
}

type MCPTokenRecord struct {
	TokenHash  string
	UserID     string
	ClientID   *string
	ClientName *string
	CreatedAt  time.Time
	ExpiresAt  time.Time
}

type UserBaseAccess struct {
	UserID          string
	BaseID          string
	PermissionLevel string
	LastVerifiedAt  time.Time
}

type PendingOperation struct {
	ID                      string
	UserID                  string
	BaseID                  string
	Status                  string
	OperationType           string
	PayloadCiphertext       []byte
	CurrentValuesCiphertext []byte
	ResultCiphertext        []byte
	Error                   *string
	CreatedAt               time.Time
	ExpiresAt               time.Time
	ResolvedAt              *time.Time
}

type SyncState struct {
	BaseID             string
	LastSyncedAt       *time.Time
	LastSyncDurationMS *int64
	TotalRecords       *int64
	TotalTables        *int
	ActiveUntil        *time.Time
	SyncTokenUserID    *string
}

type OAuthClient struct {
	ClientID         string
	ClientSecretHash *string
	RedirectURIs     []string
	ClientName       *string
	CreatedAt        time.Time
}

func Open(ctx context.Context, databaseURL string) (*Store, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse postgres config: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("open postgres pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return &Store{pool: pool}, nil
}

func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

func (s *Store) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *Store) Migrate(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS users (
			id TEXT PRIMARY KEY,
			airtable_user_id TEXT UNIQUE,
			email TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS airtable_tokens (
			user_id TEXT PRIMARY KEY REFERENCES users(id),
			access_token_ciphertext BYTEA NOT NULL,
			refresh_token_ciphertext BYTEA NOT NULL,
			expires_at TIMESTAMPTZ NOT NULL,
			scopes TEXT NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			reauth_required_at TIMESTAMPTZ
		)`,
		`CREATE TABLE IF NOT EXISTS mcp_tokens (
			token_hash TEXT PRIMARY KEY,
			user_id TEXT NOT NULL REFERENCES users(id),
			client_id TEXT,
			client_name TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			expires_at TIMESTAMPTZ NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS user_base_access (
			user_id TEXT NOT NULL REFERENCES users(id),
			base_id TEXT NOT NULL,
			permission_level TEXT NOT NULL,
			last_verified_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (user_id, base_id)
		)`,
		`CREATE TABLE IF NOT EXISTS pending_operations (
			id TEXT PRIMARY KEY,
			user_id TEXT NOT NULL REFERENCES users(id),
			base_id TEXT NOT NULL,
			status TEXT NOT NULL,
			operation_type TEXT NOT NULL,
			payload_ciphertext BYTEA NOT NULL,
			current_values_ciphertext BYTEA,
			result_ciphertext BYTEA,
			error TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			expires_at TIMESTAMPTZ NOT NULL,
			resolved_at TIMESTAMPTZ
		)`,
		`CREATE TABLE IF NOT EXISTS sync_state (
			base_id TEXT PRIMARY KEY,
			last_synced_at TIMESTAMPTZ,
			last_sync_duration_ms BIGINT,
			total_records BIGINT,
			total_tables INTEGER,
			active_until TIMESTAMPTZ,
			sync_token_user_id TEXT REFERENCES users(id)
		)`,
		`CREATE TABLE IF NOT EXISTS oauth_clients (
			client_id TEXT PRIMARY KEY,
			client_secret_hash TEXT,
			redirect_uris TEXT[] NOT NULL,
			client_name TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
	}

	for _, statement := range statements {
		if _, err := s.pool.Exec(ctx, statement); err != nil {
			return fmt.Errorf("run migration: %w", err)
		}
	}
	if _, err := s.pool.Exec(ctx, `ALTER TABLE airtable_tokens ADD COLUMN IF NOT EXISTS reauth_required_at TIMESTAMPTZ`); err != nil {
		return fmt.Errorf("ensure airtable_tokens.reauth_required_at column: %w", err)
	}
	if _, err := s.pool.Exec(ctx, `ALTER TABLE mcp_tokens ADD COLUMN IF NOT EXISTS client_id TEXT`); err != nil {
		return fmt.Errorf("ensure mcp_tokens.client_id column: %w", err)
	}
	if _, err := s.pool.Exec(ctx, `ALTER TABLE mcp_tokens ADD COLUMN IF NOT EXISTS client_name TEXT`); err != nil {
		return fmt.Errorf("ensure mcp_tokens.client_name column: %w", err)
	}

	return nil
}

func (s *Store) UpsertUser(ctx context.Context, user User) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO users (id, airtable_user_id, email)
		VALUES ($1, $2, $3)
		ON CONFLICT (id) DO UPDATE
		SET airtable_user_id = EXCLUDED.airtable_user_id,
		    email = EXCLUDED.email
	`, user.ID, user.AirtableUserID, user.Email)
	if err != nil {
		return fmt.Errorf("upsert user: %w", err)
	}
	return nil
}

func (s *Store) GetUser(ctx context.Context, id string) (User, error) {
	var user User
	err := s.pool.QueryRow(ctx, `
		SELECT id, airtable_user_id, email, created_at
		FROM users
		WHERE id = $1
	`, id).Scan(&user.ID, &user.AirtableUserID, &user.Email, &user.CreatedAt)
	if err != nil {
		return User{}, fmt.Errorf("get user: %w", err)
	}
	return user, nil
}

func (s *Store) PutAirtableToken(ctx context.Context, record AirtableTokenRecord) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO airtable_tokens (
			user_id,
			access_token_ciphertext,
			refresh_token_ciphertext,
			expires_at,
			scopes,
			updated_at,
			reauth_required_at
		) VALUES ($1, $2, $3, $4, $5, NOW(), NULL)
		ON CONFLICT (user_id) DO UPDATE
		SET access_token_ciphertext = EXCLUDED.access_token_ciphertext,
		    refresh_token_ciphertext = EXCLUDED.refresh_token_ciphertext,
		    expires_at = EXCLUDED.expires_at,
		    scopes = EXCLUDED.scopes,
		    updated_at = NOW(),
		    reauth_required_at = NULL
	`, record.UserID, record.AccessTokenCiphertext, record.RefreshTokenCiphertext, record.ExpiresAt, record.Scopes)
	if err != nil {
		return fmt.Errorf("put airtable token: %w", err)
	}
	return nil
}

func (s *Store) GetAirtableToken(ctx context.Context, userID string) (AirtableTokenRecord, error) {
	var record AirtableTokenRecord
	err := s.pool.QueryRow(ctx, `
		SELECT user_id, access_token_ciphertext, refresh_token_ciphertext, expires_at, scopes, updated_at, reauth_required_at
		FROM airtable_tokens
		WHERE user_id = $1
	`, userID).Scan(
		&record.UserID,
		&record.AccessTokenCiphertext,
		&record.RefreshTokenCiphertext,
		&record.ExpiresAt,
		&record.Scopes,
		&record.UpdatedAt,
		&record.ReauthRequiredAt,
	)
	if err != nil {
		return AirtableTokenRecord{}, fmt.Errorf("get airtable token: %w", err)
	}
	return record, nil
}

func (s *Store) MarkAirtableTokenReauthRequired(ctx context.Context, userID string, requiredAt time.Time) error {
	commandTag, err := s.pool.Exec(ctx, `
		UPDATE airtable_tokens
		SET reauth_required_at = $2,
		    updated_at = NOW()
		WHERE user_id = $1
	`, userID, requiredAt)
	if err != nil {
		return fmt.Errorf("mark airtable token reauth required: %w", err)
	}
	if commandTag.RowsAffected() == 0 {
		return fmt.Errorf("mark airtable token reauth required: %w", pgx.ErrNoRows)
	}
	return nil
}

func (s *Store) ListAirtableTokensExpiringBefore(ctx context.Context, before time.Time) ([]AirtableTokenRecord, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT user_id, access_token_ciphertext, refresh_token_ciphertext, expires_at, scopes, updated_at, reauth_required_at
		FROM airtable_tokens
		WHERE expires_at <= $1
		  AND reauth_required_at IS NULL
		ORDER BY expires_at ASC
	`, before)
	if err != nil {
		return nil, fmt.Errorf("list expiring airtable tokens: %w", err)
	}
	defer rows.Close()

	var records []AirtableTokenRecord
	for rows.Next() {
		var record AirtableTokenRecord
		if err := rows.Scan(
			&record.UserID,
			&record.AccessTokenCiphertext,
			&record.RefreshTokenCiphertext,
			&record.ExpiresAt,
			&record.Scopes,
			&record.UpdatedAt,
			&record.ReauthRequiredAt,
		); err != nil {
			return nil, fmt.Errorf("scan expiring airtable token: %w", err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate expiring airtable tokens: %w", err)
	}
	return records, nil
}

func (s *Store) PutMCPToken(ctx context.Context, record MCPTokenRecord) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO mcp_tokens (token_hash, user_id, client_id, client_name, created_at, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (token_hash) DO UPDATE
		SET user_id = EXCLUDED.user_id,
		    client_id = EXCLUDED.client_id,
		    client_name = EXCLUDED.client_name,
		    created_at = EXCLUDED.created_at,
		    expires_at = EXCLUDED.expires_at
	`, record.TokenHash, record.UserID, record.ClientID, record.ClientName, record.CreatedAt, record.ExpiresAt)
	if err != nil {
		return fmt.Errorf("put mcp token: %w", err)
	}
	return nil
}

func (s *Store) GetMCPToken(ctx context.Context, tokenHash string) (MCPTokenRecord, error) {
	var record MCPTokenRecord
	err := s.pool.QueryRow(ctx, `
		SELECT token_hash, user_id, client_id, client_name, created_at, expires_at
		FROM mcp_tokens
		WHERE token_hash = $1
	`, tokenHash).Scan(&record.TokenHash, &record.UserID, &record.ClientID, &record.ClientName, &record.CreatedAt, &record.ExpiresAt)
	if err != nil {
		return MCPTokenRecord{}, fmt.Errorf("get mcp token: %w", err)
	}
	return record, nil
}

func (s *Store) UpsertUserBaseAccess(ctx context.Context, record UserBaseAccess) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO user_base_access (user_id, base_id, permission_level, last_verified_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (user_id, base_id) DO UPDATE
		SET permission_level = EXCLUDED.permission_level,
		    last_verified_at = EXCLUDED.last_verified_at
	`, record.UserID, record.BaseID, record.PermissionLevel, record.LastVerifiedAt)
	if err != nil {
		return fmt.Errorf("upsert user base access: %w", err)
	}
	return nil
}

func (s *Store) GetUserBaseAccess(ctx context.Context, userID, baseID string) (UserBaseAccess, error) {
	var record UserBaseAccess
	err := s.pool.QueryRow(ctx, `
		SELECT user_id, base_id, permission_level, last_verified_at
		FROM user_base_access
		WHERE user_id = $1 AND base_id = $2
	`, userID, baseID).Scan(&record.UserID, &record.BaseID, &record.PermissionLevel, &record.LastVerifiedAt)
	if err != nil {
		return UserBaseAccess{}, fmt.Errorf("get user base access: %w", err)
	}
	return record, nil
}

func (s *Store) PutPendingOperation(ctx context.Context, operation PendingOperation) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO pending_operations (
			id,
			user_id,
			base_id,
			status,
			operation_type,
			payload_ciphertext,
			current_values_ciphertext,
			result_ciphertext,
			error,
			created_at,
			expires_at,
			resolved_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (id) DO UPDATE
		SET user_id = EXCLUDED.user_id,
		    base_id = EXCLUDED.base_id,
		    status = EXCLUDED.status,
		    operation_type = EXCLUDED.operation_type,
		    payload_ciphertext = EXCLUDED.payload_ciphertext,
		    current_values_ciphertext = EXCLUDED.current_values_ciphertext,
		    result_ciphertext = EXCLUDED.result_ciphertext,
		    error = EXCLUDED.error,
		    created_at = EXCLUDED.created_at,
		    expires_at = EXCLUDED.expires_at,
		    resolved_at = EXCLUDED.resolved_at
	`,
		operation.ID,
		operation.UserID,
		operation.BaseID,
		operation.Status,
		operation.OperationType,
		operation.PayloadCiphertext,
		nilIfEmpty(operation.CurrentValuesCiphertext),
		nilIfEmpty(operation.ResultCiphertext),
		operation.Error,
		operation.CreatedAt,
		operation.ExpiresAt,
		operation.ResolvedAt,
	)
	if err != nil {
		return fmt.Errorf("put pending operation: %w", err)
	}
	return nil
}

func (s *Store) GetPendingOperation(ctx context.Context, id string) (PendingOperation, error) {
	var operation PendingOperation
	err := s.pool.QueryRow(ctx, `
		SELECT
			id,
			user_id,
			base_id,
			status,
			operation_type,
			payload_ciphertext,
			current_values_ciphertext,
			result_ciphertext,
			error,
			created_at,
			expires_at,
			resolved_at
		FROM pending_operations
		WHERE id = $1
	`, id).Scan(
		&operation.ID,
		&operation.UserID,
		&operation.BaseID,
		&operation.Status,
		&operation.OperationType,
		&operation.PayloadCiphertext,
		&operation.CurrentValuesCiphertext,
		&operation.ResultCiphertext,
		&operation.Error,
		&operation.CreatedAt,
		&operation.ExpiresAt,
		&operation.ResolvedAt,
	)
	if err != nil {
		return PendingOperation{}, fmt.Errorf("get pending operation: %w", err)
	}
	return operation, nil
}

func (s *Store) UpdatePendingOperationStatus(ctx context.Context, id, status string, resultCiphertext []byte, errText *string, resolvedAt *time.Time) error {
	commandTag, err := s.pool.Exec(ctx, `
		UPDATE pending_operations
		SET status = $2,
		    result_ciphertext = $3,
		    error = $4,
		    resolved_at = $5
		WHERE id = $1
	`, id, status, nilIfEmpty(resultCiphertext), errText, resolvedAt)
	if err != nil {
		return fmt.Errorf("update pending operation: %w", err)
	}
	if commandTag.RowsAffected() == 0 {
		return fmt.Errorf("update pending operation: %w", pgx.ErrNoRows)
	}
	return nil
}

func (s *Store) ExpirePendingOperations(ctx context.Context, now time.Time) (int64, error) {
	commandTag, err := s.pool.Exec(ctx, `
		UPDATE pending_operations
		SET status = 'expired',
		    resolved_at = $2
		WHERE status = 'pending_approval'
		  AND expires_at < $1
	`, now, now)
	if err != nil {
		return 0, fmt.Errorf("expire pending operations: %w", err)
	}
	return commandTag.RowsAffected(), nil
}

func (s *Store) PutSyncState(ctx context.Context, state SyncState) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO sync_state (
			base_id,
			last_synced_at,
			last_sync_duration_ms,
			total_records,
			total_tables,
			active_until,
			sync_token_user_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (base_id) DO UPDATE
		SET last_synced_at = EXCLUDED.last_synced_at,
		    last_sync_duration_ms = EXCLUDED.last_sync_duration_ms,
		    total_records = EXCLUDED.total_records,
		    total_tables = EXCLUDED.total_tables,
		    active_until = EXCLUDED.active_until,
		    sync_token_user_id = EXCLUDED.sync_token_user_id
	`,
		state.BaseID,
		state.LastSyncedAt,
		state.LastSyncDurationMS,
		state.TotalRecords,
		state.TotalTables,
		state.ActiveUntil,
		state.SyncTokenUserID,
	)
	if err != nil {
		return fmt.Errorf("put sync state: %w", err)
	}
	return nil
}

func (s *Store) TouchSyncState(ctx context.Context, baseID string, activeUntil time.Time, syncTokenUserID string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO sync_state (
			base_id,
			active_until,
			sync_token_user_id
		) VALUES ($1, $2, $3)
		ON CONFLICT (base_id) DO UPDATE
		SET active_until = EXCLUDED.active_until,
		    sync_token_user_id = EXCLUDED.sync_token_user_id
	`, baseID, activeUntil, syncTokenUserID)
	if err != nil {
		return fmt.Errorf("touch sync state: %w", err)
	}
	return nil
}

func (s *Store) ListActiveSyncStates(ctx context.Context, now time.Time) ([]SyncState, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			base_id,
			last_synced_at,
			last_sync_duration_ms,
			total_records,
			total_tables,
			active_until,
			sync_token_user_id
		FROM sync_state
		WHERE active_until > $1
		  AND sync_token_user_id IS NOT NULL
		ORDER BY base_id
	`, now)
	if err != nil {
		return nil, fmt.Errorf("list active sync states: %w", err)
	}
	defer rows.Close()

	states := make([]SyncState, 0)
	for rows.Next() {
		var state SyncState
		if err := rows.Scan(
			&state.BaseID,
			&state.LastSyncedAt,
			&state.LastSyncDurationMS,
			&state.TotalRecords,
			&state.TotalTables,
			&state.ActiveUntil,
			&state.SyncTokenUserID,
		); err != nil {
			return nil, fmt.Errorf("scan active sync state: %w", err)
		}
		states = append(states, state)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active sync states: %w", err)
	}
	return states, nil
}

func (s *Store) GetSyncState(ctx context.Context, baseID string) (SyncState, error) {
	var state SyncState
	err := s.pool.QueryRow(ctx, `
		SELECT
			base_id,
			last_synced_at,
			last_sync_duration_ms,
			total_records,
			total_tables,
			active_until,
			sync_token_user_id
		FROM sync_state
		WHERE base_id = $1
	`, baseID).Scan(
		&state.BaseID,
		&state.LastSyncedAt,
		&state.LastSyncDurationMS,
		&state.TotalRecords,
		&state.TotalTables,
		&state.ActiveUntil,
		&state.SyncTokenUserID,
	)
	if err != nil {
		return SyncState{}, fmt.Errorf("get sync state: %w", err)
	}
	return state, nil
}

func (s *Store) UpsertOAuthClient(ctx context.Context, client OAuthClient) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO oauth_clients (client_id, client_secret_hash, redirect_uris, client_name)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (client_id) DO UPDATE
		SET client_secret_hash = EXCLUDED.client_secret_hash,
		    redirect_uris = EXCLUDED.redirect_uris,
		    client_name = EXCLUDED.client_name
	`, client.ClientID, client.ClientSecretHash, client.RedirectURIs, client.ClientName)
	if err != nil {
		return fmt.Errorf("upsert oauth client: %w", err)
	}
	return nil
}

func (s *Store) GetOAuthClient(ctx context.Context, clientID string) (OAuthClient, error) {
	var client OAuthClient
	err := s.pool.QueryRow(ctx, `
		SELECT client_id, client_secret_hash, redirect_uris, client_name, created_at
		FROM oauth_clients
		WHERE client_id = $1
	`, clientID).Scan(&client.ClientID, &client.ClientSecretHash, &client.RedirectURIs, &client.ClientName, &client.CreatedAt)
	if err != nil {
		return OAuthClient{}, fmt.Errorf("get oauth client: %w", err)
	}
	return client, nil
}

func nilIfEmpty(value []byte) []byte {
	if len(value) == 0 {
		return nil
	}
	return value
}
