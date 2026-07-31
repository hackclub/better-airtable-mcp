//go:build integration

package mcp_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"

	"github.com/hackclub/better-airtable-mcp/internal/config"
	"github.com/hackclub/better-airtable-mcp/internal/cryptoutil"
	"github.com/hackclub/better-airtable-mcp/internal/db"
	"github.com/hackclub/better-airtable-mcp/internal/mcp"
	"github.com/hackclub/better-airtable-mcp/internal/oauth"
	syncer "github.com/hackclub/better-airtable-mcp/internal/sync"
	"github.com/hackclub/better-airtable-mcp/internal/tools"
)

// Reproduces the oversized list_schema output seen on real bases: a linked
// record cell holding hundreds of record IDs, an attachment cell carrying
// signed thumbnail URLs, and a very long text cell. The schema preview must
// clamp all three while the underlying DuckDB cache keeps full fidelity for
// the query tool.
func TestListSchemaClampsOversizedSampleValuesOverMCP(t *testing.T) {
	port := mcpFreePort(t)
	postgres := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Port(uint32(port)).
			Database("better_airtable_mcp_sample_clamp_test").
			Username("postgres").
			Password("postgres").
			BinariesPath(filepath.Join(t.TempDir(), "postgres-binaries")).
			DataPath(filepath.Join(t.TempDir(), "postgres-data")).
			RuntimePath(filepath.Join(t.TempDir(), "postgres-runtime")),
	)
	if err := postgres.Start(); err != nil {
		t.Fatalf("embedded postgres start failed: %v", err)
	}
	defer postgres.Stop()

	store, err := db.Open(context.Background(), fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/better_airtable_mcp_sample_clamp_test?sslmode=disable", port))
	if err != nil {
		t.Fatalf("db.Open() returned error: %v", err)
	}
	defer store.Close()
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("store.Migrate() returned error: %v", err)
	}

	linkedIDs := make([]string, 452)
	for index := range linkedIDs {
		linkedIDs[index] = fmt.Sprintf("recTask%010d", index)
	}
	longNotes := strings.Repeat("n", 5000)
	signedURL := "https://v5.airtableusercontent.com/v3/u/55/55/" + strings.Repeat("x", 250)

	fakeAirtable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v0/meta/bases":
			writeMCPJSON(t, w, map[string]any{
				"bases": []map[string]any{
					{"id": "appBulky", "name": "Bulky Base", "permissionLevel": "create"},
				},
			})
		case "/v0/meta/bases/appBulky/tables":
			writeMCPJSON(t, w, map[string]any{
				"tables": []map[string]any{
					{
						"id":   "tblJams",
						"name": "Jams",
						"fields": []map[string]any{
							{"id": "fldName", "name": "Name", "type": "singleLineText"},
							{"id": "fldNotes", "name": "Notes", "type": "multilineText"},
							{"id": "fldSignUps", "name": "Sign Up Records", "type": "multipleRecordLinks"},
							{"id": "fldScreenshot", "name": "Screenshot", "type": "multipleAttachments"},
						},
					},
				},
			})
		case "/v0/appBulky/tblJams":
			writeMCPJSON(t, w, map[string]any{
				"records": []map[string]any{
					{
						"id":          "recJam1",
						"createdTime": "2026-07-01T12:00:00Z",
						"fields": map[string]any{
							"Name":            "Summer Jam",
							"Notes":           longNotes,
							"Sign Up Records": linkedIDs,
							"Screenshot": []map[string]any{
								{
									"id":       "attScreenshot1",
									"filename": "ceiling.gif",
									"size":     6642,
									"type":     "image/gif",
									"width":    356,
									"height":   349,
									"url":      signedURL,
									"thumbnails": map[string]any{
										"small": map[string]any{"url": signedURL, "width": 36, "height": 36},
										"large": map[string]any{"url": signedURL, "width": 356, "height": 349},
										"full":  map[string]any{"url": signedURL, "width": 356, "height": 349},
									},
								},
							},
						},
					},
				},
			})
		default:
			t.Fatalf("unexpected Airtable path %q", r.URL.Path)
		}
	}))
	defer fakeAirtable.Close()

	secret, err := cryptoutil.New([]byte(strings.Repeat("k", 32)))
	if err != nil {
		t.Fatalf("cryptoutil.New() returned error: %v", err)
	}

	if err := store.UpsertUser(context.Background(), db.User{ID: "user_1"}); err != nil {
		t.Fatalf("store.UpsertUser() returned error: %v", err)
	}

	encryptedToken, err := secret.Encrypt([]byte("airtable-access-token"))
	if err != nil {
		t.Fatalf("secret.Encrypt() returned error: %v", err)
	}
	if err := store.PutAirtableToken(context.Background(), db.AirtableTokenRecord{
		UserID:                 "user_1",
		AccessTokenCiphertext:  encryptedToken,
		RefreshTokenCiphertext: encryptedToken,
		ExpiresAt:              time.Now().Add(time.Hour),
		Scopes:                 "data.records:read data.records:write schema.bases:read",
	}); err != nil {
		t.Fatalf("store.PutAirtableToken() returned error: %v", err)
	}

	bearerToken := "mcp-access-token"
	if err := store.PutMCPToken(context.Background(), db.MCPTokenRecord{
		TokenHash:  oauth.HashToken(bearerToken),
		UserID:     "user_1",
		ClientID:   ptr("client_claude"),
		ClientName: ptr("Claude"),
		CreatedAt:  time.Now().UTC(),
		ExpiresAt:  time.Now().Add(time.Hour).UTC(),
	}); err != nil {
		t.Fatalf("store.PutMCPToken() returned error: %v", err)
	}

	cfg := config.Config{
		SyncInterval:      time.Minute,
		QueryDefaultLimit: 100,
		QueryMaxLimit:     1000,
	}
	runtime := &tools.Runtime{
		Store:  store,
		Cipher: secret,
		Syncer: syncer.NewService(syncer.NewHTTPClient(fakeAirtable.URL, fakeAirtable.Client()), t.TempDir()),
		Config: cfg,
	}
	runtime.SyncManager = syncer.NewManager(runtime.Syncer, store, runtime, cfg.SyncInterval, 10*time.Minute)

	handler := oauth.NewMiddleware(store, "").RequireBearer(mcp.NewHandler("better-airtable-mcp", "0.1.0", tools.NewCatalog(cfg, runtime)))

	schemaResponse := waitForToolResult(t, func() map[string]any {
		return performAuthenticatedToolCall(t, handler, bearerToken, "list_schema", map[string]any{
			"base": "appBulky",
		})
	}, func(response map[string]any) bool {
		result, ok := response["result"].(map[string]any)
		if !ok {
			return false
		}
		structured, ok := result["structuredContent"].(map[string]any)
		if !ok {
			return false
		}
		tables, ok := structured["tables"].([]any)
		if !ok || len(tables) != 1 {
			return false
		}
		table := tables[0].(map[string]any)
		complete, _ := table["table_complete"].(bool)
		samples, _ := table["sample_rows"].([]any)
		return complete && len(samples) == 1
	})

	structured := schemaResponse["result"].(map[string]any)["structuredContent"].(map[string]any)
	table := structured["tables"].([]any)[0].(map[string]any)
	sample := table["sample_rows"].([]any)[0].(map[string]any)

	recordID, _ := sample["id"].(string)
	if recordID != "recJam1" {
		t.Fatalf("expected sample row to keep its record id, got %#v", sample)
	}

	links, ok := sample["sign_up_records"].([]any)
	if !ok {
		t.Fatalf("expected clamped linked record array, got %#v", sample["sign_up_records"])
	}
	if len(links) != 4 || links[3] != "[+449 more, 452 total]" {
		t.Fatalf("expected 3 linked ids plus overflow marker, got %#v", links)
	}

	notes, _ := sample["notes"].(string)
	if !strings.HasSuffix(notes, " [truncated]") || len(notes) > 200 {
		t.Fatalf("expected long notes to be truncated with marker, got %d chars %q", len(notes), notes)
	}

	attachments, ok := sample["screenshot"].([]any)
	if !ok || len(attachments) != 1 {
		t.Fatalf("expected one attachment in sample, got %#v", sample["screenshot"])
	}
	attachment := attachments[0].(map[string]any)
	if _, hasURL := attachment["url"]; hasURL {
		t.Fatalf("expected signed url to be stripped from attachment, got %#v", attachment)
	}
	if _, hasThumbnails := attachment["thumbnails"]; hasThumbnails {
		t.Fatalf("expected thumbnails to be stripped from attachment, got %#v", attachment)
	}
	if attachment["filename"] != "ceiling.gif" {
		t.Fatalf("expected attachment filename to survive, got %#v", attachment)
	}

	encoded, err := json.Marshal(structured)
	if err != nil {
		t.Fatalf("json.Marshal(structured) returned error: %v", err)
	}
	if len(encoded) > 8000 {
		t.Fatalf("expected clamped schema payload to stay small, got %d bytes", len(encoded))
	}

	// The clamp is display-only: the query tool must still return the full
	// values from the DuckDB cache using the record id from the sample.
	queryResponse := performAuthenticatedToolCall(t, handler, bearerToken, "query", map[string]any{
		"base": "appBulky",
		"sql": []string{
			"SELECT len(sign_up_records) AS link_count, length(notes) AS notes_len FROM jams WHERE id = 'recJam1' LIMIT 1",
		},
	})
	queryResult := queryResponse["result"].(map[string]any)["structuredContent"].(map[string]any)
	queryRows := queryResult["results"].([]any)[0].(map[string]any)["rows"].([]any)
	if len(queryRows) != 1 {
		t.Fatalf("expected 1 query row, got %#v", queryResult)
	}
	row := queryRows[0].([]any)
	if int(row[0].(float64)) != 452 || int(row[1].(float64)) != 5000 {
		t.Fatalf("expected full-fidelity link_count=452 notes_len=5000, got %#v", row)
	}
}