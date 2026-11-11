package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(testLookup(map[string]string{
		"DATABASE_URL":           "postgres://localhost/better_airtable",
		"AIRTABLE_CLIENT_ID":     "client-id",
		"AIRTABLE_CLIENT_SECRET": "client-secret",
		"BASE_URL":               "https://better-airtable-mcp.hackclub.com",
		"APP_ENCRYPTION_KEY":     strings.Repeat("x", 32),
	}))
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

	if cfg.Port != defaultPort {
		t.Fatalf("expected default port %d, got %d", defaultPort, cfg.Port)
	}
	if cfg.DuckDBDataDir != defaultDuckDBDataDir {
		t.Fatalf("expected default DuckDB data dir %q, got %q", defaultDuckDBDataDir, cfg.DuckDBDataDir)
	}
	if cfg.SyncInterval != time.Minute {
		t.Fatalf("expected default sync interval %v, got %v", time.Minute, cfg.SyncInterval)
	}
	if cfg.ApprovalTTL != 24*time.Hour {
		t.Fatalf("expected default approval TTL 24h, got %v", cfg.ApprovalTTL)
	}
	if cfg.QueryDefaultLimit != defaultQueryLimit {
		t.Fatalf("expected default query limit %d, got %d", defaultQueryLimit, cfg.QueryDefaultLimit)
	}
	if cfg.QueryMaxLimit != defaultQueryMaxLimit {
		t.Fatalf("expected default query max limit %d, got %d", defaultQueryMaxLimit, cfg.QueryMaxLimit)
	}
	if cfg.SyncMaxConcurrent != defaultSyncMaxConcurrent {
		t.Fatalf("expected default sync concurrency %d, got %d", defaultSyncMaxConcurrent, cfg.SyncMaxConcurrent)
	}
	if got := cfg.MCPURL(); got != "https://better-airtable-mcp.hackclub.com/mcp" {
		t.Fatalf("expected MCP URL to be populated, got %q", got)
	}
}

func TestLoadApprovalTTL(t *testing.T) {
	for _, tc := range []struct {
		name    string
		value   string
		want    time.Duration
		wantErr bool
	}{
		{name: "blank uses default", value: " ", want: 24 * time.Hour},
		{name: "shorter override", value: "60", want: time.Hour},
		{name: "longer override", value: "2880", want: 48 * time.Hour},
		{name: "zero", value: "0", wantErr: true},
		{name: "negative", value: "-1", wantErr: true},
		{name: "invalid", value: "one day", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := Load(testLookup(map[string]string{
				"DATABASE_URL":           "postgres://localhost/better_airtable",
				"AIRTABLE_CLIENT_ID":     "client-id",
				"AIRTABLE_CLIENT_SECRET": "client-secret",
				"BASE_URL":               "https://better-airtable-mcp.hackclub.com",
				"APP_ENCRYPTION_KEY":     strings.Repeat("x", 32),
				"APPROVAL_TTL_MINUTES":   tc.value,
			}))
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "APPROVAL_TTL_MINUTES") {
					t.Fatalf("expected approval TTL error, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() returned error: %v", err)
			}
			if cfg.ApprovalTTL != tc.want {
				t.Fatalf("expected approval TTL %v, got %v", tc.want, cfg.ApprovalTTL)
			}
		})
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	_, err := Load(testLookup(map[string]string{
		"DATABASE_URL":           "postgres://localhost/better_airtable",
		"AIRTABLE_CLIENT_ID":     "client-id",
		"AIRTABLE_CLIENT_SECRET": "client-secret",
		"BASE_URL":               "not-a-url",
		"APP_ENCRYPTION_KEY":     "too-short",
		"QUERY_DEFAULT_LIMIT":    "500",
		"QUERY_MAX_LIMIT":        "100",
		"SYNC_MAX_CONCURRENT":    "0",
	}))
	if err == nil {
		t.Fatal("expected Load() to fail for invalid configuration")
	}

	message := err.Error()
	for _, fragment := range []string{
		"BASE_URL must be a valid absolute URL",
		"APP_ENCRYPTION_KEY must be exactly 32 bytes",
		"QUERY_DEFAULT_LIMIT must be less than or equal to QUERY_MAX_LIMIT",
		"SYNC_MAX_CONCURRENT must be greater than zero",
	} {
		if !strings.Contains(message, fragment) {
			t.Fatalf("expected error to contain %q, got %q", fragment, message)
		}
	}
}

func testLookup(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}
