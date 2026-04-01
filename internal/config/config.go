package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultPort              = 8080
	defaultDuckDBDataDir     = "/data/duckdb"
	defaultSyncInterval      = 60 * time.Second
	defaultSyncTTL           = 10 * time.Minute
	defaultApprovalTTL       = 10 * time.Minute
	defaultQueryLimit        = 100
	defaultQueryMaxLimit     = 1000
	requiredEncryptionKeyLen = 32
)

type Config struct {
	Port                 int
	DatabaseURL          string
	DuckDBDataDir        string
	AirtableClientID     string
	AirtableClientSecret string
	BaseURL              *url.URL
	AppEncryptionKey     string
	SyncInterval         time.Duration
	SyncTTL              time.Duration
	ApprovalTTL          time.Duration
	QueryDefaultLimit    int
	QueryMaxLimit        int
}

func LoadFromEnv() (Config, error) {
	return Load(os.LookupEnv)
}

func Load(lookup func(string) (string, bool)) (Config, error) {
	cfg := Config{
		Port:              defaultPort,
		DuckDBDataDir:     defaultDuckDBDataDir,
		SyncInterval:      defaultSyncInterval,
		SyncTTL:           defaultSyncTTL,
		ApprovalTTL:       defaultApprovalTTL,
		QueryDefaultLimit: defaultQueryLimit,
		QueryMaxLimit:     defaultQueryMaxLimit,
	}

	var problems []string

	if value, ok := lookup("PORT"); ok && strings.TrimSpace(value) != "" {
		port, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil || port < 1 || port > 65535 {
			problems = append(problems, "PORT must be an integer between 1 and 65535")
		} else {
			cfg.Port = port
		}
	}

	cfg.DatabaseURL = requireString(lookup, "DATABASE_URL", &problems)
	cfg.DuckDBDataDir = stringWithDefault(lookup, "DUCKDB_DATA_DIR", cfg.DuckDBDataDir)
	cfg.AirtableClientID = requireString(lookup, "AIRTABLE_CLIENT_ID", &problems)
	cfg.AirtableClientSecret = requireString(lookup, "AIRTABLE_CLIENT_SECRET", &problems)
	cfg.AppEncryptionKey = requireString(lookup, "APP_ENCRYPTION_KEY", &problems)

	if cfg.AppEncryptionKey != "" && len([]byte(cfg.AppEncryptionKey)) != requiredEncryptionKeyLen {
		problems = append(problems, "APP_ENCRYPTION_KEY must be exactly 32 bytes")
	}

	baseURL := requireString(lookup, "BASE_URL", &problems)
	if baseURL != "" {
		parsed, err := url.Parse(baseURL)
		if err != nil || !parsed.IsAbs() || parsed.Host == "" {
			problems = append(problems, "BASE_URL must be a valid absolute URL")
		} else {
			cfg.BaseURL = parsed
		}
	}

	cfg.SyncInterval = durationFromSeconds(lookup, "SYNC_INTERVAL_SECONDS", cfg.SyncInterval, &problems)
	cfg.SyncTTL = durationFromMinutes(lookup, "SYNC_TTL_MINUTES", cfg.SyncTTL, &problems)
	cfg.ApprovalTTL = durationFromMinutes(lookup, "APPROVAL_TTL_MINUTES", cfg.ApprovalTTL, &problems)
	cfg.QueryDefaultLimit = intWithDefault(lookup, "QUERY_DEFAULT_LIMIT", cfg.QueryDefaultLimit, &problems)
	cfg.QueryMaxLimit = intWithDefault(lookup, "QUERY_MAX_LIMIT", cfg.QueryMaxLimit, &problems)

	if cfg.QueryDefaultLimit <= 0 {
		problems = append(problems, "QUERY_DEFAULT_LIMIT must be greater than zero")
	}
	if cfg.QueryMaxLimit <= 0 {
		problems = append(problems, "QUERY_MAX_LIMIT must be greater than zero")
	}
	if cfg.QueryDefaultLimit > cfg.QueryMaxLimit {
		problems = append(problems, "QUERY_DEFAULT_LIMIT must be less than or equal to QUERY_MAX_LIMIT")
	}
	if cfg.SyncInterval <= 0 {
		problems = append(problems, "SYNC_INTERVAL_SECONDS must be greater than zero")
	}
	if cfg.SyncTTL <= 0 {
		problems = append(problems, "SYNC_TTL_MINUTES must be greater than zero")
	}
	if cfg.ApprovalTTL <= 0 {
		problems = append(problems, "APPROVAL_TTL_MINUTES must be greater than zero")
	}

	if len(problems) > 0 {
		return Config{}, fmt.Errorf("invalid configuration: %s", strings.Join(problems, "; "))
	}

	return cfg, nil
}

func (c Config) ListenAddr() string {
	return fmt.Sprintf(":%d", c.Port)
}

func (c Config) BaseURLString() string {
	if c.BaseURL == nil {
		return ""
	}

	return strings.TrimRight(c.BaseURL.String(), "/")
}

func (c Config) MCPURL() string {
	return c.BaseURLString() + "/mcp"
}

func requireString(lookup func(string) (string, bool), key string, problems *[]string) string {
	value, ok := lookup(key)
	if !ok || strings.TrimSpace(value) == "" {
		*problems = append(*problems, key+" is required")
		return ""
	}

	return strings.TrimSpace(value)
}

func stringWithDefault(lookup func(string) (string, bool), key, fallback string) string {
	value, ok := lookup(key)
	if !ok || strings.TrimSpace(value) == "" {
		return fallback
	}

	return strings.TrimSpace(value)
}

func intWithDefault(lookup func(string) (string, bool), key string, fallback int, problems *[]string) int {
	value, ok := lookup(key)
	if !ok || strings.TrimSpace(value) == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		*problems = append(*problems, key+" must be an integer")
		return fallback
	}

	return parsed
}

func durationFromSeconds(lookup func(string) (string, bool), key string, fallback time.Duration, problems *[]string) time.Duration {
	value := intWithDefault(lookup, key, int(fallback/time.Second), problems)
	return time.Duration(value) * time.Second
}

func durationFromMinutes(lookup func(string) (string, bool), key string, fallback time.Duration, problems *[]string) time.Duration {
	value := intWithDefault(lookup, key, int(fallback/time.Minute), problems)
	return time.Duration(value) * time.Minute
}
