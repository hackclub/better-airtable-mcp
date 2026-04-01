package tools

import (
	"testing"
	"time"

	"github.com/hackclub/better-airtable-mcp/internal/config"
)

func TestRuntimeNextSyncTimeUsesLastSyncStart(t *testing.T) {
	runtime := &Runtime{Config: config.Config{SyncInterval: time.Minute}}
	lastSyncedAt := time.Date(2026, 4, 1, 12, 1, 0, 0, time.UTC)

	got := runtime.NextSyncTime(lastSyncedAt, 15*time.Second)
	want := time.Date(2026, 4, 1, 12, 1, 45, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("NextSyncTime() = %s, want %s", got, want)
	}
}

func TestRuntimeNextSyncTimeClampsToImmediateWhenSyncExceedsInterval(t *testing.T) {
	runtime := &Runtime{Config: config.Config{SyncInterval: time.Minute}}
	lastSyncedAt := time.Date(2026, 4, 1, 12, 1, 0, 0, time.UTC)

	got := runtime.NextSyncTime(lastSyncedAt, 90*time.Second)
	if !got.Equal(lastSyncedAt) {
		t.Fatalf("NextSyncTime() = %s, want immediate %s", got, lastSyncedAt)
	}
}
