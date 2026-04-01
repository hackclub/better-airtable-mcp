package tools

import (
	"context"

	"github.com/hackclub/better-airtable-mcp/internal/mcp"
	"github.com/hackclub/better-airtable-mcp/internal/oauth"
)

func authenticatedUserID(ctx context.Context) (string, bool) {
	return oauth.UserIDFromContext(ctx)
}

func authenticatedSessionID(ctx context.Context) (string, bool) {
	return mcp.SessionIDFromContext(ctx)
}

func authenticatedClientID(ctx context.Context) (string, bool) {
	return oauth.ClientIDFromContext(ctx)
}

func authenticatedClientName(ctx context.Context) (string, bool) {
	return oauth.ClientNameFromContext(ctx)
}
