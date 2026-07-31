// Command server runs the better-airtable-mcp HTTP server.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/hackclub/better-airtable-mcp/internal/approval"
	"github.com/hackclub/better-airtable-mcp/internal/config"
	"github.com/hackclub/better-airtable-mcp/internal/cryptoutil"
	"github.com/hackclub/better-airtable-mcp/internal/db"
	"github.com/hackclub/better-airtable-mcp/internal/health"
	"github.com/hackclub/better-airtable-mcp/internal/landing"
	"github.com/hackclub/better-airtable-mcp/internal/logx"
	"github.com/hackclub/better-airtable-mcp/internal/mcp"
	"github.com/hackclub/better-airtable-mcp/internal/oauth"
	syncer "github.com/hackclub/better-airtable-mcp/internal/sync"
	"github.com/hackclub/better-airtable-mcp/internal/tools"
)

func main() {
	slog.SetDefault(logx.NewLogger(os.Stdout))

	cfg, err := config.LoadFromEnv()
	if err != nil {
		logx.Event(context.Background(), "server", "server.startup_failed",
			"stage", "load_config",
			"error_kind", logx.ErrorKind(err),
			"error_message", logx.ErrorPreview(err),
			"fatal", true,
		)
		os.Exit(1)
	}

	store, err := db.Open(context.Background(), cfg.DatabaseURL)
	if err != nil {
		logx.Event(context.Background(), "server", "server.startup_failed",
			"stage", "open_postgres",
			"error_kind", logx.ErrorKind(err),
			"error_message", logx.ErrorPreview(err),
			"fatal", true,
		)
		os.Exit(1)
	}
	defer store.Close()

	if err := store.Migrate(context.Background()); err != nil {
		logx.Event(context.Background(), "server", "server.startup_failed",
			"stage", "run_postgres_migrations",
			"error_kind", logx.ErrorKind(err),
			"error_message", logx.ErrorPreview(err),
			"fatal", true,
		)
		os.Exit(1)
	}

	cipher, err := cryptoutil.New([]byte(cfg.AppEncryptionKey))
	if err != nil {
		logx.Event(context.Background(), "server", "server.startup_failed",
			"stage", "create_cipher",
			"error_kind", logx.ErrorKind(err),
			"error_message", logx.ErrorPreview(err),
			"fatal", true,
		)
		os.Exit(1)
	}

	airtableOAuth := oauth.NewAirtableOAuthClient(
		cfg.AirtableClientID,
		cfg.AirtableClientSecret,
		cfg.BaseURLString()+"/oauth/airtable/callback",
		nil,
		"",
		"",
	)
	tokenManager := oauth.NewTokenManager(store, cipher, airtableOAuth)

	mux := http.NewServeMux()

	mux.Handle("/healthz", logx.Route("/healthz", health.NewHandler(store)))
	mux.Handle("/", logx.Route("/", landing.NewHandler("README.md")))

	syncService := syncer.NewService(syncer.NewHTTPClient("", nil), cfg.DuckDBDataDir)
	toolRuntime := &tools.Runtime{
		Store:          store,
		Cipher:         cipher,
		Syncer:         syncService,
		AirtableTokens: tokenManager,
		Config:         cfg,
	}
	toolRuntime.SyncManager = syncer.NewManager(syncService, store, tokenManager, cfg.SyncInterval, cfg.SyncTTL)
	toolRuntime.Approval = approval.NewService(store, cipher, syncService, toolRuntime.SyncManager, tokenManager, syncer.NewHTTPClient("", nil), cfg.BaseURLString(), cfg.ApprovalTTL)
	if err := toolRuntime.SyncManager.SweepStaleDuckDBFiles(context.Background()); err != nil {
		logx.Event(context.Background(), "server", "server.background_init_failed",
			"stage", "sweep_stale_duckdb_files",
			"error_kind", logx.ErrorKind(err),
			"error_message", logx.ErrorPreview(err),
		)
	}
	if err := toolRuntime.SyncManager.RestoreActiveWorkers(context.Background()); err != nil {
		logx.Event(context.Background(), "server", "server.background_init_failed",
			"stage", "restore_active_sync_workers",
			"error_kind", logx.ErrorKind(err),
			"error_message", logx.ErrorPreview(err),
		)
	}

	mcpHandler := mcp.NewHandler("better-airtable-mcp", "0.1.0", tools.NewCatalog(cfg, toolRuntime))
	middleware := oauth.NewMiddleware(store, cfg.BaseURLString()+"/.well-known/oauth-protected-resource")
	mux.Handle("/mcp", logx.Route("/mcp", middleware.RequireBearer(mcpHandler)))

	registerLimiter := oauth.NewRequestLimiter(1, 5) // client registration persists rows: strict
	publicLimiter := oauth.NewRequestLimiter(10, 20) // other unauthenticated endpoints
	wrapPublic := func(template string, limiter *oauth.RequestLimiter, handler http.HandlerFunc) http.Handler {
		return logx.Route(template, oauth.PublicRateLimit(limiter, handler))
	}

	oauthHandler := oauth.NewHandler(cfg, store, cipher, airtableOAuth)
	mux.Handle("/.well-known/oauth-protected-resource", logx.Route("/.well-known/oauth-protected-resource", http.HandlerFunc(oauthHandler.ProtectedResource)))
	mux.Handle("/.well-known/oauth-authorization-server", logx.Route("/.well-known/oauth-authorization-server", http.HandlerFunc(oauthHandler.AuthorizationServer)))
	mux.Handle("/oauth/register", wrapPublic("/oauth/register", registerLimiter, oauthHandler.Register))
	mux.Handle("/oauth/authorize", wrapPublic("/oauth/authorize", publicLimiter, oauthHandler.Authorize))
	mux.Handle("/oauth/token", wrapPublic("/oauth/token", publicLimiter, oauthHandler.Token))
	mux.Handle("/oauth/airtable/callback", wrapPublic("/oauth/airtable/callback", publicLimiter, oauthHandler.AirtableCallback))

	go toolRuntime.Approval.RunExpiryLoop(context.Background(), time.Minute)
	go tokenManager.RunRefreshLoop(context.Background(), time.Minute)
	go oauthHandler.RunCleanupLoop(context.Background(), time.Minute)
	go mcpHandler.RunSessionExpiryLoop(context.Background(), time.Minute)

	approvalHandler := approval.NewHandler(toolRuntime.Approval)
	mux.Handle("/approve/", wrapPublic("/approve/:operation", publicLimiter, approvalHandler.ServeApprovalPage))
	mux.Handle("/debug", logx.Route("/debug", http.HandlerFunc(approvalHandler.ServeDebugPage)))
	mux.Handle("/approval-ui/", logx.Route("/approval-ui/*", http.HandlerFunc(approvalHandler.ServeAssets)))
	mux.Handle("/api/operations/", wrapPublic("/api/operations/:operation", publicLimiter, approvalHandler.ServeOperationAPI))

	server := &http.Server{
		Addr:              cfg.ListenAddr(),
		Handler:           logx.HTTPMiddleware(logx.SecurityHeaders(mux)),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}

	logx.Event(context.Background(), "server", "server.listening",
		"listen_addr", cfg.ListenAddr(),
		"mcp_url", cfg.MCPURL(),
	)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logx.Event(context.Background(), "server", "server.serve_failed",
			"error_kind", logx.ErrorKind(err),
			"error_message", logx.ErrorPreview(err),
			"fatal", true,
		)
		os.Exit(1)
	}
}
