package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/hackclub/better-airtable-mcp/internal/approval"
	"github.com/hackclub/better-airtable-mcp/internal/config"
	"github.com/hackclub/better-airtable-mcp/internal/cryptoutil"
	"github.com/hackclub/better-airtable-mcp/internal/db"
	"github.com/hackclub/better-airtable-mcp/internal/landing"
	"github.com/hackclub/better-airtable-mcp/internal/mcp"
	"github.com/hackclub/better-airtable-mcp/internal/oauth"
	syncer "github.com/hackclub/better-airtable-mcp/internal/sync"
	"github.com/hackclub/better-airtable-mcp/internal/tools"
)

func main() {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	store, err := db.Open(context.Background(), cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("open postgres: %v", err)
	}
	defer store.Close()

	if err := store.Migrate(context.Background()); err != nil {
		log.Fatalf("run postgres migrations: %v", err)
	}

	cipher, err := cryptoutil.New([]byte(cfg.AppEncryptionKey))
	if err != nil {
		log.Fatalf("create application cipher: %v", err)
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

	mux.Handle("/", landing.NewHandler("README.md"))

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
		log.Printf("sweep stale duckdb files: %v", err)
	}
	if err := toolRuntime.SyncManager.RestoreActiveWorkers(context.Background()); err != nil {
		log.Printf("restore active sync workers: %v", err)
	}

	mcpHandler := mcp.NewHandler("better-airtable-mcp", "0.1.0", tools.NewCatalog(cfg, toolRuntime))
	middleware := oauth.NewMiddleware(store)
	mux.Handle("/mcp", middleware.RequireBearer(mcpHandler))

	oauthHandler := oauth.NewHandler(cfg, store, cipher, airtableOAuth)
	mux.HandleFunc("/.well-known/oauth-protected-resource", oauthHandler.ProtectedResource)
	mux.HandleFunc("/.well-known/oauth-authorization-server", oauthHandler.AuthorizationServer)
	mux.HandleFunc("/oauth/register", oauthHandler.Register)
	mux.HandleFunc("/oauth/authorize", oauthHandler.Authorize)
	mux.HandleFunc("/oauth/token", oauthHandler.Token)
	mux.HandleFunc("/oauth/airtable/callback", oauthHandler.AirtableCallback)

	go toolRuntime.Approval.RunExpiryLoop(context.Background(), time.Minute)
	go tokenManager.RunRefreshLoop(context.Background(), time.Minute)

	approvalHandler := approval.NewHandler(toolRuntime.Approval)
	mux.HandleFunc("/approve/", approvalHandler.ServeApprovalPage)
	mux.HandleFunc("/api/operations/", approvalHandler.ServeOperationAPI)

	server := &http.Server{
		Addr:              cfg.ListenAddr(),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}

	log.Printf("listening on %s", cfg.ListenAddr())
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server failed: %v", err)
	}
}
