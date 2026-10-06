// Command hubd is the Control Hub engine daemon (PROTOCOL.md). It serves the
// JSON-RPC protocol over WebSocket and HTTP transports.
//
// Usage:
//
//	hubd -addr :8080 -auth none -demo 3
//	hubd -addr :8080 -auth token -token secret -demo 3
//	hubd -addr :8080 -auth basic -user admin -pass pw -demo 3
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"musubi/engine/internal/httpapi"
	"musubi/engine/internal/hub"
)

func main() {
	var (
		addr     = flag.String("addr", ":8080", "listen address")
		authMode = flag.String("auth", "none", "auth mode: none | token | basic")
		token    = flag.String("token", "", "static token (token mode)")
		user     = flag.String("user", "", "username (basic mode)")
		pass     = flag.String("pass", "", "password (basic mode)")
		demo     = flag.Int("demo", 3, "number of simulated nodes n-1..n-N")
		verbose  = flag.Bool("verbose", false, "enable debug logging")
	)
	flag.Parse()

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	if *authMode != "none" && *authMode != "token" && *authMode != "basic" {
		logger.Error("invalid auth mode", "auth", *authMode)
		os.Exit(2)
	}
	if *authMode == "token" && *token == "" {
		logger.Error("token mode requires -token")
		os.Exit(2)
	}
	if *authMode == "basic" && (*user == "" || *pass == "") {
		logger.Error("basic mode requires -user and -pass")
		os.Exit(2)
	}

	h := hub.NewHub(hub.Config{
		AuthMode:  *authMode,
		Token:     *token,
		User:      *user,
		Pass:      *pass,
		DemoNodes: *demo,
		Logger:    logger,
	})
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	h.Start(ctx)

	srv := &http.Server{
		Addr:              *addr,
		Handler:           httpapi.New(h, logger).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("hubd listening", "addr", *addr, "auth", *authMode, "demo", *demo)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "error", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		logger.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}
	fmt.Println("hubd stopped")
}
