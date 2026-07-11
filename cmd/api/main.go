package main

import (
	"prizeforge/internal/bootstrap"
	"prizeforge/pkg/logger"
	"context"
	"os/signal"
	"syscall"
)

func main() {
	app := bootstrap.NewHTTPApp()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Start API HTTP server
	apiServer := app.NewAPIServer()
	go func() {
		logger.Info("starting API server", "addr", app.Config.Server.API.Addr)
		if err := apiServer.Run(); err != nil {
			logger.Error("API server stopped", "error", err)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down...")
}
