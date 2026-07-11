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

	ctx, _ := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	// Start Admin HTTP server
	adminServer := app.NewAdminServer()
	go func() {
		logger.Info("starting Admin server", "addr", app.Config.Server.Admin.Addr)
		if err := adminServer.Run(); err != nil {
			logger.Error("Admin server stopped", "error", err)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down...")
}
