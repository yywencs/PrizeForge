package main

import (
	"context"
	"os/signal"
	"prizeforge/internal/bootstrap"
	"prizeforge/pkg/logger"
	"syscall"
	"time"
)

func main() {
	app := bootstrap.NewAdminApp()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	adminServer := app.AdminServer()

	// Start Admin HTTP server
	go func() {
		logger.Info("starting Admin server", "addr", app.Config.Server.Admin.Addr)
		if err := adminServer.Run(); err != nil {
			logger.Error("Admin server stopped", "error", err)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := adminServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("Admin server shutdown error", "error", err)
	} else {
		logger.Info("Admin server shut down gracefully")
	}

	logger.Info("shutdown complete")
}
