package main

import (
	"context"
	"log"
	"os/signal"
	"prizeforge/internal/bootstrap"
	"prizeforge/pkg/logger"
	"syscall"
	"time"
)

func main() {
	app, err := bootstrap.NewAPIApp()
	if err != nil {
		log.Fatalf("bootstrap API app: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	apiServer := app.APIServer()

	// 启动 API HTTP 服务
	go func() {
		logger.Info("starting API server", "addr", app.Config.Server.API.Addr)
		if err := apiServer.Run(); err != nil {
			logger.Error("API server stopped", "error", err)
		}
	}()

	// 启动 Asynq worker
	go func() {
		logger.Info("starting Asynq worker")
		if err := app.AsynqWorker().Start(ctx); err != nil {
			logger.Error("Asynq worker stopped", "error", err)
		}
	}()

	// 启动 RabbitMQ 消费者
	go func() {
		logger.Info("starting RabbitMQ consumer")
		if err := app.RabbitMQConsumer().Start(ctx); err != nil {
			logger.Error("RabbitMQ consumer stopped", "error", err)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down...")

	// 优雅关闭：先关闭 HTTP 服务（停止接收新请求）
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := apiServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("API server shutdown error", "error", err)
	} else {
		logger.Info("API server shut down gracefully")
	}

	app.AsynqWorker().Shutdown()
	app.RabbitMQConsumer().Shutdown()

	logger.Info("shutdown complete")
}
