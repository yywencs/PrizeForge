package worker

import (
	"prizeforge/pkg/config"
	"prizeforge/pkg/logger"
	"fmt"

	"github.com/hibiken/asynq"
)

// AsynqWorker wraps the Asynq task queue server.
type AsynqWorker struct {
	server    *asynq.Server
	mux       *asynq.ServeMux
	scheduler *asynq.Scheduler
}

// NewAsynqWorker creates an Asynq worker server from config.
func NewAsynqWorker(cfg *config.AsynqConfig) *AsynqWorker {
	srv := asynq.NewServer(
		asynq.RedisClientOpt{
			Addr: fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port),
			DB:   cfg.Redis.DB,
		},
		asynq.Config{
			Concurrency: cfg.Concurrency,
			Queues: map[string]int{
				"critical": 6,
				"default":  3,
				"low":      1,
			},
		},
	)

	mux := asynq.NewServeMux()
	scheduler := asynq.NewScheduler(
		asynq.RedisClientOpt{
			Addr: fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port),
			DB:   cfg.Redis.DB,
		},
		nil,
	)

	return &AsynqWorker{
		server:    srv,
		mux:       mux,
		scheduler: scheduler,
	}
}

// Mux returns the ServeMux for registering task handlers.
func (w *AsynqWorker) Mux() *asynq.ServeMux {
	return w.mux
}

// Scheduler returns the Scheduler for registering periodic tasks.
func (w *AsynqWorker) Scheduler() *asynq.Scheduler {
	return w.scheduler
}

// Run starts the Asynq worker.
func (w *AsynqWorker) Run() error {
	logger.Info("asynq worker starting")
	if err := w.scheduler.Start(); err != nil {
		return fmt.Errorf("asynq scheduler start error: %w", err)
	}
	if err := w.server.Run(w.mux); err != nil {
		return fmt.Errorf("asynq server run error: %w", err)
	}
	return nil
}

// Shutdown stops the Asynq worker gracefully.
func (w *AsynqWorker) Shutdown() {
	w.scheduler.Shutdown()
	w.server.Shutdown()
}
