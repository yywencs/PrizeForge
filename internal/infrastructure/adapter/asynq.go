package adapter

import (
	"fmt"
	"prizeforge/pkg/config"

	"github.com/hibiken/asynq"
)

// NewAsynqClient creates an Asynq client for enqueuing tasks.
func NewAsynqClient(cfg *config.AsynqConfig) *asynq.Client {
	return asynq.NewClient(asynq.RedisClientOpt{
		Addr:     fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port),
		DB:       cfg.Redis.DB,
		PoolSize: cfg.Redis.PoolSize,
	})
}

// NewAsynqInspector creates an Asynq inspector for queue management.
func NewAsynqInspector(cfg *config.AsynqConfig) *asynq.Inspector {
	return asynq.NewInspector(asynq.RedisClientOpt{
		Addr:     fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port),
		DB:       cfg.Redis.DB,
		PoolSize: cfg.Redis.PoolSize,
	})
}
