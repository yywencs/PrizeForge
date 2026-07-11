package infra

import (
	confpb "big-market-kratos/internal/conf"
	"fmt"

	"github.com/hibiken/asynq"
)

func NewAsynqClient(cfg *confpb.Asynq) *asynq.Client {
	return asynq.NewClient(asynq.RedisClientOpt{
		Addr:     fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port),
		DB:       int(cfg.Redis.Db),
		PoolSize: int(cfg.Redis.PoolSize),
		// Password: cfg.Redis.Password,
	})
}

func NewAsynqInspector(cfg *confpb.Asynq) *asynq.Inspector {
	return asynq.NewInspector(asynq.RedisClientOpt{
		Addr:     fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port),
		DB:       int(cfg.Redis.Db),
		PoolSize: int(cfg.Redis.PoolSize),
		// Password: cfg.Redis.Password,
	})
}
