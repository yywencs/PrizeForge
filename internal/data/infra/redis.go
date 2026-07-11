package infra

import (
	confpb "big-market-kratos/internal/conf"
	"big-market-kratos/pkg/cache"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

func NewRedisClient(cfg *confpb.Data_Redis) *cache.Cache {
	opts := &redis.Options{
		Addr:            fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		DB:              int(cfg.Db),
		PoolSize:        int(cfg.PoolSize),
		MinIdleConns:    int(cfg.MinIdleSize),
		DialTimeout:     time.Duration(cfg.ConnectTimeout) * time.Millisecond,
		ConnMaxIdleTime: time.Duration(cfg.IdleTimeout) * time.Millisecond,
		MaxRetries:      int(cfg.RetryAttempts),
		MinRetryBackoff: time.Duration(cfg.RetryInterval) * time.Millisecond,
		// Password: cfg.Password,
	}

	client := redis.NewClient(opts)
	client.AddHook(newRedisMetricsHook())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		panic(fmt.Sprintf("failed to connect to redis: %v", err))
	}

	return cache.New(&cache.Options{
		Redis: client,
		Marshal: func(v interface{}) ([]byte, error) {
			return json.Marshal(v)
		},
		Unmarshal: func(b []byte, v interface{}) error {
			return json.Unmarshal(b, v)
		},
	})
}
