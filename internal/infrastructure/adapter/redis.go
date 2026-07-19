package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"prizeforge/pkg/cache"
	"prizeforge/pkg/config"
	"time"

	"github.com/redis/go-redis/v9"
)

// NewRedisClient creates a Redis-backed cache.Cache from config.
func NewRedisClient(cfg *config.RedisConfig) *cache.Cache {
	opts := &redis.Options{
		Addr:            fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Password:        cfg.Password,
		DB:              cfg.DB,
		PoolSize:        cfg.PoolSize,
		MinIdleConns:    cfg.MinIdleSize,
		DialTimeout:     time.Duration(cfg.ConnectTimeout) * time.Millisecond,
		ConnMaxIdleTime: time.Duration(cfg.IdleTimeout) * time.Millisecond,
		MaxRetries:      cfg.RetryAttempts,
		MinRetryBackoff: time.Duration(cfg.RetryInterval) * time.Millisecond,
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
