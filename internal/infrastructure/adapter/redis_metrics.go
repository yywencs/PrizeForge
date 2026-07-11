package adapter

import (
	"context"
	stderrors "errors"
	"time"

	"prizeforge/internal/metrics"

	"github.com/redis/go-redis/v9"
)

type redisMetricsHook struct{}

func newRedisMetricsHook() redis.Hook {
	return redisMetricsHook{}
}

func (h redisMetricsHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h redisMetricsHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		start := time.Now()
		err := next(ctx, cmd)
		metrics.IncRedisOp(redisCmdName(cmd), redisCmdResult(err))
		metrics.ObserveRedisOpDuration(redisCmdName(cmd), time.Since(start))
		return err
	}
}

func (h redisMetricsHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		start := time.Now()
		err := next(ctx, cmds)

		cmdName := "pipeline"
		if len(cmds) == 1 {
			cmdName = redisCmdName(cmds[0])
		}

		metrics.IncRedisOp(cmdName, redisCmdResult(err))
		metrics.ObserveRedisOpDuration(cmdName, time.Since(start))
		return err
	}
}

func redisCmdName(cmd redis.Cmder) string {
	if cmd == nil {
		return "unknown"
	}
	return cmd.FullName()
}

func redisCmdResult(err error) string {
	switch {
	case err == nil:
		return "success"
	case stderrors.Is(err, redis.Nil):
		return "nil"
	default:
		return "error"
	}
}
