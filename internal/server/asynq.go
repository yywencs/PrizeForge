// internal/server/asynq.go
package server

import (
	"big-market-kratos/pkg/logger"
	"context"
	"fmt"

	"big-market-kratos/internal/biz/activity"
	"big-market-kratos/internal/biz/task"
	"big-market-kratos/internal/conf"
	"big-market-kratos/internal/job" // 假设你把 Job 放在了 internal/job 目录下

	"github.com/go-kratos/kratos/v2/transport"
	"github.com/hibiken/asynq"
)

// 🌟 断言：确保 AsynqServer 严格实现了 Kratos 的 Server 接口
var _ transport.Server = (*AsynqServer)(nil)

type AsynqServer struct {
	server    *asynq.Server
	mux       *asynq.ServeMux
	scheduler *asynq.Scheduler
}

// 🌟 构造函数：直接向 Wire 伸手要组装好的 Job！不要再传 Usecase 进来了！
func NewAsynqServer(
	cfg *conf.Asynq,
	skuStockJob *job.ActivitySkuStockConsumeJob,
	stateSyncJob *job.SendAwardMessage,
	strategyAwardStockJob *job.StrategyAwardStockConsumeJob,
) *AsynqServer {
	redisOpt := asynq.RedisClientOpt{
		Addr:     fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port),
		DB:       int(cfg.Redis.Db),
		PoolSize: int(cfg.Redis.PoolSize),
	}

	server := asynq.NewServer(
		redisOpt,
		asynq.Config{
			Concurrency: int(cfg.Concurrency),
			Queues: map[string]int{
				activity.TaskTypeActivitySkuStockConsume: 6,
				"default":                                3,
				"low":                                    1,
			},
			ErrorHandler: asynq.ErrorHandlerFunc(func(ctx context.Context, task *asynq.Task, err error) {
				logger.Error("process task failed", "type", task.Type(), "payload", string(task.Payload()), "err", err)
			}),
		},
	)

	mux := asynq.NewServeMux()
	scheduler := asynq.NewScheduler(redisOpt, &asynq.SchedulerOpts{})

	// 🌟 在构造函数里直接挂载路由！一行废话都没有！
	mux.HandleFunc(activity.TaskTypeActivitySkuStockConsume, skuStockJob.ProcessTask)
	mux.HandleFunc(activity.TaskTypeActivityStateSync, stateSyncJob.ProcessTask)
	mux.HandleFunc(task.TaskTypeStrategyAwardStockConsume, strategyAwardStockJob.ProcessTask)

	// 注册定时任务
	if _, err := scheduler.Register("@every 5s", asynq.NewTask(activity.TaskTypeActivityStateSync, nil)); err != nil {
		logger.Error("Register scheduler failed", "err", err)
	}

	return &AsynqServer{
		server:    server,
		mux:       mux,
		scheduler: scheduler,
	}
}

func (s *AsynqServer) Start(ctx context.Context) error {
	logger.Info("Asynq Server starting...")
	if err := s.scheduler.Start(); err != nil {
		return fmt.Errorf("scheduler start failed: %w", err)
	}

	if err := s.server.Start(s.mux); err != nil {
		return fmt.Errorf("asynq server start failed: %w", err)
	}
	return nil
}

// 🌟 改造 2：适配 Kratos 的 Stop(ctx)
func (s *AsynqServer) Stop(ctx context.Context) error {
	logger.Info("Asynq Server stopping...")
	s.scheduler.Shutdown()
	s.server.Shutdown()
	return nil
}
