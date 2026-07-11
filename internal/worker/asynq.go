package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"prizeforge/internal/domain/activity"
	"prizeforge/internal/domain/task"
	"prizeforge/internal/job"
	"prizeforge/internal/metrics"
	"prizeforge/pkg/config"
	"prizeforge/pkg/logger"

	"github.com/hibiken/asynq"
)

// AsynqWorker wraps the Asynq task queue server.
type AsynqWorker struct {
	server    *asynq.Server
	mux       *asynq.ServeMux
	scheduler *asynq.Scheduler
	inspector *asynq.Inspector
	queues    []string
}

// NewAsynqWorker creates an Asynq worker server and registers all task handlers.
func NewAsynqWorker(
	cfg *config.AsynqConfig,
	skuStockJob *job.ActivitySkuStockConsumeJob,
	stateSyncJob *job.SendAwardMessage,
	strategyAwardStockJob *job.StrategyAwardStockConsumeJob,
) *AsynqWorker {
	redisOpt := asynq.RedisClientOpt{
		Addr: fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port),
		DB:   cfg.Redis.DB,
	}

	server := asynq.NewServer(
		redisOpt,
		asynq.Config{
			Concurrency: cfg.Concurrency,
			Queues: map[string]int{
				activity.TaskTypeActivitySkuStockConsume: 6,
				"default":                                3,
				"low":                                    1,
			},
			ErrorHandler: asynq.ErrorHandlerFunc(func(ctx context.Context, task *asynq.Task, err error) {
				result := asynqTaskResult(err)
				metrics.IncAsynqTask(task.Type(), result)
				logger.Error("process task failed", "type", task.Type(), "payload", string(task.Payload()), "err", err)
			}),
		},
	)

	mux := asynq.NewServeMux()
	scheduler := asynq.NewScheduler(redisOpt, &asynq.SchedulerOpts{})
	inspector := asynq.NewInspector(redisOpt)
	queues := []string{
		activity.TaskTypeActivitySkuStockConsume,
		"default",
		"low",
	}

	// 注册任务处理器
	mux.HandleFunc(activity.TaskTypeActivitySkuStockConsume, wrapAsynqHandler(activity.TaskTypeActivitySkuStockConsume, skuStockJob.ProcessTask))
	mux.HandleFunc(activity.TaskTypeActivityStateSync, wrapAsynqHandler(activity.TaskTypeActivityStateSync, stateSyncJob.ProcessTask))
	mux.HandleFunc(task.TaskTypeStrategyAwardStockConsume, wrapAsynqHandler(task.TaskTypeStrategyAwardStockConsume, strategyAwardStockJob.ProcessTask))

	// 注册定时任务：每 5 秒扫描 task 表并投递消息
	if _, err := scheduler.Register("@every 5s", asynq.NewTask(activity.TaskTypeActivityStateSync, nil)); err != nil {
		logger.Error("register scheduler failed", "err", err)
	}

	return &AsynqWorker{
		server:    server,
		mux:       mux,
		scheduler: scheduler,
		inspector: inspector,
		queues:    queues,
	}
}

// Start 启动 Asynq worker。
func (w *AsynqWorker) Start(ctx context.Context) error {
	logger.Info("Asynq Worker starting...")
	w.startQueueMetricsCollector(ctx)

	if err := w.scheduler.Start(); err != nil {
		return fmt.Errorf("scheduler start failed: %w", err)
	}

	if err := w.server.Start(w.mux); err != nil {
		return fmt.Errorf("asynq server start failed: %w", err)
	}
	return nil
}

// Shutdown 优雅停止 Asynq worker。
func (w *AsynqWorker) Shutdown() {
	logger.Info("Asynq Worker stopping...")
	w.scheduler.Shutdown()
	w.server.Shutdown()
	if w.inspector != nil {
		_ = w.inspector.Close()
	}
}

// wrapAsynqHandler 包装任务处理函数，添加耗时和成功指标。
func wrapAsynqHandler(taskType string, handler func(context.Context, *asynq.Task) error) func(context.Context, *asynq.Task) error {
	return func(ctx context.Context, task *asynq.Task) error {
		start := time.Now()
		err := handler(ctx, task)
		metrics.ObserveAsynqTaskDuration(taskType, time.Since(start))
		if err == nil {
			metrics.IncAsynqTask(taskType, "success")
		}
		return err
	}
}

func (w *AsynqWorker) startQueueMetricsCollector(ctx context.Context) {
	go func() {
		w.collectQueueMetrics()

		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				w.collectQueueMetrics()
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (w *AsynqWorker) collectQueueMetrics() {
	if w.inspector == nil {
		return
	}

	queueSet := make(map[string]struct{}, len(w.queues))
	for _, queue := range w.queues {
		queueSet[queue] = struct{}{}
	}

	queues, err := w.inspector.Queues()
	if err == nil {
		for _, queue := range queues {
			queueSet[queue] = struct{}{}
		}
	}

	for queue := range queueSet {
		info, err := w.inspector.GetQueueInfo(queue)
		if err != nil {
			if errors.Is(err, asynq.ErrQueueNotFound) {
				metrics.SetAsynqQueueStats(queue, 0, 0, 0)
			}
			continue
		}
		metrics.SetAsynqQueueStats(queue, info.Size, info.Retry, info.Scheduled)
	}
}

func asynqTaskResult(err error) string {
	if errors.Is(err, asynq.SkipRetry) {
		return "skip_retry"
	}
	return "error"
}
