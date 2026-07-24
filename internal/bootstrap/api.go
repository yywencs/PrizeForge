package bootstrap

import (
	"context"
	"errors"
	"fmt"

	"prizeforge/internal/application/admin"
	"prizeforge/internal/application/api"
	"prizeforge/internal/domain/activity"
	"prizeforge/internal/domain/award"
	"prizeforge/internal/domain/rebate"
	"prizeforge/internal/domain/strategy"
	"prizeforge/internal/domain/task"
	"prizeforge/internal/infrastructure/adapter"
	"prizeforge/internal/infrastructure/repository/activityrepo"
	"prizeforge/internal/infrastructure/repository/awardrepo"
	"prizeforge/internal/infrastructure/repository/rebaterepo"
	"prizeforge/internal/infrastructure/repository/strategyrepo"
	"prizeforge/internal/infrastructure/repository/taskrepo"
	"prizeforge/internal/job"
	"prizeforge/internal/listener"
	"prizeforge/internal/worker"
	"prizeforge/pkg/cache"
	"prizeforge/pkg/config"
	"prizeforge/pkg/logger"
	httpserver "prizeforge/server/http"
	adminhttp "prizeforge/server/http/admin"
	apihttp "prizeforge/server/http/api"
	"prizeforge/server/http/common"

	"github.com/hibiken/asynq"
	"gorm.io/gorm"
)

// HTTPApp holds the wired application dependencies.
//
// 区分两个入口：
//   - NewAdminApp：只装配 admin HTTP 所需的最小依赖（strategy 链 + admin server）。
//     不建 RabbitMQ 连接、不建 Asynq worker / RabbitMQ consumer，asynqWorker/rabbitMQConsumer 为 nil。
//   - NewAPIApp：装配 API HTTP 全链路 + Asynq worker + RabbitMQ consumer。
//
// 这样 cmd/admin 在没有 RabbitMQ 的环境也能正常启动；
// cmd/api 用 NewAPIApp 并通过 AsynqWorker()/RabbitMQConsumer() 取出 worker/consumer 启动。
type HTTPApp struct {
	Config *config.Config

	// HTTP servers
	apiServer   httpserver.Server
	adminServer httpserver.Server

	// Async workers（仅 NewAPIApp 路径填充）
	asynqWorker      *worker.AsynqWorker
	rabbitMQConsumer *listener.RabbitMQConsumer
}

// baseDeps 是 NewAdminApp 与 NewAPIApp 共享的基础设施：config、logger、db、redis、asynq client。
// strategy 链（strategy repo + domain service）也为两端共用，因为 admin 依赖它。
type baseDeps struct {
	cfg         *config.Config
	dbRouter    *adapter.DBRouter
	gormDB      *gorm.DB
	redis       *cache.Cache
	asynqClient *asynq.Client
	strategySvc *strategy.StrategyUsecase
}

// loadBase 初始化配置、logger、db、redis、asynq client 以及 strategy 链。
// 不触碰 RabbitMQ；构造 asynq client 不触发 eager 拨号，admin 只读不会入队。
//
// 注：此处 db 用的是 adapter 提供的 *gorm.DB / DBRouter，asynq client 用 *asynq.Client，
// 具体类型随 adapter 包内工厂函数返回值而定。
func loadBase() (*baseDeps, error) {
	config.InitViperConfig()
	cfg := config.Conf

	logger.Init(logger.Config{
		Level:      cfg.Log.Level,
		Filename:   cfg.Log.Filename,
		MaxSize:    cfg.Log.MaxSize,
		MaxBackups: cfg.Log.MaxBackups,
		MaxAge:     cfg.Log.MaxAge,
		Compress:   cfg.Log.Compress,
	})

	gormDB := adapter.NewDB(&cfg.Data.Database)
	dbRouter := adapter.NewDBRouter(&cfg.Data.Database)
	redis := adapter.NewRedisClient(&cfg.Data.Redis)
	asynqClient := adapter.NewAsynqClient(&cfg.Asynq)

	strategyRepo := strategyrepo.NewStrategyRepository(gormDB, redis, asynqClient, dbRouter)
	strategySvc := strategy.NewStrategyUsecase(strategyRepo)

	return &baseDeps{
		cfg:         cfg,
		dbRouter:    dbRouter,
		gormDB:      gormDB,
		redis:       redis,
		asynqClient: asynqClient,
		strategySvc: strategySvc,
	}, nil
}

// NewAdminApp 装配仅运营后台所需依赖：strategy 链 + admin HTTP server。
// 不建 RabbitMQ 连接、不建 Asynq worker / RabbitMQ consumer，
// 因此 admin 进程可在无 RabbitMQ / 无 Asynq worker 的环境启动。
func NewAdminApp() *HTTPApp {
	b, err := loadBase()
	if err != nil {
		panic(err)
	}
	cfg := b.cfg

	adminStrategyUsecase := admin.NewStrategyUsecase(b.strategySvc)
	adminServer := adminhttp.NewServer(resolveAdminAddr(cfg), adminStrategyUsecase, baseReadinessChecks(b))

	return &HTTPApp{
		Config:      cfg,
		adminServer: adminServer,
	}
}

// NewAPIApp 装配 API HTTP 全链路：RabbitMQ 连接 + 全部 repo + 全部 domain service
// + api usecase + api server + Asynq worker + RabbitMQ consumer。
// RabbitMQ 不可达时返回 error，交由 cmd/api 决定是否 fatal。
func NewAPIApp() (*HTTPApp, error) {
	b, err := loadBase()
	if err != nil {
		return nil, err
	}
	cfg := b.cfg
	dbRouter := b.dbRouter
	gormDB := b.gormDB
	redis := b.redis
	asynqClient := b.asynqClient
	strategySvc := b.strategySvc

	if err := cfg.RabbitMQ.Topic.Validate(); err != nil {
		return nil, fmt.Errorf("rabbitmq topic config: %w", err)
	}

	// RabbitMQ 连接 + publisher（API 链路强依赖）
	conn, err := adapter.NewConnection(&cfg.RabbitMQ)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: %w", err)
	}
	rabbitPublisher, err := adapter.NewRabbitMQPublisher(conn)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq publisher: %w", err)
	}
	typedPublisher := adapter.NewPublisher(rabbitPublisher, &cfg.RabbitMQ)

	asynqInspector := adapter.NewAsynqInspector(&cfg.Asynq)

	// repo（除 strategy 已在 loadBase 构造）
	activityRepo := activityrepo.NewRepository(dbRouter, gormDB, redis, typedPublisher, asynqClient, asynqInspector)
	awardRepo := awardrepo.NewUserAwardRecordRepository(dbRouter, redis, typedPublisher)
	taskRepo := taskrepo.NewTaskRepository(dbRouter, typedPublisher)
	rebateRepo := rebaterepo.NewRebateRepository(gormDB, dbRouter, typedPublisher)

	// domain service
	activityPartakeSvc := activity.NewActivityPartakeUsecase(activityRepo)
	activityQuotaSvc := activity.NewActivityQuotaUsecase(activityRepo)
	stockManager := activity.NewStockManager(activityRepo)
	awardSvc := award.NewAwardUsecase(awardRepo)
	taskSvc := task.NewTaskUsecase(taskRepo)
	rebateSvc := rebate.NewBehaviorRebateUsecase(rebateRepo)

	// Asynq jobs
	skuStockJob := job.NewActivitySkuStockConsumeJob(activityQuotaSvc)
	dbCount := cfg.Data.Database.DbCount
	sendAwardMsgJob := job.NewSendAwardMessage(taskSvc, strategySvc, dbCount)
	strategyAwardStockJob := job.NewStrategyAwardStockConsumeJob(strategySvc)
	drawResultPublisher := job.NewDrawResultPublisher(activityPartakeSvc, typedPublisher)
	drawResultRecoveryJob := job.NewDrawResultRecoveryJob(activityPartakeSvc, drawResultPublisher)

	// RabbitMQ listeners
	stockListener := listener.NewActivityStockListener(activityQuotaSvc)
	rebateListener := listener.NewRebateListener(activityQuotaSvc)
	drawResultListener := listener.NewDrawResultListener(activityPartakeSvc)
	sendAwardListener := listener.NewSendAwardListener(awardSvc)

	// Asynq worker + RabbitMQ consumer
	asynqWorker := worker.NewAsynqWorker(&cfg.Asynq, skuStockJob, sendAwardMsgJob, strategyAwardStockJob, drawResultRecoveryJob)
	rabbitMQConsumer := listener.NewRabbitMQConsumer(
		conn,
		listener.WithPrefetch(cfg.RabbitMQ.Listener.Simple.Prefetch),
		listener.WithDefaultConcurrency(cfg.RabbitMQ.Listener.Simple.DefaultConcurrency),
		listener.WithQueueConcurrency(cfg.RabbitMQ.Listener.Simple.Concurrency),
	)
	rabbitMQConsumer.RegisterListener(cfg.RabbitMQ.Topic.ActivitySkuStockZero, stockListener)
	rabbitMQConsumer.RegisterListener(cfg.RabbitMQ.Topic.SendRebate, rebateListener)
	rabbitMQConsumer.RegisterListener(cfg.RabbitMQ.Topic.DrawResult, drawResultListener)
	rabbitMQConsumer.RegisterListener(cfg.RabbitMQ.Topic.SendAward, sendAwardListener)

	// application usecases
	apiStrategyUsecase := api.NewStrategyUsecase(strategySvc)
	apiActivityUsecase := api.NewActivityUsecase(activityPartakeSvc, activityQuotaSvc, stockManager, strategySvc, drawResultPublisher, rebateSvc)

	readinessChecks := baseReadinessChecks(b)
	readinessChecks["asynq_redis"] = func(context.Context) error {
		return asynqClient.Ping()
	}
	readinessChecks["rabbitmq"] = func(context.Context) error {
		if conn.IsClosed() {
			return errors.New("rabbitmq connection is closed")
		}
		return nil
	}
	apiServer := apihttp.NewServer(resolveAPIAddr(cfg), apiStrategyUsecase, apiActivityUsecase, readinessChecks)

	return &HTTPApp{
		Config:           cfg,
		apiServer:        apiServer,
		asynqWorker:      asynqWorker,
		rabbitMQConsumer: rabbitMQConsumer,
	}, nil
}

// APIServer returns the API HTTP server.
func (a *HTTPApp) APIServer() httpserver.Server { return a.apiServer }

// AdminServer returns the Admin HTTP server.
func (a *HTTPApp) AdminServer() httpserver.Server { return a.adminServer }

// AsynqWorker returns the Asynq worker（仅 NewAPIApp 路径非 nil）。
func (a *HTTPApp) AsynqWorker() *worker.AsynqWorker { return a.asynqWorker }

// RabbitMQConsumer returns the RabbitMQ consumer（仅 NewAPIApp 路径非 nil）。
func (a *HTTPApp) RabbitMQConsumer() *listener.RabbitMQConsumer { return a.rabbitMQConsumer }

func baseReadinessChecks(b *baseDeps) common.ReadinessChecks {
	return common.ReadinessChecks{
		"mysql": func(ctx context.Context) error {
			sqlDB, err := b.gormDB.DB()
			if err != nil {
				return fmt.Errorf("get default database: %w", err)
			}
			if err := sqlDB.PingContext(ctx); err != nil {
				return fmt.Errorf("ping default database: %w", err)
			}
			return b.dbRouter.Ping(ctx)
		},
		"redis": b.redis.Ping,
	}
}

func resolveAPIAddr(cfg *config.Config) string {
	if cfg != nil && cfg.Server.API.Addr != "" {
		return cfg.Server.API.Addr
	}
	if cfg != nil && cfg.Server.Http.Addr != "" {
		return cfg.Server.Http.Addr
	}
	return ":8080"
}

func resolveAdminAddr(cfg *config.Config) string {
	if cfg != nil && cfg.Server.Admin.Addr != "" {
		return cfg.Server.Admin.Addr
	}
	return ":8081"
}
