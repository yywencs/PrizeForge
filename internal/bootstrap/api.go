package bootstrap

import (
	"prizeforge/internal/application/admin"
	"prizeforge/internal/application/api"
	strategydomain "prizeforge/internal/domain/strategy"
	"prizeforge/internal/infrastructure/adapter"
	strategyrepo "prizeforge/internal/infrastructure/repository/strategy"
	"prizeforge/pkg/config"
	"prizeforge/pkg/logger"
	"prizeforge/server"
	httpserver "prizeforge/server/http"
)

// HTTPApp holds the fully wired application dependencies.
type HTTPApp struct {
	Config               *config.Config
	APIStrategyUsecase   *api.StrategyUsecase
	AdminStrategyUsecase *admin.StrategyUsecase
}

// NewHTTPApp wires all dependencies (composition root).
// This wires the strategy domain end-to-end.
// TODO: Expand to include activity, award, rebate, task, workers, listeners.
func NewHTTPApp() *HTTPApp {
	// 1. Load configuration
	config.InitViperConfig()
	cfg := config.Conf

	// 2. Initialize logger
	logger.Init(logger.Config{
		Level:      cfg.Log.Level,
		Filename:   cfg.Log.Filename,
		MaxSize:    cfg.Log.MaxSize,
		MaxBackups: cfg.Log.MaxBackups,
		MaxAge:     cfg.Log.MaxAge,
		Compress:   cfg.Log.Compress,
	})

	// 3. Initialize infrastructure adapters
	db := adapter.NewDB(&cfg.Data.Database)
	dbRouter := adapter.NewDBRouter(&cfg.Data.Database)
	redis := adapter.NewRedisClient(&cfg.Data.Redis)

	// 4. Initialize publisher (RabbitMQ)
	conn, err := adapter.NewConnection(&cfg.RabbitMQ)
	if err != nil {
		logger.Error("failed to connect to RabbitMQ", "error", err)
		panic(err)
	}
	publisher, err := adapter.NewRabbitMQPublisher(conn)
	if err != nil {
		logger.Error("failed to create RabbitMQ publisher", "error", err)
		panic(err)
	}
	typedPublisher := adapter.NewPublisher(publisher, &cfg.RabbitMQ)
	_ = typedPublisher

	// 5. Initialize Asynq client
	asynqClient := adapter.NewAsynqClient(&cfg.Asynq)
	_ = asynqClient

	// 6. Initialize repositories
	strategyRepo := strategyrepo.NewStrategyRepository(db, redis, asynqClient, dbRouter)

	// 7. Initialize domain services
	strategySvc := strategydomain.NewStrategyUsecase(strategyRepo)

	return &HTTPApp{
		Config:               cfg,
		APIStrategyUsecase:   api.NewStrategyUsecase(strategySvc),
		AdminStrategyUsecase: admin.NewStrategyUsecase(strategySvc),
	}
}

// NewAPIServer creates the API HTTP server.
func (a *HTTPApp) NewAPIServer() httpserver.Server {
	addr := resolveAPIAddr(a.Config)
	return server.NewAPIServer(addr, a.APIStrategyUsecase, nil)
}

// NewAdminServer creates the Admin HTTP server.
func (a *HTTPApp) NewAdminServer() httpserver.Server {
	addr := resolveAdminAddr(a.Config)
	return server.NewAdminServer(addr, a.AdminStrategyUsecase)
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
