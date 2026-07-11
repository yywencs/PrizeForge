package data

import (
	confpb "big-market-kratos/internal/conf"

	activitydata "big-market-kratos/internal/data/activity"
	awarddata "big-market-kratos/internal/data/award"
	"big-market-kratos/internal/data/infra"
	rebatedata "big-market-kratos/internal/data/rebate"
	strategydata "big-market-kratos/internal/data/strategy"
	taskdata "big-market-kratos/internal/data/task"

	"github.com/google/wire"
)

var ProviderSet = wire.NewSet(
	infra.NewDB,
	infra.NewDBRouter,
	infra.NewRedisClient,
	infra.NewConnection,
	infra.NewRabbitMQPublisher,
	infra.NewPublisher,
	infra.NewAsynqClient,
	infra.NewAsynqInspector,
	strategydata.NewStrategyRepository,
	activitydata.NewRepository,
	awarddata.NewUserAwardRecordRepository,
	taskdata.NewTaskRepository,
	rebatedata.NewRebateRepository,
	NewMysqlConfig,
	NewRedisConfig,
	NewTBCount,
)

func NewMysqlConfig(c *confpb.Data) *confpb.Data_Mysql {
	return c.Mysql
}

func NewRedisConfig(c *confpb.Data) *confpb.Data_Redis {
	return c.Redis
}

func NewTBCount() int {
	return 2
}
