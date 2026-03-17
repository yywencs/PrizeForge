package biz

import (
	"big-market-kratos/internal/biz/activity"
	"big-market-kratos/internal/biz/award"
	"big-market-kratos/internal/biz/rebate"
	"big-market-kratos/internal/biz/strategy"
	"big-market-kratos/internal/biz/task"

	"github.com/google/wire"
)

// ProviderSet is biz providers.
var ProviderSet = wire.NewSet(
	activity.NewActivityQuotaUsecase,
	activity.NewActivityPartakeUsecase,
	activity.NewStockManager,
	activity.NewFactory,
	award.NewAwardUsecase,
	rebate.NewBehaviorRebateUsecase,
	strategy.NewStrategyUsecase, // 咱们之前一起设计的那个指挥官
	task.NewTaskUsecase,
)
