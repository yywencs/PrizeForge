package strategy

import "context"

// fakeStrategyRepository 通过嵌入完整 Repo，只覆写当前测试会调用的方法。
// 未覆写方法一旦被意外调用会因 nil 嵌入接口而失败，避免测试静默放过额外交互。
type fakeStrategyRepository struct {
	Repo

	queryStrategyEntityFn        func(context.Context, int64) (*Strategy, error)
	reserveAwardStockFn          func(context.Context, string, string, int64, int64) (int64, bool, error)
	subtractionAwardStockFn      func(context.Context, int64, int64) (bool, error)
	awardStockConsumeSendQueueFn func(context.Context, string, string, int64, int64) error
}

func (f *fakeStrategyRepository) QueryStrategyEntityByStrategyId(ctx context.Context, strategyID int64) (*Strategy, error) {
	return f.queryStrategyEntityFn(ctx, strategyID)
}

func (f *fakeStrategyRepository) ReserveAwardStock(ctx context.Context, userID string, orderID string, strategyID int64, awardID int64) (int64, bool, error) {
	return f.reserveAwardStockFn(ctx, userID, orderID, strategyID, awardID)
}

func (f *fakeStrategyRepository) SubtractionAwardStock(ctx context.Context, strategyID int64, awardID int64) (bool, error) {
	return f.subtractionAwardStockFn(ctx, strategyID, awardID)
}

func (f *fakeStrategyRepository) AwardStockConsumeSendQueue(ctx context.Context, userID string, orderID string, strategyID int64, awardID int64) error {
	return f.awardStockConsumeSendQueueFn(ctx, userID, orderID, strategyID, awardID)
}
