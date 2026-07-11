package admin

import (
	"context"
	"prizeforge/internal/domain/strategy"
)

// StrategyUsecase Admin侧策略用例——只暴露管理操作
type StrategyUsecase struct {
	svc *strategy.StrategyUsecase
}

func NewStrategyUsecase(svc *strategy.StrategyUsecase) *StrategyUsecase {
	return &StrategyUsecase{svc: svc}
}

func (u *StrategyUsecase) AssembleLotteryStrategy(ctx context.Context, strategyID int64) (bool, error) {
	return u.svc.AssembleLotteryStrategy(ctx, strategyID)
}

func (u *StrategyUsecase) QueryStrategyAwardList(ctx context.Context, strategyID int64) ([]*strategy.StrategyAward, error) {
	return u.svc.QueryStrategyAwardList(ctx, strategyID)
}

func (u *StrategyUsecase) QueryAwardRuleWeightByActivityId(ctx context.Context, activityID int64, userID string) ([]*strategy.WeightBucket, int64, error) {
	buckets, err := u.svc.QueryAwardRuleWeightByActivityId(ctx, activityID)
	if err != nil {
		return nil, 0, err
	}
	// TODO: get total use count from activity domain
	return buckets, 0, nil
}
