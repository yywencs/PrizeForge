package rebate

import "context"

type Repo interface {
	QueryDailyBehaviorRebateConfig(ctx context.Context, behaviorType BehaviorType) ([]*DailyBehaviorRebate, error)
	SaveUserRebateOrder(ctx context.Context, userId string, aggregate *BehaviorRebate) error
	QueryUserRebateOrder(ctx context.Context, userId string, outBusinessNo string) ([]*BehaviorRebateOrder, error)
}
