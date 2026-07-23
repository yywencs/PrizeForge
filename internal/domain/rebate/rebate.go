package rebate

import (
	"context"
	"fmt"
	"prizeforge/pkg/idgen"
)

type BehaviorRebateUsecase struct {
	repository Repo
}

func NewBehaviorRebateUsecase(repository Repo) *BehaviorRebateUsecase {
	return &BehaviorRebateUsecase{
		repository: repository,
	}
}

func (s *BehaviorRebateUsecase) CreateOrder(ctx context.Context, behavior *Behavior) ([]string, error) {
	// 1. Query rebate configuration
	rebateConfigs, err := s.repository.QueryDailyBehaviorRebateConfig(ctx, behavior.BehaviorType)
	if err != nil {
		return nil, err
	}
	if len(rebateConfigs) == 0 {
		return []string{}, nil
	}

	// 2. Construct aggregate and orders
	agg := &BehaviorRebate{
		UserID:   behavior.UserID,
		Behavior: behavior,
	}

	orderIDs := make([]string, 0, len(rebateConfigs))
	orders := make([]*BehaviorRebateOrder, 0, len(rebateConfigs))

	for _, config := range rebateConfigs {
		orderID, err := idgen.NewOrderID()
		if err != nil {
			return nil, err
		}

		// Construct unique BizID for this specific rebate order
		// e.g. OutBusinessNo_RebateType_RebateConfig
		bizId := fmt.Sprintf("%s_%s_%s", behavior.UserID, config.RebateType, behavior.OutBusinessNo)

		order := &BehaviorRebateOrder{
			UserID:        behavior.UserID,
			OrderID:       orderID,
			BehaviorType:  config.BehaviorType,
			RebateDesc:    config.RebateDesc,
			RebateType:    config.RebateType,
			OutBusinessNo: behavior.OutBusinessNo,
			RebateConfig:  config.RebateConfig,
			BizID:         bizId,
		}
		orders = append(orders, order)
		orderIDs = append(orderIDs, orderID)
	}
	agg.BehaviorRebateOrders = orders

	// 3. Save aggregate
	if err := s.repository.SaveUserRebateOrder(ctx, behavior.UserID, agg); err != nil {
		return nil, err
	}

	return orderIDs, nil
}

func (s *BehaviorRebateUsecase) QueryOrderByOutBusinessNo(ctx context.Context, userID string, outBusinessNo string) ([]*BehaviorRebateOrder, error) {
	return s.repository.QueryUserRebateOrder(ctx, userID, outBusinessNo)
}
