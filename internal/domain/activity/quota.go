package activity

import (
	"context"
	"prizeforge/pkg/idgen"
	"prizeforge/pkg/logger"
	"time"
)

type ActivityQuotaUsecase struct {
	repo QuotaRepository
}

func NewActivityQuotaUsecase(repo QuotaRepository) *ActivityQuotaUsecase {
	return &ActivityQuotaUsecase{
		repo: repo,
	}
}

// CreateRaffleActivityOrder 创建抽奖活动订单
func (s *ActivityQuotaUsecase) CreateRaffleActivityOrder(ctx context.Context, skuRecharge *SkuRecharge) (string, error) {
	if skuRecharge.UserID == "" || skuRecharge.OutBusinessNo == "" {
		return "", ErrInvalidParams
	}

	activitySku, err := s.repo.QueryActivitySku(ctx, skuRecharge.Sku)
	if err != nil {
		return "", err
	}

	activity, err := s.repo.QueryRaffleActivityByActivityId(ctx, activitySku.ActivityID)
	if err != nil {
		return "", err
	}

	activityCount, err := s.repo.QueryRaffleActivityCountByActivityCountId(ctx, activitySku.ActivityCountID)
	if err != nil {
		return "", err
	}

	logger.Info("CreateRaffleActivityOrder", "activity", activity, "activityCount", activityCount)

	quotaOrder, err := s.buildOrderAggregate(skuRecharge, activitySku, activity, activityCount)
	if err != nil {
		return "", err
	}

	err = s.repo.SaveOrder(ctx, quotaOrder)
	if err != nil {
		return "", err
	}

	return quotaOrder.ActivityOrder.OrderID, nil
}

func (s *ActivityQuotaUsecase) buildOrderAggregate(skuRecharge *SkuRecharge, activitySku *ActivitySku, activity *Activity, activityCount *ActivityCount) (*CreateQuotaOrder, error) {
	orderID, err := idgen.NewOrderID()
	if err != nil {
		return nil, err
	}
	// 1. 构建订单对象
	activityOrder := &ActivityOrder{
		UserID:       skuRecharge.UserID,
		Sku:          skuRecharge.Sku,
		ActivityID:   activity.ActivityID,
		ActivityName: activity.ActivityName,
		StrategyID:   activity.StrategyID,

		OrderID:       orderID,
		OrderTime:     time.Now(),
		TotalCount:    activityCount.TotalCount,
		DayCount:      activityCount.DayCount,
		MonthCount:    activityCount.MonthCount,
		State:         ActivityOrderStateCompleted,
		OutBusinessNo: skuRecharge.OutBusinessNo,
	}

	// 2. 构建聚合对象
	return &CreateQuotaOrder{
		UserID:        skuRecharge.UserID,
		ActivityID:    activitySku.ActivityID,
		TotalCount:    activityCount.TotalCount,
		DayCount:      activityCount.DayCount,
		MonthCount:    activityCount.MonthCount,
		ActivityOrder: activityOrder,
	}, nil
}

func (s *ActivityQuotaUsecase) ClearActivitySkuStock(ctx context.Context, sku int64) error {
	return s.repo.ClearActivitySkuStock(ctx, sku)
}

func (s *ActivityQuotaUsecase) ClearQueueValue(ctx context.Context) error {
	return s.repo.ClearQueueValue(ctx)
}

func (s *ActivityQuotaUsecase) UpdateActivitySkuStock(ctx context.Context, sku int64) error {
	return s.repo.UpdateActivitySkuStock(ctx, sku)
}

func (s *ActivityQuotaUsecase) QueryActivityAccountEntity(ctx context.Context, userID string, activityID int64) (*ActivityAccount, error) {
	return s.repo.QueryActivityAccountEntity(ctx, userID, activityID)
}

func (s *ActivityQuotaUsecase) QueryRaffleActivityAccountPartakeCount(ctx context.Context, userID string, activityID int64) (int64, error) {
	return s.repo.QueryRaffleActivityAccountPartakeCount(ctx, userID, activityID)
}

func (s *ActivityQuotaUsecase) QueryRaffleActivityAccountDayPartakeCount(ctx context.Context, userID string, activityID int64) (int64, error) {
	return s.repo.QueryRaffleActivityAccountDayPartakeCount(ctx, userID, activityID)
}

func (s *ActivityQuotaUsecase) AssembleActivityAccountByActivityId(ctx context.Context, activityID int64) error {
	return s.repo.AssembleActivityAccountByActivityId(ctx, activityID)
}

func (s *ActivityQuotaUsecase) AssembleActivityAccountByUserId(ctx context.Context, userID string, activityID int64) error {
	return s.repo.AssembleActivityAccountByUserId(ctx, userID, activityID)
}
