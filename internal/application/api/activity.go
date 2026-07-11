package api

import (
	"context"
	"prizeforge/internal/domain/activity"
	"prizeforge/internal/domain/award"
	"prizeforge/internal/domain/rebate"
	"prizeforge/internal/domain/strategy"
	"time"
)

// ActivityUsecase API侧活动用例——编排多个 domain service 完成抽奖完整链路。
type ActivityUsecase struct {
	partakeSvc  *activity.ActivityPartakeUsecase
	quotaSvc    *activity.ActivityQuotaUsecase
	stockMgr    *activity.StockManager
	strategySvc *strategy.StrategyUsecase
	awardSvc    *award.AwardUsecase
	rebateSvc   *rebate.BehaviorRebateUsecase
}

// NewActivityUsecase creates an ActivityUsecase with all needed domain services.
func NewActivityUsecase(
	partakeSvc *activity.ActivityPartakeUsecase,
	quotaSvc *activity.ActivityQuotaUsecase,
	stockMgr *activity.StockManager,
	strategySvc *strategy.StrategyUsecase,
	awardSvc *award.AwardUsecase,
	rebateSvc *rebate.BehaviorRebateUsecase,
) *ActivityUsecase {
	return &ActivityUsecase{
		partakeSvc:  partakeSvc,
		quotaSvc:    quotaSvc,
		stockMgr:    stockMgr,
		strategySvc: strategySvc,
		awardSvc:    awardSvc,
		rebateSvc:   rebateSvc,
	}
}

// Draw 执行完整抽奖链路：创建订单 → 执行抽奖 → 保存中奖记录。
func (u *ActivityUsecase) Draw(ctx context.Context, userID string, activityID int64) (awardID int64, awardTitle string, awardIndex int, err error) {
	// 1. 创建或复用抽奖订单
	aggregate, err := u.partakeSvc.CreateOrder(ctx, &activity.PartakeRaffleActivity{
		UserID:     userID,
		ActivityID: activityID,
	})
	if err != nil {
		return 0, "", 0, err
	}
	userRaffleOrder := aggregate.UserRaffleOrder

	// 2. 执行抽奖策略
	raffleAward, err := u.strategySvc.PerformRaffle(ctx, &strategy.RaffleFactor{
		ActivityID: activityID,
		UserID:     userID,
		StrategyID: userRaffleOrder.StrategyID,
	})
	if err != nil {
		return 0, "", 0, err
	}

	// 3. 保存中奖记录（同步写 record + task，由 Asynq 异步发奖）
	err = u.awardSvc.SaveUserAwardRecord(ctx, &award.UserAwardRecord{
		UserID:     userID,
		ActivityID: activityID,
		StrategyID: userRaffleOrder.StrategyID,
		OrderID:    userRaffleOrder.OrderID,
		AwardID:    int(raffleAward.AwardID),
		AwardTitle: raffleAward.AwardTitle,
		AwardTime:  time.Now(),
		AwardState: award.AwardStateCreate,
	})
	if err != nil {
		return 0, "", 0, err
	}

	return raffleAward.AwardID, raffleAward.AwardTitle, raffleAward.Sort, nil
}

// CalendarSignRebate 执行签到返利。
func (u *ActivityUsecase) CalendarSignRebate(ctx context.Context, userID string) (bool, error) {
	_, err := u.rebateSvc.CreateOrder(ctx, &rebate.Behavior{
		UserID:        userID,
		BehaviorType:  rebate.Sign,
		OutBusinessNo: time.Now().Format("20060102"),
	})
	if err != nil {
		return false, err
	}
	return true, nil
}

// IsCalendarSignRebate 查询用户今日是否已签到。
func (u *ActivityUsecase) IsCalendarSignRebate(ctx context.Context, userID string) (bool, error) {
	orders, err := u.rebateSvc.QueryOrderByOutBusinessNo(ctx, userID, time.Now().Format("20060102"))
	if err != nil {
		return false, err
	}
	return len(orders) > 0, nil
}

// QueryUserActivityAccount 查询用户活动账户。
func (u *ActivityUsecase) QueryUserActivityAccount(ctx context.Context, userID string, activityID int64) (*activity.ActivityAccount, error) {
	return u.quotaSvc.QueryActivityAccountEntity(ctx, userID, activityID)
}

// LoadUserActivityAccount 加载用户活动账户到缓存。
func (u *ActivityUsecase) LoadUserActivityAccount(ctx context.Context, userID string, activityID int64) error {
	return u.quotaSvc.AssembleActivityAccountByUserId(ctx, userID, activityID)
}
