package api

import (
	"context"
	"prizeforge/internal/domain/activity"
	"prizeforge/internal/domain/award"
	"prizeforge/internal/domain/rebate"
	"prizeforge/internal/domain/strategy"
	"prizeforge/pkg/logger"
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
//
// 幂等边界：
//   - requestID 唯一映射到数据库抽奖订单，第一次事务同步扣额度并创建订单；
//   - draw_state 条件更新保证同一订单只有一个执行者；
//   - 奖品库存以 userID+orderID 预占；
//   - 第二次事务同步保存标准中奖结果、发奖 task、库存同步 task 和订单成功状态。
func (u *ActivityUsecase) Draw(ctx context.Context, userID string, activityID int64, requestID string) (awardID int64, awardTitle string, awardIndex int, err error) {
	// 1. 创建或复用抽奖订单
	aggregate, err := u.partakeSvc.CreateOrder(ctx, &activity.PartakeRaffleActivity{
		UserID:     userID,
		ActivityID: activityID,
		RequestID:  requestID,
	})
	if err != nil {
		return 0, "", 0, err
	}
	userRaffleOrder := aggregate.UserRaffleOrder

	// 幂等预查：仅在复用老订单时，查这笔订单是否已抽过并落库
	if aggregate.Reused {
		existing, qerr := u.awardSvc.QueryByOrderID(ctx, userID, userRaffleOrder.OrderID)
		if qerr != nil {
			// 查询失败必须 fail closed；继续重抽会重复扣减奖品库存。
			return 0, "", 0, qerr
		} else if existing != nil {
			logger.Info("draw reuse existing award record", "userID", userID, "activityID", activityID, "orderID", userRaffleOrder.OrderID, "awardID", existing.AwardID)
			return u.buildAwardResult(ctx, userRaffleOrder.StrategyID, existing)
		}
	}

	// 2. 原子抢占订单执行权。同一订单同时只允许一个请求进入抽奖策略。
	claim, err := u.partakeSvc.TryClaimDraw(ctx, userID, userRaffleOrder.OrderID)
	if err != nil {
		return 0, "", 0, err
	}
	if claim == nil {
		return 0, "", 0, activity.ErrDrawInProgress
	}
	switch claim.Status {
	case activity.DrawClaimCompleted:
		existing, queryErr := u.awardSvc.QueryByOrderID(ctx, userID, userRaffleOrder.OrderID)
		if queryErr != nil {
			return 0, "", 0, queryErr
		}
		if existing == nil {
			return 0, "", 0, activity.ErrRecordNotFound
		}
		return u.buildAwardResult(ctx, userRaffleOrder.StrategyID, existing)
	case activity.DrawClaimProcessing:
		// 并发请求不允许重抽；客户端可携带同一 request_id 重试。
		return 0, "", 0, activity.ErrDrawInProgress
	case activity.DrawClaimCancelled:
		return 0, "", 0, activity.ErrDrawCancelled
	case activity.DrawClaimAcquired:
		// 当前请求负责完成该订单。
	default:
		return 0, "", 0, activity.ErrDrawInProgress
	}

	releaseClaim := func() {
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		if releaseErr := u.partakeSvc.ReleaseDrawClaim(releaseCtx, userID, userRaffleOrder.OrderID, claim.Owner); releaseErr != nil {
			logger.Warn("release draw claim failed", "orderID", userRaffleOrder.OrderID, "err", releaseErr)
		}
	}

	// 3. 执行抽奖策略
	raffleAward, err := u.strategySvc.PerformRaffle(ctx, &strategy.RaffleFactor{
		ActivityID: activityID,
		UserID:     userID,
		StrategyID: userRaffleOrder.StrategyID,
		OrderID:    userRaffleOrder.OrderID,
	})
	if err != nil {
		releaseClaim()
		return 0, "", 0, err
	}

	// 4. 第二次同步事务：保存标准中奖结果、发奖 task，并将订单更新为 success。
	savedRecord, err := u.awardSvc.SaveUserAwardRecord(ctx, &award.UserAwardRecord{
		UserID:        userID,
		ActivityID:    activityID,
		StrategyID:    userRaffleOrder.StrategyID,
		OrderID:       userRaffleOrder.OrderID,
		AwardID:       int(raffleAward.AwardID),
		AwardTitle:    raffleAward.AwardTitle,
		AwardTime:     time.Now(),
		AwardState:    award.AwardStateCreate,
		StockReserved: raffleAward.StockReserved,
		DrawOwner:     claim.Owner,
	})
	if err != nil {
		releaseClaim()
		return 0, "", 0, err
	}

	return u.buildAwardResult(ctx, userRaffleOrder.StrategyID, savedRecord)
}

func (u *ActivityUsecase) buildAwardResult(ctx context.Context, strategyID int64, record *award.UserAwardRecord) (int64, string, int, error) {
	if record == nil {
		return 0, "", 0, activity.ErrRecordNotFound
	}
	awardSort := 0
	strategyAward, err := u.strategySvc.QueryStrategyAward(ctx, strategyID, int64(record.AwardID))
	if err != nil {
		return 0, "", 0, err
	}
	if strategyAward != nil {
		awardSort = strategyAward.Sort
	}
	return int64(record.AwardID), record.AwardTitle, awardSort, nil
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
