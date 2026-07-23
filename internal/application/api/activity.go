package api

import (
	"context"
	"errors"
	"fmt"
	"prizeforge/internal/domain/activity"
	"prizeforge/internal/domain/rebate"
	"prizeforge/internal/domain/strategy"
	"prizeforge/pkg/logger"
	"time"
)

// activityPartakeService 定义活动应用层创建和抢占抽奖订单所需的最小能力。
type activityPartakeService interface {
	CreateOrder(context.Context, *activity.PartakeRaffleActivity) (*activity.CreatePartakeOrder, error)
	TryClaimDraw(context.Context, string, int64, string, string) (*activity.DrawClaim, error)
	ReleaseDrawClaim(context.Context, string, int64, string, string) error
	CompleteDraw(context.Context, *activity.DrawResult, string) (*activity.DrawResultPublication, error)
}

// activityQuotaService 定义活动应用层查询和预热用户额度所需的最小能力。
type activityQuotaService interface {
	QueryActivityAccountEntity(context.Context, string, int64) (*activity.ActivityAccount, error)
	AssembleActivityAccountByUserId(context.Context, string, int64) error
}

// raffleStrategyService 定义活动抽奖编排所需的策略能力。
type raffleStrategyService interface {
	PerformRaffle(context.Context, *strategy.RaffleFactor) (*strategy.RaffleAward, error)
	QueryStrategyAward(context.Context, int64, int64) (*strategy.StrategyAward, error)
}

// drawResultPublisher 将 Redis Stream 结果可靠投递到 RabbitMQ，并等待 Broker Confirm。
type drawResultPublisher interface {
	Publish(context.Context, *activity.DrawResultPublication) error
}

// behaviorRebateService 定义活动应用层签到返利所需的最小能力。
type behaviorRebateService interface {
	CreateOrder(context.Context, *rebate.Behavior) ([]string, error)
	QueryOrderByOutBusinessNo(context.Context, string, string) ([]*rebate.BehaviorRebateOrder, error)
}

// ActivityUsecase API侧活动用例——编排多个 domain service 完成抽奖完整链路。
type ActivityUsecase struct {
	partakeSvc  activityPartakeService
	quotaSvc    activityQuotaService
	stockMgr    *activity.StockManager
	strategySvc raffleStrategyService
	resultPub   drawResultPublisher
	rebateSvc   behaviorRebateService
	now         func() time.Time
}

// NewActivityUsecase creates an ActivityUsecase with all needed domain services.
func NewActivityUsecase(
	partakeSvc activityPartakeService,
	quotaSvc activityQuotaService,
	stockMgr *activity.StockManager,
	strategySvc raffleStrategyService,
	resultPub drawResultPublisher,
	rebateSvc behaviorRebateService,
) *ActivityUsecase {
	return &ActivityUsecase{
		partakeSvc:  partakeSvc,
		quotaSvc:    quotaSvc,
		stockMgr:    stockMgr,
		strategySvc: strategySvc,
		resultPub:   resultPub,
		rebateSvc:   rebateSvc,
		now:         time.Now,
	}
}

// Draw 执行 Redis-first 抽奖链路：额度/订单预占 → 抽奖/库存预占 → 结果入 Stream
// → RabbitMQ Confirm。MySQL 订单、额度、中奖记录与发奖 Outbox 由结果消费者异步落库。
//
// 幂等边界：
//   - requestID 在 Redis Lua 中原子预占总/月/日额度，并唯一映射到 Redis 临时订单；
//   - Redis draw_state/owner 条件更新保证同一订单只有一个执行者；
//   - 奖品库存以 userID+orderID 预占；
//   - 标准结果与 Redis Stream 事件原子保存，Confirm 前不向用户返回成功。
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

	if aggregate.DrawResultPublication != nil {
		publication := aggregate.DrawResultPublication
		if publication.Result == nil {
			return 0, "", 0, activity.ErrRecordNotFound
		}
		if !publication.BrokerConfirmed {
			if err := u.publishResult(ctx, publication); err != nil {
				return 0, "", 0, err
			}
		}
		logger.Info("draw reuse Redis result", "userID", userID, "activityID", activityID, "orderID", userRaffleOrder.OrderID, "awardID", publication.Result.AwardID)
		return u.buildAwardResult(ctx, publication.Result)
	}

	// 2. 原子抢占订单执行权。同一订单同时只允许一个请求进入抽奖策略。
	claim, err := u.partakeSvc.TryClaimDraw(ctx, userID, activityID, requestID, userRaffleOrder.OrderID)
	if err != nil {
		return 0, "", 0, err
	}
	if claim == nil {
		return 0, "", 0, activity.ErrDrawInProgress
	}
	switch claim.Status {
	case activity.DrawClaimCompleted:
		if claim.Publication == nil || claim.Publication.Result == nil {
			return 0, "", 0, activity.ErrRecordNotFound
		}
		if !claim.Publication.BrokerConfirmed {
			if err := u.publishResult(ctx, claim.Publication); err != nil {
				return 0, "", 0, err
			}
		}
		return u.buildAwardResult(ctx, claim.Publication.Result)
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
		if releaseErr := u.partakeSvc.ReleaseDrawClaim(releaseCtx, userID, activityID, userRaffleOrder.OrderID, claim.Owner); releaseErr != nil {
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

	// 4. 原子保存标准结果并写入 Redis Stream。
	result := &activity.DrawResult{
		UserID:        userID,
		ActivityID:    activityID,
		ActivityName:  userRaffleOrder.ActivityName,
		StrategyID:    userRaffleOrder.StrategyID,
		OrderID:       userRaffleOrder.OrderID,
		RequestID:     requestID,
		OrderTime:     userRaffleOrder.OrderTime,
		AwardID:       int(raffleAward.AwardID),
		AwardTitle:    raffleAward.AwardTitle,
		AwardTime:     u.now(),
		StockReserved: raffleAward.StockReserved,
	}
	publication, err := u.partakeSvc.CompleteDraw(ctx, result, claim.Owner)
	if err != nil {
		releaseClaim()
		return 0, "", 0, err
	}

	// 5. 等待 RabbitMQ Publisher Confirm；失败时结果仍留在 Stream，由重试/后台补偿发布。
	if err := u.publishResult(ctx, publication); err != nil {
		return 0, "", 0, err
	}
	return u.buildAwardResult(ctx, publication.Result)
}

func (u *ActivityUsecase) publishResult(ctx context.Context, publication *activity.DrawResultPublication) error {
	if u.resultPub == nil {
		return errors.New("draw result publisher is not configured")
	}
	if err := u.resultPub.Publish(ctx, publication); err != nil {
		return fmt.Errorf("%w: publish draw result: %w", activity.ErrDrawInProgress, err)
	}
	return nil
}

func (u *ActivityUsecase) buildAwardResult(ctx context.Context, result *activity.DrawResult) (int64, string, int, error) {
	if result == nil {
		return 0, "", 0, activity.ErrRecordNotFound
	}
	awardSort := 0
	strategyAward, err := u.strategySvc.QueryStrategyAward(ctx, result.StrategyID, int64(result.AwardID))
	if err != nil {
		return 0, "", 0, err
	}
	if strategyAward != nil {
		awardSort = strategyAward.Sort
	}
	return int64(result.AwardID), result.AwardTitle, awardSort, nil
}

// CalendarSignRebate 执行签到返利。
func (u *ActivityUsecase) CalendarSignRebate(ctx context.Context, userID string) (bool, error) {
	_, err := u.rebateSvc.CreateOrder(ctx, &rebate.Behavior{
		UserID:        userID,
		BehaviorType:  rebate.Sign,
		OutBusinessNo: u.now().Format("20060102"),
	})
	if err != nil {
		return false, err
	}
	return true, nil
}

// IsCalendarSignRebate 查询用户今日是否已签到。
func (u *ActivityUsecase) IsCalendarSignRebate(ctx context.Context, userID string) (bool, error) {
	orders, err := u.rebateSvc.QueryOrderByOutBusinessNo(ctx, userID, u.now().Format("20060102"))
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
