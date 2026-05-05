package service

import (
	v1 "big-market-kratos/api/bigmarket/v1"
	"big-market-kratos/internal/biz/activity"
	"big-market-kratos/internal/biz/award"
	"big-market-kratos/internal/biz/rebate"
	"big-market-kratos/internal/biz/strategy"
	"context"
	"time"

	kerrors "github.com/go-kratos/kratos/v2/errors"
)

type ActivityService struct {
	v1.UnimplementedActivityServer
	stockManager          *activity.StockManager
	partakeUsecase        *activity.ActivityPartakeUsecase
	quotaUsecase          *activity.ActivityQuotaUsecase
	strategyUsecase       *strategy.StrategyUsecase
	awardUsecase          *award.AwardUsecase
	behaviorRebateUsecase *rebate.BehaviorRebateUsecase
}

func NewActivityService(stockManager *activity.StockManager, partakeUsecase *activity.ActivityPartakeUsecase, quotaUsecase *activity.ActivityQuotaUsecase, strategyUsecase *strategy.StrategyUsecase, awardUsecase *award.AwardUsecase, behaviorRebateUsecase *rebate.BehaviorRebateUsecase) *ActivityService {
	return &ActivityService{
		stockManager:          stockManager,
		partakeUsecase:        partakeUsecase,
		quotaUsecase:          quotaUsecase,
		strategyUsecase:       strategyUsecase,
		awardUsecase:          awardUsecase,
		behaviorRebateUsecase: behaviorRebateUsecase,
	}
}

func (s *ActivityService) RaffleActivityArmory(ctx context.Context, req *v1.RaffleActivityArmoryRequest) (*v1.RaffleActivityArmoryReply, error) {
	if req.GetActivityId() <= 0 {
		return nil, kerrors.BadRequest("INVALID_ACTIVITY_ID", "invalid activity_id")
	}
	activityReady, err := s.stockManager.AssembleActivitySkuByActivityId(ctx, req.GetActivityId())
	if err != nil {
		return nil, err
	}
	if !activityReady {
		return &v1.RaffleActivityArmoryReply{Success: false}, nil
	}
	strategyReady, err := s.strategyUsecase.AssembleLotteryStrategyByActivityId(ctx, req.GetActivityId())
	if err != nil {
		return nil, err
	}

	// 装配用户额度到缓存
	err = s.quotaUsecase.AssembleActivityAccountByActivityId(ctx, req.GetActivityId())
	if err != nil {
		return nil, err
	}

	return &v1.RaffleActivityArmoryReply{Success: strategyReady}, nil
}

func (s *ActivityService) Draw(ctx context.Context, req *v1.DrawRequest) (*v1.DrawReply, error) {
	if req.GetUserId() == "" || req.GetActivityId() <= 0 {
		return nil, kerrors.BadRequest("INVALID_DRAW_PARAMS", "invalid user_id or activity_id")
	}
	createPartakeOrderAggregate, err := s.partakeUsecase.CreateOrder(ctx, &activity.PartakeRaffleActivity{
		UserID:     req.GetUserId(),
		ActivityID: req.GetActivityId(),
	})
	if err != nil {
		return nil, err
	}
	userRaffleOrder := createPartakeOrderAggregate.UserRaffleOrder

	raffleAward, err := s.strategyUsecase.PerformRaffle(ctx, &strategy.RaffleFactor{
		UserID:     req.GetUserId(),
		StrategyID: userRaffleOrder.StrategyID,
	})
	if err != nil {
		return nil, err
	}

	// 同步记录中奖事实和 outbox task，再由后台任务异步发奖。
	err = s.awardUsecase.SaveUserAwardRecord(ctx, &award.UserAwardRecord{
		UserID:     req.GetUserId(),
		ActivityID: req.GetActivityId(),
		StrategyID: userRaffleOrder.StrategyID,
		OrderID:    userRaffleOrder.OrderID,
		AwardID:    int(raffleAward.AwardID),
		AwardTitle: raffleAward.AwardTitle,
		AwardTime:  time.Now(),
		AwardState: award.AwardStateCreate,
	})
	if err != nil {
		return nil, err
	}

	return &v1.DrawReply{
		AwardId:    raffleAward.AwardID,
		AwardTitle: raffleAward.AwardTitle,
		AwardIndex: int32(raffleAward.Sort),
	}, nil
}

func (s *ActivityService) CalendarSignRebate(ctx context.Context, req *v1.CalendarSignRebateRequest) (*v1.CalendarSignRebateReply, error) {
	if req.GetUserId() == "" {
		return nil, kerrors.BadRequest("INVALID_USER_ID", "invalid user_id")
	}
	_, err := s.behaviorRebateUsecase.CreateOrder(ctx, &rebate.Behavior{
		UserID:        req.GetUserId(),
		BehaviorType:  rebate.Sign,
		OutBusinessNo: time.Now().Format("20060102"),
	})
	if err != nil {
		return nil, err
	}
	return &v1.CalendarSignRebateReply{Success: true}, nil
}

func (s *ActivityService) IsCalendarSignRebate(ctx context.Context, req *v1.IsCalendarSignRebateRequest) (*v1.IsCalendarSignRebateReply, error) {
	if req.GetUserId() == "" {
		return nil, kerrors.BadRequest("INVALID_USER_ID", "invalid user_id")
	}
	orders, err := s.behaviorRebateUsecase.QueryOrderByOutBusinessNo(ctx, req.GetUserId(), time.Now().Format("20060102"))
	if err != nil {
		return nil, err
	}
	return &v1.IsCalendarSignRebateReply{IsSigned: len(orders) > 0}, nil
}

func (s *ActivityService) QueryUserActivityAccount(ctx context.Context, req *v1.QueryUserActivityAccountRequest) (*v1.QueryUserActivityAccountReply, error) {
	if req.GetUserId() == "" || req.GetActivityId() <= 0 {
		return nil, kerrors.BadRequest("INVALID_ACCOUNT_QUERY_PARAMS", "invalid user_id or activity_id")
	}
	account, err := s.quotaUsecase.QueryActivityAccountEntity(ctx, req.GetUserId(), req.GetActivityId())
	if err != nil {
		return nil, err
	}
	return &v1.QueryUserActivityAccountReply{
		ActivityId:        account.ActivityID,
		TotalCount:        int64(account.TotalCount),
		TotalCountSurplus: int64(account.TotalCountSurplus),
		DayCount:          int64(account.DayCount),
		DayCountSurplus:   int64(account.DayCountSurplus),
		MonthCount:        int64(account.MonthCount),
		MonthCountSurplus: int64(account.MonthCountSurplus),
	}, nil
}
