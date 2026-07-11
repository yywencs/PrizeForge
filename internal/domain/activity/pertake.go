package activity

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"prizeforge/internal/metrics"
	"time"
)

type ActivityPartakeUsecase struct {
	repo Repo
}

func NewActivityPartakeUsecase(repo Repo) *ActivityPartakeUsecase {
	return &ActivityPartakeUsecase{repo: repo}
}

func (s *ActivityPartakeUsecase) CreateOrder(ctx context.Context, partake *PartakeRaffleActivity) (*CreatePartakeOrder, error) {
	userID, activityID := partake.UserID, partake.ActivityID
	currentTime := time.Now()

	activity, err := s.repo.QueryRaffleActivity(ctx, activityID)
	if err != nil {
		return nil, err
	}

	// 检验活动状态
	if activity.State != ActivityStateOpen {
		return nil, ErrActivityStateError
	}

	// 检验活动时间
	if currentTime.Before(activity.BeginDateTime) || currentTime.After(activity.EndDateTime) {
		return nil, ErrActivityTimeError
	}

	createPartakeOrderAggregate, err := s.doFilterAccount(ctx, userID, activityID, currentTime)
	if err != nil {
		return nil, err
	}

	newOrder := s.buildUserRaffleOrder(userID, activity, currentTime)
	userRaffleOrder, reused, err := s.repo.CacheGetOrCreateNoUsedRaffleOrder(ctx, newOrder)
	if err != nil {
		return nil, err
	}
	if reused {
		return &CreatePartakeOrder{
			UserID:          userID,
			ActivityID:      activityID,
			UserRaffleOrder: userRaffleOrder,
		}, nil
	}

	createPartakeOrderAggregate.UserRaffleOrder = userRaffleOrder

	// 先同步写入最小化抽奖订单，确保后续抽奖和发奖都有可靠订单根。
	err = s.repo.SaveLiteUserRaffleOrder(ctx, createPartakeOrderAggregate)
	if err != nil {
		return nil, err
	}

	return createPartakeOrderAggregate, nil
}

func (s *ActivityPartakeUsecase) SaveOrderRecord(ctx context.Context, aggregate *CreatePartakeOrder) error {
	return s.repo.SaveCreatePartakeOrderAggregate(ctx, aggregate)
}

func (s *ActivityPartakeUsecase) doFilterAccount(ctx context.Context, userID string, activityID int64, currentTime time.Time) (aggregate *CreatePartakeOrder, err error) {
	defer func() {
		metrics.IncActivityQuota(activityID, quotaCheckResultFromErr(err))
	}()

	// 检验用户是否已参与活动
	account, err := s.repo.QueryActivityAccount(ctx, userID, activityID)
	if err != nil {
		return nil, err
	}
	if account == nil || account.TotalCountSurplus <= 0 {
		return nil, ErrActivityQuotaError
	}

	day, month := currentTime.Format("2006-01-02"), currentTime.Format("2006-01")

	dayAccount, err := s.repo.QueryActivityAccountDay(ctx, userID, activityID, day)
	if err != nil {
		return nil, err
	}
	if dayAccount != nil && dayAccount.DayCountSurplus <= 0 {
		return nil, ErrActivityAccountDayCountSurplusNotEnough
	}

	isExistAccountDay := dayAccount != nil

	if dayAccount == nil {
		dayAccount = &ActivityAccountDay{
			UserID:          userID,
			ActivityID:      activityID,
			Day:             day,
			DayCount:        account.DayCount,
			DayCountSurplus: account.DayCountSurplus,
		}
	}

	monthAccount, err := s.repo.QueryActivityAccountMonth(ctx, userID, activityID, month)
	if err != nil {
		return nil, err
	}
	if monthAccount != nil && monthAccount.MonthCountSurplus <= 0 {
		return nil, ErrActivityAccountMonthCountSurplusNotEnough
	}

	isExistAccountMonth := monthAccount != nil

	if monthAccount == nil {
		monthAccount = &ActivityAccountMonth{
			UserID:            userID,
			ActivityID:        activityID,
			Month:             month,
			MonthCount:        account.MonthCount,
			MonthCountSurplus: account.MonthCountSurplus,
		}
	}

	return &CreatePartakeOrder{
		UserID:               userID,
		ActivityID:           activityID,
		ActivityAccount:      account,
		ActivityAccountDay:   dayAccount,
		ActivityAccountMonth: monthAccount,
		IsExistAccountDay:    isExistAccountDay,
		IsExistAccountMonth:  isExistAccountMonth,
	}, nil
}

func quotaCheckResultFromErr(err error) string {
	switch {
	case err == nil:
		return "success"
	case errors.Is(err, ErrActivityQuotaError):
		return "total_quota_exhausted"
	case errors.Is(err, ErrActivityAccountDayCountSurplusNotEnough):
		return "day_quota_exhausted"
	case errors.Is(err, ErrActivityAccountMonthCountSurplusNotEnough):
		return "month_quota_exhausted"
	default:
		return "error"
	}
}

func (s *ActivityPartakeUsecase) buildUserRaffleOrder(userID string, activity *Activity, currentTime time.Time) *UserRaffleOrder {
	userRaffleOrder := &UserRaffleOrder{}
	userRaffleOrder.UserID = userID
	userRaffleOrder.ActivityID = activity.ActivityID
	userRaffleOrder.ActivityName = activity.ActivityName
	userRaffleOrder.StrategyID = activity.StrategyID
	userRaffleOrder.OrderID = fmt.Sprintf("%012d", rand.New(rand.NewSource(time.Now().UnixNano())).Int63n(1000000000000))
	userRaffleOrder.OrderTime = currentTime
	userRaffleOrder.OrderState = UserRaffleOrderStateCreate
	return userRaffleOrder
}
