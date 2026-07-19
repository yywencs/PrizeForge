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

func (s *ActivityPartakeUsecase) CreateOrder(ctx context.Context, partake *PartakeRaffleActivity) (aggregate *CreatePartakeOrder, err error) {
	userID, activityID, requestID := partake.UserID, partake.ActivityID, partake.RequestID
	defer func() {
		metrics.IncActivityQuota(activityID, quotaCheckResultFromErr(err))
	}()
	if userID == "" || activityID <= 0 || requestID == "" {
		return nil, ErrInvalidParams
	}
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

	newOrder := s.buildUserRaffleOrder(userID, requestID, activity, currentTime)
	userRaffleOrder, reused, err := s.repo.CreateOrLoadUserRaffleOrder(ctx, newOrder)
	if err != nil {
		return nil, err
	}

	return &CreatePartakeOrder{
		UserID:          userID,
		ActivityID:      activityID,
		UserRaffleOrder: userRaffleOrder,
		Reused:          reused,
	}, nil
}

func (s *ActivityPartakeUsecase) TryClaimDraw(ctx context.Context, userID string, orderID string) (*DrawClaim, error) {
	return s.repo.TryClaimUserRaffleOrder(ctx, userID, orderID)
}

func (s *ActivityPartakeUsecase) ReleaseDrawClaim(ctx context.Context, userID string, orderID string, owner string) error {
	return s.repo.ReleaseUserRaffleOrderClaim(ctx, userID, orderID, owner)
}

func (s *ActivityPartakeUsecase) SaveOrderRecord(ctx context.Context, aggregate *CreatePartakeOrder) error {
	return s.repo.SaveCreatePartakeOrderAggregate(ctx, aggregate)
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
	case errors.Is(err, ErrDrawInProgress):
		return "order_in_progress"
	default:
		return "error"
	}
}

func (s *ActivityPartakeUsecase) buildUserRaffleOrder(userID string, requestID string, activity *Activity, currentTime time.Time) *UserRaffleOrder {
	userRaffleOrder := &UserRaffleOrder{}
	userRaffleOrder.UserID = userID
	userRaffleOrder.ActivityID = activity.ActivityID
	userRaffleOrder.ActivityName = activity.ActivityName
	userRaffleOrder.StrategyID = activity.StrategyID
	userRaffleOrder.OrderID = fmt.Sprintf("%012d", rand.New(rand.NewSource(time.Now().UnixNano())).Int63n(1000000000000))
	userRaffleOrder.RequestID = requestID
	userRaffleOrder.OrderTime = currentTime
	userRaffleOrder.OrderState = UserRaffleOrderStateCreate
	userRaffleOrder.DrawState = DrawStateCreated
	return userRaffleOrder
}
