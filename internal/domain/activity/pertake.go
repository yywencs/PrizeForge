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
	repo PartakeRepository
	now  func() time.Time
}

func NewActivityPartakeUsecase(repo PartakeRepository) *ActivityPartakeUsecase {
	return &ActivityPartakeUsecase{
		repo: repo,
		now:  time.Now,
	}
}

func (s *ActivityPartakeUsecase) CreateOrder(ctx context.Context, partake *PartakeRaffleActivity) (aggregate *CreatePartakeOrder, err error) {
	if partake == nil {
		metrics.IncActivityQuota(0, quotaCheckResultFromErr(ErrInvalidParams))
		return nil, ErrInvalidParams
	}

	userID, activityID, requestID := partake.UserID, partake.ActivityID, partake.RequestID
	defer func() {
		metrics.IncActivityQuota(activityID, quotaCheckResultFromErr(err))
	}()
	if userID == "" || activityID <= 0 || requestID == "" {
		return nil, ErrInvalidParams
	}
	currentTime := s.now()

	activity, err := s.repo.QueryRaffleActivity(ctx, activityID)
	if err != nil {
		return nil, err
	}
	if activity == nil {
		return nil, ErrRecordNotFound
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
	userRaffleOrder, publication, reused, err := s.repo.CreateOrLoadUserRaffleOrder(ctx, newOrder)
	if err != nil {
		return nil, err
	}

	aggregate = &CreatePartakeOrder{
		UserID:                userID,
		ActivityID:            activityID,
		UserRaffleOrder:       userRaffleOrder,
		Reused:                reused,
		DrawResultPublication: publication,
	}
	return aggregate, nil
}

func (s *ActivityPartakeUsecase) TryClaimDraw(ctx context.Context, userID string, activityID int64, requestID string, orderID string) (*DrawClaim, error) {
	return s.repo.TryClaimUserRaffleOrder(ctx, userID, activityID, requestID, orderID)
}

func (s *ActivityPartakeUsecase) ReleaseDrawClaim(ctx context.Context, userID string, activityID int64, orderID string, owner string) error {
	return s.repo.ReleaseUserRaffleOrderClaim(ctx, userID, activityID, orderID, owner)
}

func (s *ActivityPartakeUsecase) CompleteDraw(ctx context.Context, result *DrawResult, owner string) (*DrawResultPublication, error) {
	return s.repo.CompleteUserRaffleOrder(ctx, result, owner)
}

func (s *ActivityPartakeUsecase) QueryPendingDrawResults(ctx context.Context, limit int64) ([]*DrawResultPublication, error) {
	return s.repo.QueryPendingDrawResultPublications(ctx, limit)
}

func (s *ActivityPartakeUsecase) MarkDrawResultPublished(ctx context.Context, publication *DrawResultPublication) error {
	return s.repo.MarkDrawResultPublished(ctx, publication)
}

func (s *ActivityPartakeUsecase) SaveDrawResult(ctx context.Context, result *DrawResult) error {
	return s.repo.SaveDrawResult(ctx, result)
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
