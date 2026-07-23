package api

import (
	"context"
	"errors"
	"testing"
	"time"

	"prizeforge/internal/domain/activity"
	"prizeforge/internal/domain/strategy"
)

type fakeActivityPartakeService struct {
	createOrderFn      func(context.Context, *activity.PartakeRaffleActivity) (*activity.CreatePartakeOrder, error)
	tryClaimDrawFn     func(context.Context, string, int64, string, string) (*activity.DrawClaim, error)
	releaseDrawClaimFn func(context.Context, string, int64, string, string) error
	completeDrawFn     func(context.Context, *activity.DrawResult, string) (*activity.DrawResultPublication, error)
}

func (f *fakeActivityPartakeService) CreateOrder(ctx context.Context, req *activity.PartakeRaffleActivity) (*activity.CreatePartakeOrder, error) {
	return f.createOrderFn(ctx, req)
}
func (f *fakeActivityPartakeService) TryClaimDraw(ctx context.Context, userID string, activityID int64, requestID string, orderID string) (*activity.DrawClaim, error) {
	if f.tryClaimDrawFn == nil {
		panic("unexpected TryClaimDraw call")
	}
	return f.tryClaimDrawFn(ctx, userID, activityID, requestID, orderID)
}
func (f *fakeActivityPartakeService) ReleaseDrawClaim(ctx context.Context, userID string, activityID int64, orderID string, owner string) error {
	if f.releaseDrawClaimFn == nil {
		panic("unexpected ReleaseDrawClaim call")
	}
	return f.releaseDrawClaimFn(ctx, userID, activityID, orderID, owner)
}
func (f *fakeActivityPartakeService) CompleteDraw(ctx context.Context, result *activity.DrawResult, owner string) (*activity.DrawResultPublication, error) {
	if f.completeDrawFn == nil {
		panic("unexpected CompleteDraw call")
	}
	return f.completeDrawFn(ctx, result, owner)
}

type fakeRaffleStrategyService struct {
	performFn func(context.Context, *strategy.RaffleFactor) (*strategy.RaffleAward, error)
	queryFn   func(context.Context, int64, int64) (*strategy.StrategyAward, error)
}

func (f *fakeRaffleStrategyService) PerformRaffle(ctx context.Context, factor *strategy.RaffleFactor) (*strategy.RaffleAward, error) {
	if f.performFn == nil {
		panic("unexpected PerformRaffle call")
	}
	return f.performFn(ctx, factor)
}
func (f *fakeRaffleStrategyService) QueryStrategyAward(ctx context.Context, strategyID int64, awardID int64) (*strategy.StrategyAward, error) {
	if f.queryFn == nil {
		return nil, nil
	}
	return f.queryFn(ctx, strategyID, awardID)
}

type fakeDrawResultPublisher struct {
	publishFn func(context.Context, *activity.DrawResultPublication) error
}

func (f *fakeDrawResultPublisher) Publish(ctx context.Context, publication *activity.DrawResultPublication) error {
	if f.publishFn == nil {
		panic("unexpected Publish call")
	}
	return f.publishFn(ctx, publication)
}

func testDrawOrder(publication *activity.DrawResultPublication) *activity.CreatePartakeOrder {
	return &activity.CreatePartakeOrder{
		UserID:                "user-1",
		ActivityID:            100301,
		DrawResultPublication: publication,
		UserRaffleOrder: &activity.UserRaffleOrder{
			UserID:       "user-1",
			ActivityID:   100301,
			ActivityName: "活动",
			StrategyID:   100001,
			OrderID:      "000000000001",
			RequestID:    "request-1",
			OrderTime:    time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC),
		},
	}
}

func TestActivityUsecaseDrawReturnsPublishedRedisResult(t *testing.T) {
	result := &activity.DrawResult{
		UserID: "user-1", ActivityID: 100301, StrategyID: 100001,
		OrderID: "000000000001", RequestID: "request-1",
		AwardID: 101, AwardTitle: "一等奖",
	}
	publication := &activity.DrawResultPublication{StreamID: "1-0", BrokerConfirmed: true, Result: result}
	partake := &fakeActivityPartakeService{
		createOrderFn: func(context.Context, *activity.PartakeRaffleActivity) (*activity.CreatePartakeOrder, error) {
			return testDrawOrder(publication), nil
		},
	}
	strategySvc := &fakeRaffleStrategyService{queryFn: func(context.Context, int64, int64) (*strategy.StrategyAward, error) {
		return &strategy.StrategyAward{Sort: 3}, nil
	}}
	usecase := NewActivityUsecase(partake, nil, nil, strategySvc, nil, nil)

	awardID, title, sort, err := usecase.Draw(context.Background(), "user-1", 100301, "request-1")
	if err != nil || awardID != 101 || title != "一等奖" || sort != 3 {
		t.Fatalf("Draw() = (%d, %q, %d, %v), want (101, 一等奖, 3, nil)", awardID, title, sort, err)
	}
}

func TestActivityUsecaseDrawPublishesCachedPendingResult(t *testing.T) {
	result := &activity.DrawResult{
		UserID: "user-1", ActivityID: 100301, StrategyID: 100001,
		OrderID: "000000000001", RequestID: "request-1",
		AwardID: 101, AwardTitle: "一等奖",
	}
	publication := &activity.DrawResultPublication{StreamID: "1-0", Result: result}
	partake := &fakeActivityPartakeService{
		createOrderFn: func(context.Context, *activity.PartakeRaffleActivity) (*activity.CreatePartakeOrder, error) {
			return testDrawOrder(publication), nil
		},
	}
	published := false
	publisher := &fakeDrawResultPublisher{publishFn: func(_ context.Context, publication *activity.DrawResultPublication) error {
		published = publication.StreamID == "1-0" && publication.Result == result
		return nil
	}}
	strategySvc := &fakeRaffleStrategyService{}
	usecase := NewActivityUsecase(partake, nil, nil, strategySvc, publisher, nil)

	if _, _, _, err := usecase.Draw(context.Background(), "user-1", 100301, "request-1"); err != nil {
		t.Fatalf("Draw() error = %v", err)
	}
	if !published {
		t.Fatal("cached Redis result was not published")
	}
}

func TestActivityUsecaseDrawCompletesRedisResultBeforePublishing(t *testing.T) {
	fixedNow := time.Date(2026, 7, 20, 13, 14, 15, 0, time.UTC)
	orderAggregate := testDrawOrder(nil)
	partake := &fakeActivityPartakeService{
		createOrderFn: func(context.Context, *activity.PartakeRaffleActivity) (*activity.CreatePartakeOrder, error) {
			return orderAggregate, nil
		},
		tryClaimDrawFn: func(context.Context, string, int64, string, string) (*activity.DrawClaim, error) {
			return &activity.DrawClaim{Status: activity.DrawClaimAcquired, Owner: "owner-1"}, nil
		},
		completeDrawFn: func(_ context.Context, result *activity.DrawResult, owner string) (*activity.DrawResultPublication, error) {
			if owner != "owner-1" || result.AwardID != 101 || result.AwardTime != fixedNow || !result.StockReserved {
				t.Fatalf("CompleteDraw() result=%#v owner=%q", result, owner)
			}
			return &activity.DrawResultPublication{StreamID: "2-0", Result: result}, nil
		},
	}
	strategySvc := &fakeRaffleStrategyService{
		performFn: func(context.Context, *strategy.RaffleFactor) (*strategy.RaffleAward, error) {
			return &strategy.RaffleAward{AwardID: 101, AwardTitle: "一等奖", StockReserved: true}, nil
		},
		queryFn: func(context.Context, int64, int64) (*strategy.StrategyAward, error) {
			return &strategy.StrategyAward{Sort: 7}, nil
		},
	}
	publisher := &fakeDrawResultPublisher{publishFn: func(_ context.Context, publication *activity.DrawResultPublication) error {
		if publication.StreamID != "2-0" {
			t.Fatalf("Publish() streamID = %q", publication.StreamID)
		}
		return nil
	}}
	usecase := NewActivityUsecase(partake, nil, nil, strategySvc, publisher, nil)
	usecase.now = func() time.Time { return fixedNow }

	awardID, title, sort, err := usecase.Draw(context.Background(), "user-1", 100301, "request-1")
	if err != nil || awardID != 101 || title != "一等奖" || sort != 7 {
		t.Fatalf("Draw() = (%d, %q, %d, %v)", awardID, title, sort, err)
	}
}

func TestActivityUsecaseDrawReleasesRedisClaimOnRaffleFailure(t *testing.T) {
	raffleErr := errors.New("raffle failed")
	released := false
	partake := &fakeActivityPartakeService{
		createOrderFn: func(context.Context, *activity.PartakeRaffleActivity) (*activity.CreatePartakeOrder, error) {
			return testDrawOrder(nil), nil
		},
		tryClaimDrawFn: func(context.Context, string, int64, string, string) (*activity.DrawClaim, error) {
			return &activity.DrawClaim{Status: activity.DrawClaimAcquired, Owner: "owner-1"}, nil
		},
		releaseDrawClaimFn: func(context.Context, string, int64, string, string) error {
			released = true
			return nil
		},
	}
	strategySvc := &fakeRaffleStrategyService{performFn: func(context.Context, *strategy.RaffleFactor) (*strategy.RaffleAward, error) {
		return nil, raffleErr
	}}
	usecase := NewActivityUsecase(partake, nil, nil, strategySvc, nil, nil)

	_, _, _, err := usecase.Draw(context.Background(), "user-1", 100301, "request-1")
	if !errors.Is(err, raffleErr) || !released {
		t.Fatalf("Draw() error=%v released=%v", err, released)
	}
}

func TestActivityUsecaseDrawReturnsProcessingWhenConfirmFails(t *testing.T) {
	result := &activity.DrawResult{
		UserID: "user-1", ActivityID: 100301, StrategyID: 100001,
		OrderID: "000000000001", RequestID: "request-1", AwardID: 101,
	}
	publication := &activity.DrawResultPublication{StreamID: "3-0", Result: result}
	partake := &fakeActivityPartakeService{createOrderFn: func(context.Context, *activity.PartakeRaffleActivity) (*activity.CreatePartakeOrder, error) {
		return testDrawOrder(publication), nil
	}}
	publisher := &fakeDrawResultPublisher{publishFn: func(context.Context, *activity.DrawResultPublication) error {
		return errors.New("confirm timeout")
	}}
	usecase := NewActivityUsecase(partake, nil, nil, &fakeRaffleStrategyService{}, publisher, nil)

	_, _, _, err := usecase.Draw(context.Background(), "user-1", 100301, "request-1")
	if !errors.Is(err, activity.ErrDrawInProgress) {
		t.Fatalf("Draw() error = %v, want ErrDrawInProgress", err)
	}
}
