package api

import (
	"context"
	"errors"
	"testing"
	"time"

	"prizeforge/internal/domain/activity"
	"prizeforge/internal/domain/award"
	"prizeforge/internal/domain/strategy"
)

type fakeActivityPartakeService struct {
	createOrderFn      func(context.Context, *activity.PartakeRaffleActivity) (*activity.CreatePartakeOrder, error)
	tryClaimDrawFn     func(context.Context, string, string) (*activity.DrawClaim, error)
	releaseDrawClaimFn func(context.Context, string, string, string) error
}

func (f *fakeActivityPartakeService) CreateOrder(ctx context.Context, partake *activity.PartakeRaffleActivity) (*activity.CreatePartakeOrder, error) {
	if f.createOrderFn == nil {
		panic("unexpected CreateOrder call")
	}
	return f.createOrderFn(ctx, partake)
}

func (f *fakeActivityPartakeService) TryClaimDraw(ctx context.Context, userID string, orderID string) (*activity.DrawClaim, error) {
	if f.tryClaimDrawFn == nil {
		panic("unexpected TryClaimDraw call")
	}
	return f.tryClaimDrawFn(ctx, userID, orderID)
}

func (f *fakeActivityPartakeService) ReleaseDrawClaim(ctx context.Context, userID string, orderID string, owner string) error {
	if f.releaseDrawClaimFn == nil {
		panic("unexpected ReleaseDrawClaim call")
	}
	return f.releaseDrawClaimFn(ctx, userID, orderID, owner)
}

type fakeRaffleStrategyService struct {
	performRaffleFn      func(context.Context, *strategy.RaffleFactor) (*strategy.RaffleAward, error)
	queryStrategyAwardFn func(context.Context, int64, int64) (*strategy.StrategyAward, error)
}

func (f *fakeRaffleStrategyService) PerformRaffle(ctx context.Context, factor *strategy.RaffleFactor) (*strategy.RaffleAward, error) {
	if f.performRaffleFn == nil {
		panic("unexpected PerformRaffle call")
	}
	return f.performRaffleFn(ctx, factor)
}

func (f *fakeRaffleStrategyService) QueryStrategyAward(ctx context.Context, strategyID int64, awardID int64) (*strategy.StrategyAward, error) {
	if f.queryStrategyAwardFn == nil {
		panic("unexpected QueryStrategyAward call")
	}
	return f.queryStrategyAwardFn(ctx, strategyID, awardID)
}

type fakeUserAwardService struct {
	saveUserAwardRecordFn func(context.Context, *award.UserAwardRecord) (*award.UserAwardRecord, error)
	queryByOrderIDFn      func(context.Context, string, string) (*award.UserAwardRecord, error)
}

func (f *fakeUserAwardService) SaveUserAwardRecord(ctx context.Context, record *award.UserAwardRecord) (*award.UserAwardRecord, error) {
	if f.saveUserAwardRecordFn == nil {
		panic("unexpected SaveUserAwardRecord call")
	}
	return f.saveUserAwardRecordFn(ctx, record)
}

func (f *fakeUserAwardService) QueryByOrderID(ctx context.Context, userID string, orderID string) (*award.UserAwardRecord, error) {
	if f.queryByOrderIDFn == nil {
		panic("unexpected QueryByOrderID call")
	}
	return f.queryByOrderIDFn(ctx, userID, orderID)
}

func drawOrder(reused bool) *activity.CreatePartakeOrder {
	return &activity.CreatePartakeOrder{
		UserID:     "user-1",
		ActivityID: 100301,
		Reused:     reused,
		UserRaffleOrder: &activity.UserRaffleOrder{
			UserID:     "user-1",
			ActivityID: 100301,
			StrategyID: 100001,
			OrderID:    "000000000001",
			RequestID:  "request-1",
		},
	}
}

// TestActivityUsecaseDrawReturnsExistingAwardForReusedOrder 验证相同 requestID 复用旧订单时，
// 如果中奖记录已经落库，会直接返回标准记录并补全奖品排序，不会再次抢占订单或执行抽奖。
func TestActivityUsecaseDrawReturnsExistingAwardForReusedOrder(t *testing.T) {
	partakeSvc := &fakeActivityPartakeService{
		createOrderFn: func(_ context.Context, partake *activity.PartakeRaffleActivity) (*activity.CreatePartakeOrder, error) {
			if partake.UserID != "user-1" || partake.ActivityID != 100301 || partake.RequestID != "request-1" {
				t.Fatalf("CreateOrder() request = %#v, want user-1/100301/request-1", partake)
			}
			return drawOrder(true), nil
		},
	}
	awardSvc := &fakeUserAwardService{
		queryByOrderIDFn: func(_ context.Context, userID string, orderID string) (*award.UserAwardRecord, error) {
			if userID != "user-1" || orderID != "000000000001" {
				t.Fatalf("QueryByOrderID() args = (%q, %q), want (%q, %q)", userID, orderID, "user-1", "000000000001")
			}
			return &award.UserAwardRecord{AwardID: 101, AwardTitle: "一等奖"}, nil
		},
	}
	strategySvc := &fakeRaffleStrategyService{
		queryStrategyAwardFn: func(_ context.Context, strategyID int64, awardID int64) (*strategy.StrategyAward, error) {
			if strategyID != 100001 || awardID != 101 {
				t.Fatalf("QueryStrategyAward() args = (%d, %d), want (100001, 101)", strategyID, awardID)
			}
			return &strategy.StrategyAward{Sort: 3}, nil
		},
	}
	usecase := NewActivityUsecase(partakeSvc, nil, nil, strategySvc, awardSvc, nil)

	awardID, title, index, err := usecase.Draw(context.Background(), "user-1", 100301, "request-1")

	if err != nil {
		t.Fatalf("Draw() error = %v, want nil", err)
	}
	if awardID != 101 || title != "一等奖" || index != 3 {
		t.Fatalf("Draw() result = (%d, %q, %d), want (101, %q, 3)", awardID, title, index, "一等奖")
	}
}

// TestActivityUsecaseDrawHandlesClaimStates 验证订单执行权未取得、正在执行、已取消、
// 已完成但缺少中奖记录等状态会返回对应领域错误，只有 completed 且有记录时才返回历史结果。
func TestActivityUsecaseDrawHandlesClaimStates(t *testing.T) {
	tests := []struct {
		name       string
		claim      *activity.DrawClaim
		record     *award.UserAwardRecord
		wantErr    error
		wantAward  int64
		wantTitle  string
		wantIndex  int
		queryAward bool
	}{
		{name: "nil claim", wantErr: activity.ErrDrawInProgress},
		{name: "processing", claim: &activity.DrawClaim{Status: activity.DrawClaimProcessing}, wantErr: activity.ErrDrawInProgress},
		{name: "cancelled", claim: &activity.DrawClaim{Status: activity.DrawClaimCancelled}, wantErr: activity.ErrDrawCancelled},
		{name: "unknown status", claim: &activity.DrawClaim{Status: "unknown"}, wantErr: activity.ErrDrawInProgress},
		{name: "completed without record", claim: &activity.DrawClaim{Status: activity.DrawClaimCompleted}, wantErr: activity.ErrRecordNotFound, queryAward: true},
		{
			name:       "completed with record",
			claim:      &activity.DrawClaim{Status: activity.DrawClaimCompleted},
			record:     &award.UserAwardRecord{AwardID: 102, AwardTitle: "二等奖"},
			wantAward:  102,
			wantTitle:  "二等奖",
			wantIndex:  5,
			queryAward: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			partakeSvc := &fakeActivityPartakeService{
				createOrderFn: func(context.Context, *activity.PartakeRaffleActivity) (*activity.CreatePartakeOrder, error) {
					return drawOrder(false), nil
				},
				tryClaimDrawFn: func(_ context.Context, userID string, orderID string) (*activity.DrawClaim, error) {
					if userID != "user-1" || orderID != "000000000001" {
						t.Fatalf("TryClaimDraw() args = (%q, %q), want (%q, %q)", userID, orderID, "user-1", "000000000001")
					}
					return tt.claim, nil
				},
			}
			awardSvc := &fakeUserAwardService{}
			if tt.queryAward {
				awardSvc.queryByOrderIDFn = func(context.Context, string, string) (*award.UserAwardRecord, error) {
					return tt.record, nil
				}
			}
			strategySvc := &fakeRaffleStrategyService{}
			if tt.record != nil {
				strategySvc.queryStrategyAwardFn = func(context.Context, int64, int64) (*strategy.StrategyAward, error) {
					return &strategy.StrategyAward{Sort: tt.wantIndex}, nil
				}
			}
			usecase := NewActivityUsecase(partakeSvc, nil, nil, strategySvc, awardSvc, nil)

			awardID, title, index, err := usecase.Draw(context.Background(), "user-1", 100301, "request-1")

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Draw() error = %v, want %v", err, tt.wantErr)
			}
			if awardID != tt.wantAward || title != tt.wantTitle || index != tt.wantIndex {
				t.Fatalf("Draw() result = (%d, %q, %d), want (%d, %q, %d)", awardID, title, index, tt.wantAward, tt.wantTitle, tt.wantIndex)
			}
		})
	}
}

// TestActivityUsecaseDrawReleasesClaimAfterFailure 验证当前请求取得订单执行权后，
// 无论抽奖策略失败还是中奖记录保存失败，都会释放 owner 对应的执行权并返回原始错误。
func TestActivityUsecaseDrawReleasesClaimAfterFailure(t *testing.T) {
	strategyErr := errors.New("perform raffle")
	saveErr := errors.New("save award")
	tests := []struct {
		name        string
		strategyErr error
		saveErr     error
		wantErr     error
	}{
		{name: "strategy failure", strategyErr: strategyErr, wantErr: strategyErr},
		{name: "save failure", saveErr: saveErr, wantErr: saveErr},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			releaseCalls := 0
			partakeSvc := &fakeActivityPartakeService{
				createOrderFn: func(context.Context, *activity.PartakeRaffleActivity) (*activity.CreatePartakeOrder, error) {
					return drawOrder(false), nil
				},
				tryClaimDrawFn: func(context.Context, string, string) (*activity.DrawClaim, error) {
					return &activity.DrawClaim{Status: activity.DrawClaimAcquired, Owner: "owner-1"}, nil
				},
				releaseDrawClaimFn: func(ctx context.Context, userID string, orderID string, owner string) error {
					releaseCalls++
					if ctx.Err() != nil {
						t.Fatalf("ReleaseDrawClaim() context error = %v, want nil", ctx.Err())
					}
					if userID != "user-1" || orderID != "000000000001" || owner != "owner-1" {
						t.Fatalf("ReleaseDrawClaim() args = (%q, %q, %q), want (%q, %q, %q)", userID, orderID, owner, "user-1", "000000000001", "owner-1")
					}
					return nil
				},
			}
			strategySvc := &fakeRaffleStrategyService{
				performRaffleFn: func(context.Context, *strategy.RaffleFactor) (*strategy.RaffleAward, error) {
					if tt.strategyErr != nil {
						return nil, tt.strategyErr
					}
					return &strategy.RaffleAward{AwardID: 101, AwardTitle: "一等奖"}, nil
				},
			}
			awardSvc := &fakeUserAwardService{}
			if tt.strategyErr == nil {
				awardSvc.saveUserAwardRecordFn = func(context.Context, *award.UserAwardRecord) (*award.UserAwardRecord, error) {
					return nil, tt.saveErr
				}
			}
			usecase := NewActivityUsecase(partakeSvc, nil, nil, strategySvc, awardSvc, nil)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			_, _, _, err := usecase.Draw(ctx, "user-1", 100301, "request-1")

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Draw() error = %v, want %v", err, tt.wantErr)
			}
			if releaseCalls != 1 {
				t.Fatalf("ReleaseDrawClaim() calls = %d, want 1", releaseCalls)
			}
		})
	}
}

// TestActivityUsecaseDrawSavesCanonicalAward 验证成功抽奖会把订单、奖品、库存预占、
// owner 和可控中奖时间完整写入记录，并以保存后返回的标准记录构造最终响应且不释放执行权。
func TestActivityUsecaseDrawSavesCanonicalAward(t *testing.T) {
	fixedNow := time.Date(2026, time.July, 20, 13, 14, 15, 0, time.UTC)
	partakeSvc := &fakeActivityPartakeService{
		createOrderFn: func(context.Context, *activity.PartakeRaffleActivity) (*activity.CreatePartakeOrder, error) {
			return drawOrder(false), nil
		},
		tryClaimDrawFn: func(context.Context, string, string) (*activity.DrawClaim, error) {
			return &activity.DrawClaim{Status: activity.DrawClaimAcquired, Owner: "owner-1"}, nil
		},
	}
	strategySvc := &fakeRaffleStrategyService{
		performRaffleFn: func(_ context.Context, factor *strategy.RaffleFactor) (*strategy.RaffleAward, error) {
			if factor.UserID != "user-1" || factor.ActivityID != 100301 || factor.StrategyID != 100001 || factor.OrderID != "000000000001" {
				t.Fatalf("PerformRaffle() factor = %#v, want complete order identity", factor)
			}
			return &strategy.RaffleAward{
				AwardID:       101,
				AwardTitle:    "策略返回标题",
				StockReserved: true,
			}, nil
		},
		queryStrategyAwardFn: func(_ context.Context, strategyID int64, awardID int64) (*strategy.StrategyAward, error) {
			if strategyID != 100001 || awardID != 101 {
				t.Fatalf("QueryStrategyAward() args = (%d, %d), want (100001, 101)", strategyID, awardID)
			}
			return &strategy.StrategyAward{Sort: 7}, nil
		},
	}
	awardSvc := &fakeUserAwardService{
		saveUserAwardRecordFn: func(_ context.Context, record *award.UserAwardRecord) (*award.UserAwardRecord, error) {
			if record.UserID != "user-1" || record.ActivityID != 100301 || record.StrategyID != 100001 || record.OrderID != "000000000001" {
				t.Fatalf("SaveUserAwardRecord() identity = %#v, want complete order identity", record)
			}
			if record.AwardID != 101 || record.AwardTitle != "策略返回标题" || record.AwardState != award.AwardStateCreate {
				t.Fatalf("SaveUserAwardRecord() award = %#v, want strategy result in create state", record)
			}
			if record.AwardTime != fixedNow || !record.StockReserved || record.DrawOwner != "owner-1" {
				t.Fatalf("SaveUserAwardRecord() execution metadata = %#v, want fixed time/reserved stock/owner-1", record)
			}
			return &award.UserAwardRecord{AwardID: 101, AwardTitle: "数据库标准标题"}, nil
		},
	}
	usecase := NewActivityUsecase(partakeSvc, nil, nil, strategySvc, awardSvc, nil)
	usecase.now = func() time.Time { return fixedNow }

	awardID, title, index, err := usecase.Draw(context.Background(), "user-1", 100301, "request-1")

	if err != nil {
		t.Fatalf("Draw() error = %v, want nil", err)
	}
	if awardID != 101 || title != "数据库标准标题" || index != 7 {
		t.Fatalf("Draw() result = (%d, %q, %d), want (101, %q, 7)", awardID, title, index, "数据库标准标题")
	}
}
