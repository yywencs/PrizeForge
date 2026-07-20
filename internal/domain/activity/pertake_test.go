package activity

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"
)

type fakePartakeRepository struct {
	queryRaffleActivityFn             func(context.Context, int64) (*Activity, error)
	createOrLoadUserRaffleOrderFn     func(context.Context, *UserRaffleOrder) (*UserRaffleOrder, bool, error)
	tryClaimUserRaffleOrderFn         func(context.Context, string, string) (*DrawClaim, error)
	releaseUserRaffleOrderClaimFn     func(context.Context, string, string, string) error
	saveCreatePartakeOrderAggregateFn func(context.Context, *CreatePartakeOrder) error
}

func (f *fakePartakeRepository) QueryRaffleActivity(ctx context.Context, activityID int64) (*Activity, error) {
	if f.queryRaffleActivityFn == nil {
		panic("unexpected QueryRaffleActivity call")
	}
	return f.queryRaffleActivityFn(ctx, activityID)
}

func (f *fakePartakeRepository) CreateOrLoadUserRaffleOrder(ctx context.Context, order *UserRaffleOrder) (*UserRaffleOrder, bool, error) {
	if f.createOrLoadUserRaffleOrderFn == nil {
		panic("unexpected CreateOrLoadUserRaffleOrder call")
	}
	return f.createOrLoadUserRaffleOrderFn(ctx, order)
}

func (f *fakePartakeRepository) TryClaimUserRaffleOrder(ctx context.Context, userID string, orderID string) (*DrawClaim, error) {
	if f.tryClaimUserRaffleOrderFn == nil {
		panic("unexpected TryClaimUserRaffleOrder call")
	}
	return f.tryClaimUserRaffleOrderFn(ctx, userID, orderID)
}

func (f *fakePartakeRepository) ReleaseUserRaffleOrderClaim(ctx context.Context, userID string, orderID string, owner string) error {
	if f.releaseUserRaffleOrderClaimFn == nil {
		panic("unexpected ReleaseUserRaffleOrderClaim call")
	}
	return f.releaseUserRaffleOrderClaimFn(ctx, userID, orderID, owner)
}

func (f *fakePartakeRepository) SaveCreatePartakeOrderAggregate(ctx context.Context, aggregate *CreatePartakeOrder) error {
	if f.saveCreatePartakeOrderAggregateFn == nil {
		panic("unexpected SaveCreatePartakeOrderAggregate call")
	}
	return f.saveCreatePartakeOrderAggregateFn(ctx, aggregate)
}

// TestActivityPartakeUsecaseCreateOrderRejectsInvalidParams 验证创建抽奖订单时，
// nil 请求、空用户 ID、非法活动 ID 和空请求 ID 都会返回 ErrInvalidParams，且不会访问仓储。
func TestActivityPartakeUsecaseCreateOrderRejectsInvalidParams(t *testing.T) {
	tests := []struct {
		name    string
		partake *PartakeRaffleActivity
	}{
		{name: "nil request"},
		{
			name: "empty user ID",
			partake: &PartakeRaffleActivity{
				ActivityID: 1,
				RequestID:  "request-1",
			},
		},
		{
			name: "invalid activity ID",
			partake: &PartakeRaffleActivity{
				UserID:    "user-1",
				RequestID: "request-1",
			},
		},
		{
			name: "empty request ID",
			partake: &PartakeRaffleActivity{
				UserID:     "user-1",
				ActivityID: 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			usecase := NewActivityPartakeUsecase(&fakePartakeRepository{})

			aggregate, err := usecase.CreateOrder(context.Background(), tt.partake)

			if !errors.Is(err, ErrInvalidParams) {
				t.Fatalf("CreateOrder() error = %v, want %v", err, ErrInvalidParams)
			}
			if aggregate != nil {
				t.Fatalf("CreateOrder() aggregate = %#v, want nil", aggregate)
			}
		})
	}
}

// TestActivityPartakeUsecaseCreateOrderChecksActivity 验证创建订单前的活动查询、存在性、
// 开启状态和有效时间检查，并确认对应仓储错误或领域错误能够原样返回。
func TestActivityPartakeUsecaseCreateOrderChecksActivity(t *testing.T) {
	fixedNow := time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC)
	queryErr := errors.New("query activity")

	tests := []struct {
		name     string
		activity *Activity
		queryErr error
		wantErr  error
	}{
		{
			name:     "query failure",
			queryErr: queryErr,
			wantErr:  queryErr,
		},
		{
			name:    "activity not found",
			wantErr: ErrRecordNotFound,
		},
		{
			name: "activity is not open",
			activity: &Activity{
				State:         ActivityStateClose,
				BeginDateTime: fixedNow.Add(-time.Hour),
				EndDateTime:   fixedNow.Add(time.Hour),
			},
			wantErr: ErrActivityStateError,
		},
		{
			name: "activity has not started",
			activity: &Activity{
				State:         ActivityStateOpen,
				BeginDateTime: fixedNow.Add(time.Second),
				EndDateTime:   fixedNow.Add(time.Hour),
			},
			wantErr: ErrActivityTimeError,
		},
		{
			name: "activity has ended",
			activity: &Activity{
				State:         ActivityStateOpen,
				BeginDateTime: fixedNow.Add(-time.Hour),
				EndDateTime:   fixedNow.Add(-time.Second),
			},
			wantErr: ErrActivityTimeError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &fakePartakeRepository{
				queryRaffleActivityFn: func(_ context.Context, activityID int64) (*Activity, error) {
					if activityID != 1 {
						t.Fatalf("QueryRaffleActivity() activityID = %d, want 1", activityID)
					}
					return tt.activity, tt.queryErr
				},
			}
			usecase := NewActivityPartakeUsecase(repo)
			usecase.now = func() time.Time { return fixedNow }

			aggregate, err := usecase.CreateOrder(context.Background(), &PartakeRaffleActivity{
				UserID:     "user-1",
				ActivityID: 1,
				RequestID:  "request-1",
			})

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("CreateOrder() error = %v, want %v", err, tt.wantErr)
			}
			if aggregate != nil {
				t.Fatalf("CreateOrder() aggregate = %#v, want nil", aggregate)
			}
		})
	}
}

// TestActivityPartakeUsecaseCreateOrderBuildsOrder 验证有效请求会构造包含正确用户、活动、
// 策略、请求时间和初始状态的 12 位订单，并返回仓储确认后的标准订单。
func TestActivityPartakeUsecaseCreateOrderBuildsOrder(t *testing.T) {
	fixedNow := time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC)
	activity := &Activity{
		ActivityID:    100301,
		ActivityName:  "summer raffle",
		StrategyID:    100001,
		State:         ActivityStateOpen,
		BeginDateTime: fixedNow.Add(-time.Hour),
		EndDateTime:   fixedNow.Add(time.Hour),
	}

	var capturedOrder *UserRaffleOrder
	repo := &fakePartakeRepository{
		queryRaffleActivityFn: func(_ context.Context, activityID int64) (*Activity, error) {
			if activityID != activity.ActivityID {
				t.Fatalf("QueryRaffleActivity() activityID = %d, want %d", activityID, activity.ActivityID)
			}
			return activity, nil
		},
		createOrLoadUserRaffleOrderFn: func(_ context.Context, order *UserRaffleOrder) (*UserRaffleOrder, bool, error) {
			capturedOrder = order
			return order, false, nil
		},
	}
	usecase := NewActivityPartakeUsecase(repo)
	usecase.now = func() time.Time { return fixedNow }

	aggregate, err := usecase.CreateOrder(context.Background(), &PartakeRaffleActivity{
		UserID:     "user-1",
		ActivityID: activity.ActivityID,
		RequestID:  "request-1",
	})
	if err != nil {
		t.Fatalf("CreateOrder() error = %v, want nil", err)
	}
	if aggregate == nil {
		t.Fatal("CreateOrder() aggregate = nil")
	}
	if aggregate.UserID != "user-1" || aggregate.ActivityID != activity.ActivityID {
		t.Fatalf("CreateOrder() aggregate identity = (%q, %d), want (%q, %d)", aggregate.UserID, aggregate.ActivityID, "user-1", activity.ActivityID)
	}
	if aggregate.Reused {
		t.Fatal("CreateOrder() Reused = true, want false")
	}
	if aggregate.UserRaffleOrder != capturedOrder {
		t.Fatal("CreateOrder() did not return the repository's canonical order")
	}
	if capturedOrder == nil {
		t.Fatal("CreateOrLoadUserRaffleOrder() order = nil")
	}
	if capturedOrder.UserID != "user-1" || capturedOrder.RequestID != "request-1" {
		t.Fatalf("created order identity = (%q, %q), want (%q, %q)", capturedOrder.UserID, capturedOrder.RequestID, "user-1", "request-1")
	}
	if capturedOrder.ActivityID != activity.ActivityID || capturedOrder.ActivityName != activity.ActivityName || capturedOrder.StrategyID != activity.StrategyID {
		t.Fatalf("created order activity fields = (%d, %q, %d), want (%d, %q, %d)", capturedOrder.ActivityID, capturedOrder.ActivityName, capturedOrder.StrategyID, activity.ActivityID, activity.ActivityName, activity.StrategyID)
	}
	if !capturedOrder.OrderTime.Equal(fixedNow) {
		t.Fatalf("created order time = %v, want %v", capturedOrder.OrderTime, fixedNow)
	}
	if capturedOrder.OrderState != UserRaffleOrderStateCreate || capturedOrder.DrawState != DrawStateCreated {
		t.Fatalf("created order states = (%q, %q), want (%q, %q)", capturedOrder.OrderState, capturedOrder.DrawState, UserRaffleOrderStateCreate, DrawStateCreated)
	}
	if len(capturedOrder.OrderID) != 12 {
		t.Fatalf("created order ID = %q, want 12 digits", capturedOrder.OrderID)
	}
	if _, err := strconv.ParseUint(capturedOrder.OrderID, 10, 64); err != nil {
		t.Fatalf("created order ID = %q, want decimal digits: %v", capturedOrder.OrderID, err)
	}
}

// TestActivityPartakeUsecaseCreateOrderPropagatesRepositoryResult 验证仓储创建错误会向上返回，
// 并验证相同请求命中已有订单时会保留仓储返回的订单对象和 Reused 标记。
func TestActivityPartakeUsecaseCreateOrderPropagatesRepositoryResult(t *testing.T) {
	fixedNow := time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC)
	activity := &Activity{
		ActivityID:    100301,
		State:         ActivityStateOpen,
		BeginDateTime: fixedNow.Add(-time.Hour),
		EndDateTime:   fixedNow.Add(time.Hour),
	}
	repositoryErr := errors.New("create or load order")
	existingOrder := &UserRaffleOrder{OrderID: "000000000001"}

	tests := []struct {
		name      string
		order     *UserRaffleOrder
		reused    bool
		repoErr   error
		wantErr   error
		wantOrder *UserRaffleOrder
	}{
		{
			name:    "repository failure",
			repoErr: repositoryErr,
			wantErr: repositoryErr,
		},
		{
			name:      "reused order",
			order:     existingOrder,
			reused:    true,
			wantOrder: existingOrder,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &fakePartakeRepository{
				queryRaffleActivityFn: func(context.Context, int64) (*Activity, error) {
					return activity, nil
				},
				createOrLoadUserRaffleOrderFn: func(context.Context, *UserRaffleOrder) (*UserRaffleOrder, bool, error) {
					return tt.order, tt.reused, tt.repoErr
				},
			}
			usecase := NewActivityPartakeUsecase(repo)
			usecase.now = func() time.Time { return fixedNow }

			aggregate, err := usecase.CreateOrder(context.Background(), &PartakeRaffleActivity{
				UserID:     "user-1",
				ActivityID: activity.ActivityID,
				RequestID:  "request-1",
			})

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("CreateOrder() error = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				if aggregate != nil {
					t.Fatalf("CreateOrder() aggregate = %#v, want nil", aggregate)
				}
				return
			}
			if aggregate == nil {
				t.Fatal("CreateOrder() aggregate = nil")
			}
			if aggregate.UserRaffleOrder != tt.wantOrder || aggregate.Reused != tt.reused {
				t.Fatalf("CreateOrder() result = (%p, %v), want (%p, %v)", aggregate.UserRaffleOrder, aggregate.Reused, tt.wantOrder, tt.reused)
			}
		})
	}
}
