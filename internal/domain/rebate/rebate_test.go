package rebate

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
)

type fakeRebateRepository struct {
	queryDailyBehaviorRebateConfigFn func(context.Context, BehaviorType) ([]*DailyBehaviorRebate, error)
	saveUserRebateOrderFn            func(context.Context, string, *BehaviorRebate) error
	queryUserRebateOrderFn           func(context.Context, string, string) ([]*BehaviorRebateOrder, error)
}

func (f *fakeRebateRepository) QueryDailyBehaviorRebateConfig(ctx context.Context, behaviorType BehaviorType) ([]*DailyBehaviorRebate, error) {
	if f.queryDailyBehaviorRebateConfigFn == nil {
		panic("unexpected QueryDailyBehaviorRebateConfig call")
	}
	return f.queryDailyBehaviorRebateConfigFn(ctx, behaviorType)
}

func (f *fakeRebateRepository) SaveUserRebateOrder(ctx context.Context, userID string, aggregate *BehaviorRebate) error {
	if f.saveUserRebateOrderFn == nil {
		panic("unexpected SaveUserRebateOrder call")
	}
	return f.saveUserRebateOrderFn(ctx, userID, aggregate)
}

func (f *fakeRebateRepository) QueryUserRebateOrder(ctx context.Context, userID string, outBusinessNo string) ([]*BehaviorRebateOrder, error) {
	if f.queryUserRebateOrderFn == nil {
		panic("unexpected QueryUserRebateOrder call")
	}
	return f.queryUserRebateOrderFn(ctx, userID, outBusinessNo)
}

// TestBehaviorRebateUsecaseCreateOrderStopsBeforeSave 验证返利配置查询失败时返回原始错误，
// 没有可用配置时返回空订单列表，并且两种情况都不会调用订单保存仓储。
func TestBehaviorRebateUsecaseCreateOrderStopsBeforeSave(t *testing.T) {
	queryErr := errors.New("query rebate config")
	tests := []struct {
		name        string
		configs     []*DailyBehaviorRebate
		queryErr    error
		wantNil     bool
		wantErr     error
		wantOrderID int
	}{
		{
			name:     "query failure",
			queryErr: queryErr,
			wantNil:  true,
			wantErr:  queryErr,
		},
		{
			name:        "no rebate config",
			configs:     []*DailyBehaviorRebate{},
			wantOrderID: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &fakeRebateRepository{
				queryDailyBehaviorRebateConfigFn: func(_ context.Context, behaviorType BehaviorType) ([]*DailyBehaviorRebate, error) {
					if behaviorType != Sign {
						t.Fatalf("QueryDailyBehaviorRebateConfig() behaviorType = %q, want %q", behaviorType, Sign)
					}
					return tt.configs, tt.queryErr
				},
			}
			usecase := NewBehaviorRebateUsecase(repo)

			orderIDs, err := usecase.CreateOrder(context.Background(), &Behavior{
				UserID:        "user-1",
				BehaviorType:  Sign,
				OutBusinessNo: "20260720",
			})

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("CreateOrder() error = %v, want %v", err, tt.wantErr)
			}
			if tt.wantNil {
				if orderIDs != nil {
					t.Fatalf("CreateOrder() orderIDs = %#v, want nil", orderIDs)
				}
				return
			}
			if orderIDs == nil || len(orderIDs) != tt.wantOrderID {
				t.Fatalf("CreateOrder() orderIDs = %#v, want non-nil slice with length %d", orderIDs, tt.wantOrderID)
			}
		})
	}
}

// TestBehaviorRebateUsecaseCreateOrderBuildsAndSavesOrders 验证每条返利配置都会生成一条
// UUIDv7 订单，并把用户、行为、返利配置、外部业务号和幂等 BizID 完整写入保存聚合。
func TestBehaviorRebateUsecaseCreateOrderBuildsAndSavesOrders(t *testing.T) {
	configs := []*DailyBehaviorRebate{
		{
			BehaviorType: string(Sign),
			RebateDesc:   "签到赠送抽奖次数",
			RebateType:   string(Sku),
			RebateConfig: "9011",
		},
		{
			BehaviorType: string(Sign),
			RebateDesc:   "签到赠送积分",
			RebateType:   string(Integral),
			RebateConfig: "100",
		},
	}
	behavior := &Behavior{
		UserID:        "user-1",
		BehaviorType:  Sign,
		OutBusinessNo: "20260720",
	}

	var captured *BehaviorRebate
	repo := &fakeRebateRepository{
		queryDailyBehaviorRebateConfigFn: func(context.Context, BehaviorType) ([]*DailyBehaviorRebate, error) {
			return configs, nil
		},
		saveUserRebateOrderFn: func(_ context.Context, userID string, aggregate *BehaviorRebate) error {
			if userID != behavior.UserID {
				t.Fatalf("SaveUserRebateOrder() userID = %q, want %q", userID, behavior.UserID)
			}
			captured = aggregate
			return nil
		},
	}
	usecase := NewBehaviorRebateUsecase(repo)

	orderIDs, err := usecase.CreateOrder(context.Background(), behavior)

	if err != nil {
		t.Fatalf("CreateOrder() error = %v, want nil", err)
	}
	if len(orderIDs) != len(configs) {
		t.Fatalf("CreateOrder() order count = %d, want %d", len(orderIDs), len(configs))
	}
	if captured == nil {
		t.Fatal("SaveUserRebateOrder() aggregate = nil")
	}
	if captured.UserID != behavior.UserID || captured.Behavior != behavior {
		t.Fatalf("saved aggregate identity = %#v, want user %q and original behavior", captured, behavior.UserID)
	}
	if len(captured.BehaviorRebateOrders) != len(configs) {
		t.Fatalf("saved aggregate order count = %d, want %d", len(captured.BehaviorRebateOrders), len(configs))
	}

	for i, order := range captured.BehaviorRebateOrders {
		config := configs[i]
		if len(order.OrderID) != 32 {
			t.Fatalf("order[%d].OrderID length = %d, want 32: %q", i, len(order.OrderID), order.OrderID)
		}
		parsedOrderID, parseErr := uuid.Parse(order.OrderID)
		if parseErr != nil {
			t.Fatalf("order[%d].OrderID = %q, want UUID: %v", i, order.OrderID, parseErr)
		}
		if parsedOrderID.Version() != 7 {
			t.Fatalf("order[%d].OrderID version = %d, want 7", i, parsedOrderID.Version())
		}
		if orderIDs[i] != order.OrderID {
			t.Fatalf("returned orderIDs[%d] = %q, want saved order ID %q", i, orderIDs[i], order.OrderID)
		}
		if order.UserID != behavior.UserID || order.BehaviorType != config.BehaviorType || order.OutBusinessNo != behavior.OutBusinessNo {
			t.Fatalf("order[%d] behavior fields = %#v, want user/config/out-business fields", i, order)
		}
		if order.RebateDesc != config.RebateDesc || order.RebateType != config.RebateType || order.RebateConfig != config.RebateConfig {
			t.Fatalf("order[%d] rebate fields = %#v, want config %#v", i, order, config)
		}
		wantBizID := behavior.UserID + "_" + config.RebateType + "_" + behavior.OutBusinessNo
		if order.BizID != wantBizID {
			t.Fatalf("order[%d].BizID = %q, want %q", i, order.BizID, wantBizID)
		}
	}
}

// TestBehaviorRebateUsecaseCreateOrderPropagatesSaveError 验证返利订单聚合构造完成后，
// 仓储保存失败会返回原始错误且不会向调用方返回尚未真正落库的订单 ID。
func TestBehaviorRebateUsecaseCreateOrderPropagatesSaveError(t *testing.T) {
	saveErr := errors.New("save rebate order")
	repo := &fakeRebateRepository{
		queryDailyBehaviorRebateConfigFn: func(context.Context, BehaviorType) ([]*DailyBehaviorRebate, error) {
			return []*DailyBehaviorRebate{
				{
					BehaviorType: string(Sign),
					RebateType:   string(Sku),
					RebateConfig: "9011",
				},
			}, nil
		},
		saveUserRebateOrderFn: func(context.Context, string, *BehaviorRebate) error {
			return saveErr
		},
	}
	usecase := NewBehaviorRebateUsecase(repo)

	orderIDs, err := usecase.CreateOrder(context.Background(), &Behavior{
		UserID:        "user-1",
		BehaviorType:  Sign,
		OutBusinessNo: "20260720",
	})

	if !errors.Is(err, saveErr) {
		t.Fatalf("CreateOrder() error = %v, want %v", err, saveErr)
	}
	if orderIDs != nil {
		t.Fatalf("CreateOrder() orderIDs = %#v, want nil", orderIDs)
	}
}

// TestBehaviorRebateUsecaseQueryOrderByOutBusinessNoDelegatesToRepository 验证签到幂等查询
// 会把用户 ID 和外部业务号原样传给仓储，并原样返回查询结果或查询错误。
func TestBehaviorRebateUsecaseQueryOrderByOutBusinessNoDelegatesToRepository(t *testing.T) {
	queryErr := errors.New("query rebate order")
	existing := []*BehaviorRebateOrder{
		{UserID: "user-1", OrderID: "000000000001", OutBusinessNo: "20260720"},
	}
	tests := []struct {
		name    string
		orders  []*BehaviorRebateOrder
		repoErr error
	}{
		{name: "existing order", orders: existing},
		{name: "order not found", orders: []*BehaviorRebateOrder{}},
		{name: "repository failure", repoErr: queryErr},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &fakeRebateRepository{
				queryUserRebateOrderFn: func(_ context.Context, userID string, outBusinessNo string) ([]*BehaviorRebateOrder, error) {
					if userID != "user-1" || outBusinessNo != "20260720" {
						t.Fatalf("QueryUserRebateOrder() args = (%q, %q), want (%q, %q)", userID, outBusinessNo, "user-1", "20260720")
					}
					return tt.orders, tt.repoErr
				},
			}
			usecase := NewBehaviorRebateUsecase(repo)

			orders, err := usecase.QueryOrderByOutBusinessNo(context.Background(), "user-1", "20260720")

			if !errors.Is(err, tt.repoErr) {
				t.Fatalf("QueryOrderByOutBusinessNo() error = %v, want %v", err, tt.repoErr)
			}
			if len(orders) != len(tt.orders) {
				t.Fatalf("QueryOrderByOutBusinessNo() order count = %d, want %d", len(orders), len(tt.orders))
			}
			for i := range orders {
				if orders[i] != tt.orders[i] {
					t.Fatalf("QueryOrderByOutBusinessNo() orders[%d] = %p, want %p", i, orders[i], tt.orders[i])
				}
			}
		})
	}
}
