package activity

import (
	"testing"

	"github.com/google/uuid"
)

// TestActivityQuotaUsecaseBuildOrderAggregateUsesUUIDv7 验证活动额度订单
// 使用 32 位 UUIDv7，并完整保留充值和活动配置。
func TestActivityQuotaUsecaseBuildOrderAggregateUsesUUIDv7(t *testing.T) {
	usecase := &ActivityQuotaUsecase{}
	recharge := &SkuRecharge{
		UserID:        "user-1",
		Sku:           9011,
		OutBusinessNo: "sign-20260723",
	}
	activitySKU := &ActivitySku{
		Sku:             recharge.Sku,
		ActivityID:      100301,
		ActivityCountID: 10,
	}
	raffleActivity := &Activity{
		ActivityID:   activitySKU.ActivityID,
		ActivityName: "summer raffle",
		StrategyID:   100001,
	}
	activityCount := &ActivityCount{
		TotalCount: 20,
		DayCount:   10,
		MonthCount: 15,
	}

	aggregate, err := usecase.buildOrderAggregate(recharge, activitySKU, raffleActivity, activityCount)
	if err != nil {
		t.Fatalf("buildOrderAggregate() error = %v, want nil", err)
	}
	if aggregate == nil || aggregate.ActivityOrder == nil {
		t.Fatal("buildOrderAggregate() aggregate or order = nil")
	}
	order := aggregate.ActivityOrder
	if len(order.OrderID) != 32 {
		t.Fatalf("order ID length = %d, want 32: %q", len(order.OrderID), order.OrderID)
	}
	parsedOrderID, err := uuid.Parse(order.OrderID)
	if err != nil {
		t.Fatalf("order ID = %q, want UUID: %v", order.OrderID, err)
	}
	if parsedOrderID.Version() != 7 {
		t.Fatalf("order ID version = %d, want 7", parsedOrderID.Version())
	}
	if order.UserID != recharge.UserID || order.Sku != recharge.Sku ||
		order.OutBusinessNo != recharge.OutBusinessNo {
		t.Fatalf("order recharge fields = %#v, want values from %#v", order, recharge)
	}
	if order.ActivityID != raffleActivity.ActivityID ||
		order.ActivityName != raffleActivity.ActivityName ||
		order.StrategyID != raffleActivity.StrategyID {
		t.Fatalf("order activity fields = %#v, want values from %#v", order, raffleActivity)
	}
	if aggregate.TotalCount != activityCount.TotalCount ||
		aggregate.DayCount != activityCount.DayCount ||
		aggregate.MonthCount != activityCount.MonthCount {
		t.Fatalf("aggregate counts = (%d, %d, %d), want (%d, %d, %d)",
			aggregate.TotalCount, aggregate.DayCount, aggregate.MonthCount,
			activityCount.TotalCount, activityCount.DayCount, activityCount.MonthCount)
	}
}
