//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"prizeforge/internal/domain/activity"
	"prizeforge/internal/infrastructure/adapter"
	"prizeforge/internal/infrastructure/repository/activityrepo"
	"prizeforge/internal/infrastructure/repository/po"
	"prizeforge/pkg/xrand"

	"gorm.io/gorm"
)

const (
	integrationOrderActivityID int64 = 900301
	integrationOrderStrategyID int64 = 900006
)

type activityOrderFixture struct {
	db         *gorm.DB
	userID     string
	orderTable string
	awardTable string
	orderTime  time.Time
}

func newActivityOrderFixture(t *testing.T, totalSurplus, daySurplus, monthSurplus int) *activityOrderFixture {
	t.Helper()
	userID := "it-order-" + xrand.RandomNumeric(12)
	db, suffix := integrationDBRouter.DBStrategy(userID)
	fixture := &activityOrderFixture{
		db: db, userID: userID,
		orderTable: "user_raffle_order_" + suffix,
		awardTable: "user_award_record_" + suffix,
		orderTime:  time.Now().Truncate(time.Second),
	}
	trackIntegrationRedisKeys(t,
		adapter.GetActivityAccountKey(integrationOrderActivityID, userID),
		adapter.GetActivityAccountTotalSurplusKey(integrationOrderActivityID, userID),
		adapter.GetActivityAccountMonthSurplusKey(integrationOrderActivityID, userID, fixture.orderTime.Format("2006-01")),
		adapter.GetActivityAccountDaySurplusKey(integrationOrderActivityID, userID, fixture.orderTime.Format("2006-01-02")),
		adapter.GetPendingRaffleOrderKey(integrationOrderActivityID, userID),
	)
	t.Cleanup(func() {
		deleteIntegrationRows(t, db, "task", "user_id", userID)
		deleteIntegrationRows(t, db, fixture.awardTable, "user_id", userID)
		deleteIntegrationRows(t, db, fixture.orderTable, "user_id", userID)
		deleteIntegrationRows(t, db, "raffle_activity_account_day", "user_id", userID)
		deleteIntegrationRows(t, db, "raffle_activity_account_month", "user_id", userID)
		deleteIntegrationRows(t, db, "raffle_activity_account", "user_id", userID)
	})
	account := &po.RaffleActivityAccount{
		UserID: userID, ActivityID: integrationOrderActivityID,
		TotalCount: 10, TotalCountSurplus: totalSurplus,
		DayCount: 10, DayCountSurplus: daySurplus,
		MonthCount: 10, MonthCountSurplus: monthSurplus,
		CreateTime: fixture.orderTime, UpdateTime: fixture.orderTime,
	}
	if err := db.Create(account).Error; err != nil {
		t.Fatalf("prepare activity account: %v", err)
	}
	month := &po.RaffleActivityAccountMonth{
		UserID: userID, ActivityID: integrationOrderActivityID,
		Month: fixture.orderTime.Format("2006-01"), MonthCount: 10, MonthCountSurplus: monthSurplus,
		CreateTime: fixture.orderTime, UpdateTime: fixture.orderTime,
	}
	if err := db.Create(month).Error; err != nil {
		t.Fatalf("prepare activity month account: %v", err)
	}
	day := &po.RaffleActivityAccountDay{
		UserID: userID, ActivityID: integrationOrderActivityID,
		Day: fixture.orderTime.Format("2006-01-02"), DayCount: 10, DayCountSurplus: daySurplus,
		CreateTime: fixture.orderTime, UpdateTime: fixture.orderTime,
	}
	if err := db.Create(day).Error; err != nil {
		t.Fatalf("prepare activity day account: %v", err)
	}
	return fixture
}

func (f *activityOrderFixture) repository() *activityrepo.Repository {
	return activityrepo.NewRepository(integrationDBRouter, integrationDefaultDB, integrationRedis, nil, nil, nil)
}

func (f *activityOrderFixture) order(orderID, requestID string) *activity.UserRaffleOrder {
	return &activity.UserRaffleOrder{
		UserID: f.userID, ActivityID: integrationOrderActivityID, ActivityName: "集成测试活动",
		StrategyID: integrationOrderStrategyID, OrderID: orderID, RequestID: requestID,
		OrderTime: f.orderTime, OrderState: activity.UserRaffleOrderStateCreate, DrawState: activity.DrawStateCreated,
	}
}

func (f *activityOrderFixture) drawResult(order *activity.UserRaffleOrder) *activity.DrawResult {
	return &activity.DrawResult{
		UserID: order.UserID, ActivityID: order.ActivityID, ActivityName: order.ActivityName,
		StrategyID: order.StrategyID, OrderID: order.OrderID, RequestID: order.RequestID,
		OrderTime: order.OrderTime, AwardID: 101, AwardTitle: "一等奖",
		AwardTime: f.orderTime.Add(time.Minute), StockReserved: true,
	}
}

func TestActivityRepositoryPreheatsOnlyMissingCurrentQuotaKeys(t *testing.T) {
	fixture := newActivityOrderFixture(t, 7, 5, 6)
	repo := fixture.repository()
	ctx := context.Background()
	totalKey := adapter.GetActivityAccountTotalSurplusKey(integrationOrderActivityID, fixture.userID)
	monthKey := adapter.GetActivityAccountMonthSurplusKey(integrationOrderActivityID, fixture.userID, fixture.orderTime.Format("2006-01"))
	dayKey := adapter.GetActivityAccountDaySurplusKey(integrationOrderActivityID, fixture.userID, fixture.orderTime.Format("2006-01-02"))

	if err := integrationRedisClient.Set(ctx, totalKey, 4, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := integrationRedisClient.Set(ctx, monthKey, 3, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := integrationRedisClient.Del(ctx, dayKey).Err(); err != nil {
		t.Fatal(err)
	}

	if err := repo.AssembleActivityAccountByUserId(ctx, fixture.userID, integrationOrderActivityID); err != nil {
		t.Fatalf("AssembleActivityAccountByUserId() error = %v", err)
	}
	// 已存在的总/月额度不能被 MySQL 快照覆盖；缺失的当日额度从当日明细补齐。
	assertActivityRedisQuota(t, fixture, 4, 5, 3)

	if err := integrationRedisClient.Decr(ctx, dayKey).Err(); err != nil {
		t.Fatal(err)
	}
	if err := repo.AssembleActivityAccountByUserId(ctx, fixture.userID, integrationOrderActivityID); err != nil {
		t.Fatalf("repeated AssembleActivityAccountByUserId() error = %v", err)
	}
	assertActivityRedisQuota(t, fixture, 4, 4, 3)
}

func TestActivityRepositoryPreheatsNewPeriodFromConfiguredLimits(t *testing.T) {
	fixture := newActivityOrderFixture(t, 7, 1, 2)
	repo := fixture.repository()
	ctx := context.Background()
	totalKey := adapter.GetActivityAccountTotalSurplusKey(integrationOrderActivityID, fixture.userID)
	monthKey := adapter.GetActivityAccountMonthSurplusKey(integrationOrderActivityID, fixture.userID, fixture.orderTime.Format("2006-01"))
	dayKey := adapter.GetActivityAccountDaySurplusKey(integrationOrderActivityID, fixture.userID, fixture.orderTime.Format("2006-01-02"))

	// 模拟进入一个尚未创建日/月明细的新周期。主账户上的旧 surplus 不可带入新周期。
	if err := fixture.db.Where("user_id = ? AND activity_id = ?", fixture.userID, integrationOrderActivityID).
		Delete(&po.RaffleActivityAccountDay{}).Error; err != nil {
		t.Fatal(err)
	}
	if err := fixture.db.Where("user_id = ? AND activity_id = ?", fixture.userID, integrationOrderActivityID).
		Delete(&po.RaffleActivityAccountMonth{}).Error; err != nil {
		t.Fatal(err)
	}
	if err := integrationRedisClient.Set(ctx, totalKey, 7, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := integrationRedisClient.Del(ctx, monthKey, dayKey).Err(); err != nil {
		t.Fatal(err)
	}

	if err := repo.AssembleActivityAccountByUserId(ctx, fixture.userID, integrationOrderActivityID); err != nil {
		t.Fatalf("AssembleActivityAccountByUserId() error = %v", err)
	}
	assertActivityRedisQuota(t, fixture, 7, 10, 10)
}

func TestActivityRepositoryRedisFirstReservationAndResultStream(t *testing.T) {
	fixture := newActivityOrderFixture(t, 3, 3, 3)
	repo := fixture.repository()
	requestID := "request-" + xrand.RandomNumeric(12)
	resultKey := adapter.GetDrawRequestResultKey(integrationOrderActivityID, fixture.userID, requestID)
	trackIntegrationRedisKeys(t, resultKey)

	order, result, reused, err := repo.CreateOrLoadUserRaffleOrder(
		context.Background(), fixture.order(xrand.RandomNumeric(12), requestID),
	)
	if err != nil || result != nil || reused {
		t.Fatalf("first reservation = (%#v, %#v, %v, %v)", order, result, reused, err)
	}
	assertActivityOrderCount(t, fixture, 0)
	assertActivityRedisQuota(t, fixture, 2, 2, 2)

	retried, _, reused, err := repo.CreateOrLoadUserRaffleOrder(
		context.Background(), fixture.order(xrand.RandomNumeric(12), requestID),
	)
	if err != nil || !reused || retried.OrderID != order.OrderID {
		t.Fatalf("retry reservation = (%#v, %v, %v)", retried, reused, err)
	}

	claim, err := repo.TryClaimUserRaffleOrder(context.Background(), fixture.userID, integrationOrderActivityID, requestID, order.OrderID)
	if err != nil || claim.Status != activity.DrawClaimAcquired {
		t.Fatalf("TryClaimUserRaffleOrder() = (%#v, %v)", claim, err)
	}
	publication, err := repo.CompleteUserRaffleOrder(context.Background(), fixture.drawResult(order), claim.Owner)
	if err != nil || publication.StreamID == "" {
		t.Fatalf("CompleteUserRaffleOrder() = (%#v, %v)", publication, err)
	}
	t.Cleanup(func() {
		_ = integrationRedisClient.XDel(context.Background(), adapter.GetDrawResultStreamKey(), publication.StreamID).Err()
	})

	pending, err := repo.QueryPendingDrawResultPublications(context.Background(), 10)
	if err != nil || len(pending) == 0 {
		t.Fatalf("QueryPendingDrawResultPublications() = (%d, %v)", len(pending), err)
	}
	if err := repo.MarkDrawResultPublished(context.Background(), publication); err != nil {
		t.Fatalf("MarkDrawResultPublished() error = %v", err)
	}

	_, completed, reused, err := repo.CreateOrLoadUserRaffleOrder(
		context.Background(), fixture.order(xrand.RandomNumeric(12), requestID),
	)
	if err != nil || !reused || completed == nil || !completed.BrokerConfirmed ||
		completed.Result == nil || completed.Result.AwardID != 101 {
		t.Fatalf("completed retry = (%#v, %v, %v)", completed, reused, err)
	}
}

func TestActivityRepositoryPersistsCompleteDrawTransactionIdempotently(t *testing.T) {
	fixture := newActivityOrderFixture(t, 3, 3, 3)
	repo := fixture.repository()
	requestID := "request-" + xrand.RandomNumeric(12)
	trackIntegrationRedisKeys(t, adapter.GetDrawRequestResultKey(integrationOrderActivityID, fixture.userID, requestID))
	order, _, _, err := repo.CreateOrLoadUserRaffleOrder(
		context.Background(), fixture.order(xrand.RandomNumeric(12), requestID),
	)
	if err != nil {
		t.Fatalf("CreateOrLoadUserRaffleOrder() error = %v", err)
	}
	result := fixture.drawResult(order)

	if err := repo.SaveDrawResult(context.Background(), result); err != nil {
		t.Fatalf("first SaveDrawResult() error = %v", err)
	}
	if err := repo.SaveDrawResult(context.Background(), result); err != nil {
		t.Fatalf("duplicate SaveDrawResult() error = %v", err)
	}

	assertActivityOrderCount(t, fixture, 1)
	assertActivityAccountState(t, fixture, 2, 3, 3, "")
	assertActivityPeriodQuota(t, fixture, 2, 2)
	var awardCount, taskCount int64
	if err := fixture.db.Table(fixture.awardTable).Where("user_id = ? AND order_id = ?", fixture.userID, order.OrderID).Count(&awardCount).Error; err != nil {
		t.Fatal(err)
	}
	if err := fixture.db.Table("task").Where("user_id = ?", fixture.userID).Count(&taskCount).Error; err != nil {
		t.Fatal(err)
	}
	if awardCount != 1 || taskCount != 2 {
		t.Fatalf("persisted counts award=%d task=%d, want 1/2", awardCount, taskCount)
	}
	exists, err := integrationRedisClient.Exists(context.Background(), adapter.GetPendingRaffleOrderKey(integrationOrderActivityID, fixture.userID)).Result()
	if err != nil || exists != 0 {
		t.Fatalf("pending reservation exists=%d err=%v, want deleted", exists, err)
	}
}

func TestActivityRepositoryRollsBackCompleteDrawWhenMySQLQuotaIsExhausted(t *testing.T) {
	fixture := newActivityOrderFixture(t, 0, 3, 3)
	result := fixture.drawResult(fixture.order(xrand.RandomNumeric(12), "request-"+xrand.RandomNumeric(12)))
	err := fixture.repository().SaveDrawResult(context.Background(), result)
	if !errors.Is(err, activity.ErrActivityQuotaError) {
		t.Fatalf("SaveDrawResult() error = %v, want ErrActivityQuotaError", err)
	}
	assertActivityOrderCount(t, fixture, 0)
	assertActivityAccountState(t, fixture, 0, 3, 3, "")
}

func assertActivityOrderCount(t *testing.T, fixture *activityOrderFixture, want int64) {
	t.Helper()
	var got int64
	if err := fixture.db.Table(fixture.orderTable).Where("user_id = ?", fixture.userID).Count(&got).Error; err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("activity order count = %d, want %d", got, want)
	}
}

func assertActivityRedisQuota(t *testing.T, fixture *activityOrderFixture, total, day, month int64) {
	t.Helper()
	assertIntegrationRedisInt(t, adapter.GetActivityAccountTotalSurplusKey(integrationOrderActivityID, fixture.userID), total)
	assertIntegrationRedisInt(t, adapter.GetActivityAccountDaySurplusKey(integrationOrderActivityID, fixture.userID, fixture.orderTime.Format("2006-01-02")), day)
	assertIntegrationRedisInt(t, adapter.GetActivityAccountMonthSurplusKey(integrationOrderActivityID, fixture.userID, fixture.orderTime.Format("2006-01")), month)
}

func assertActivityAccountState(t *testing.T, fixture *activityOrderFixture, total, day, month int, currentOrderID string) {
	t.Helper()
	var account po.RaffleActivityAccount
	if err := fixture.db.Table("raffle_activity_account").
		Where("user_id = ? AND activity_id = ?", fixture.userID, integrationOrderActivityID).
		First(&account).Error; err != nil {
		t.Fatal(err)
	}
	if account.TotalCountSurplus != total || account.DayCountSurplus != day ||
		account.MonthCountSurplus != month || account.CurrentOrderID != currentOrderID {
		t.Fatalf("activity account = (%d,%d,%d,%q), want (%d,%d,%d,%q)",
			account.TotalCountSurplus, account.DayCountSurplus, account.MonthCountSurplus, account.CurrentOrderID,
			total, day, month, currentOrderID)
	}
}

func assertActivityPeriodQuota(t *testing.T, fixture *activityOrderFixture, day, month int) {
	t.Helper()
	var dayPO po.RaffleActivityAccountDay
	if err := fixture.db.Table("raffle_activity_account_day").
		Where("user_id = ? AND activity_id = ? AND day = ?", fixture.userID, integrationOrderActivityID, fixture.orderTime.Format("2006-01-02")).
		First(&dayPO).Error; err != nil {
		t.Fatal(err)
	}
	var monthPO po.RaffleActivityAccountMonth
	if err := fixture.db.Table("raffle_activity_account_month").
		Where("user_id = ? AND activity_id = ? AND month = ?", fixture.userID, integrationOrderActivityID, fixture.orderTime.Format("2006-01")).
		First(&monthPO).Error; err != nil {
		t.Fatal(err)
	}
	if dayPO.DayCountSurplus != day || monthPO.MonthCountSurplus != month {
		t.Fatalf("period quota day/month = %d/%d, want %d/%d", dayPO.DayCountSurplus, monthPO.MonthCountSurplus, day, month)
	}
}
