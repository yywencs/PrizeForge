//go:build integration

package integration

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"prizeforge/internal/domain/activity"
	"prizeforge/internal/infrastructure/repository/activityrepo"
	"prizeforge/internal/infrastructure/repository/po"
	"prizeforge/pkg/cache"
	"prizeforge/pkg/xrand"

	"gorm.io/gorm"
)

const (
	integrationOrderActivityID int64 = 900301
	integrationOrderStrategyID int64 = 900006
)

type activityOrderFixture struct {
	db           *gorm.DB
	userID       string
	orderTable   string
	orderTime    time.Time
	totalSurplus int
	daySurplus   int
	monthSurplus int
}

func newActivityOrderFixture(t *testing.T, totalSurplus, daySurplus, monthSurplus int) *activityOrderFixture {
	t.Helper()

	userID := "it-order-" + xrand.RandomNumeric(12)
	db, tableSuffix := integrationDBRouter.DBStrategy(userID)
	if db == nil {
		t.Fatal("DBStrategy() database = nil")
	}
	fixture := &activityOrderFixture{
		db:           db,
		userID:       userID,
		orderTable:   "user_raffle_order_" + tableSuffix,
		orderTime:    time.Now().Truncate(time.Second),
		totalSurplus: totalSurplus,
		daySurplus:   daySurplus,
		monthSurplus: monthSurplus,
	}
	t.Cleanup(func() {
		deleteIntegrationRows(t, db, fixture.orderTable, "user_id", userID)
		deleteIntegrationRows(t, db, "raffle_activity_account_day", "user_id", userID)
		deleteIntegrationRows(t, db, "raffle_activity_account_month", "user_id", userID)
		deleteIntegrationRows(t, db, "raffle_activity_account", "user_id", userID)
	})

	account := &po.RaffleActivityAccount{
		UserID:            userID,
		ActivityID:        integrationOrderActivityID,
		TotalCount:        10,
		TotalCountSurplus: totalSurplus,
		DayCount:          10,
		DayCountSurplus:   daySurplus,
		MonthCount:        10,
		MonthCountSurplus: monthSurplus,
		CurrentOrderID:    "",
		CreateTime:        fixture.orderTime,
		UpdateTime:        fixture.orderTime,
	}
	if err := db.Create(account).Error; err != nil {
		t.Fatalf("prepare activity account: %v", err)
	}
	return fixture
}

func (f *activityOrderFixture) repository() activity.Repo {
	localCache := cache.New(&cache.Options{
		LocalCache: cache.NewTinyLFU(64, time.Minute),
	})
	return activityrepo.NewRepository(integrationDBRouter, integrationDefaultDB, localCache, nil, nil, nil)
}

func (f *activityOrderFixture) order(orderID, requestID string) *activity.UserRaffleOrder {
	return &activity.UserRaffleOrder{
		UserID:       f.userID,
		ActivityID:   integrationOrderActivityID,
		ActivityName: "集成测试活动",
		StrategyID:   integrationOrderStrategyID,
		OrderID:      orderID,
		RequestID:    requestID,
		OrderTime:    f.orderTime,
		OrderState:   activity.UserRaffleOrderStateCreate,
		DrawState:    activity.DrawStateCreated,
	}
}

// TestActivityRepositoryCreatesOrderAndReusesRequest 验证首次请求会原子创建订单并扣减
// 总、月、日额度，而相同 request_id 重试会复用原订单且不再次扣减额度。
func TestActivityRepositoryCreatesOrderAndReusesRequest(t *testing.T) {
	fixture := newActivityOrderFixture(t, 3, 3, 3)
	repository := fixture.repository()
	requestID := "request-" + xrand.RandomNumeric(12)
	firstOrderID := xrand.RandomNumeric(12)

	first, reused, err := repository.CreateOrLoadUserRaffleOrder(
		context.Background(), fixture.order(firstOrderID, requestID),
	)
	if err != nil {
		t.Fatalf("first CreateOrLoadUserRaffleOrder() error = %v, want nil", err)
	}
	if reused {
		t.Fatal("first CreateOrLoadUserRaffleOrder() reused = true, want false")
	}
	if first.OrderID != firstOrderID {
		t.Fatalf("first order ID = %q, want %q", first.OrderID, firstOrderID)
	}

	retryCandidateID := xrand.RandomNumeric(12)
	retried, reused, err := repository.CreateOrLoadUserRaffleOrder(
		context.Background(), fixture.order(retryCandidateID, requestID),
	)
	if err != nil {
		t.Fatalf("retry CreateOrLoadUserRaffleOrder() error = %v, want nil", err)
	}
	if !reused {
		t.Fatal("retry CreateOrLoadUserRaffleOrder() reused = false, want true")
	}
	if retried.OrderID != firstOrderID {
		t.Fatalf("retry order ID = %q, want canonical %q", retried.OrderID, firstOrderID)
	}

	assertActivityOrderCount(t, fixture, 1)
	assertActivityAccountState(t, fixture, 2, 2, 2, firstOrderID)
	assertActivityPeriodQuota(t, fixture, 2, 2)
}

// TestActivityRepositoryRejectsNewRequestWhileOrderPending 验证已有 created 订单时，新的
// request_id 会收到 ErrDrawInProgress，不会创建第二条订单或再次扣减额度。
func TestActivityRepositoryRejectsNewRequestWhileOrderPending(t *testing.T) {
	fixture := newActivityOrderFixture(t, 3, 3, 3)
	repository := fixture.repository()
	firstOrderID := xrand.RandomNumeric(12)

	if _, _, err := repository.CreateOrLoadUserRaffleOrder(
		context.Background(), fixture.order(firstOrderID, "request-"+xrand.RandomNumeric(12)),
	); err != nil {
		t.Fatalf("first CreateOrLoadUserRaffleOrder() error = %v, want nil", err)
	}
	_, _, err := repository.CreateOrLoadUserRaffleOrder(
		context.Background(), fixture.order(xrand.RandomNumeric(12), "request-"+xrand.RandomNumeric(12)),
	)
	if !errors.Is(err, activity.ErrDrawInProgress) {
		t.Fatalf("new request error = %v, want ErrDrawInProgress", err)
	}

	assertActivityOrderCount(t, fixture, 1)
	assertActivityAccountState(t, fixture, 2, 2, 2, firstOrderID)
	assertActivityPeriodQuota(t, fixture, 2, 2)
}

// TestActivityRepositoryRollsBackWhenQuotaInsufficient 验证总、月或日额度不足时不会创建
// 抽奖订单或修改总账户；日额度失败发生在月额度处理之后，仍必须回滚已创建的月记录。
func TestActivityRepositoryRollsBackWhenQuotaInsufficient(t *testing.T) {
	tests := []struct {
		name          string
		totalSurplus  int
		daySurplus    int
		monthSurplus  int
		prepare       func(*testing.T, *activityOrderFixture)
		wantErr       error
		wantDayRows   int64
		wantMonthRows int64
	}{
		{
			name:         "total quota exhausted",
			totalSurplus: 0,
			daySurplus:   2,
			monthSurplus: 2,
			wantErr:      activity.ErrActivityQuotaError,
		},
		{
			name:         "month quota exhausted",
			totalSurplus: 2,
			daySurplus:   2,
			monthSurplus: 0,
			wantErr:      activity.ErrActivityAccountMonthCountSurplusNotEnough,
		},
		{
			name:          "existing day quota exhausted after month step",
			totalSurplus:  2,
			daySurplus:    2,
			monthSurplus:  2,
			prepare:       prepareExhaustedActivityDay,
			wantErr:       activity.ErrActivityAccountDayCountSurplusNotEnough,
			wantDayRows:   1,
			wantMonthRows: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := newActivityOrderFixture(t, tt.totalSurplus, tt.daySurplus, tt.monthSurplus)
			if tt.prepare != nil {
				tt.prepare(t, fixture)
			}
			_, _, err := fixture.repository().CreateOrLoadUserRaffleOrder(
				context.Background(),
				fixture.order(xrand.RandomNumeric(12), "request-"+xrand.RandomNumeric(12)),
			)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("CreateOrLoadUserRaffleOrder() error = %v, want %v", err, tt.wantErr)
			}

			assertActivityOrderCount(t, fixture, 0)
			assertActivityAccountState(t, fixture, tt.totalSurplus, tt.daySurplus, tt.monthSurplus, "")
			assertActivityPeriodRowCount(t, fixture, tt.wantDayRows, tt.wantMonthRows)
		})
	}
}

// TestActivityRepositoryCreatesOneOrderForConcurrentRetries 验证多个 goroutine 使用相同
// request_id 并发进入真实 MySQL 时，账户行锁和事务内二次查询只允许创建一条订单、扣一次额度。
func TestActivityRepositoryCreatesOneOrderForConcurrentRetries(t *testing.T) {
	fixture := newActivityOrderFixture(t, 5, 5, 5)
	repository := fixture.repository()
	requestID := "request-" + xrand.RandomNumeric(12)
	const workers = 8

	type result struct {
		order  *activity.UserRaffleOrder
		reused bool
		err    error
	}
	results := make(chan result, workers)
	start := make(chan struct{})
	var waitGroup sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		candidateOrderID := xrand.RandomNumeric(12)
		waitGroup.Add(1)
		go func(orderID string) {
			defer waitGroup.Done()
			<-start
			order, reused, err := repository.CreateOrLoadUserRaffleOrder(
				context.Background(), fixture.order(orderID, requestID),
			)
			results <- result{order: order, reused: reused, err: err}
		}(candidateOrderID)
	}
	close(start)
	waitGroup.Wait()
	close(results)

	canonicalOrderID := ""
	createdCount := 0
	for got := range results {
		if got.err != nil {
			t.Fatalf("concurrent CreateOrLoadUserRaffleOrder() error = %v, want nil", got.err)
		}
		if got.order == nil {
			t.Fatal("concurrent CreateOrLoadUserRaffleOrder() order = nil")
		}
		if canonicalOrderID == "" {
			canonicalOrderID = got.order.OrderID
		}
		if got.order.OrderID != canonicalOrderID {
			t.Fatalf("concurrent order ID = %q, want canonical %q", got.order.OrderID, canonicalOrderID)
		}
		if !got.reused {
			createdCount++
		}
	}
	if createdCount != 1 {
		t.Fatalf("fresh concurrent result count = %d, want 1", createdCount)
	}

	assertActivityOrderCount(t, fixture, 1)
	assertActivityAccountState(t, fixture, 4, 4, 4, canonicalOrderID)
	assertActivityPeriodQuota(t, fixture, 4, 4)
}

func prepareExhaustedActivityDay(t *testing.T, fixture *activityOrderFixture) {
	t.Helper()
	day := &po.RaffleActivityAccountDay{
		UserID:          fixture.userID,
		ActivityID:      integrationOrderActivityID,
		Day:             fixture.orderTime.Format("2006-01-02"),
		DayCount:        10,
		DayCountSurplus: 0,
		CreateTime:      fixture.orderTime,
		UpdateTime:      fixture.orderTime,
	}
	if err := fixture.db.Create(day).Error; err != nil {
		t.Fatalf("prepare exhausted activity day: %v", err)
	}
}

func assertActivityOrderCount(t *testing.T, fixture *activityOrderFixture, wantCount int64) {
	t.Helper()
	var count int64
	if err := fixture.db.Table(fixture.orderTable).Where("user_id = ?", fixture.userID).Count(&count).Error; err != nil {
		t.Fatalf("count activity orders: %v", err)
	}
	if count != wantCount {
		t.Fatalf("activity order count = %d, want %d", count, wantCount)
	}
}

func assertActivityAccountState(
	t *testing.T,
	fixture *activityOrderFixture,
	wantTotal, wantDay, wantMonth int,
	wantCurrentOrderID string,
) {
	t.Helper()
	var account po.RaffleActivityAccount
	if err := fixture.db.
		Where("user_id = ? AND activity_id = ?", fixture.userID, integrationOrderActivityID).
		First(&account).Error; err != nil {
		t.Fatalf("query activity account: %v", err)
	}
	if account.TotalCountSurplus != wantTotal || account.DayCountSurplus != wantDay ||
		account.MonthCountSurplus != wantMonth || account.CurrentOrderID != wantCurrentOrderID {
		t.Fatalf(
			"activity account = total:%d day:%d month:%d current:%q, want total:%d day:%d month:%d current:%q",
			account.TotalCountSurplus, account.DayCountSurplus, account.MonthCountSurplus, account.CurrentOrderID,
			wantTotal, wantDay, wantMonth, wantCurrentOrderID,
		)
	}
}

func assertActivityPeriodQuota(t *testing.T, fixture *activityOrderFixture, wantDay, wantMonth int) {
	t.Helper()
	var day po.RaffleActivityAccountDay
	if err := fixture.db.
		Where("user_id = ? AND activity_id = ? AND day = ?", fixture.userID, integrationOrderActivityID, fixture.orderTime.Format("2006-01-02")).
		First(&day).Error; err != nil {
		t.Fatalf("query activity day account: %v", err)
	}
	if day.DayCountSurplus != wantDay {
		t.Fatalf("activity day surplus = %d, want %d", day.DayCountSurplus, wantDay)
	}

	var month po.RaffleActivityAccountMonth
	if err := fixture.db.
		Where("user_id = ? AND activity_id = ? AND month = ?", fixture.userID, integrationOrderActivityID, fixture.orderTime.Format("2006-01")).
		First(&month).Error; err != nil {
		t.Fatalf("query activity month account: %v", err)
	}
	if month.MonthCountSurplus != wantMonth {
		t.Fatalf("activity month surplus = %d, want %d", month.MonthCountSurplus, wantMonth)
	}
}

func assertActivityPeriodRowCount(t *testing.T, fixture *activityOrderFixture, wantDay, wantMonth int64) {
	t.Helper()
	var dayCount int64
	if err := fixture.db.Model(&po.RaffleActivityAccountDay{}).Where("user_id = ?", fixture.userID).Count(&dayCount).Error; err != nil {
		t.Fatalf("count activity day rows: %v", err)
	}
	var monthCount int64
	if err := fixture.db.Model(&po.RaffleActivityAccountMonth{}).Where("user_id = ?", fixture.userID).Count(&monthCount).Error; err != nil {
		t.Fatalf("count activity month rows: %v", err)
	}
	if dayCount != wantDay || monthCount != wantMonth {
		t.Fatalf("period row count = day:%d month:%d, want day:%d month:%d", dayCount, monthCount, wantDay, wantMonth)
	}
}
