//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"prizeforge/internal/domain/award"
	"prizeforge/internal/domain/strategy"
	"prizeforge/internal/infrastructure/repository/awardrepo"
	"prizeforge/internal/infrastructure/repository/po"
	"prizeforge/pkg/cache"
	"prizeforge/pkg/xrand"

	"gorm.io/gorm"
)

const (
	integrationActivityID int64 = 100301
	integrationStrategyID int64 = 100006
	integrationAwardID          = 101
)

type awardTransactionFixture struct {
	db           *gorm.DB
	userID       string
	orderID      string
	drawOwner    string
	awardTable   string
	raffleTable  string
	processingAt time.Time
}

func newAwardTransactionFixture(t *testing.T) *awardTransactionFixture {
	t.Helper()

	orderID := xrand.RandomNumeric(12)
	userID := "it-award-" + orderID
	drawOwner := "owner-" + orderID
	db, tableSuffix := integrationDBRouter.DBStrategy(userID)
	if db == nil {
		t.Fatal("DBStrategy() database = nil")
	}
	if len(tableSuffix) != 3 {
		t.Fatalf("DBStrategy() table suffix = %q, want three digits", tableSuffix)
	}

	fixture := &awardTransactionFixture{
		db:           db,
		userID:       userID,
		orderID:      orderID,
		drawOwner:    drawOwner,
		awardTable:   "user_award_record_" + tableSuffix,
		raffleTable:  "user_raffle_order_" + tableSuffix,
		processingAt: time.Now().Truncate(time.Second),
	}
	fixture.registerCleanup(t)

	now := time.Now().Truncate(time.Second)
	account := &po.RaffleActivityAccount{
		UserID:            userID,
		ActivityID:        integrationActivityID,
		TotalCount:        1,
		TotalCountSurplus: 0,
		DayCount:          1,
		DayCountSurplus:   0,
		MonthCount:        1,
		MonthCountSurplus: 0,
		CurrentOrderID:    orderID,
		CreateTime:        now,
		UpdateTime:        now,
	}
	raffleOrder := &po.UserRaffleOrder{
		UserID:           userID,
		ActivityID:       integrationActivityID,
		ActivityName:     "集成测试活动",
		StrategyID:       integrationStrategyID,
		OrderID:          orderID,
		RequestID:        "request-" + orderID,
		OrderTime:        now,
		OrderState:       "create",
		DrawState:        "processing",
		ProcessingAt:     &fixture.processingAt,
		DrawOwner:        drawOwner,
		AccountSyncState: "completed",
		CreateTime:       now,
		UpdateTime:       now,
	}

	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(account).Error; err != nil {
			return err
		}
		return tx.Table(fixture.raffleTable).Create(raffleOrder).Error
	}); err != nil {
		t.Fatalf("prepare award transaction fixture: %v", err)
	}

	return fixture
}

func (f *awardTransactionFixture) registerCleanup(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		deleteIntegrationRows(t, f.db, "task", "user_id", f.userID)
		deleteIntegrationRows(t, f.db, f.awardTable, "user_id", f.userID)
		deleteIntegrationRows(t, f.db, f.raffleTable, "user_id", f.userID)
		deleteIntegrationRows(t, f.db, "raffle_activity_account", "user_id", f.userID)
	})
}

func (f *awardTransactionFixture) awardRecord() *award.UserAwardRecord {
	return &award.UserAwardRecord{
		UserID:        f.userID,
		ActivityID:    integrationActivityID,
		StrategyID:    integrationStrategyID,
		OrderID:       f.orderID,
		AwardID:       integrationAwardID,
		AwardTitle:    "集成测试奖品",
		AwardTime:     time.Now().Truncate(time.Second),
		AwardState:    award.AwardStateCreate,
		StockReserved: true,
		DrawOwner:     f.drawOwner,
	}
}

func newIntegrationAwardUsecase() *award.AwardUsecase {
	localCache := cache.New(&cache.Options{
		LocalCache: cache.NewTinyLFU(32, time.Minute),
	})
	repository := awardrepo.NewUserAwardRecordRepository(integrationDBRouter, localCache, nil)
	return award.NewAwardUsecase(repository)
}

// TestAwardRepositoryCommitsAwardAndOutboxAtomically 验证真实 MySQL 事务会同时写入中奖记录、
// 发奖任务和库存同步任务，并将抽奖订单与账户的进行中状态一并收尾。
func TestAwardRepositoryCommitsAwardAndOutboxAtomically(t *testing.T) {
	fixture := newAwardTransactionFixture(t)
	usecase := newIntegrationAwardUsecase()

	got, err := usecase.SaveUserAwardRecord(context.Background(), fixture.awardRecord())
	if err != nil {
		t.Fatalf("SaveUserAwardRecord() error = %v, want nil", err)
	}
	if got.UserID != fixture.userID || got.OrderID != fixture.orderID || got.AwardID != integrationAwardID {
		t.Fatalf("SaveUserAwardRecord() record = %#v, want fixture identity fields", got)
	}

	var storedAward po.UserAwardRecord
	if err := fixture.db.Table(fixture.awardTable).
		Where("user_id = ? AND order_id = ?", fixture.userID, fixture.orderID).
		First(&storedAward).Error; err != nil {
		t.Fatalf("query stored award: %v", err)
	}
	if storedAward.AwardState != string(award.AwardStateCreate) || storedAward.AwardTitle != "集成测试奖品" {
		t.Fatalf("stored award = %#v, want create state and original title", storedAward)
	}

	var tasks []po.Task
	if err := fixture.db.Where("user_id = ?", fixture.userID).Order("topic ASC").Find(&tasks).Error; err != nil {
		t.Fatalf("query outbox tasks: %v", err)
	}
	if len(tasks) != 2 {
		t.Fatalf("outbox task count = %d, want 2", len(tasks))
	}
	assertAwardOutboxTasks(t, fixture, tasks)

	var raffleOrder po.UserRaffleOrder
	if err := fixture.db.Table(fixture.raffleTable).
		Where("user_id = ? AND order_id = ?", fixture.userID, fixture.orderID).
		First(&raffleOrder).Error; err != nil {
		t.Fatalf("query finalized raffle order: %v", err)
	}
	if raffleOrder.OrderState != "used" || raffleOrder.DrawState != "success" || raffleOrder.ProcessingAt != nil || raffleOrder.DrawOwner != "" {
		t.Fatalf("finalized raffle order = %#v, want used/success with released owner", raffleOrder)
	}

	var account po.RaffleActivityAccount
	if err := fixture.db.Where("user_id = ? AND activity_id = ?", fixture.userID, integrationActivityID).
		First(&account).Error; err != nil {
		t.Fatalf("query finalized activity account: %v", err)
	}
	if account.CurrentOrderID != "" {
		t.Fatalf("activity account current_order_id = %q, want empty", account.CurrentOrderID)
	}
}

// TestAwardRepositoryReusesCanonicalResultOnDuplicate 验证相同 order_id 重试时返回数据库中的
// 原始中奖结果，而且唯一索引不会产生重复中奖记录或重复 Outbox 任务。
func TestAwardRepositoryReusesCanonicalResultOnDuplicate(t *testing.T) {
	fixture := newAwardTransactionFixture(t)
	usecase := newIntegrationAwardUsecase()

	first := fixture.awardRecord()
	if _, err := usecase.SaveUserAwardRecord(context.Background(), first); err != nil {
		t.Fatalf("first SaveUserAwardRecord() error = %v, want nil", err)
	}
	duplicate := fixture.awardRecord()
	duplicate.AwardID = 999
	duplicate.AwardTitle = "不应覆盖原结果"

	got, err := usecase.SaveUserAwardRecord(context.Background(), duplicate)
	if err != nil {
		t.Fatalf("duplicate SaveUserAwardRecord() error = %v, want nil", err)
	}
	if got.AwardID != integrationAwardID || got.AwardTitle != first.AwardTitle {
		t.Fatalf("duplicate SaveUserAwardRecord() record = %#v, want canonical first result", got)
	}

	var awardCount int64
	if err := fixture.db.Table(fixture.awardTable).Where("order_id = ?", fixture.orderID).Count(&awardCount).Error; err != nil {
		t.Fatalf("count award rows: %v", err)
	}
	if awardCount != 1 {
		t.Fatalf("award row count = %d, want 1", awardCount)
	}

	var taskCount int64
	if err := fixture.db.Model(&po.Task{}).Where("user_id = ?", fixture.userID).Count(&taskCount).Error; err != nil {
		t.Fatalf("count outbox tasks: %v", err)
	}
	if taskCount != 2 {
		t.Fatalf("outbox task count after duplicate = %d, want 2", taskCount)
	}
}

// TestAwardRepositoryCompletesAwardIdempotently 验证真实分库分表中的中奖记录只能从
// create 推进到 complete，重复消费同一订单仍然返回成功且不会产生额外记录。
func TestAwardRepositoryCompletesAwardIdempotently(t *testing.T) {
	fixture := newAwardTransactionFixture(t)
	usecase := newIntegrationAwardUsecase()
	if _, err := usecase.SaveUserAwardRecord(context.Background(), fixture.awardRecord()); err != nil {
		t.Fatalf("SaveUserAwardRecord() error = %v, want nil", err)
	}

	for attempt := 1; attempt <= 2; attempt++ {
		if err := usecase.CompleteUserAward(context.Background(), fixture.userID, fixture.orderID); err != nil {
			t.Fatalf("CompleteUserAward() attempt %d error = %v, want nil", attempt, err)
		}
	}

	var stored po.UserAwardRecord
	if err := fixture.db.Table(fixture.awardTable).
		Where("user_id = ? AND order_id = ?", fixture.userID, fixture.orderID).
		First(&stored).Error; err != nil {
		t.Fatalf("query completed award: %v", err)
	}
	if stored.AwardState != string(award.AwardStateComplete) {
		t.Fatalf("award state = %q, want %q", stored.AwardState, award.AwardStateComplete)
	}
}

// TestAwardRepositoryRollsBackWhenOutboxInsertFails 验证 Outbox 唯一键冲突会回滚整笔事务：
// 不留下中奖记录，也不会提前完成抽奖订单或清空账户的当前订单。
func TestAwardRepositoryRollsBackWhenOutboxInsertFails(t *testing.T) {
	fixture := newAwardTransactionFixture(t)
	now := time.Now().Truncate(time.Second)
	conflictingTask := &po.Task{
		UserID:     fixture.userID,
		Topic:      "conflicting_task",
		MessageID:  fixture.userID + ":" + fixture.orderID,
		Message:    "{}",
		State:      string(award.TaskStateCreate),
		CreateTime: now,
		UpdateTime: now,
	}
	if err := fixture.db.Create(conflictingTask).Error; err != nil {
		t.Fatalf("prepare conflicting outbox task: %v", err)
	}

	_, err := newIntegrationAwardUsecase().SaveUserAwardRecord(context.Background(), fixture.awardRecord())
	if err == nil {
		t.Fatal("SaveUserAwardRecord() error = nil, want task unique-key error")
	}

	var awardCount int64
	if countErr := fixture.db.Table(fixture.awardTable).Where("order_id = ?", fixture.orderID).Count(&awardCount).Error; countErr != nil {
		t.Fatalf("count rolled-back awards: %v", countErr)
	}
	if awardCount != 0 {
		t.Fatalf("award row count after rollback = %d, want 0", awardCount)
	}

	var taskCount int64
	if countErr := fixture.db.Model(&po.Task{}).Where("user_id = ?", fixture.userID).Count(&taskCount).Error; countErr != nil {
		t.Fatalf("count tasks after rollback: %v", countErr)
	}
	if taskCount != 1 {
		t.Fatalf("task count after rollback = %d, want only pre-existing conflict", taskCount)
	}

	var raffleOrder po.UserRaffleOrder
	if queryErr := fixture.db.Table(fixture.raffleTable).
		Where("user_id = ? AND order_id = ?", fixture.userID, fixture.orderID).
		First(&raffleOrder).Error; queryErr != nil {
		t.Fatalf("query raffle order after rollback: %v", queryErr)
	}
	if raffleOrder.OrderState != "create" || raffleOrder.DrawState != "processing" || raffleOrder.ProcessingAt == nil || raffleOrder.DrawOwner != fixture.drawOwner {
		t.Fatalf("raffle order after rollback = %#v, want original processing state", raffleOrder)
	}

	var account po.RaffleActivityAccount
	if queryErr := fixture.db.Where("user_id = ? AND activity_id = ?", fixture.userID, integrationActivityID).
		First(&account).Error; queryErr != nil {
		t.Fatalf("query account after rollback: %v", queryErr)
	}
	if account.CurrentOrderID != fixture.orderID {
		t.Fatalf("account current_order_id after rollback = %q, want %q", account.CurrentOrderID, fixture.orderID)
	}
}

func assertAwardOutboxTasks(t *testing.T, fixture *awardTransactionFixture, tasks []po.Task) {
	t.Helper()
	tasksByTopic := make(map[string]po.Task, len(tasks))
	for _, task := range tasks {
		tasksByTopic[task.Topic] = task
		if task.State != string(award.TaskStateCreate) {
			t.Fatalf("outbox task %q state = %q, want create", task.Topic, task.State)
		}
	}

	awardTask, ok := tasksByTopic[award.SendAwardTopic]
	if !ok {
		t.Fatalf("outbox tasks = %#v, missing %q", tasks, award.SendAwardTopic)
	}
	if awardTask.MessageID != fixture.userID+":"+fixture.orderID {
		t.Fatalf("award task message_id = %q, want user/order idempotency key", awardTask.MessageID)
	}
	var awardMessage award.SendAwardMessage
	if err := json.Unmarshal([]byte(awardTask.Message), &awardMessage); err != nil {
		t.Fatalf("unmarshal award task message: %v", err)
	}
	if awardMessage.UserID != fixture.userID || awardMessage.OrderID != fixture.orderID || awardMessage.AwardID != integrationAwardID {
		t.Fatalf("award task message = %#v, want fixture identity fields", awardMessage)
	}

	stockTask, ok := tasksByTopic[strategy.AwardStockSyncTopic]
	if !ok {
		t.Fatalf("outbox tasks = %#v, missing %q", tasks, strategy.AwardStockSyncTopic)
	}
	if stockTask.MessageID != "stock:"+fixture.userID+":"+fixture.orderID {
		t.Fatalf("stock task message_id = %q, want stock/user/order idempotency key", stockTask.MessageID)
	}
	var stockMessage strategy.AwardStockConsumeMessage
	if err := json.Unmarshal([]byte(stockTask.Message), &stockMessage); err != nil {
		t.Fatalf("unmarshal stock task message: %v", err)
	}
	if stockMessage.UserID != fixture.userID || stockMessage.OrderID != fixture.orderID ||
		stockMessage.StrategyID != integrationStrategyID || stockMessage.AwardID != integrationAwardID {
		t.Fatalf("stock task message = %#v, want fixture identity fields", stockMessage)
	}
}
