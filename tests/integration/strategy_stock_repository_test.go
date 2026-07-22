//go:build integration

package integration

import (
	"context"
	"strconv"
	"testing"
	"time"

	"prizeforge/internal/domain/strategy"
	"prizeforge/internal/infrastructure/repository/po"
	"prizeforge/internal/infrastructure/repository/strategyrepo"
	"prizeforge/pkg/xrand"
)

type strategyStockFixture struct {
	strategyID int64
	awardIDs   []int64
}

// TestStrategyRepositoryBatchConsumesStockOnce 验证同一奖品的一批订单只触发一次聚合扣减，
// 整批重试时不会重复扣库存，批次中混入新订单时也只扣新增预占数量。
func TestStrategyRepositoryBatchConsumesStockOnce(t *testing.T) {
	fixture := newStrategyStockFixture(t, 5)
	repository := strategyrepo.NewStrategyRepository(integrationDefaultDB, nil, nil, integrationDBRouter)
	awardID := fixture.awardIDs[0]
	userPrefix := "it-stock-batch-" + xrand.RandomNumeric(6)
	messages := []strategy.AwardStockConsumeMessage{
		{UserID: userPrefix + "-1", OrderID: xrand.RandomNumeric(12), StrategyID: fixture.strategyID, AwardID: awardID},
		{UserID: userPrefix + "-2", OrderID: xrand.RandomNumeric(12), StrategyID: fixture.strategyID, AwardID: awardID},
		{UserID: userPrefix + "-3", OrderID: xrand.RandomNumeric(12), StrategyID: fixture.strategyID, AwardID: awardID},
	}

	if err := repository.UpdateStrategyAwardStockBatch(context.Background(), messages); err != nil {
		t.Fatalf("first UpdateStrategyAwardStockBatch() error = %v, want nil", err)
	}
	assertIntegrationStock(t, fixture.strategyID, awardID, 2)
	assertIntegrationReservationCount(t, fixture.strategyID, 3)

	if err := repository.UpdateStrategyAwardStockBatch(context.Background(), messages); err != nil {
		t.Fatalf("duplicate UpdateStrategyAwardStockBatch() error = %v, want nil", err)
	}
	assertIntegrationStock(t, fixture.strategyID, awardID, 2)
	assertIntegrationReservationCount(t, fixture.strategyID, 3)

	mixedBatch := append([]strategy.AwardStockConsumeMessage(nil), messages[0])
	mixedBatch = append(mixedBatch, strategy.AwardStockConsumeMessage{
		UserID: userPrefix + "-4", OrderID: xrand.RandomNumeric(12), StrategyID: fixture.strategyID, AwardID: awardID,
	})
	if err := repository.UpdateStrategyAwardStockBatch(context.Background(), mixedBatch); err != nil {
		t.Fatalf("mixed UpdateStrategyAwardStockBatch() error = %v, want nil", err)
	}
	assertIntegrationStock(t, fixture.strategyID, awardID, 1)
	assertIntegrationReservationCount(t, fixture.strategyID, 4)
}

// TestStrategyRepositoryBatchRollsBackWhenStockIsInsufficient 验证聚合扣减库存不足时，
// 整批幂等预占记录与库存更新一起回滚，不会留下部分成功状态。
func TestStrategyRepositoryBatchRollsBackWhenStockIsInsufficient(t *testing.T) {
	fixture := newStrategyStockFixture(t, 2)
	repository := strategyrepo.NewStrategyRepository(integrationDefaultDB, nil, nil, integrationDBRouter)
	awardID := fixture.awardIDs[0]
	messages := []strategy.AwardStockConsumeMessage{
		{UserID: "it-stock-batch-" + xrand.RandomNumeric(8), OrderID: xrand.RandomNumeric(12), StrategyID: fixture.strategyID, AwardID: awardID},
		{UserID: "it-stock-batch-" + xrand.RandomNumeric(8), OrderID: xrand.RandomNumeric(12), StrategyID: fixture.strategyID, AwardID: awardID},
		{UserID: "it-stock-batch-" + xrand.RandomNumeric(8), OrderID: xrand.RandomNumeric(12), StrategyID: fixture.strategyID, AwardID: awardID},
	}

	if err := repository.UpdateStrategyAwardStockBatch(context.Background(), messages); err == nil {
		t.Fatal("UpdateStrategyAwardStockBatch() error = nil, want insufficient-stock error")
	}
	assertIntegrationStock(t, fixture.strategyID, awardID, 2)
	assertIntegrationReservationCount(t, fixture.strategyID, 0)
}

func newStrategyStockFixture(t *testing.T, stocks ...int) *strategyStockFixture {
	t.Helper()

	randomID, err := strconv.ParseInt(xrand.RandomNumeric(12), 10, 64)
	if err != nil {
		t.Fatalf("parse random strategy id: %v", err)
	}
	fixture := &strategyStockFixture{
		strategyID: 9_000_000_000_000 + randomID,
		awardIDs:   make([]int64, 0, len(stocks)),
	}
	t.Cleanup(func() {
		deleteIntegrationRows(t, integrationDefaultDB, "strategy_award_stock_reservation", "strategy_id", fixture.strategyID)
		deleteIntegrationRows(t, integrationDefaultDB, "strategy_award", "strategy_id", fixture.strategyID)
	})

	now := time.Now().Truncate(time.Second)
	awards := make([]po.StrategyAward, 0, len(stocks))
	for index, stock := range stocks {
		awardID := int64(index + 1)
		fixture.awardIDs = append(fixture.awardIDs, awardID)
		awards = append(awards, po.StrategyAward{
			StrategyID:        fixture.strategyID,
			AwardID:           awardID,
			AwardTitle:        "集成测试库存奖品",
			AwardCount:        stock,
			AwardCountSurplus: stock,
			AwardRate:         1,
			Sort:              index + 1,
			CreateTime:        now,
			UpdateTime:        now,
		})
	}
	if err := integrationDefaultDB.Create(&awards).Error; err != nil {
		t.Fatalf("prepare strategy stock fixtures: %v", err)
	}
	return fixture
}

// TestStrategyRepositoryConsumesStockOncePerOrder 验证真实 MySQL 幂等表会让相同用户和订单
// 重复消费时只扣一次库存，而不同订单仍能各自完成一次正常扣减。
func TestStrategyRepositoryConsumesStockOncePerOrder(t *testing.T) {
	fixture := newStrategyStockFixture(t, 3)
	repository := strategyrepo.NewStrategyRepository(integrationDefaultDB, nil, nil, integrationDBRouter)
	userID := "it-stock-" + xrand.RandomNumeric(12)
	firstOrderID := xrand.RandomNumeric(12)
	secondOrderID := xrand.RandomNumeric(12)
	awardID := fixture.awardIDs[0]

	if err := repository.UpdateStrategyAwardStock(context.Background(), userID, firstOrderID, fixture.strategyID, awardID); err != nil {
		t.Fatalf("first UpdateStrategyAwardStock() error = %v, want nil", err)
	}
	if err := repository.UpdateStrategyAwardStock(context.Background(), userID, firstOrderID, fixture.strategyID, awardID); err != nil {
		t.Fatalf("duplicate UpdateStrategyAwardStock() error = %v, want nil", err)
	}
	if err := repository.UpdateStrategyAwardStock(context.Background(), userID, secondOrderID, fixture.strategyID, awardID); err != nil {
		t.Fatalf("second order UpdateStrategyAwardStock() error = %v, want nil", err)
	}

	assertIntegrationStock(t, fixture.strategyID, awardID, 1)
	assertIntegrationReservationCount(t, fixture.strategyID, 2)
}

// TestStrategyRepositoryRollsBackReservationWithoutStock 验证库存为零时，库存预留记录和扣减
// 位于同一事务并一起回滚；补充库存后，相同订单仍可以重新消费成功。
func TestStrategyRepositoryRollsBackReservationWithoutStock(t *testing.T) {
	fixture := newStrategyStockFixture(t, 0)
	repository := strategyrepo.NewStrategyRepository(integrationDefaultDB, nil, nil, integrationDBRouter)
	userID := "it-stock-" + xrand.RandomNumeric(12)
	orderID := xrand.RandomNumeric(12)
	awardID := fixture.awardIDs[0]

	if err := repository.UpdateStrategyAwardStock(context.Background(), userID, orderID, fixture.strategyID, awardID); err == nil {
		t.Fatal("UpdateStrategyAwardStock() error = nil, want insufficient-stock error")
	}
	assertIntegrationStock(t, fixture.strategyID, awardID, 0)
	assertIntegrationReservationCount(t, fixture.strategyID, 0)

	if err := integrationDefaultDB.Model(&po.StrategyAward{}).
		Where("strategy_id = ? AND award_id = ?", fixture.strategyID, awardID).
		Update("award_count_surplus", 1).Error; err != nil {
		t.Fatalf("replenish integration stock: %v", err)
	}
	if err := repository.UpdateStrategyAwardStock(context.Background(), userID, orderID, fixture.strategyID, awardID); err != nil {
		t.Fatalf("retry UpdateStrategyAwardStock() error = %v, want nil", err)
	}
	assertIntegrationStock(t, fixture.strategyID, awardID, 0)
	assertIntegrationReservationCount(t, fixture.strategyID, 1)
}

// TestStrategyRepositoryRejectsConflictingReservationPayload 验证相同用户和订单若携带不同奖品，
// 仓储会报告幂等冲突，不会静默接受消息，也不会扣减第二个奖品的库存。
func TestStrategyRepositoryRejectsConflictingReservationPayload(t *testing.T) {
	fixture := newStrategyStockFixture(t, 2, 2)
	repository := strategyrepo.NewStrategyRepository(integrationDefaultDB, nil, nil, integrationDBRouter)
	userID := "it-stock-" + xrand.RandomNumeric(12)
	orderID := xrand.RandomNumeric(12)
	firstAwardID := fixture.awardIDs[0]
	secondAwardID := fixture.awardIDs[1]

	if err := repository.UpdateStrategyAwardStock(context.Background(), userID, orderID, fixture.strategyID, firstAwardID); err != nil {
		t.Fatalf("first UpdateStrategyAwardStock() error = %v, want nil", err)
	}
	if err := repository.UpdateStrategyAwardStock(context.Background(), userID, orderID, fixture.strategyID, secondAwardID); err == nil {
		t.Fatal("conflicting UpdateStrategyAwardStock() error = nil, want idempotency conflict")
	}

	assertIntegrationStock(t, fixture.strategyID, firstAwardID, 1)
	assertIntegrationStock(t, fixture.strategyID, secondAwardID, 2)
	assertIntegrationReservationCount(t, fixture.strategyID, 1)

	var reservation po.StrategyAwardStockReservation
	if err := integrationDefaultDB.
		Where("user_id = ? AND order_id = ?", userID, orderID).
		First(&reservation).Error; err != nil {
		t.Fatalf("query canonical stock reservation: %v", err)
	}
	if reservation.StrategyID != fixture.strategyID || reservation.AwardID != firstAwardID {
		t.Fatalf("stock reservation = %#v, want original strategy and award", reservation)
	}
}

func assertIntegrationStock(t *testing.T, strategyID, awardID int64, wantStock int) {
	t.Helper()
	var stored po.StrategyAward
	if err := integrationDefaultDB.
		Where("strategy_id = ? AND award_id = ?", strategyID, awardID).
		First(&stored).Error; err != nil {
		t.Fatalf("query strategy stock: %v", err)
	}
	if stored.AwardCountSurplus != wantStock {
		t.Fatalf("strategy %d award %d stock = %d, want %d", strategyID, awardID, stored.AwardCountSurplus, wantStock)
	}
}

func assertIntegrationReservationCount(t *testing.T, strategyID int64, wantCount int64) {
	t.Helper()
	var count int64
	if err := integrationDefaultDB.Model(&po.StrategyAwardStockReservation{}).
		Where("strategy_id = ?", strategyID).
		Count(&count).Error; err != nil {
		t.Fatalf("count stock reservations: %v", err)
	}
	if count != wantCount {
		t.Fatalf("stock reservation count = %d, want %d", count, wantCount)
	}
}
