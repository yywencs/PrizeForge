//go:build integration

package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"prizeforge/internal/domain/rebate"
	"prizeforge/internal/infrastructure/repository/rebaterepo"
	"prizeforge/pkg/rabbitmq"
	"prizeforge/pkg/xrand"
)

type recordingRebatePublisher struct {
	mu     sync.Mutex
	events []*rabbitmq.BaseEvent
}

func (p *recordingRebatePublisher) PublishSendRebate(_ context.Context, event *rabbitmq.BaseEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, event)
	return nil
}

func (p *recordingRebatePublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.events)
}

// TestRebateRepositoryQueriesSeededConfig 验证正式初始化 SQL 已在真实主库写入开启状态的
// 签到返利配置，并且 GORM 查询能够完整映射行为、返利类型和 SKU 配置。
func TestRebateRepositoryQueriesSeededConfig(t *testing.T) {
	repository := rebaterepo.NewRebateRepository(integrationDefaultDB, integrationDBRouter, &recordingRebatePublisher{})

	configs, err := repository.QueryDailyBehaviorRebateConfig(context.Background(), rebate.Sign)

	if err != nil {
		t.Fatalf("QueryDailyBehaviorRebateConfig() error = %v, want nil", err)
	}
	if len(configs) != 1 {
		t.Fatalf("QueryDailyBehaviorRebateConfig() count = %d, want 1", len(configs))
	}
	config := configs[0]
	if config.BehaviorType != string(rebate.Sign) || config.RebateType != string(rebate.Sku) || config.RebateConfig != "9011" {
		t.Fatalf("QueryDailyBehaviorRebateConfig() config = %#v, want sign/sku/9011", config)
	}
}

// TestRebateRepositoryPersistsAndReusesOrder 验证真实 MySQL 分片和分表路由、事务插入、
// out_business_no 查询以及唯一索引幂等：同一 BizID 保存两次后数据库仍只有一条订单。
func TestRebateRepositoryPersistsAndReusesOrder(t *testing.T) {
	orderID := xrand.RandomNumeric(12)
	userID := "it-rebate-" + orderID
	outBusinessNo := "sign-" + orderID
	bizID := userID + "_sku_" + outBusinessNo

	db, tableSuffix := integrationDBRouter.DBStrategy(userID)
	if db == nil {
		t.Fatal("DBStrategy() database = nil")
	}
	tableName := "user_behavior_rebate_order_" + tableSuffix
	if len(tableSuffix) != 3 {
		t.Fatalf("DBStrategy() table suffix = %q, want three digits", tableSuffix)
	}
	t.Cleanup(func() {
		deleteSQL := fmt.Sprintf("DELETE FROM `%s` WHERE user_id = ?", tableName)
		if err := db.Exec(deleteSQL, userID).Error; err != nil {
			t.Errorf("cleanup integration rebate order: %v", err)
		}
	})

	order := &rebate.BehaviorRebateOrder{
		UserID:        userID,
		OrderID:       orderID,
		BehaviorType:  string(rebate.Sign),
		RebateDesc:    "集成测试签到返利",
		RebateType:    string(rebate.Sku),
		OutBusinessNo: outBusinessNo,
		RebateConfig:  "9011",
		BizID:         bizID,
	}
	aggregate := &rebate.BehaviorRebate{
		UserID:               userID,
		BehaviorRebateOrders: []*rebate.BehaviorRebateOrder{order},
	}
	publisher := &recordingRebatePublisher{}
	repository := rebaterepo.NewRebateRepository(integrationDefaultDB, integrationDBRouter, publisher)

	if err := repository.SaveUserRebateOrder(context.Background(), userID, aggregate); err != nil {
		t.Fatalf("first SaveUserRebateOrder() error = %v, want nil", err)
	}
	if err := repository.SaveUserRebateOrder(context.Background(), userID, aggregate); err != nil {
		t.Fatalf("duplicate SaveUserRebateOrder() error = %v, want nil", err)
	}

	orders, err := repository.QueryUserRebateOrder(context.Background(), userID, outBusinessNo)
	if err != nil {
		t.Fatalf("QueryUserRebateOrder() error = %v, want nil", err)
	}
	if len(orders) != 1 {
		t.Fatalf("QueryUserRebateOrder() count = %d, want 1", len(orders))
	}
	got := orders[0]
	if got.UserID != userID || got.OrderID != orderID || got.OutBusinessNo != outBusinessNo || got.BizID != bizID {
		t.Fatalf("QueryUserRebateOrder() order = %#v, want identity fields from saved order", got)
	}

	var storedCount int64
	if err := db.Table(tableName).Where("biz_id = ?", bizID).Count(&storedCount).Error; err != nil {
		t.Fatalf("count saved rebate orders: %v", err)
	}
	if storedCount != 1 {
		t.Fatalf("stored rebate order count = %d, want 1", storedCount)
	}
	if publisher.count() != 2 {
		t.Fatalf("PublishSendRebate() calls = %d, want 2 idempotent delivery attempts", publisher.count())
	}

	t.Logf("verified shard table %s for user %s", tableName, userID)
}
