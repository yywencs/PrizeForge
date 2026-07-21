//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"prizeforge/internal/domain/activity"
	"prizeforge/internal/infrastructure/adapter"
	"prizeforge/internal/infrastructure/repository/activityrepo"
	"prizeforge/pkg/rabbitmq"
	"prizeforge/pkg/xrand"
)

// TestActivityRepositoryRejectsUninitializedRedisStock 验证库存键不存在时返回明确错误，
// 并且不会为用户写入任何活动结果。
func TestActivityRepositoryRejectsUninitializedRedisStock(t *testing.T) {
	ctx := context.Background()
	skuID := newIntegrationRedisActivityID(t)
	activityID := newIntegrationRedisActivityID(t)
	stockKey := adapter.GetActivitySkuStockCountKey(skuID)
	trackIntegrationRedisKeys(t, stockKey)
	trackIntegrationActivityResultKeys(t, activityID)
	publisher := &integrationActivityPublisher{}
	repository := activityrepo.NewRepository(nil, nil, integrationRedis, publisher, nil, nil)

	result, err := repository.SubtractionActivitySkuStock(ctx, skuID, activityID, "it-uninitialized", time.Now().Add(time.Hour))
	if err == nil || err.Error() != "库存未初始化" {
		t.Fatalf("SubtractionActivitySkuStock() error = %v, want 库存未初始化", err)
	}
	if result != nil {
		t.Fatalf("SubtractionActivitySkuStock() result = %#v, want nil", result)
	}
	assertIntegrationActivityResultCount(t, activityID, 0)
	assertIntegrationStockZeroPublishCount(t, publisher, 0, skuID)
}

// TestActivityRepositorySubtractsRedisStockAndStoresResult 验证库存充足时 Lua 脚本
// 原子扣减一份库存，并把成功结果写入用户对应的 Redis Hash 分片。
func TestActivityRepositorySubtractsRedisStockAndStoresResult(t *testing.T) {
	ctx := context.Background()
	skuID := newIntegrationRedisActivityID(t)
	activityID := newIntegrationRedisActivityID(t)
	stockKey := prepareIntegrationActivityRedisStock(t, skuID, 2)
	trackIntegrationActivityResultKeys(t, activityID)
	publisher := &integrationActivityPublisher{}
	repository := activityrepo.NewRepository(nil, nil, integrationRedis, publisher, nil, nil)
	userID := "it-activity-success-" + xrand.RandomNumeric(12)

	result, err := repository.SubtractionActivitySkuStock(ctx, skuID, activityID, userID, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("SubtractionActivitySkuStock() error = %v, want nil", err)
	}
	assertIntegrationActivityResult(t, result, userID, activity.ActivityResultStatusSuccess, fmt.Sprintf("SKU_%d", skuID))
	assertStoredIntegrationActivityResult(t, activityID, userID, result)
	assertIntegrationRedisInt(t, stockKey, 1)
	assertIntegrationStockZeroPublishCount(t, publisher, 0, skuID)
}

// TestActivityRepositoryPublishesWhenRedisStockReachesZero 验证最后一份库存被扣减后，
// 库存保持为零、用户得到 SKU，并且只发布一次库存耗尽事件。
func TestActivityRepositoryPublishesWhenRedisStockReachesZero(t *testing.T) {
	ctx := context.Background()
	skuID := newIntegrationRedisActivityID(t)
	activityID := newIntegrationRedisActivityID(t)
	stockKey := prepareIntegrationActivityRedisStock(t, skuID, 1)
	trackIntegrationActivityResultKeys(t, activityID)
	publisher := &integrationActivityPublisher{}
	repository := activityrepo.NewRepository(nil, nil, integrationRedis, publisher, nil, nil)
	userID := "it-activity-last-" + xrand.RandomNumeric(12)

	result, err := repository.SubtractionActivitySkuStock(ctx, skuID, activityID, userID, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("SubtractionActivitySkuStock() error = %v, want nil", err)
	}
	assertIntegrationActivityResult(t, result, userID, activity.ActivityResultStatusSuccess, fmt.Sprintf("SKU_%d", skuID))
	assertStoredIntegrationActivityResult(t, activityID, userID, result)
	assertIntegrationRedisInt(t, stockKey, 0)
	assertIntegrationStockZeroPublishCount(t, publisher, 1, skuID)
}

// TestActivityRepositoryFallsBackToPointsWithoutRedisStock 验证库存已经为零时，
// 不会继续扣成负数，而是保存并返回积分兜底结果。
func TestActivityRepositoryFallsBackToPointsWithoutRedisStock(t *testing.T) {
	ctx := context.Background()
	skuID := newIntegrationRedisActivityID(t)
	activityID := newIntegrationRedisActivityID(t)
	stockKey := prepareIntegrationActivityRedisStock(t, skuID, 0)
	trackIntegrationActivityResultKeys(t, activityID)
	publisher := &integrationActivityPublisher{}
	repository := activityrepo.NewRepository(nil, nil, integrationRedis, publisher, nil, nil)
	userID := "it-activity-points-" + xrand.RandomNumeric(12)

	result, err := repository.SubtractionActivitySkuStock(ctx, skuID, activityID, userID, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("SubtractionActivitySkuStock() error = %v, want nil", err)
	}
	assertIntegrationActivityResult(t, result, userID, activity.ActivityResultStatusCredit, "POINTS_100")
	assertStoredIntegrationActivityResult(t, activityID, userID, result)
	assertIntegrationRedisInt(t, stockKey, 0)
	assertIntegrationStockZeroPublishCount(t, publisher, 0, skuID)
}

// TestActivityRepositoryDoesNotOversellRedisStockUnderConcurrency 验证 100 个不同用户
// 并发争抢 10 份库存时，恰好 10 人获得 SKU、其余用户得到积分，库存不会小于零。
func TestActivityRepositoryDoesNotOversellRedisStockUnderConcurrency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	skuID := newIntegrationRedisActivityID(t)
	activityID := newIntegrationRedisActivityID(t)
	stockKey := prepareIntegrationActivityRedisStock(t, skuID, 10)
	trackIntegrationActivityResultKeys(t, activityID)
	publisher := &integrationActivityPublisher{}
	repository := activityrepo.NewRepository(nil, nil, integrationRedis, publisher, nil, nil)

	const requestCount = 100
	type subtractionResult struct {
		result *activity.ActivityResult
		err    error
	}
	results := make(chan subtractionResult, requestCount)
	start := make(chan struct{})
	var waitGroup sync.WaitGroup
	waitGroup.Add(requestCount)
	for index := range requestCount {
		go func(index int) {
			defer waitGroup.Done()
			<-start
			userID := fmt.Sprintf("it-activity-concurrent-%03d", index)
			result, err := repository.SubtractionActivitySkuStock(ctx, skuID, activityID, userID, time.Now().Add(time.Hour))
			results <- subtractionResult{result: result, err: err}
		}(index)
	}
	close(start)
	waitGroup.Wait()
	close(results)

	var successCount int
	var creditCount int
	for callResult := range results {
		if callResult.err != nil {
			t.Fatalf("concurrent SubtractionActivitySkuStock() error = %v, want nil", callResult.err)
		}
		if callResult.result == nil {
			t.Fatal("concurrent SubtractionActivitySkuStock() result = nil")
		}
		switch callResult.result.Status {
		case activity.ActivityResultStatusSuccess:
			successCount++
		case activity.ActivityResultStatusCredit:
			creditCount++
		default:
			t.Fatalf("concurrent activity result status = %d, want success or credit", callResult.result.Status)
		}
	}
	if successCount != 10 || creditCount != 90 {
		t.Fatalf("concurrent results = success:%d credit:%d, want success:10 credit:90", successCount, creditCount)
	}
	assertIntegrationRedisInt(t, stockKey, 0)
	assertIntegrationActivityResultCount(t, activityID, requestCount)
	assertIntegrationStockZeroPublishCount(t, publisher, 1, skuID)
}

type integrationActivityPublisher struct {
	mu              sync.Mutex
	stockZeroEvents []*rabbitmq.BaseEvent
}

func (p *integrationActivityPublisher) PublishStockZero(_ context.Context, event *rabbitmq.BaseEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stockZeroEvents = append(p.stockZeroEvents, event)
	return nil
}

func (p *integrationActivityPublisher) PublishSaveOrder(context.Context, *rabbitmq.BaseEvent) error {
	return nil
}

func (p *integrationActivityPublisher) snapshotStockZeroEvents() []*rabbitmq.BaseEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*rabbitmq.BaseEvent(nil), p.stockZeroEvents...)
}

func newIntegrationRedisActivityID(t *testing.T) int64 {
	t.Helper()
	randomID, err := strconv.ParseInt(xrand.RandomNumeric(12), 10, 64)
	if err != nil {
		t.Fatalf("parse random Redis activity id: %v", err)
	}
	return 7_000_000_000_000 + randomID
}

func prepareIntegrationActivityRedisStock(t *testing.T, skuID int64, stock int) string {
	t.Helper()
	key := adapter.GetActivitySkuStockCountKey(skuID)
	trackIntegrationRedisKeys(t, key)
	if err := integrationRedisClient.Set(context.Background(), key, stock, 0).Err(); err != nil {
		t.Fatalf("prepare activity Redis stock %s: %v", key, err)
	}
	return key
}

func trackIntegrationActivityResultKeys(t *testing.T, activityID int64) {
	t.Helper()
	keys := make([]string, 0, 100)
	for shard := range 100 {
		keys = append(keys, adapter.GetActivityResultHashKey(activityID, shard))
	}
	trackIntegrationRedisKeys(t, keys...)
}

func integrationActivityResultShard(userID string) int {
	shard := 0
	for _, character := range userID {
		shard = (shard*31 + int(character)) % 100
	}
	return shard
}

func assertIntegrationActivityResult(t *testing.T, result *activity.ActivityResult, userID string, status int, value string) {
	t.Helper()
	if result == nil {
		t.Fatal("activity result = nil")
	}
	if result.UserID != userID || result.Status != status || result.Result != value || result.Timestamp <= 0 {
		t.Fatalf("activity result = %#v, want user=%q status=%d result=%q and positive timestamp", result, userID, status, value)
	}
}

func assertStoredIntegrationActivityResult(t *testing.T, activityID int64, userID string, want *activity.ActivityResult) {
	t.Helper()
	key := adapter.GetActivityResultHashKey(activityID, integrationActivityResultShard(userID))
	storedJSON, err := integrationRedisClient.HGet(context.Background(), key, userID).Result()
	if err != nil {
		t.Fatalf("get stored activity result: %v", err)
	}
	var stored activity.ActivityResult
	if err := json.Unmarshal([]byte(storedJSON), &stored); err != nil {
		t.Fatalf("unmarshal stored activity result: %v", err)
	}
	if stored != *want {
		t.Fatalf("stored activity result = %#v, want %#v", stored, *want)
	}
}

func assertIntegrationActivityResultCount(t *testing.T, activityID int64, want int) {
	t.Helper()
	var count int64
	for shard := range 100 {
		key := adapter.GetActivityResultHashKey(activityID, shard)
		shardCount, err := integrationRedisClient.HLen(context.Background(), key).Result()
		if err != nil {
			t.Fatalf("count activity result hash %s: %v", key, err)
		}
		count += shardCount
	}
	if count != int64(want) {
		t.Fatalf("stored activity result count = %d, want %d", count, want)
	}
}

func assertIntegrationStockZeroPublishCount(t *testing.T, publisher *integrationActivityPublisher, want int, skuID int64) {
	t.Helper()
	events := publisher.snapshotStockZeroEvents()
	if len(events) != want {
		t.Fatalf("stock-zero publish count = %d, want %d", len(events), want)
	}
	for _, event := range events {
		gotSkuID, ok := event.Data.(int64)
		if !ok || gotSkuID != skuID {
			t.Fatalf("stock-zero event data = %#v, want sku %d", event.Data, skuID)
		}
	}
}
