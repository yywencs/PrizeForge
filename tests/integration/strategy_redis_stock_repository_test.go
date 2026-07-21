//go:build integration

package integration

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"prizeforge/internal/infrastructure/adapter"
	"prizeforge/internal/infrastructure/repository/strategyrepo"
	"prizeforge/pkg/xrand"
)

// TestStrategyRepositoryReservesRedisStockOncePerOrder 验证同一用户和订单重复预占时，
// Redis Lua 脚本只扣减一次库存，而不同订单仍会分别扣减库存。
func TestStrategyRepositoryReservesRedisStockOncePerOrder(t *testing.T) {
	ctx := context.Background()
	strategyID := newIntegrationRedisStrategyID(t)
	const awardID int64 = 1
	stockKey := prepareIntegrationRedisStock(t, strategyID, awardID, 2)
	repository := strategyrepo.NewStrategyRepository(nil, integrationRedis, nil, nil)
	userID := "it-redis-stock-" + xrand.RandomNumeric(12)
	firstOrderID := xrand.RandomNumeric(12)
	secondOrderID := xrand.RandomNumeric(12)
	trackIntegrationRedisReservation(t, userID, firstOrderID)
	trackIntegrationRedisReservation(t, userID, secondOrderID)

	reservedAwardID, ok, err := repository.ReserveAwardStock(ctx, userID, firstOrderID, strategyID, awardID)
	assertRedisReservationResult(t, reservedAwardID, ok, err, awardID, true)
	assertIntegrationRedisInt(t, stockKey, 1)

	reservedAwardID, ok, err = repository.ReserveAwardStock(ctx, userID, firstOrderID, strategyID, awardID)
	assertRedisReservationResult(t, reservedAwardID, ok, err, awardID, true)
	assertIntegrationRedisInt(t, stockKey, 1)

	reservedAwardID, ok, err = repository.ReserveAwardStock(ctx, userID, secondOrderID, strategyID, awardID)
	assertRedisReservationResult(t, reservedAwardID, ok, err, awardID, true)
	assertIntegrationRedisInt(t, stockKey, 0)
}

// TestStrategyRepositoryRejectsUnavailableRedisStock 验证库存键不存在或库存为零时，
// 预占失败、库存不会变成负数，并且不会创建订单预占键。
func TestStrategyRepositoryRejectsUnavailableRedisStock(t *testing.T) {
	testCases := []struct {
		name       string
		initialize bool
	}{
		{name: "missing stock key", initialize: false},
		{name: "zero stock", initialize: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()
			strategyID := newIntegrationRedisStrategyID(t)
			const awardID int64 = 1
			stockKey := adapter.GetStrategyAwardCountKey(strategyID, awardID)
			trackIntegrationRedisKeys(t, stockKey)
			if testCase.initialize {
				if err := integrationRedisClient.Set(ctx, stockKey, 0, 0).Err(); err != nil {
					t.Fatalf("initialize zero Redis stock: %v", err)
				}
			}
			repository := strategyrepo.NewStrategyRepository(nil, integrationRedis, nil, nil)
			userID := "it-redis-empty-" + xrand.RandomNumeric(12)
			orderID := xrand.RandomNumeric(12)
			reservationKey := adapter.GetStrategyAwardReservationKey(userID, orderID)
			trackIntegrationRedisKeys(t, reservationKey)

			reservedAwardID, ok, err := repository.ReserveAwardStock(ctx, userID, orderID, strategyID, awardID)
			assertRedisReservationResult(t, reservedAwardID, ok, err, awardID, false)

			if count, err := integrationRedisClient.Exists(ctx, reservationKey).Result(); err != nil {
				t.Fatalf("query Redis reservation existence: %v", err)
			} else if count != 0 {
				t.Fatalf("Redis reservation key exists = %d, want 0", count)
			}
			if testCase.initialize {
				assertIntegrationRedisInt(t, stockKey, 0)
			} else if count, err := integrationRedisClient.Exists(ctx, stockKey).Result(); err != nil {
				t.Fatalf("query Redis stock existence: %v", err)
			} else if count != 0 {
				t.Fatalf("Redis stock key exists = %d, want 0", count)
			}
		})
	}
}

// TestStrategyRepositoryReturnsCanonicalRedisReservation 验证同一用户和订单再次请求不同奖品时，
// 返回第一次预占的奖品，并且不会扣减第二个奖品的库存。
func TestStrategyRepositoryReturnsCanonicalRedisReservation(t *testing.T) {
	ctx := context.Background()
	strategyID := newIntegrationRedisStrategyID(t)
	const firstAwardID int64 = 1
	const secondAwardID int64 = 2
	firstStockKey := prepareIntegrationRedisStock(t, strategyID, firstAwardID, 1)
	secondStockKey := prepareIntegrationRedisStock(t, strategyID, secondAwardID, 1)
	repository := strategyrepo.NewStrategyRepository(nil, integrationRedis, nil, nil)
	userID := "it-redis-canonical-" + xrand.RandomNumeric(12)
	orderID := xrand.RandomNumeric(12)
	trackIntegrationRedisReservation(t, userID, orderID)

	reservedAwardID, ok, err := repository.ReserveAwardStock(ctx, userID, orderID, strategyID, firstAwardID)
	assertRedisReservationResult(t, reservedAwardID, ok, err, firstAwardID, true)

	reservedAwardID, ok, err = repository.ReserveAwardStock(ctx, userID, orderID, strategyID, secondAwardID)
	assertRedisReservationResult(t, reservedAwardID, ok, err, firstAwardID, true)
	assertIntegrationRedisInt(t, firstStockKey, 0)
	assertIntegrationRedisInt(t, secondStockKey, 1)
}

// TestStrategyRepositoryReservesRedisStockAtomicallyUnderConcurrency 验证多个并发请求使用
// 同一用户和订单时，Lua 脚本原子地创建一份预占记录并且只扣减一份库存。
func TestStrategyRepositoryReservesRedisStockAtomicallyUnderConcurrency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	strategyID := newIntegrationRedisStrategyID(t)
	const awardID int64 = 1
	stockKey := prepareIntegrationRedisStock(t, strategyID, awardID, 10)
	repository := strategyrepo.NewStrategyRepository(nil, integrationRedis, nil, nil)
	userID := "it-redis-concurrent-" + xrand.RandomNumeric(12)
	orderID := xrand.RandomNumeric(12)
	trackIntegrationRedisReservation(t, userID, orderID)

	const requestCount = 50
	type reservationResult struct {
		awardID int64
		ok      bool
		err     error
	}
	results := make(chan reservationResult, requestCount)
	start := make(chan struct{})
	var waitGroup sync.WaitGroup
	waitGroup.Add(requestCount)
	for range requestCount {
		go func() {
			defer waitGroup.Done()
			<-start
			reservedAwardID, ok, err := repository.ReserveAwardStock(ctx, userID, orderID, strategyID, awardID)
			results <- reservationResult{awardID: reservedAwardID, ok: ok, err: err}
		}()
	}
	close(start)
	waitGroup.Wait()
	close(results)

	for result := range results {
		assertRedisReservationResult(t, result.awardID, result.ok, result.err, awardID, true)
	}
	assertIntegrationRedisInt(t, stockKey, 9)
}

// TestStrategyRepositorySetsRedisReservationTTL 验证成功预占后写入正确奖品，并为订单预占键
// 设置接近 30 天的有效期，避免幂等记录永久占用 Redis 内存。
func TestStrategyRepositorySetsRedisReservationTTL(t *testing.T) {
	ctx := context.Background()
	strategyID := newIntegrationRedisStrategyID(t)
	const awardID int64 = 1
	prepareIntegrationRedisStock(t, strategyID, awardID, 1)
	repository := strategyrepo.NewStrategyRepository(nil, integrationRedis, nil, nil)
	userID := "it-redis-ttl-" + xrand.RandomNumeric(12)
	orderID := xrand.RandomNumeric(12)
	reservationKey := adapter.GetStrategyAwardReservationKey(userID, orderID)
	trackIntegrationRedisKeys(t, reservationKey)

	reservedAwardID, ok, err := repository.ReserveAwardStock(ctx, userID, orderID, strategyID, awardID)
	assertRedisReservationResult(t, reservedAwardID, ok, err, awardID, true)
	assertIntegrationRedisInt(t, reservationKey, awardID)

	ttl, err := integrationRedisClient.TTL(ctx, reservationKey).Result()
	if err != nil {
		t.Fatalf("query Redis reservation TTL: %v", err)
	}
	if ttl < 29*24*time.Hour || ttl > 30*24*time.Hour {
		t.Fatalf("Redis reservation TTL = %s, want between 29 and 30 days", ttl)
	}
}

func newIntegrationRedisStrategyID(t *testing.T) int64 {
	t.Helper()
	randomID, err := strconv.ParseInt(xrand.RandomNumeric(12), 10, 64)
	if err != nil {
		t.Fatalf("parse random Redis strategy id: %v", err)
	}
	return 8_000_000_000_000 + randomID
}

func prepareIntegrationRedisStock(t *testing.T, strategyID, awardID int64, stock int) string {
	t.Helper()
	key := adapter.GetStrategyAwardCountKey(strategyID, awardID)
	trackIntegrationRedisKeys(t, key)
	if err := integrationRedisClient.Set(context.Background(), key, stock, 0).Err(); err != nil {
		t.Fatalf("prepare Redis stock %s: %v", key, err)
	}
	return key
}

func trackIntegrationRedisReservation(t *testing.T, userID, orderID string) {
	t.Helper()
	trackIntegrationRedisKeys(t, adapter.GetStrategyAwardReservationKey(userID, orderID))
}

func trackIntegrationRedisKeys(t *testing.T, keys ...string) {
	t.Helper()
	t.Cleanup(func() {
		if err := integrationRedisClient.Del(context.Background(), keys...).Err(); err != nil {
			t.Errorf("cleanup integration Redis keys %s: %v", fmt.Sprint(keys), err)
		}
	})
}

func assertIntegrationRedisInt(t *testing.T, key string, want int64) {
	t.Helper()
	got, err := integrationRedisClient.Get(context.Background(), key).Int64()
	if err != nil {
		t.Fatalf("get Redis integer %s: %v", key, err)
	}
	if got != want {
		t.Fatalf("Redis value %s = %d, want %d", key, got, want)
	}
}

func assertRedisReservationResult(t *testing.T, gotAwardID int64, gotOK bool, gotErr error, wantAwardID int64, wantOK bool) {
	t.Helper()
	if gotErr != nil {
		t.Fatalf("ReserveAwardStock() error = %v, want nil", gotErr)
	}
	if gotAwardID != wantAwardID || gotOK != wantOK {
		t.Fatalf("ReserveAwardStock() = (%d, %t), want (%d, %t)", gotAwardID, gotOK, wantAwardID, wantOK)
	}
}
