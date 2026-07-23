package main

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"prizeforge/internal/infrastructure/adapter"

	redis "github.com/redis/go-redis/v9"
)

// TestParsePrepareConfig 验证 prepare 参数能够从环境变量和命令行组合成完整配置。
func TestParsePrepareConfig(t *testing.T) {
	t.Setenv("PRIZEFORGE_BENCHMARK_MYSQL_DSN", "root:password@tcp(localhost:3306)/prizeforge%s")
	t.Setenv("PRIZEFORGE_BENCHMARK_REDIS_ADDR", "localhost:6379")

	config, err := parsePrepareConfig([]string{
		"--confirm-reset",
		"--users", "2000",
		"--quota", "30",
		"--user-prefix", "load-0721",
		"--timeout", "90s",
	}, io.Discard)
	if err != nil {
		t.Fatalf("parsePrepareConfig() error = %v, want nil", err)
	}
	if config.Users != 2000 || config.Quota != 30 || config.UserPrefix != "load-0721" {
		t.Fatalf("config = %+v, want provided benchmark data settings", config)
	}
	if config.Timeout != 90*time.Second || config.RedisAddr != "localhost:6379" {
		t.Fatalf("timeout/redis = %s/%q, want 90s/localhost:6379", config.Timeout, config.RedisAddr)
	}
}

// TestParsePrepareConfigRequiresExplicitConfirmation 验证 prepare 未显式确认时不会修改数据库。
func TestParsePrepareConfigRequiresExplicitConfirmation(t *testing.T) {
	t.Setenv("PRIZEFORGE_BENCHMARK_MYSQL_DSN", "root:password@tcp(localhost:3306)/prizeforge%s")

	_, err := parsePrepareConfig(nil, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "--confirm-reset") {
		t.Fatalf("parsePrepareConfig() error = %v, want confirmation error", err)
	}
}

// TestBenchmarkShardIndexDistributesUsersAcrossConfiguredDatabases 验证用户按生产 CRC32 公式分布到有效分库。
func TestBenchmarkShardIndexDistributesUsersAcrossConfiguredDatabases(t *testing.T) {
	counts := make([]int, 2)
	for index := 1; index <= 1000; index++ {
		shardIndex := benchmarkShardIndex(benchmarkUserID("benchmark-user", index), 2, 4)
		if shardIndex < 0 || shardIndex >= len(counts) {
			t.Fatalf("shard index = %d, want range [0, %d)", shardIndex, len(counts))
		}
		counts[shardIndex]++
	}
	if counts[0] == 0 || counts[1] == 0 || counts[0]+counts[1] != 1000 {
		t.Fatalf("shard counts = %v, want 1000 users across both shards", counts)
	}
}

// TestRepeatedValues 验证批量插入占位符数量与用户批次大小一致。
func TestRepeatedValues(t *testing.T) {
	if got, want := repeatedValues(3, "(?, ?)"), "(?, ?),(?, ?),(?, ?)"; got != want {
		t.Fatalf("repeatedValues() = %q, want %q", got, want)
	}
}

type recordingBenchmarkQuotaPipeline struct {
	deleted []string
	values  map[string]interface{}
	ttls    map[string]time.Duration
}

func (p *recordingBenchmarkQuotaPipeline) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	p.deleted = append(p.deleted, keys...)
	return redis.NewIntCmd(ctx)
}

func (p *recordingBenchmarkQuotaPipeline) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	if p.values == nil {
		p.values = make(map[string]interface{})
		p.ttls = make(map[string]time.Duration)
	}
	p.values[key] = value
	p.ttls[key] = expiration
	return redis.NewStatusCmd(ctx)
}

func TestQueueBenchmarkQuotaPreheatUsesProductionQuotaKeys(t *testing.T) {
	const (
		activityID = int64(100301)
		userID     = "benchmark-user-000001"
		quota      = 20
	)
	currentTime := time.Date(2026, 7, 23, 12, 0, 0, 0, time.Local)
	pipe := &recordingBenchmarkQuotaPipeline{}

	queueBenchmarkQuotaPreheat(context.Background(), pipe, activityID, userID, quota, currentTime)

	if len(pipe.deleted) != 2 ||
		pipe.deleted[0] != adapter.GetActivityAccountKey(activityID, userID) ||
		pipe.deleted[1] != adapter.GetPendingRaffleOrderKey(activityID, userID) {
		t.Fatalf("deleted keys = %#v, want account snapshot and pending order", pipe.deleted)
	}
	wantKeys := []string{
		adapter.GetActivityAccountTotalSurplusKey(activityID, userID),
		adapter.GetActivityAccountMonthSurplusKey(activityID, userID, "2026-07"),
		adapter.GetActivityAccountDaySurplusKey(activityID, userID, "2026-07-23"),
	}
	for _, key := range wantKeys {
		if pipe.values[key] != quota {
			t.Fatalf("quota key %q value = %#v, want %d", key, pipe.values[key], quota)
		}
		if pipe.ttls[key] != 0 {
			t.Fatalf("quota key %q TTL = %s, want no expiration", key, pipe.ttls[key])
		}
	}
	if len(pipe.values) != len(wantKeys) {
		t.Fatalf("preheated quota keys = %#v, want exactly %d keys", pipe.values, len(wantKeys))
	}
}
