package main

import (
	"io"
	"strings"
	"testing"
	"time"
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
