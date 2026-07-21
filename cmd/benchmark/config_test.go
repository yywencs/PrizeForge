package main

import (
	"io"
	"testing"
	"time"
)

// TestParseConfig 验证命令行参数能够生成完整且正确的压测配置。
func TestParseConfig(t *testing.T) {
	config, err := parseConfig([]string{
		"--url", "http://example.test:8080/",
		"--activity-id", "200001",
		"--users", "200",
		"--concurrency", "20",
		"--duration", "45s",
		"--timeout", "3s",
		"--user-prefix", "load-user",
	}, io.Discard)
	if err != nil {
		t.Fatalf("parseConfig() error = %v, want nil", err)
	}

	if got, want := config.endpoint(), "http://example.test:8080"+drawPath; got != want {
		t.Fatalf("endpoint() = %q, want %q", got, want)
	}
	if config.ActivityID != 200001 || config.Users != 200 || config.Concurrency != 20 {
		t.Fatalf("parsed numeric config = %+v, want provided values", config)
	}
	if config.Duration != 45*time.Second || config.Timeout != 3*time.Second {
		t.Fatalf("parsed durations = %s/%s, want 45s/3s", config.Duration, config.Timeout)
	}
	if config.UserPrefix != "load-user" {
		t.Fatalf("UserPrefix = %q, want load-user", config.UserPrefix)
	}
}

// TestParseConfigRejectsInvalidValues 验证无效地址和非正并发数会在发起请求前被拒绝。
func TestParseConfigRejectsInvalidValues(t *testing.T) {
	testCases := []struct {
		name string
		args []string
	}{
		{name: "invalid URL", args: []string{"--url", "127.0.0.1:8080"}},
		{name: "URL with path", args: []string{"--url", "http://127.0.0.1:8080/api"}},
		{name: "zero concurrency", args: []string{"--concurrency", "0"}},
		{name: "zero users", args: []string{"--users", "0"}},
		{name: "zero duration", args: []string{"--duration", "0s"}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := parseConfig(testCase.args, io.Discard); err == nil {
				t.Fatal("parseConfig() error = nil, want validation error")
			}
		})
	}
}
