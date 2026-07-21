package main

import (
	"flag"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"
)

const drawPath = "/api/v1/raffle/activity/draw"

type benchmarkConfig struct {
	BaseURL     string
	ActivityID  int64
	Users       int
	Concurrency int
	Duration    time.Duration
	Timeout     time.Duration
	UserPrefix  string
}

func parseConfig(args []string, output io.Writer) (benchmarkConfig, error) {
	cfg := benchmarkConfig{}
	flags := flag.NewFlagSet("benchmark", flag.ContinueOnError)
	flags.SetOutput(output)

	flags.StringVar(&cfg.BaseURL, "url", "http://127.0.0.1:8080", "API 服务根地址")
	flags.Int64Var(&cfg.ActivityID, "activity-id", 100301, "抽奖活动 ID")
	flags.IntVar(&cfg.Users, "users", 1000, "压测用户池大小")
	flags.IntVar(&cfg.Concurrency, "concurrency", 10, "并发工作协程数")
	flags.DurationVar(&cfg.Duration, "duration", 30*time.Second, "持续压测时间")
	flags.DurationVar(&cfg.Timeout, "timeout", 5*time.Second, "单个 HTTP 请求超时")
	flags.StringVar(&cfg.UserPrefix, "user-prefix", "benchmark-user", "压测用户 ID 前缀")

	if err := flags.Parse(args); err != nil {
		return benchmarkConfig{}, err
	}
	if flags.NArg() != 0 {
		return benchmarkConfig{}, fmt.Errorf("不支持位置参数: %s", strings.Join(flags.Args(), " "))
	}
	if err := cfg.validate(); err != nil {
		return benchmarkConfig{}, err
	}
	return cfg, nil
}

func (c benchmarkConfig) validate() error {
	parsedURL, err := url.Parse(c.BaseURL)
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" {
		return fmt.Errorf("url 必须是有效的 HTTP(S) 地址: %q", c.BaseURL)
	}
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return fmt.Errorf("url 只支持 http 或 https: %q", c.BaseURL)
	}
	if parsedURL.RawQuery != "" || parsedURL.Fragment != "" {
		return fmt.Errorf("url 不能包含 query 或 fragment: %q", c.BaseURL)
	}
	if parsedURL.Path != "" && parsedURL.Path != "/" {
		return fmt.Errorf("url 应为服务根地址，不能包含路径: %q", c.BaseURL)
	}
	if c.ActivityID <= 0 {
		return fmt.Errorf("activity-id 必须大于 0")
	}
	if c.Users <= 0 {
		return fmt.Errorf("users 必须大于 0")
	}
	if c.Concurrency <= 0 {
		return fmt.Errorf("concurrency 必须大于 0")
	}
	if c.Duration <= 0 {
		return fmt.Errorf("duration 必须大于 0")
	}
	if c.Timeout <= 0 {
		return fmt.Errorf("timeout 必须大于 0")
	}
	if strings.TrimSpace(c.UserPrefix) == "" {
		return fmt.Errorf("user-prefix 不能为空")
	}
	if userID := benchmarkUserID(c.UserPrefix, c.Users); len(userID) > 32 {
		return fmt.Errorf("生成的 user_id %q 超过数据库 varchar(32)", userID)
	}
	return nil
}

func (c benchmarkConfig) endpoint() string {
	return strings.TrimRight(c.BaseURL, "/") + drawPath
}
