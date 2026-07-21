package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	config, err := parseConfig(os.Args[1:], os.Stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintf(os.Stderr, "benchmark 配置错误: %v\n", err)
		os.Exit(2)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	fmt.Println("PrizeForge draw benchmark")
	fmt.Printf("  endpoint:    %s\n", config.endpoint())
	fmt.Printf("  activity:    %d\n", config.ActivityID)
	fmt.Printf("  users:       %d\n", config.Users)
	fmt.Printf("  concurrency: %d\n", config.Concurrency)
	fmt.Printf("  duration:    %s\n", config.Duration)
	fmt.Printf("  timeout:     %s\n", config.Timeout)

	runner := newBenchmarkRunner(config, nil)
	printSummary(runner.run(ctx))
}
