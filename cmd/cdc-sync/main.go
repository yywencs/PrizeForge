package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"prizeforge/internal/cdc"
)

func main() {
	cfg, err := cdc.LoadConfigFromEnv()
	if err != nil {
		log.Fatalf("load cdc config: %v", err)
	}

	writer := cdc.NewESWriter(cfg)
	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := writer.Ping(pingCtx); err != nil {
		log.Fatalf("ping elasticsearch: %v", err)
	}

	handler := cdc.NewEventHandler(cfg, writer)
	syncInstance, err := cdc.NewSyncer(cfg, handler)
	if err != nil {
		log.Fatalf("new cdc syncer: %v", err)
	}
	defer syncInstance.Close()

	log.Printf(
		"cdc-sync starting mysql=%s es=%s include=%v server_id=%d",
		cfg.MySQLAddr,
		cfg.ESAddr,
		cfg.IncludeTableRegex,
		cfg.MySQLServerID,
	)

	if err := syncInstance.Run(); err != nil {
		log.Fatalf("run cdc syncer: %v", fmt.Errorf("sync loop exited: %w", err))
	}
}
