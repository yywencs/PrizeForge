package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInitViperConfigEnvironmentOverrides(t *testing.T) {
	tempDir := t.TempDir()
	configBody := []byte(`
data:
  mysql:
    dsn: file-dsn
  redis:
    password: file-redis-password
asynq:
  redis:
    password: file-asynq-password
rabbitmq:
  username: file-user
  password: file-rabbitmq-password
  topic:
    activity_sku_stock_zero: file-stock-zero
    send_award: file-send-award
    send_rebate: file-send-rebate
    draw_result: file-draw-result
  listener:
    simple:
      prefetch: 1
      default_concurrency: 1
      concurrency:
        draw_result_queue: 8
        send_award_queue: 4
`)
	if err := os.WriteFile(filepath.Join(tempDir, "config.yaml"), configBody, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	originalDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("change working directory: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(originalDir)
		Conf = nil
	})

	t.Setenv("PRIZEFORGE_DATA_MYSQL_DSN", "env-dsn")
	t.Setenv("PRIZEFORGE_DATA_REDIS_PASSWORD", "env-redis-password")
	t.Setenv("PRIZEFORGE_ASYNQ_REDIS_PASSWORD", "env-asynq-password")
	t.Setenv("PRIZEFORGE_RABBITMQ_USERNAME", "env-user")
	t.Setenv("PRIZEFORGE_RABBITMQ_PASSWORD", "env-rabbitmq-password")
	t.Setenv("PRIZEFORGE_RABBITMQ_TOPIC_ACTIVITY_SKU_STOCK_ZERO", "env-stock-zero")
	t.Setenv("PRIZEFORGE_RABBITMQ_TOPIC_SEND_AWARD", "env-send-award")
	t.Setenv("PRIZEFORGE_RABBITMQ_TOPIC_SEND_REBATE", "env-send-rebate")
	t.Setenv("PRIZEFORGE_RABBITMQ_TOPIC_DRAW_RESULT", "env-draw-result")
	t.Setenv("PRIZEFORGE_RABBITMQ_LISTENER_SIMPLE_DEFAULT_CONCURRENCY", "2")
	t.Setenv("PRIZEFORGE_RABBITMQ_LISTENER_SIMPLE_CONCURRENCY_DRAW_RESULT_QUEUE", "6")
	t.Setenv("PRIZEFORGE_RABBITMQ_LISTENER_SIMPLE_CONCURRENCY_SEND_AWARD_QUEUE", "3")

	InitViperConfig()

	if Conf.Data.Database.Dsn != "env-dsn" {
		t.Fatalf("mysql dsn = %q, want environment override", Conf.Data.Database.Dsn)
	}
	if Conf.Data.Redis.Password != "env-redis-password" {
		t.Fatalf("redis password = %q, want environment override", Conf.Data.Redis.Password)
	}
	if Conf.Asynq.Redis.Password != "env-asynq-password" {
		t.Fatalf("asynq redis password = %q, want environment override", Conf.Asynq.Redis.Password)
	}
	if Conf.RabbitMQ.Username != "env-user" || Conf.RabbitMQ.Password != "env-rabbitmq-password" {
		t.Fatalf("rabbitmq credentials were not overridden by environment")
	}
	if Conf.RabbitMQ.Topic.ActivitySkuStockZero != "env-stock-zero" ||
		Conf.RabbitMQ.Topic.SendAward != "env-send-award" ||
		Conf.RabbitMQ.Topic.SendRebate != "env-send-rebate" ||
		Conf.RabbitMQ.Topic.DrawResult != "env-draw-result" {
		t.Fatalf("rabbitmq topic config = %#v, want environment overrides", Conf.RabbitMQ.Topic)
	}
	if Conf.RabbitMQ.Listener.Simple.Prefetch != 1 ||
		Conf.RabbitMQ.Listener.Simple.DefaultConcurrency != 2 ||
		Conf.RabbitMQ.Listener.Simple.Concurrency["draw_result_queue"] != 6 ||
		Conf.RabbitMQ.Listener.Simple.Concurrency["send_award_queue"] != 3 {
		t.Fatalf("rabbitmq listener config = %#v, want prefetch=1 default=2 draw=6 award=3",
			Conf.RabbitMQ.Listener.Simple)
	}
}

func TestRabbitMQTopicConfigValidate(t *testing.T) {
	valid := RabbitMQTopicConfig{
		ActivitySkuStockZero: "stock-zero",
		SendAward:            "send-award",
		SendRebate:           "send-rebate",
		DrawResult:           "draw-result",
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("Validate() error = %v, want nil", err)
	}

	missing := valid
	missing.DrawResult = ""
	if err := missing.Validate(); err == nil || !strings.Contains(err.Error(), "draw_result is required") {
		t.Fatalf("missing topic Validate() error = %v, want draw_result required", err)
	}

	duplicate := valid
	duplicate.SendRebate = duplicate.SendAward
	if err := duplicate.Validate(); err == nil || !strings.Contains(err.Error(), "must use different topics") {
		t.Fatalf("duplicate topic Validate() error = %v, want duplicate error", err)
	}
}
