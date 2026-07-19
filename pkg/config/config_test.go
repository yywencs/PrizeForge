package config

import (
	"os"
	"path/filepath"
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
}
