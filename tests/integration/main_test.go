//go:build integration

package integration

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"prizeforge/internal/infrastructure/adapter"
	"prizeforge/pkg/config"

	mysqlDriver "github.com/go-sql-driver/mysql"
	gormMySQL "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const defaultIntegrationDSN = "root:prizeforge-integration@tcp(127.0.0.1:13306)/prizeforge%s?charset=utf8mb4&parseTime=True&loc=Local&timeout=5s"

var (
	integrationDBRouter  *adapter.DBRouter
	integrationDefaultDB *gorm.DB
)

// TestMain 连接由 compose.integration.yaml 创建的临时 MySQL，验证三个数据库可用，
// 并在全部集成测试结束后关闭默认库和两个分片库的连接池。
func TestMain(m *testing.M) {
	dsn := strings.TrimSpace(os.Getenv("PRIZEFORGE_INTEGRATION_MYSQL_DSN"))
	if dsn == "" {
		dsn = defaultIntegrationDSN
	}
	if err := validateIntegrationDSN(dsn); err != nil {
		fmt.Fprintf(os.Stderr, "invalid integration MySQL DSN: %v\n", err)
		os.Exit(1)
	}

	cfg := &config.DatabaseConfig{
		Dsn:          dsn,
		MaxOpenConns: 4,
		MaxIdleConns: 2,
		DbCount:      2,
		TbCount:      4,
	}

	var err error
	integrationDefaultDB, err = gorm.Open(gormMySQL.Open(strings.Replace(dsn, "%s", "", 1)), &gorm.Config{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "open integration default database: %v\n", err)
		os.Exit(1)
	}
	integrationDBRouter = adapter.NewDBRouter(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if sqlDB, dbErr := integrationDefaultDB.DB(); dbErr != nil {
		cancel()
		fmt.Fprintf(os.Stderr, "get integration default database: %v\n", dbErr)
		os.Exit(1)
	} else if pingErr := sqlDB.PingContext(ctx); pingErr != nil {
		cancel()
		fmt.Fprintf(os.Stderr, "ping integration default database: %v\n", pingErr)
		os.Exit(1)
	}
	if err := integrationDBRouter.Ping(ctx); err != nil {
		cancel()
		fmt.Fprintf(os.Stderr, "ping integration database shards: %v\n", err)
		os.Exit(1)
	}
	cancel()

	code := m.Run()
	closeIntegrationDatabases()
	os.Exit(code)
}

func validateIntegrationDSN(dsn string) error {
	const databaseTemplate = "/prizeforge%s?"
	if !strings.Contains(dsn, databaseTemplate) {
		return fmt.Errorf("database template must be prizeforge%%s")
	}

	// go-sql-driver/mysql 会把数据库名当作 URL 路径解析，因此先将路由占位符
	// 替换为空字符串，再校验实际连接参数。
	parsed, err := mysqlDriver.ParseDSN(strings.Replace(dsn, "%s", "", 1))
	if err != nil {
		return err
	}
	if parsed.DBName != "prizeforge" {
		return fmt.Errorf("base database must be prizeforge, got %q", parsed.DBName)
	}
	host, _, err := net.SplitHostPort(parsed.Addr)
	if err != nil {
		return fmt.Errorf("parse address %q: %w", parsed.Addr, err)
	}
	if host != "127.0.0.1" && host != "localhost" && host != "::1" {
		return fmt.Errorf("refusing non-local database host %q", host)
	}
	return nil
}

func closeIntegrationDatabases() {
	if integrationDefaultDB != nil {
		if sqlDB, err := integrationDefaultDB.DB(); err == nil {
			_ = sqlDB.Close()
		}
	}
	if integrationDBRouter == nil {
		return
	}
	for dbIndex := 1; dbIndex <= integrationDBRouter.GetDBCount(); dbIndex++ {
		if db := integrationDBRouter.GetDB(dbIndex); db != nil {
			if sqlDB, err := db.DB(); err == nil {
				_ = sqlDB.Close()
			}
		}
	}
}
