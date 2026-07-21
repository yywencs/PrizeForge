package adapter

import (
	"database/sql"
	"fmt"
	stdlog "log"
	"os"
	"strings"
	"time"

	"prizeforge/internal/metrics"
	"prizeforge/pkg/config"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// NewDB creates a GORM DB connection from config.
func NewDB(cfg *config.DatabaseConfig) *gorm.DB {
	defaultCfg := *cfg
	defaultCfg.Dsn = resolveDatabaseDSN(cfg.Dsn, "")
	db, sqlDB := openMySQLDB(&defaultCfg)
	startMySQLStatsCollector(sqlDB, "default", "primary")
	return db
}

func resolveDatabaseDSN(dsnTemplate, suffix string) string {
	return strings.Replace(dsnTemplate, "%s", suffix, 1)
}

// openMySQLDB opens a MySQL connection using GORM.
func openMySQLDB(cfg *config.DatabaseConfig) (*gorm.DB, *sql.DB) {
	// 生产环境只记录 SQL 错误和慢查询，避免在高并发场景下为每条 SQL
	// 执行日志格式化及磁盘写入。查询不存在是仓储层的正常分支，不作为错误记录。
	databaseLogger := gormlogger.New(
		stdlog.New(os.Stdout, "\r\n", stdlog.LstdFlags),
		gormlogger.Config{
			SlowThreshold:             200 * time.Millisecond,
			LogLevel:                  gormlogger.Warn,
			IgnoreRecordNotFoundError: true,
			ParameterizedQueries:      true,
			Colorful:                  false,
		},
	)
	gormConfig := &gorm.Config{
		Logger: databaseLogger,
	}

	dsn := cfg.Dsn

	db, err := gorm.Open(mysql.Open(dsn), gormConfig)
	if err != nil {
		panic(fmt.Sprintf("failed to connect database: %v", err))
	}

	sqlDB, err := db.DB()
	if err != nil {
		panic(fmt.Sprintf("failed to get sql.DB instance: %v", err))
	}

	if cfg.MaxIdleConns > 0 {
		sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.MaxOpenConns > 0 {
		sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxLifeTime > 0 {
		sqlDB.SetConnMaxLifetime(cfg.MaxLifeTime)
	}
	if cfg.MaxIdleTime > 0 {
		sqlDB.SetConnMaxIdleTime(cfg.MaxIdleTime)
	}

	return db, sqlDB
}

func startMySQLStatsCollector(sqlDB *sql.DB, dbName, role string) {
	go func() {
		collectMySQLStats(sqlDB, dbName, role)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			collectMySQLStats(sqlDB, dbName, role)
		}
	}()
}

func collectMySQLStats(sqlDB *sql.DB, dbName, role string) {
	if sqlDB == nil {
		return
	}
	metrics.SetMySQLStats(dbName, role, sqlDB.Stats())
}
