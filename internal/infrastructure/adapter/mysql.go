package adapter

import (
	"database/sql"
	"fmt"
	"prizeforge/internal/metrics"
	"prizeforge/pkg/config"
	"strings"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
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
	gormConfig := &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
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
