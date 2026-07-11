package infra

import (
	confpb "big-market-kratos/internal/conf"
	"big-market-kratos/internal/metrics"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func NewDB(conf *confpb.Data_Mysql) *gorm.DB {
	db, sqlDB := openMySQLDB(conf)
	startMySQLStatsCollector(sqlDB, "default", "primary")
	return db
}

func openMySQLDB(conf *confpb.Data_Mysql) (*gorm.DB, *sql.DB) {
	gormConfig := &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	}

	dsn := conf.Dsn
	if strings.Contains(dsn, "%s") {
		dsn = fmt.Sprintf(dsn, "")
	}

	db, err := gorm.Open(mysql.Open(dsn), gormConfig)
	if err != nil {
		panic(fmt.Sprintf("failed to connect database: %v", err))
	}

	sqlDB, err := db.DB()
	if err != nil {
		panic(fmt.Sprintf("failed to get sql.DB instance: %v", err))
	}

	if conf.MaxIdleConns > 0 {
		sqlDB.SetMaxIdleConns(int(conf.MaxIdleConns))
	}
	if conf.MaxOpenConns > 0 {
		sqlDB.SetMaxOpenConns(int(conf.MaxOpenConns))
	}
	if conf.MaxLifeTime != nil {
		sqlDB.SetConnMaxLifetime(conf.MaxLifeTime.AsDuration())
	}
	if conf.MaxIdleTime != nil {
		sqlDB.SetConnMaxIdleTime(conf.MaxIdleTime.AsDuration())
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
