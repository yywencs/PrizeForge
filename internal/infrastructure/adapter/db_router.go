package adapter

import (
	"context"
	"fmt"
	"hash/crc32"
	"prizeforge/pkg/config"

	"gorm.io/gorm"
)

// DBRouter handles hash-based database sharding.
type DBRouter struct {
	dbMap   map[string]*gorm.DB
	dbCount int
	tbCount int
}

// NewDBRouter creates a DBRouter with N database connections.
func NewDBRouter(cfg *config.DatabaseConfig) *DBRouter {
	dbCount := cfg.DbCount
	tbCount := cfg.TbCount
	dbRouter := &DBRouter{
		dbMap:   make(map[string]*gorm.DB),
		dbCount: dbCount,
		tbCount: tbCount,
	}

	for i := 1; i <= dbCount; i++ {
		dbName := fmt.Sprintf("_%02d", i)
		dsn := resolveDatabaseDSN(cfg.Dsn, dbName)

		subConf := &config.DatabaseConfig{
			Dsn:          dsn,
			MaxOpenConns: cfg.MaxOpenConns,
			MaxIdleConns: cfg.MaxIdleConns,
			MaxLifeTime:  cfg.MaxLifeTime,
			MaxIdleTime:  cfg.MaxIdleTime,
		}

		db, _ := openMySQLDB(subConf)
		dbRouter.dbMap[fmt.Sprintf("%02d", i)] = db
		fmt.Printf("DBRouter: %s\n", dsn)
	}

	return dbRouter
}

// DBStrategy returns the target DB and table suffix for a given shard key.
func (r *DBRouter) DBStrategy(shardKey string) (*gorm.DB, string) {
	crc32q := crc32.MakeTable(crc32.IEEE)
	hashVal := crc32.Checksum([]byte(shardKey), crc32q)

	size := int64(r.dbCount * r.tbCount)
	idx := int64(hashVal) % size
	dbIdx := idx/int64(r.tbCount) + 1

	dbKey := fmt.Sprintf("%02d", dbIdx)
	db := r.dbMap[dbKey]
	if db == nil {
		return nil, ""
	}

	return db, fmt.Sprintf("%03d", idx)
}

// GetDB returns a DB instance by shard index.
func (r *DBRouter) GetDB(dbIdx int) *gorm.DB {
	dbKey := fmt.Sprintf("%02d", dbIdx)
	return r.dbMap[dbKey]
}

// GetDBCount returns the number of DB shards.
func (r *DBRouter) GetDBCount() int {
	return r.dbCount
}

// Ping verifies that every configured database shard is reachable.
func (r *DBRouter) Ping(ctx context.Context) error {
	for i := 1; i <= r.dbCount; i++ {
		db := r.GetDB(i)
		if db == nil {
			return fmt.Errorf("database shard %02d is not configured", i)
		}
		sqlDB, err := db.DB()
		if err != nil {
			return fmt.Errorf("get database shard %02d: %w", i, err)
		}
		if err := sqlDB.PingContext(ctx); err != nil {
			return fmt.Errorf("ping database shard %02d: %w", i, err)
		}
	}
	return nil
}
