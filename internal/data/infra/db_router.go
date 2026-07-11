package infra

import (
	confpb "big-market-kratos/internal/conf"
	"fmt"
	"hash/crc32"

	"gorm.io/gorm"
)

// DBRouter 数据库路由结构体
type DBRouter struct {
	dbMap   map[string]*gorm.DB // 分库集合 (big_market_01, big_market_02)
	dbCount int                 // 分库数量
	tbCount int                 // 分表数量
}

func NewDBRouter(conf *confpb.Data_Mysql) *DBRouter {
	dbCount := int(conf.DbCount)
	tbCount := int(conf.TbCount)
	dbRouter := &DBRouter{
		dbMap:   make(map[string]*gorm.DB),
		dbCount: dbCount,
		tbCount: tbCount,
	}

	for i := 1; i <= dbCount; i++ {
		dbName := fmt.Sprintf("_%02d", i)
		dsn := fmt.Sprintf(conf.Dsn, dbName)

		subConf := &confpb.Data_Mysql{
			Dsn:          dsn,
			MaxOpenConns: conf.MaxOpenConns,
			MaxIdleConns: conf.MaxIdleConns,
			MaxLifeTime:  conf.MaxLifeTime,
			MaxIdleTime:  conf.MaxIdleTime,
			DbCount:      conf.DbCount,
			TbCount:      conf.TbCount,
		}

		db, _ := openMySQLDB(subConf)
		dbRouter.dbMap[fmt.Sprintf("%02d", i)] = db
		fmt.Printf("DBRouter: %s\n", dsn)
	}

	return dbRouter
}

// DBStrategy 分库分表策略
// return: 1. 目标库连接 2. 表名后缀(例如 "001")
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

// GetDB 根据分库索引返回对应的 DB 实例
func (r *DBRouter) GetDB(dbIdx int) *gorm.DB {
	dbKey := fmt.Sprintf("%02d", dbIdx)
	return r.dbMap[dbKey]
}

// GetDBCount 获取分库数量
func (r *DBRouter) GetDBCount() int {
	return r.dbCount
}
