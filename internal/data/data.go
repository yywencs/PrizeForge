package data

import (
	"big-market-kratos/internal/conf"
	"big-market-kratos/pkg/cache"
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"strings"
	"time"

	"github.com/google/wire"
	"github.com/hibiken/asynq"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// =======================Mysql ==========================

func NewDB(conf *conf.Data_Mysql) *gorm.DB {
	// 1. 配置 GORM 参数 (例如日志级别)
	gormConfig := &gorm.Config{
		// 建议开发环境用 Info，生产环境用 Error 或 Warn
		Logger: logger.Default.LogMode(logger.Info),
		// 禁用默认事务可以提高性能，根据业务需求开启
		// SkipDefaultTransaction: true,
	}

	dsn := conf.Dsn
	if strings.Contains(dsn, "%s") {
		dsn = fmt.Sprintf(dsn, "")
	}

	db, err := gorm.Open(mysql.Open(dsn), gormConfig)
	if err != nil {
		panic(fmt.Sprintf("failed to connect database: %v", err))
	}

	// 4. 获取底层的 sql.DB 对象以设置连接池参数
	sqlDB, err := db.DB()
	if err != nil {
		panic(fmt.Sprintf("failed to get sql.DB instance: %v", err))
	}

	// 5. 设置连接池 (Connection Pool)
	// SetMaxIdleConns 设置空闲连接池中连接的最大数量
	if conf.MaxIdleConns > 0 {
		sqlDB.SetMaxIdleConns(int(conf.MaxIdleConns))
	}

	// SetMaxOpenConns 设置打开数据库连接的最大数量
	if conf.MaxOpenConns > 0 {
		sqlDB.SetMaxOpenConns(int(conf.MaxOpenConns))
	}

	// SetConnMaxLifetime 设置了连接可复用的最大时间
	if conf.MaxLifeTime != nil {
		sqlDB.SetConnMaxLifetime(conf.MaxLifeTime.AsDuration())
	}

	// SetConnMaxIdleTime 设置了连接在空闲状态下的最大存活时间
	if conf.MaxIdleTime != nil {
		sqlDB.SetConnMaxIdleTime(conf.MaxIdleTime.AsDuration())
	}

	return db
}

// =======================DB Router ==========================

// DBRouter 数据库路由结构体
type DBRouter struct {
	dbMap   map[string]*gorm.DB // 分库集合 (big_market_01, big_market_02)
	dbCount int                 // 分库数量
	tbCount int                 // 分表数量
}

func NewDBRouter(conf *conf.Data_Mysql) *DBRouter {
	dbCount := int(conf.DbCount)
	tbCount := int(conf.TbCount)
	// 1. 初始化结构体，存入配置
	dbRouter := &DBRouter{
		dbMap:   make(map[string]*gorm.DB),
		dbCount: dbCount,
		tbCount: tbCount,
	}

	// 3. 初始化分库
	for i := 1; i <= dbCount; i++ {
		// 构造 DSN: big_market_%s -> big_market_01
		dbName := fmt.Sprintf("_%02d", i)
		dsn := fmt.Sprintf(conf.Dsn, dbName)

		subConf := *conf
		subConf.Dsn = dsn

		// 存入 Map: Key = "01", "02" (这样路由时比较好拼)
		dbRouter.dbMap[fmt.Sprintf("%02d", i)] = NewDB(&subConf)
		fmt.Printf("DBRouter: %s\n", dsn)
	}
	return dbRouter
}

// DBStrategy 分库分表策略
// return: 1. 目标库连接 2. 表名后缀(例如 "001")
func (r *DBRouter) DBStrategy(shardKey string) (*gorm.DB, string) {
	// 1. 计算 Hash
	crc32q := crc32.MakeTable(crc32.IEEE)
	hashVal := crc32.Checksum([]byte(shardKey), crc32q)

	// 2. 计算总容量
	size := int64(r.dbCount * r.tbCount)

	// 3. 计算全局索引 (Global Index)
	idx := int64(hashVal) % size

	// 4. 计算库索引
	dbIdx := idx/int64(r.tbCount) + 1 // +1 是为了匹配库名 _01, _02

	// 5. 获取 DB
	// key 格式 "01", "02"
	dbKey := fmt.Sprintf("%02d", dbIdx)
	db := r.dbMap[dbKey]
	if db == nil {
		// 兜底逻辑，或者 panic，说明配置不对
		return nil, ""
	}

	// 6. 返回后缀 (使用全局索引 idx)
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

// =======================Redis ==========================
func NewRedisClient(cfg *conf.Data_Redis) *cache.Cache {
	// 1. 组装 Options
	opts := &redis.Options{
		// 网络地址: host:port
		Addr: fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),

		// 如果你的配置中有密码，在这里设置
		// Password: cfg.Password,
		DB: int(cfg.Db), // 默认为0

		// --- 连接池配置 ---
		// PoolSize: 最大连接数
		PoolSize: int(cfg.PoolSize),
		// MinIdleConns: 最小空闲连接数 (对应 min-idle-size)
		MinIdleConns: int(cfg.MinIdleSize),

		// --- 超时配置 (注意单位转换: ms -> time.Duration) ---
		// DialTimeout: 建立连接超时 (对应 connect-timeout)
		DialTimeout: time.Duration(cfg.ConnectTimeout) * time.Millisecond,
		// ConnMaxIdleTime: 空闲连接存活时间 (对应 idle-timeout)
		// 如果连接空闲超过这个时间，会被连接池关闭
		ConnMaxIdleTime: time.Duration(cfg.IdleTimeout) * time.Millisecond,

		// --- 重试配置 ---
		// MaxRetries: 最大重试次数
		MaxRetries: int(cfg.RetryAttempts),
		// MinRetryBackoff: 重试间隔 (对应 retry-interval)
		// go-redis 默认是指数退避，这里设置最小间隔
		MinRetryBackoff: time.Duration(cfg.RetryInterval) * time.Millisecond,

		// 注意: ping-interval 和 keep-alive 在 go-redis 中通常不需要手动设置
		// go-redis 内部会自动管理连接健康；TCP KeepAlive 默认也是开启的。
	}

	client := redis.NewClient(opts)

	// 使用一个较短的上下文超时来检测，避免启动时卡死
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		panic(fmt.Sprintf("failed to connect to redis: %v", err))
	}

	myCache := cache.New(&cache.Options{
		Redis: client,
		// 为了方便调试，我们强制使用 JSON，这样你去 Redis 客户端看的时候不是乱码
		Marshal: func(v interface{}) ([]byte, error) {
			return json.Marshal(v)
		},
		Unmarshal: func(b []byte, v interface{}) error {
			return json.Unmarshal(b, v)
		},
	})

	return myCache
}

// =======================RabbitMQ ==========================
func NewConnection(conf *conf.RabbitMQ) (*amqp.Connection, error) {
	connStr := fmt.Sprintf("amqp://%s:%s@%s:%d/",
		conf.Username,
		conf.Password,
		conf.Addresses,
		conf.Port,
	)
	return amqp.Dial(connStr)
}

// =======================Asynq ==========================

func NewAsynqClient(cfg *conf.Asynq) *asynq.Client {
	return asynq.NewClient(asynq.RedisClientOpt{
		Addr:     fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port),
		DB:       int(cfg.Redis.Db),
		PoolSize: int(cfg.Redis.PoolSize),
		// Password: cfg.Redis.Password,
	})
}

func NewAsynqInspector(cfg *conf.Asynq) *asynq.Inspector {
	return asynq.NewInspector(asynq.RedisClientOpt{
		Addr:     fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port),
		DB:       int(cfg.Redis.Db),
		PoolSize: int(cfg.Redis.PoolSize),
		// Password: cfg.Redis.Password,
	})
}

var ProviderSet = wire.NewSet(
	NewDB,
	NewDBRouter,
	NewRedisClient,
	NewConnection,
	NewRabbitMQPublisher,
	NewPublisher,
	NewMysqlConfig,
	NewRedisConfig,
	NewTBCount,
	NewStrategyRepository,
	NewActivityRepository,
	NewUserAwardRecordRepository,
	NewTaskRepository,
	NewRebateRepository,
	NewAsynqClient,
	NewAsynqInspector,
)

func NewMysqlConfig(c *conf.Data) *conf.Data_Mysql {
	return c.Mysql
}

func NewRedisConfig(c *conf.Data) *conf.Data_Redis {
	return c.Redis
}

func NewTBCount() int {
	return 2 // Default value
}
