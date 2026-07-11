package config

import "time"

// Config 对应整个 config.yaml 文件的根节点
type Config struct {
	Server   ServerConfig   `mapstructure:"server"`
	Data     DataConfig     `mapstructure:"data"`
	Log      LogConfig      `mapstructure:"log"`
	RabbitMQ RabbitMQConfig `mapstructure:"rabbitmq"`
	Asynq    AsynqConfig    `mapstructure:"asynq"`
	Monitor  MonitorConfig  `mapstructure:"monitor"`
	Dcc      DccConfig      `mapstructure:"dcc"`
}

// --- Server 部分 ---

type ServerConfig struct {
	API   HttpConfig `mapstructure:"api"`
	Admin HttpConfig `mapstructure:"admin"`
	Http  HttpConfig `mapstructure:"http"`
	GRPC  HttpConfig `mapstructure:"grpc"`
}

type HttpConfig struct {
	Addr    string `mapstructure:"addr"`
	Timeout string `mapstructure:"timeout"`
}

// --- Data 部分 ---

type DataConfig struct {
	Database DatabaseConfig `mapstructure:"mysql"`
	Redis    RedisConfig    `mapstructure:"redis"`
	Etcd     EtcdConfig     `mapstructure:"etcd"`
}

type DatabaseConfig struct {
	Dsn          string        `mapstructure:"dsn"`
	MaxOpenConns int           `mapstructure:"max_open_conns"`
	MaxIdleConns int           `mapstructure:"max_idle_conns"`
	MaxLifeTime  time.Duration `mapstructure:"max_life_time"`
	MaxIdleTime  time.Duration `mapstructure:"max_idle_time"`
	DbCount      int           `mapstructure:"db_count"`
	TbCount      int           `mapstructure:"tb_count"`
}

type RedisConfig struct {
	Host           string `mapstructure:"host"`
	Port           int    `mapstructure:"port"`
	DB             int    `mapstructure:"db"`
	PoolSize       int    `mapstructure:"pool_size"`
	MinIdleSize    int    `mapstructure:"min_idle_size"`
	IdleTimeout    int    `mapstructure:"idle_timeout"`
	ConnectTimeout int    `mapstructure:"connect_timeout"`
	RetryAttempts  int    `mapstructure:"retry_attempts"`
	RetryInterval  int    `mapstructure:"retry_interval"`
	PingInterval   int    `mapstructure:"ping_interval"`
	KeepAlive      bool   `mapstructure:"keep_alive"`
}

type EtcdConfig struct {
	Endpoints []string      `mapstructure:"endpoints"`
	Timeout   time.Duration `mapstructure:"timeout"`
}

// --- RabbitMQ 部分 ---

type RabbitMQConfig struct {
	Addresses string              `mapstructure:"addresses"`
	Port      int                 `mapstructure:"port"`
	Username  string              `mapstructure:"username"`
	Password  string              `mapstructure:"password"`
	Listener  RabbitMQListener    `mapstructure:"listener"`
	Topic     RabbitMQTopicConfig `mapstructure:"topic"`
}

type RabbitMQListener struct {
	Simple RabbitMQSimple `mapstructure:"simple"`
}

type RabbitMQSimple struct {
	Prefetch int `mapstructure:"prefetch"`
}

type RabbitMQTopicConfig struct {
	ActivitySkuStockZero string `mapstructure:"activity_sku_stock_zero"`
	SendAward            string `mapstructure:"send_award"`
	SendRebate           string `mapstructure:"send_rebate"`
	SaveOrderRecord      string `mapstructure:"save_order_record"`
}

// --- Asynq 部分 ---

type AsynqConfig struct {
	Redis       RedisConfig `mapstructure:"redis"`
	Concurrency int         `mapstructure:"concurrency"`
}

// --- Monitor 部分 ---

type MonitorConfig struct {
	Enable bool   `mapstructure:"enable"`
	Addr   string `mapstructure:"addr"`
	Path   string `mapstructure:"path"`
}

// --- DCC 部分 ---

type DccConfig struct {
	RateLimit     int    `mapstructure:"rate_limit"`
	EnableDegrade bool   `mapstructure:"enable_degrade"`
	BlackList     string `mapstructure:"black_list"`
}

// --- Log 部分 ---

type LogConfig struct {
	Level      string `mapstructure:"level"`
	Filename   string `mapstructure:"filename"`
	MaxSize    int    `mapstructure:"max_size"`
	MaxBackups int    `mapstructure:"max_backups"`
	MaxAge     int    `mapstructure:"max_age"`
	Compress   bool   `mapstructure:"compress"`
}

