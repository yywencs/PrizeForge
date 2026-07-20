package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	HTTPRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "prizeforge",
			Subsystem: "http",
			Name:      "requests_total",
			Help:      "Total number of HTTP requests.",
		},
		[]string{"method", "path", "code"},
	)

	HTTPRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "prizeforge",
			Subsystem: "http",
			Name:      "request_duration_seconds",
			Help:      "HTTP request latency.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)

	ActivityQuotaTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "prizeforge",
			Subsystem: "activity",
			Name:      "quota_total",
			Help:      "Total activity quota check results.",
		},
		[]string{"activity_id", "result"},
	)

	RaffleTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "prizeforge",
			Subsystem: "raffle",
			Name:      "total",
			Help:      "Total raffle attempts.",
		},
		[]string{"activity_id", "strategy_id", "result"},
	)

	RaffleDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "prizeforge",
			Subsystem: "raffle",
			Name:      "duration_seconds",
			Help:      "Raffle execution latency.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"activity_id", "strategy_id"},
	)

	AwardTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "prizeforge",
			Subsystem: "award",
			Name:      "total",
			Help:      "Total award dispatch results.",
		},
		[]string{"award_id", "result"},
	)

	StockConsumeTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "prizeforge",
			Subsystem: "stock",
			Name:      "consume_total",
			Help:      "Stock consume results.",
		},
		[]string{"activity_id", "sku_id", "result"},
	)

	RabbitMQPublishTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "prizeforge",
			Subsystem: "rabbitmq",
			Name:      "publish_total",
			Help:      "Total RabbitMQ publish results.",
		},
		[]string{"topic", "result"},
	)

	RedisOpsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "prizeforge",
			Subsystem: "redis",
			Name:      "ops_total",
			Help:      "Total Redis operations.",
		},
		[]string{"cmd", "result"},
	)

	RedisOpDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "prizeforge",
			Subsystem: "redis",
			Name:      "op_duration_seconds",
			Help:      "Redis operation latency.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"cmd"},
	)

	AsynqTaskTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "prizeforge",
			Subsystem: "asynq",
			Name:      "task_total",
			Help:      "Total Asynq task execution results.",
		},
		[]string{"task_type", "result"},
	)

	AsynqTaskDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "prizeforge",
			Subsystem: "asynq",
			Name:      "task_duration_seconds",
			Help:      "Asynq task execution latency.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"task_type"},
	)

	AsynqQueueSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "prizeforge",
			Subsystem: "asynq",
			Name:      "queue_size",
			Help:      "Current total Asynq queue size.",
		},
		[]string{"queue"},
	)

	AsynqRetrySize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "prizeforge",
			Subsystem: "asynq",
			Name:      "retry_size",
			Help:      "Current Asynq retry queue size.",
		},
		[]string{"queue"},
	)

	AsynqScheduledSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "prizeforge",
			Subsystem: "asynq",
			Name:      "scheduled_size",
			Help:      "Current Asynq scheduled queue size.",
		},
		[]string{"queue"},
	)

	MySQLOpenConnections = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "prizeforge",
			Subsystem: "mysql",
			Name:      "open_connections",
			Help:      "Current number of open MySQL connections.",
		},
		[]string{"db_name", "role"},
	)

	MySQLInUse = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "prizeforge",
			Subsystem: "mysql",
			Name:      "in_use",
			Help:      "Current number of in-use MySQL connections.",
		},
		[]string{"db_name", "role"},
	)

	MySQLIdle = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "prizeforge",
			Subsystem: "mysql",
			Name:      "idle",
			Help:      "Current number of idle MySQL connections.",
		},
		[]string{"db_name", "role"},
	)

	MySQLWaitCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "prizeforge",
			Subsystem: "mysql",
			Name:      "wait_count",
			Help:      "Current sampled MySQL wait count.",
		},
		[]string{"db_name", "role"},
	)

	MySQLWaitDurationSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "prizeforge",
			Subsystem: "mysql",
			Name:      "wait_duration_seconds",
			Help:      "Current sampled MySQL wait duration in seconds.",
		},
		[]string{"db_name", "role"},
	)

	MySQLMaxIdleClosedTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "prizeforge",
			Subsystem: "mysql",
			Name:      "max_idle_closed_total",
			Help:      "Current sampled MySQL max idle closed total.",
		},
		[]string{"db_name", "role"},
	)

	MySQLMaxLifetimeClosedTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "prizeforge",
			Subsystem: "mysql",
			Name:      "max_lifetime_closed_total",
			Help:      "Current sampled MySQL max lifetime closed total.",
		},
		[]string{"db_name", "role"},
	)
)

func init() {
	prometheus.MustRegister(
		HTTPRequestsTotal,
		HTTPRequestDuration,
		ActivityQuotaTotal,
		RaffleTotal,
		RaffleDuration,
		AwardTotal,
		StockConsumeTotal,
		RabbitMQPublishTotal,
		RedisOpsTotal,
		RedisOpDuration,
		AsynqTaskTotal,
		AsynqTaskDuration,
		AsynqQueueSize,
		AsynqRetrySize,
		AsynqScheduledSize,
		MySQLOpenConnections,
		MySQLInUse,
		MySQLIdle,
		MySQLWaitCount,
		MySQLWaitDurationSeconds,
		MySQLMaxIdleClosedTotal,
		MySQLMaxLifetimeClosedTotal,
	)
}
