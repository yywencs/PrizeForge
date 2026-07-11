package metrics

import (
	"database/sql"
	"strconv"
	"time"
)

func IncActivityQuota(activityID int64, result string) {
	ActivityQuotaTotal.WithLabelValues(int64Label(activityID), normalizeLabel(result)).Inc()
}

func IncRaffle(activityID, strategyID int64, result string) {
	RaffleTotal.WithLabelValues(int64Label(activityID), int64Label(strategyID), normalizeLabel(result)).Inc()
}

func ObserveRaffleDuration(activityID, strategyID int64, duration time.Duration) {
	RaffleDuration.WithLabelValues(int64Label(activityID), int64Label(strategyID)).Observe(duration.Seconds())
}

func IncStockConsume(activityID, skuID int64, result string) {
	StockConsumeTotal.WithLabelValues(int64Label(activityID), int64Label(skuID), normalizeLabel(result)).Inc()
}

func IncAward(awardID int, result string) {
	AwardTotal.WithLabelValues(intLabel(awardID), normalizeLabel(result)).Inc()
}

func IncRabbitMQPublish(topic, result string) {
	RabbitMQPublishTotal.WithLabelValues(normalizeLabel(topic), normalizeLabel(result)).Inc()
}

func IncRedisOp(cmd, result string) {
	RedisOpsTotal.WithLabelValues(normalizeLabel(cmd), normalizeLabel(result)).Inc()
}

func ObserveRedisOpDuration(cmd string, duration time.Duration) {
	RedisOpDuration.WithLabelValues(normalizeLabel(cmd)).Observe(duration.Seconds())
}

func IncAsynqTask(taskType, result string) {
	AsynqTaskTotal.WithLabelValues(normalizeLabel(taskType), normalizeLabel(result)).Inc()
}

func ObserveAsynqTaskDuration(taskType string, duration time.Duration) {
	AsynqTaskDuration.WithLabelValues(normalizeLabel(taskType)).Observe(duration.Seconds())
}

func SetAsynqQueueStats(queue string, size, retry, scheduled int) {
	queue = normalizeLabel(queue)
	AsynqQueueSize.WithLabelValues(queue).Set(float64(size))
	AsynqRetrySize.WithLabelValues(queue).Set(float64(retry))
	AsynqScheduledSize.WithLabelValues(queue).Set(float64(scheduled))
}

func SetMySQLStats(dbName, role string, stats sql.DBStats) {
	dbName = normalizeLabel(dbName)
	role = normalizeLabel(role)

	MySQLOpenConnections.WithLabelValues(dbName, role).Set(float64(stats.OpenConnections))
	MySQLInUse.WithLabelValues(dbName, role).Set(float64(stats.InUse))
	MySQLIdle.WithLabelValues(dbName, role).Set(float64(stats.Idle))
	MySQLWaitCount.WithLabelValues(dbName, role).Set(float64(stats.WaitCount))
	MySQLWaitDurationSeconds.WithLabelValues(dbName, role).Set(stats.WaitDuration.Seconds())
	MySQLMaxIdleClosedTotal.WithLabelValues(dbName, role).Set(float64(stats.MaxIdleClosed))
	MySQLMaxLifetimeClosedTotal.WithLabelValues(dbName, role).Set(float64(stats.MaxLifetimeClosed))
}

func int64Label(v int64) string {
	if v <= 0 {
		return "unknown"
	}
	return strconv.FormatInt(v, 10)
}

func intLabel(v int) string {
	if v <= 0 {
		return "unknown"
	}
	return strconv.Itoa(v)
}

func normalizeLabel(v string) string {
	if v == "" {
		return "unknown"
	}
	return v
}

// IncHTTPRequest increments the HTTP request counter.
func IncHTTPRequest(method, path, code string) {
	HTTPRequestsTotal.WithLabelValues(method, path, code).Inc()
}

// ObserveHTTPDuration records HTTP request latency.
func ObserveHTTPDuration(method, path string, duration time.Duration) {
	HTTPRequestDuration.WithLabelValues(method, path).Observe(duration.Seconds())
}
