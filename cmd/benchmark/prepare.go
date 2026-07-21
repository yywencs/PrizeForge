package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"prizeforge/internal/infrastructure/adapter"

	_ "github.com/go-sql-driver/mysql"
	redis "github.com/redis/go-redis/v9"
)

type prepareConfig struct {
	MySQLDSN      string
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	AdminURL      string
	ActivityID    int64
	Users         int
	Quota         int
	UserPrefix    string
	DBCount       int
	TableCount    int
	BatchSize     int
	Timeout       time.Duration
	SkipArmory    bool
	ConfirmReset  bool
}

type prepareReport struct {
	StrategyID    int64
	AwardCount    int
	AwardStock    int64
	UsersByShard  []int
	PreparedUsers int
}

func runPrepareCommand(args []string, stdout, stderr io.Writer) error {
	config, err := parsePrepareConfig(args, stderr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), config.Timeout)
	defer cancel()
	report, err := prepareBenchmarkData(ctx, config)
	if err != nil {
		return err
	}

	fmt.Fprintln(stdout, "Benchmark data prepared")
	fmt.Fprintf(stdout, "  activity:        %d\n", config.ActivityID)
	fmt.Fprintf(stdout, "  strategy:        %d\n", report.StrategyID)
	fmt.Fprintf(stdout, "  users:           %d\n", report.PreparedUsers)
	fmt.Fprintf(stdout, "  quota per user:  %d\n", config.Quota)
	fmt.Fprintf(stdout, "  award kinds:     %d\n", report.AwardCount)
	fmt.Fprintf(stdout, "  stock per award: %d\n", report.AwardStock)
	for index, count := range report.UsersByShard {
		fmt.Fprintf(stdout, "  shard %02d users: %d\n", index+1, count)
	}
	if config.SkipArmory {
		fmt.Fprintln(stdout, "  armory:          skipped")
	} else {
		fmt.Fprintln(stdout, "  armory:          ready")
	}
	return nil
}

func parsePrepareConfig(args []string, output io.Writer) (prepareConfig, error) {
	config := prepareConfig{}
	flags := flag.NewFlagSet("benchmark prepare", flag.ContinueOnError)
	flags.SetOutput(output)

	flags.StringVar(&config.MySQLDSN, "mysql-dsn", os.Getenv("PRIZEFORGE_BENCHMARK_MYSQL_DSN"), "MySQL DSN 模板，数据库名需包含 %s")
	flags.StringVar(&config.RedisAddr, "redis-addr", envOrDefault("PRIZEFORGE_BENCHMARK_REDIS_ADDR", "127.0.0.1:6379"), "Redis 地址")
	flags.StringVar(&config.RedisPassword, "redis-password", os.Getenv("PRIZEFORGE_BENCHMARK_REDIS_PASSWORD"), "Redis 密码（推荐通过环境变量传入）")
	flags.IntVar(&config.RedisDB, "redis-db", 0, "Redis DB")
	flags.StringVar(&config.AdminURL, "admin-url", "http://127.0.0.1:8081", "Admin 服务根地址")
	flags.Int64Var(&config.ActivityID, "activity-id", 100301, "压测活动 ID")
	flags.IntVar(&config.Users, "users", 1000, "压测用户数量")
	flags.IntVar(&config.Quota, "quota", 20, "每个用户的总、日、月抽奖次数")
	flags.StringVar(&config.UserPrefix, "user-prefix", "benchmark-user", "压测用户 ID 前缀")
	flags.IntVar(&config.DBCount, "db-count", 2, "分库数量")
	flags.IntVar(&config.TableCount, "table-count", 4, "每个分库的分表数量")
	flags.IntVar(&config.BatchSize, "batch-size", 500, "单次批量写入用户数")
	flags.DurationVar(&config.Timeout, "timeout", 2*time.Minute, "整个数据准备过程超时")
	flags.BoolVar(&config.SkipArmory, "skip-armory", false, "跳过 Admin 策略装配调用")
	flags.BoolVar(&config.ConfirmReset, "confirm-reset", false, "确认重置指定压测用户、活动和策略库存")

	if err := flags.Parse(args); err != nil {
		return prepareConfig{}, err
	}
	if flags.NArg() != 0 {
		return prepareConfig{}, fmt.Errorf("不支持位置参数: %s", strings.Join(flags.Args(), " "))
	}
	if err := config.validate(); err != nil {
		return prepareConfig{}, err
	}
	return config, nil
}

func (c prepareConfig) validate() error {
	if !c.ConfirmReset {
		return errors.New("prepare 会修改压测数据，请添加 --confirm-reset")
	}
	if strings.Count(c.MySQLDSN, "%s") != 1 {
		return errors.New("mysql-dsn 必须包含且只能包含一个 %s 数据库后缀占位符")
	}
	if strings.TrimSpace(c.RedisAddr) == "" {
		return errors.New("redis-addr 不能为空")
	}
	if c.RedisDB < 0 {
		return errors.New("redis-db 不能小于 0")
	}
	if _, err := normalizedBaseURL(c.AdminURL); err != nil {
		return fmt.Errorf("admin-url: %w", err)
	}
	if c.ActivityID <= 0 || c.Users <= 0 || c.Quota <= 0 {
		return errors.New("activity-id、users 和 quota 必须大于 0")
	}
	if c.DBCount <= 0 || c.TableCount <= 0 {
		return errors.New("db-count 和 table-count 必须大于 0")
	}
	if c.BatchSize <= 0 || c.BatchSize > 5000 {
		return errors.New("batch-size 必须在 1 到 5000 之间")
	}
	if c.Timeout <= 0 {
		return errors.New("timeout 必须大于 0")
	}
	if strings.TrimSpace(c.UserPrefix) == "" {
		return errors.New("user-prefix 不能为空")
	}
	lastUserID := benchmarkUserID(c.UserPrefix, c.Users)
	if len(lastUserID) > 32 {
		return fmt.Errorf("生成的 user_id %q 超过数据库 varchar(32)", lastUserID)
	}
	maximumDraws := int64(c.Users) * int64(c.Quota)
	if maximumDraws > math.MaxInt32 {
		return fmt.Errorf("users * quota = %d，超过 MySQL int 上限", maximumDraws)
	}
	return nil
}

func prepareBenchmarkData(ctx context.Context, config prepareConfig) (prepareReport, error) {
	primary, err := openBenchmarkDB(ctx, resolveBenchmarkDSN(config.MySQLDSN, ""))
	if err != nil {
		return prepareReport{}, fmt.Errorf("连接主库: %w", err)
	}
	defer primary.Close()

	strategyID, awardIDs, awardStock, err := prepareActivityAndAwards(ctx, primary, config)
	if err != nil {
		return prepareReport{}, err
	}

	usersByShard := make([][]string, config.DBCount)
	for index := 1; index <= config.Users; index++ {
		userID := benchmarkUserID(config.UserPrefix, index)
		shardIndex := benchmarkShardIndex(userID, config.DBCount, config.TableCount)
		usersByShard[shardIndex] = append(usersByShard[shardIndex], userID)
	}

	shardCounts := make([]int, config.DBCount)
	for shardIndex, users := range usersByShard {
		databaseSuffix := fmt.Sprintf("_%02d", shardIndex+1)
		database, openErr := openBenchmarkDB(ctx, resolveBenchmarkDSN(config.MySQLDSN, databaseSuffix))
		if openErr != nil {
			return prepareReport{}, fmt.Errorf("连接分库 %s: %w", databaseSuffix, openErr)
		}
		seedErr := seedActivityAccounts(ctx, database, users, config)
		closeErr := database.Close()
		if seedErr != nil {
			return prepareReport{}, fmt.Errorf("准备分库 %s 用户额度: %w", databaseSuffix, seedErr)
		}
		if closeErr != nil {
			return prepareReport{}, fmt.Errorf("关闭分库 %s: %w", databaseSuffix, closeErr)
		}
		shardCounts[shardIndex] = len(users)
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:     config.RedisAddr,
		Password: config.RedisPassword,
		DB:       config.RedisDB,
	})
	defer redisClient.Close()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return prepareReport{}, fmt.Errorf("连接 Redis: %w", err)
	}
	if err := resetBenchmarkRedis(ctx, redisClient, config, strategyID, awardIDs, awardStock); err != nil {
		return prepareReport{}, err
	}

	if !config.SkipArmory {
		if err := assembleBenchmarkStrategy(ctx, config.AdminURL, strategyID); err != nil {
			return prepareReport{}, err
		}
	}

	return prepareReport{
		StrategyID:    strategyID,
		AwardCount:    len(awardIDs),
		AwardStock:    awardStock,
		UsersByShard:  shardCounts,
		PreparedUsers: config.Users,
	}, nil
}

func openBenchmarkDB(ctx context.Context, dsn string) (*sql.DB, error) {
	database, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	database.SetMaxOpenConns(4)
	database.SetMaxIdleConns(2)
	if err := database.PingContext(ctx); err != nil {
		_ = database.Close()
		return nil, err
	}
	return database, nil
}

func prepareActivityAndAwards(ctx context.Context, database *sql.DB, config prepareConfig) (int64, []int64, int64, error) {
	transaction, err := database.BeginTx(ctx, nil)
	if err != nil {
		return 0, nil, 0, fmt.Errorf("开始活动准备事务: %w", err)
	}
	defer transaction.Rollback()

	var strategyID int64
	if err := transaction.QueryRowContext(ctx,
		"SELECT strategy_id FROM raffle_activity WHERE activity_id = ? FOR UPDATE",
		config.ActivityID,
	).Scan(&strategyID); err != nil {
		return 0, nil, 0, fmt.Errorf("查询活动 %d: %w", config.ActivityID, err)
	}

	if _, err := transaction.ExecContext(ctx, `
		UPDATE raffle_activity
		SET state = 'open',
			begin_date_time = DATE_SUB(NOW(), INTERVAL 1 DAY),
			end_date_time = DATE_ADD(NOW(), INTERVAL 7 DAY),
			update_time = NOW()
		WHERE activity_id = ?`, config.ActivityID); err != nil {
		return 0, nil, 0, fmt.Errorf("开放压测活动: %w", err)
	}

	awardStock := int64(config.Users) * int64(config.Quota)
	_, err = transaction.ExecContext(ctx, `
		UPDATE strategy_award
		SET award_count = ?, award_count_surplus = ?, update_time = NOW()
		WHERE strategy_id = ?`, awardStock, awardStock, strategyID)
	if err != nil {
		return 0, nil, 0, fmt.Errorf("重置策略奖品库存: %w", err)
	}

	rows, err := transaction.QueryContext(ctx,
		"SELECT award_id FROM strategy_award WHERE strategy_id = ? ORDER BY award_id",
		strategyID,
	)
	if err != nil {
		return 0, nil, 0, fmt.Errorf("查询策略奖品: %w", err)
	}
	var awardIDs []int64
	for rows.Next() {
		var awardID int64
		if err := rows.Scan(&awardID); err != nil {
			rows.Close()
			return 0, nil, 0, fmt.Errorf("读取策略奖品: %w", err)
		}
		awardIDs = append(awardIDs, awardID)
	}
	if err := rows.Close(); err != nil {
		return 0, nil, 0, fmt.Errorf("关闭策略奖品结果集: %w", err)
	}
	if err := rows.Err(); err != nil {
		return 0, nil, 0, fmt.Errorf("遍历策略奖品: %w", err)
	}
	if len(awardIDs) == 0 {
		return 0, nil, 0, fmt.Errorf("策略 %d 奖品列表为空", strategyID)
	}
	if err := transaction.Commit(); err != nil {
		return 0, nil, 0, fmt.Errorf("提交活动准备事务: %w", err)
	}
	return strategyID, awardIDs, awardStock, nil
}

func seedActivityAccounts(ctx context.Context, database *sql.DB, users []string, config prepareConfig) error {
	if len(users) == 0 {
		return nil
	}
	transaction, err := database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer transaction.Rollback()

	day := time.Now().Format("2006-01-02")
	month := time.Now().Format("2006-01")
	for start := 0; start < len(users); start += config.BatchSize {
		end := min(start+config.BatchSize, len(users))
		batch := users[start:end]
		if err := upsertTotalAccounts(ctx, transaction, batch, config.ActivityID, config.Quota); err != nil {
			return err
		}
		if err := upsertDayAccounts(ctx, transaction, batch, config.ActivityID, config.Quota, day); err != nil {
			return err
		}
		if err := upsertMonthAccounts(ctx, transaction, batch, config.ActivityID, config.Quota, month); err != nil {
			return err
		}
	}
	return transaction.Commit()
}

func upsertTotalAccounts(ctx context.Context, transaction *sql.Tx, users []string, activityID int64, quota int) error {
	query := `INSERT INTO raffle_activity_account
		(user_id, activity_id, total_count, total_count_surplus, day_count, day_count_surplus, month_count, month_count_surplus, current_order_id)
		VALUES ` + repeatedValues(len(users), "(?, ?, ?, ?, ?, ?, ?, ?, '')") + `
		ON DUPLICATE KEY UPDATE
			total_count = VALUES(total_count), total_count_surplus = VALUES(total_count_surplus),
			day_count = VALUES(day_count), day_count_surplus = VALUES(day_count_surplus),
			month_count = VALUES(month_count), month_count_surplus = VALUES(month_count_surplus),
			current_order_id = '', update_time = CURRENT_TIMESTAMP`
	arguments := make([]any, 0, len(users)*8)
	for _, userID := range users {
		arguments = append(arguments, userID, activityID, quota, quota, quota, quota, quota, quota)
	}
	if _, err := transaction.ExecContext(ctx, query, arguments...); err != nil {
		return fmt.Errorf("写入总额度: %w", err)
	}
	return nil
}

func upsertDayAccounts(ctx context.Context, transaction *sql.Tx, users []string, activityID int64, quota int, day string) error {
	query := `INSERT INTO raffle_activity_account_day
		(user_id, activity_id, day, day_count, day_count_surplus)
		VALUES ` + repeatedValues(len(users), "(?, ?, ?, ?, ?)") + `
		ON DUPLICATE KEY UPDATE
			day_count = VALUES(day_count), day_count_surplus = VALUES(day_count_surplus),
			update_time = CURRENT_TIMESTAMP`
	arguments := make([]any, 0, len(users)*5)
	for _, userID := range users {
		arguments = append(arguments, userID, activityID, day, quota, quota)
	}
	if _, err := transaction.ExecContext(ctx, query, arguments...); err != nil {
		return fmt.Errorf("写入日额度: %w", err)
	}
	return nil
}

func upsertMonthAccounts(ctx context.Context, transaction *sql.Tx, users []string, activityID int64, quota int, month string) error {
	query := `INSERT INTO raffle_activity_account_month
		(user_id, activity_id, month, month_count, month_count_surplus)
		VALUES ` + repeatedValues(len(users), "(?, ?, ?, ?, ?)") + `
		ON DUPLICATE KEY UPDATE
			month_count = VALUES(month_count), month_count_surplus = VALUES(month_count_surplus),
			update_time = CURRENT_TIMESTAMP`
	arguments := make([]any, 0, len(users)*5)
	for _, userID := range users {
		arguments = append(arguments, userID, activityID, month, quota, quota)
	}
	if _, err := transaction.ExecContext(ctx, query, arguments...); err != nil {
		return fmt.Errorf("写入月额度: %w", err)
	}
	return nil
}

func resetBenchmarkRedis(
	ctx context.Context,
	client *redis.Client,
	config prepareConfig,
	strategyID int64,
	awardIDs []int64,
	awardStock int64,
) error {
	fixedKeys := []string{
		adapter.GetActivityKey(config.ActivityID),
		fmt.Sprintf("strategy_id_by_activity_%d", config.ActivityID),
		adapter.GetStrategyKey(strategyID),
		adapter.GetStrategyAwardKey(strategyID),
	}
	for _, awardID := range awardIDs {
		fixedKeys = append(fixedKeys, fmt.Sprintf("%s:%d", adapter.GetStrategyAwardKey(strategyID), awardID))
	}
	if err := client.Del(ctx, fixedKeys...).Err(); err != nil {
		return fmt.Errorf("清理活动和策略缓存: %w", err)
	}

	patterns := []string{
		adapter.GetStrategyRateRangeKey(strconv.FormatInt(strategyID, 10)) + "*",
		adapter.GetStrategyRateTableKey(strconv.FormatInt(strategyID, 10)) + "*",
		fmt.Sprintf("prizeforge_strategy_award_reservation_%s-*", config.UserPrefix),
	}
	for _, awardID := range awardIDs {
		patterns = append(patterns, adapter.GetStrategyAwardCountKey(strategyID, awardID)+"*")
	}
	for _, pattern := range patterns {
		if err := deleteRedisPattern(ctx, client, pattern); err != nil {
			return fmt.Errorf("清理 Redis pattern %q: %w", pattern, err)
		}
	}

	pipe := client.Pipeline()
	for _, awardID := range awardIDs {
		pipe.Set(ctx, adapter.GetStrategyAwardCountKey(strategyID, awardID), awardStock, 0)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("设置 Redis 奖品库存: %w", err)
	}

	day := time.Now().Format("2006-01-02")
	month := time.Now().Format("2006-01")
	for start := 1; start <= config.Users; start += config.BatchSize {
		end := min(start+config.BatchSize-1, config.Users)
		pipe = client.Pipeline()
		for index := start; index <= end; index++ {
			userID := benchmarkUserID(config.UserPrefix, index)
			pipe.Del(ctx,
				adapter.GetActivityAccountKey(config.ActivityID, userID),
				adapter.GetActivityAccountTotalSurplusKey(config.ActivityID, userID),
				adapter.GetActivityAccountDaySurplusKey(config.ActivityID, userID, day),
				adapter.GetActivityAccountMonthSurplusKey(config.ActivityID, userID, month),
				adapter.GetPendingRaffleOrderKey(config.ActivityID, userID),
			)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("清理用户额度缓存: %w", err)
		}
	}
	return nil
}

func deleteRedisPattern(ctx context.Context, client *redis.Client, pattern string) error {
	var cursor uint64
	for {
		keys, nextCursor, err := client.Scan(ctx, cursor, pattern, 500).Result()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			if err := client.Del(ctx, keys...).Err(); err != nil {
				return err
			}
		}
		cursor = nextCursor
		if cursor == 0 {
			return nil
		}
	}
}

func assembleBenchmarkStrategy(ctx context.Context, adminURL string, strategyID int64) error {
	baseURL, err := normalizedBaseURL(adminURL)
	if err != nil {
		return err
	}
	endpoint := fmt.Sprintf("%s/admin/v1/strategy/armory?strategy_id=%d", baseURL, strategyID)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return fmt.Errorf("创建策略装配请求: %w", err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return fmt.Errorf("调用策略装配接口: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("策略装配 HTTP 状态码: %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return fmt.Errorf("读取策略装配响应: %w", err)
	}
	if len(body) > maxResponseBytes {
		return fmt.Errorf("策略装配响应超过 %d 字节", maxResponseBytes)
	}
	var result struct {
		Code int `json:"code"`
		Data struct {
			Success bool `json:"success"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("解析策略装配响应: %w", err)
	}
	if result.Code != 0 || !result.Data.Success {
		return fmt.Errorf("策略装配业务失败: %s", strings.TrimSpace(string(body)))
	}
	return nil
}

func normalizedBaseURL(rawURL string) (string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("必须是有效的 HTTP(S) 地址: %q", rawURL)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("只支持 http 或 https: %q", rawURL)
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return "", fmt.Errorf("必须是没有 path、query 和 fragment 的服务根地址: %q", rawURL)
	}
	return strings.TrimRight(rawURL, "/"), nil
}

func benchmarkShardIndex(userID string, dbCount, tableCount int) int {
	slot := int64(crc32.ChecksumIEEE([]byte(userID))) % int64(dbCount*tableCount)
	return int(slot) / tableCount
}

func benchmarkUserID(prefix string, index int) string {
	return fmt.Sprintf("%s-%06d", prefix, index)
}

func repeatedValues(count int, value string) string {
	values := make([]string, count)
	for index := range values {
		values[index] = value
	}
	return strings.Join(values, ",")
}

func resolveBenchmarkDSN(template, suffix string) string {
	return strings.Replace(template, "%s", suffix, 1)
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
