package data

import (
	"big-market-kratos/internal/biz/activity"
	"big-market-kratos/internal/data/po"
	"big-market-kratos/pkg/cache"
	"big-market-kratos/pkg/logger"
	"big-market-kratos/pkg/rabbitmq"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/hibiken/asynq"
	"gorm.io/gorm"
)

type Repository struct {
	routerDB           *DBRouter
	db                 *gorm.DB
	redis              *cache.Cache
	stockZeroPublisher *Publisher
	queue              *asynq.Client
	inspector          *asynq.Inspector
}

// NewRepository 构造活动仓储实现
func NewActivityRepository(routerDB *DBRouter, db *gorm.DB, redis *cache.Cache, stockZeroPublisher *Publisher, queue *asynq.Client, inspector *asynq.Inspector) activity.Repo {
	return &Repository{
		routerDB:           routerDB,
		db:                 db,
		redis:              redis,
		stockZeroPublisher: stockZeroPublisher,
		queue:              queue,
		inspector:          inspector,
	}
}

// QueryActivitySkuByActivityID 根据活动ID查询活动商品配置数量
func (d *Repository) QueryActivitySkuByActivityID(ctx context.Context, activityID int64) ([]*activity.ActivitySku, error) {
	var activitySkus []*po.RaffleActivitySku
	err := d.db.WithContext(ctx).
		Model(&po.RaffleActivitySku{}).
		Where("activity_id = ?", activityID).
		Find(&activitySkus).Error
	if err != nil {
		return nil, err
	}
	var activitySkusResult []*activity.ActivitySku
	for _, activitySku := range activitySkus {
		activitySkusResult = append(activitySkusResult, activitySku.ToEntity())
	}
	return activitySkusResult, nil
}

// QueryActivitySku 根据 sku 查询活动商品配置
func (d *Repository) QueryActivitySku(ctx context.Context, sku int64) (*activity.ActivitySku, error) {
	var activitySku activity.ActivitySku

	cacheKey := GetActivitySkuKey(sku)
	err := d.redis.Once(&cache.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &activitySku,
		TTL:   10 * 24 * time.Hour,
		Do: func(*cache.Item) (interface{}, error) {
			var dbResult po.RaffleActivitySku
			err := d.db.WithContext(ctx).
				Where("sku = ?", sku).
				First(&dbResult).Error
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return nil, nil
				}
				return nil, err
			}

			return dbResult.ToEntity(), nil
		},
	})
	if err != nil {
		return nil, err
	}
	if activitySku.Sku == 0 {
		return nil, nil
	}
	return &activitySku, nil
}

// QueryRaffleActivityByActivityId 根据活动ID查询活动配置
func (d *Repository) QueryRaffleActivityByActivityId(ctx context.Context, activityID int64) (*activity.Activity, error) {
	var activity activity.Activity

	cacheKey := GetActivityKey(activityID)
	err := d.redis.Once(&cache.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &activity,
		TTL:   10 * 24 * time.Hour,
		Do: func(*cache.Item) (interface{}, error) {
			var dbResult po.RaffleActivity
			err := d.db.WithContext(ctx).
				Where("activity_id = ?", activityID).
				First(&dbResult).Error
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return nil, nil
				}
				return nil, err
			}

			return dbResult.ToEntity(), nil
		},
	})
	if err != nil {
		return nil, err
	}
	if activity.ActivityID == 0 {
		return nil, nil
	}
	return &activity, nil
}

// QueryRaffleActivityCountByActivityCountId 根据次数配置ID查询活动次数配置
func (d *Repository) QueryRaffleActivityCountByActivityCountId(ctx context.Context, activityCountID int64) (*activity.ActivityCount, error) {
	var activityCount activity.ActivityCount

	cacheKey := GetActivityCountKey(activityCountID)
	err := d.redis.Once(&cache.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &activityCount,
		TTL:   10 * 24 * time.Hour,
		Do: func(*cache.Item) (interface{}, error) {
			var dbResult po.RaffleActivityCount
			err := d.db.WithContext(ctx).
				Where("activity_count_id = ?", activityCountID).
				First(&dbResult).Error
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return nil, nil
				}
				return nil, err
			}

			return dbResult.ToEntity(), nil
		},
	})
	if err != nil {
		return nil, err
	}
	if activityCount.ActivityCountID == 0 {
		return nil, nil
	}
	return &activityCount, nil
}

func (d *Repository) SaveOrder(ctx context.Context, activityOrderAggregate *activity.CreateQuotaOrder) error {
	// 1. 获取 DB 和 分表后缀
	db, tableSuffix := d.routerDB.DBStrategy(activityOrderAggregate.UserID)
	if db == nil {
		return activity.ErrDBRouterError
	}

	// 2. 转换对象 PO
	// 2.1 订单对象
	activityOrderEntity := activityOrderAggregate.ActivityOrder
	raffleActivityOrder := &po.RaffleActivityOrder{
		UserID:        activityOrderEntity.UserID,
		Sku:           activityOrderEntity.Sku,
		ActivityID:    activityOrderEntity.ActivityID,
		ActivityName:  activityOrderEntity.ActivityName,
		StrategyID:    activityOrderEntity.StrategyID,
		OrderID:       activityOrderEntity.OrderID,
		OrderTime:     activityOrderEntity.OrderTime,
		TotalCount:    activityOrderEntity.TotalCount,
		DayCount:      activityOrderEntity.DayCount,
		MonthCount:    activityOrderEntity.MonthCount,
		State:         activityOrderEntity.State,
		OutBusinessNo: activityOrderEntity.OutBusinessNo,
	}
	// 2.2 账户对象
	raffleActivityAccount := &po.RaffleActivityAccount{
		UserID:            activityOrderAggregate.UserID,
		ActivityID:        activityOrderAggregate.ActivityID,
		TotalCount:        activityOrderAggregate.TotalCount,
		TotalCountSurplus: activityOrderAggregate.TotalCount,
		DayCount:          activityOrderAggregate.DayCount,
		DayCountSurplus:   activityOrderAggregate.DayCount,
		MonthCount:        activityOrderAggregate.MonthCount,
		MonthCountSurplus: activityOrderAggregate.MonthCount,
	}

	// 2.3 账户对象 - 日
	raffleActivityAccountDay := &po.RaffleActivityAccountDay{
		UserID:          activityOrderAggregate.UserID,
		ActivityID:      activityOrderAggregate.ActivityID,
		Day:             activityOrderAggregate.ActivityOrder.OrderTime.Format("2006-01-02"),
		DayCount:        activityOrderAggregate.DayCount,
		DayCountSurplus: activityOrderAggregate.DayCount,
	}

	// 2.4 账户对象 - 月
	raffleActivityAccountMonth := &po.RaffleActivityAccountMonth{
		UserID:            activityOrderAggregate.UserID,
		ActivityID:        activityOrderAggregate.ActivityID,
		Month:             activityOrderAggregate.ActivityOrder.OrderTime.Format("2006-01"),
		MonthCount:        activityOrderAggregate.MonthCount,
		MonthCountSurplus: activityOrderAggregate.MonthCount,
	}

	// 3. 执行事务
	return db.Transaction(func(tx *gorm.DB) error {
		// 3.1 写入订单
		// 指定表名
		if err := tx.Table("raffle_activity_order_" + tableSuffix).Create(raffleActivityOrder).Error; err != nil {
			// 唯一索引冲突
			if errors.Is(err, gorm.ErrDuplicatedKey) {
				return activity.ErrDBIndexDuplicate
			}
			return err
		}

		// 3.2 更新账户
		// gorm update 更新时，如果 rows affected 为 0，不会报错，需要我们自己判断
		// Update raffle_activity_account set total_count = total_count + ?, ... where user_id = ? and activity_id = ?
		res := tx.Table("raffle_activity_account").
			Where("user_id = ? AND activity_id = ?", raffleActivityAccount.UserID, raffleActivityAccount.ActivityID).
			Updates(map[string]interface{}{
				"total_count":         gorm.Expr("total_count + ?", raffleActivityAccount.TotalCount),
				"total_count_surplus": gorm.Expr("total_count_surplus + ?", raffleActivityAccount.TotalCountSurplus),
				"day_count":           gorm.Expr("day_count + ?", raffleActivityAccount.DayCount),
				"day_count_surplus":   gorm.Expr("day_count_surplus + ?", raffleActivityAccount.DayCountSurplus),
				"month_count":         gorm.Expr("month_count + ?", raffleActivityAccount.MonthCount),
				"month_count_surplus": gorm.Expr("month_count_surplus + ?", raffleActivityAccount.MonthCountSurplus),
				"update_time":         time.Now(),
			})
		if res.Error != nil {
			return res.Error
		}

		// 3.3 创建账户 - 更新为0，则账户不存在，创建新账户
		if res.RowsAffected == 0 {
			if err := tx.Table("raffle_activity_account").Create(raffleActivityAccount).Error; err != nil {
				// 理论上这里也有可能并发冲突，但因为前面 update 没命中，说明大概率是新用户
				// 如果这里冲突了，说明就在刚才那一瞬间有人创建了，那么重试或者报错都可以
				if errors.Is(err, gorm.ErrDuplicatedKey) {
					// 兜底：如果 insert 失败是因为唯一键冲突，说明刚才那一瞬间有并发创建，
					// 此时我们应该再次尝试 update 或者直接返回错误让上层重试
					return activity.ErrDBIndexDuplicate
				}
				return err
			}
		}

		// 3.4 更新账户 - 日
		if raffleActivityAccountDay.DayCount != 0 {
			resDay := tx.Table("raffle_activity_account_day").
				Where("user_id = ? AND activity_id = ? AND day = ?", raffleActivityAccountDay.UserID, raffleActivityAccountDay.ActivityID, raffleActivityAccountDay.Day).
				Updates(map[string]interface{}{
					"day_count":         gorm.Expr("day_count + ?", raffleActivityAccountDay.DayCount),
					"day_count_surplus": gorm.Expr("day_count_surplus + ?", raffleActivityAccountDay.DayCountSurplus),
					"update_time":       time.Now(),
				})
			if resDay.Error != nil {
				return resDay.Error
			}
			if resDay.RowsAffected == 0 {
				if err := tx.Table("raffle_activity_account_day").Create(raffleActivityAccountDay).Error; err != nil {
					if errors.Is(err, gorm.ErrDuplicatedKey) {
						return activity.ErrDBIndexDuplicate
					}
					return err
				}
			}
		}

		// 3.5 更新账户 - 月
		if raffleActivityAccountMonth.MonthCount != 0 {
			resMonth := tx.Table("raffle_activity_account_month").
				Where("user_id = ? AND activity_id = ? AND month = ?", raffleActivityAccountMonth.UserID, raffleActivityAccountMonth.ActivityID, raffleActivityAccountMonth.Month).
				Updates(map[string]interface{}{
					"month_count":         gorm.Expr("month_count + ?", raffleActivityAccountMonth.MonthCount),
					"month_count_surplus": gorm.Expr("month_count_surplus + ?", raffleActivityAccountMonth.MonthCountSurplus),
					"update_time":         time.Now(),
				})
			if resMonth.Error != nil {
				return resMonth.Error
			}
			if resMonth.RowsAffected == 0 {
				if err := tx.Table("raffle_activity_account_month").Create(raffleActivityAccountMonth).Error; err != nil {
					if errors.Is(err, gorm.ErrDuplicatedKey) {
						return activity.ErrDBIndexDuplicate
					}
					return err
				}
			}
		}

		return nil
	})

}

func (d *Repository) CacheActivitySkuStockCount(ctx context.Context, cacheKey string, stockCount int) error {
	success, err := d.redis.SetNX(ctx, cacheKey, stockCount, 0)

	if err != nil {
		return err
	}

	if success {
		logger.Info("库存预热成功", "cacheKey", cacheKey, "stockCount", stockCount)
	} else {
		logger.Info("库存预热失败，key已存在", "cacheKey", cacheKey)
	}
	return nil
}

func (d *Repository) QueryActivityAccountEntity(ctx context.Context, userID string, activityID int64) (*activity.ActivityAccount, error) {
	// 1. 查询总账户
	var accountPO po.RaffleActivityAccount
	db, _ := d.routerDB.DBStrategy(userID)
	err := db.WithContext(ctx).Table("raffle_activity_account").
		Where("user_id = ? AND activity_id = ?", userID, activityID).
		First(&accountPO).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, activity.ErrRecordNotFound
		}
		return nil, err
	}

	// 2. 查询月账户
	var accountMonthPO po.RaffleActivityAccountMonth
	month := time.Now().Format("2006-01")
	err = db.WithContext(ctx).Table("raffle_activity_account_month").
		Where("user_id = ? AND activity_id = ? AND month = ?", userID, activityID, month).
		First(&accountMonthPO).Error

	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	// 3. 查询日账户
	var accountDayPO po.RaffleActivityAccountDay
	day := time.Now().Format("2006-01-02")
	err = db.WithContext(ctx).Table("raffle_activity_account_day").
		Where("user_id = ? AND activity_id = ? AND day = ?", userID, activityID, day).
		First(&accountDayPO).Error

	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	// 4. 组装实体
	activityAccount := &activity.ActivityAccount{
		UserID:            accountPO.UserID,
		ActivityID:        accountPO.ActivityID,
		TotalCount:        accountPO.TotalCount,
		TotalCountSurplus: accountPO.TotalCountSurplus,
		DayCount:          accountPO.DayCount,
		DayCountSurplus:   accountPO.DayCount,
		MonthCount:        accountPO.MonthCount,
		MonthCountSurplus: accountPO.MonthCount,
	}

	// 如果月账户存在，用月账户数据覆盖（通常分表数据更准确或实时）
	if accountMonthPO.ID > 0 {
		activityAccount.MonthCount = accountMonthPO.MonthCount
		activityAccount.MonthCountSurplus = accountMonthPO.MonthCountSurplus
	}

	// 如果日账户存在，用日账户数据覆盖
	if accountDayPO.ID > 0 {
		activityAccount.DayCount = accountDayPO.DayCount
		activityAccount.DayCountSurplus = accountDayPO.DayCountSurplus
	}

	return activityAccount, nil
}

func (d *Repository) AssembleActivityAccountByUserId(ctx context.Context, userID string, activityID int64) error {
	account, err := d.QueryActivityAccountEntity(ctx, userID, activityID)
	if err != nil {
		return err
	}
	return d.cacheActivityAccountSnapshot(ctx, account)
}

func (d *Repository) ClearActivitySkuStock(ctx context.Context, sku int64) error {
	err := d.db.WithContext(ctx).Table("raffle_activity_sku_stock").
		Where("sku = ?", sku).
		Update("stock_count_surplus", 0)
	if err != nil {
		return activity.ErrClearActivitySkuStockError
	}
	return nil
}

// ClearQueueValue 清除rabbitMQ队列
func (d *Repository) ClearQueueValue(ctx context.Context) error {
	if d.inspector == nil {
		return nil
	}
	err := d.inspector.DeleteQueue(activity.QueueNameSkuStock, true)
	if err != nil && !strings.Contains(err.Error(), "queue not found") {
		return err
	}
	return nil
}

func (d *Repository) SubtractionActivitySkuStock(ctx context.Context, skuID int64, activityID int64, userID string, endTime time.Time) (*activity.ActivityResult, error) {
	stockKey := GetActivitySkuStockCountKey(skuID)

	// 计算用户分片，避免bigkey问题
	const shardCount = 100
	shard := 0
	for _, c := range userID {
		shard = (shard*31 + int(c)) % shardCount
	}
	resultKey := GetActivityResultHashKey(activityID, shard)

	// Redis Lua脚本实现原子性库存扣减和结果存储
	script := `
		local stock_key = KEYS[1]
		local result_key = KEYS[2]
		local user_id = ARGV[1]
		local sku_id = ARGV[2]
		local current_time = ARGV[3]
		local points_result = ARGV[4]  -- 积分标识，如 "POINTS_100"
		
		-- 检查库存
		local current_stock = redis.call('GET', stock_key)
		if not current_stock then
			return {-1, "库存未初始化"}  -- 返回数组格式
		end
		
		current_stock = tonumber(current_stock)
		if current_stock <= 0 then
			-- 库存不足，返回积分结果
			local result_json = string.format('{"u":"%s","s":2,"r":"%s","t":%s}', user_id, points_result, current_time)
			redis.call('HSET', result_key, user_id, result_json)
			return {2, result_json}  -- 状态码2表示积分
		end
		
		-- 扣减库存
		local new_stock = redis.call('DECR', stock_key)
		
		-- 存储抽奖成功结果
		local result_json = string.format('{"u":"%s","s":1,"r":"SKU_%s","t":%s}', user_id, sku_id, current_time)
		redis.call('HSET', result_key, user_id, result_json)
		
		-- 如果库存刚好为0，返回特殊标记
		if new_stock == 0 then
			return {0, result_json}  -- 状态码0表示库存耗尽但成功
		end
		
		return {1, result_json}  -- 状态码1表示抽奖成功
	`

	// 生成积分标识，如 "POINTS_100"
	pointsResult := fmt.Sprintf("%s_%d", activity.ActivityResultPointsPrefix, 100) // 这里可以根据业务配置调整积分值

	result, err := d.redis.Eval(ctx, script, []string{stockKey, resultKey}, userID, strconv.FormatInt(skuID, 10), strconv.FormatInt(time.Now().Unix(), 10), pointsResult)
	if err != nil {
		return nil, err
	}

	// 解析Lua脚本返回的结果
	resultArray := result.([]interface{})
	status := resultArray[0].(int64)
	resultJSON := resultArray[1].(string)

	// 解析JSON结果
	var activityResult activity.ActivityResult
	if err := json.Unmarshal([]byte(resultJSON), &activityResult); err != nil {
		return nil, err
	}

	switch status {
	case -1:
		return nil, errors.New("库存未初始化")
	case 0:
		// 库存刚好耗尽，发送MQ消息
		stockZeroEvent := rabbitmq.NewBaseEvent(skuID)
		if err := d.stockZeroPublisher.PublishStockZero(ctx, stockZeroEvent); err != nil {
			logger.Error("发送库存耗尽MQ消息失败", "skuID", skuID, "error", err)
		}
		return &activityResult, nil
	case 1:
		return &activityResult, nil // 抽奖成功
	case 2:
		return &activityResult, nil // 返回积分
	default:
		return nil, errors.New("未知结果")
	}
}

func (d *Repository) ActivitySkuStockConsumeSendQueue(ctx context.Context, skuStockKey *activity.ActivitySkuStockKey) error {
	payload, err := json.Marshal(skuStockKey)
	if err != nil {
		return err
	}

	task := asynq.NewTask(activity.TaskTypeActivitySkuStockConsume, payload)
	info, err := d.queue.Enqueue(task, asynq.Queue(activity.QueueNameSkuStock), asynq.ProcessIn(3*time.Second))
	if err != nil {
		return err
	}

	logger.Info("ActivitySkuStockConsumeSendQueue", "taskId", info.ID, "queue", info.Queue)
	return nil
}

// TakeQueueValue 消费活动库存队列消息
func (d *Repository) TakeQueueValue(ctx context.Context, task *asynq.Task) (*activity.ActivitySkuStockKey, error) {
	var skuStockKey activity.ActivitySkuStockKey
	if err := json.Unmarshal(task.Payload(), &skuStockKey); err != nil {
		return nil, fmt.Errorf("json.Unmarshal failed: %w: %w: %w", err, activity.ErrActivitySkuStockKeyUnmarshal, asynq.SkipRetry)
	}

	return &skuStockKey, nil
}

func (d *Repository) UpdateActivitySkuStock(ctx context.Context, sku int64) error {
	// 更新数据库库存
	err := d.db.Model(&po.RaffleActivitySku{}).
		Where("sku = ? AND stock_count_surplus > 0", sku).
		Update("stock_count_surplus", gorm.Expr("stock_count_surplus - 1")).Error

	if err != nil {
		logger.Error("UpdateActivitySkuStock failed", "sku", sku, "err", err)
		return err
	}

	logger.Info("UpdateActivitySkuStock success", "sku", sku)
	return nil
}

// WARNING：为了测压测，会把所有用户的额度都装配到缓存中
func (d *Repository) AssembleActivityAccountByActivityId(ctx context.Context, activityID int64) error {
	// 使用一个脱离原始 HTTP 请求的 Background Context 来进行耗时较长的批量装配
	// 避免因为装配大量数据导致 HTTP 请求本身超时从而中止了整个装配过程
	assembleCtx := context.Background()

	dbCount := d.routerDB.GetDBCount()
	for i := 1; i <= dbCount; i++ {
		db := d.routerDB.GetDB(i)
		if db == nil {
			continue
		}

		var accounts []po.RaffleActivityAccount
		err := db.WithContext(assembleCtx).Table("raffle_activity_account").
			Where("activity_id = ?", activityID).
			Find(&accounts).Error
		if err != nil {
			logger.Error("AssembleActivityAccountByActivityId query failed", "db", i, "err", err)
			continue
		}

		for _, account := range accounts {
			// Query day and month accounts to assemble a full entity
			var accountMonthPO po.RaffleActivityAccountMonth
			month := time.Now().Format("2006-01")
			_ = db.WithContext(assembleCtx).Table("raffle_activity_account_month").
				Where("user_id = ? AND activity_id = ? AND month = ?", account.UserID, activityID, month).
				First(&accountMonthPO).Error

			var accountDayPO po.RaffleActivityAccountDay
			day := time.Now().Format("2006-01-02")
			_ = db.WithContext(assembleCtx).Table("raffle_activity_account_day").
				Where("user_id = ? AND activity_id = ? AND day = ?", account.UserID, activityID, day).
				First(&accountDayPO).Error

			activityAccount := &activity.ActivityAccount{
				UserID:            account.UserID,
				ActivityID:        account.ActivityID,
				TotalCount:        account.TotalCount,
				TotalCountSurplus: account.TotalCountSurplus,
				DayCount:          account.DayCount,
				DayCountSurplus:   account.DayCountSurplus,
				MonthCount:        account.MonthCount,
				MonthCountSurplus: account.MonthCountSurplus,
			}

			if accountMonthPO.ID > 0 {
				activityAccount.MonthCount = accountMonthPO.MonthCount
				activityAccount.MonthCountSurplus = accountMonthPO.MonthCountSurplus
			} else {
				// 如果没有月账户，使用总表中的配置
				activityAccount.MonthCount = account.MonthCount
				activityAccount.MonthCountSurplus = account.MonthCountSurplus
			}

			if accountDayPO.ID > 0 {
				activityAccount.DayCount = accountDayPO.DayCount
				activityAccount.DayCountSurplus = accountDayPO.DayCountSurplus
			} else {
				// 如果没有日账户，使用总表中的配置
				activityAccount.DayCount = account.DayCount
				activityAccount.DayCountSurplus = account.DayCountSurplus
			}

			_ = d.cacheActivityAccountSnapshot(assembleCtx, activityAccount)
		}
	}
	return nil
}

func (d *Repository) cacheActivityAccountSnapshot(ctx context.Context, activityAccount *activity.ActivityAccount) error {
	month := time.Now().Format("2006-01")
	day := time.Now().Format("2006-01-02")

	key := GetActivityAccountKey(activityAccount.ActivityID, activityAccount.UserID)
	if err := d.redis.Set(&cache.Item{
		Ctx:   ctx,
		Key:   key,
		Value: activityAccount,
		TTL:   time.Hour,
	}); err != nil {
		return err
	}

	totalKey := GetActivityAccountTotalSurplusKey(activityAccount.ActivityID, activityAccount.UserID)
	if err := d.redis.Set(&cache.Item{
		Ctx:   ctx,
		Key:   totalKey,
		Value: activityAccount.TotalCountSurplus,
		TTL:   time.Hour,
	}); err != nil {
		return err
	}

	monthKey := GetActivityAccountMonthSurplusKey(activityAccount.ActivityID, activityAccount.UserID, month)
	if err := d.redis.Set(&cache.Item{
		Ctx:   ctx,
		Key:   monthKey,
		Value: activityAccount.MonthCountSurplus,
		TTL:   time.Hour,
	}); err != nil {
		return err
	}

	dayKey := GetActivityAccountDaySurplusKey(activityAccount.ActivityID, activityAccount.UserID, day)
	if err := d.redis.Set(&cache.Item{
		Ctx:   ctx,
		Key:   dayKey,
		Value: activityAccount.DayCountSurplus,
		TTL:   time.Hour,
	}); err != nil {
		return err
	}

	return nil
}
