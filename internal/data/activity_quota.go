package data

import (
	"big-market-kratos/internal/biz/activity"
	"big-market-kratos/internal/data/po"
	"big-market-kratos/pkg/cache"
	"big-market-kratos/pkg/common"
	"big-market-kratos/pkg/rabbitmq"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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
		TTL:   10 * time.Minute,
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
		TTL:   10 * time.Minute,
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
		TTL:   10 * time.Minute,
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
		slog.Info("库存预热成功", "cacheKey", cacheKey, "stockCount", stockCount)
	} else {
		slog.Info("库存预热失败，key已存在", "cacheKey", cacheKey)
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

func (d *Repository) SubtractionActivitySkuStock(ctx context.Context, skuID int64, endTime time.Time) (bool, error) {
	cacheKey := GetActivitySkuStockCountKey(skuID)

	surplus, err := d.redis.Decr(ctx, cacheKey)
	if err != nil {
		return false, err
	}

	if surplus == 0 {
		// 发送MQ消息
		stockZeroEvent := rabbitmq.NewBaseEvent(skuID)
		if err := d.stockZeroPublisher.PublishStockZero(ctx, stockZeroEvent); err != nil {
			return false, err
		}
	} else if surplus < 0 {
		// 库存不足，回滚
		if _, err := d.redis.Incr(ctx, cacheKey); err != nil {
			return false, err
		}
		return false, nil
	}

	lockKey := cacheKey + common.UNDERLINE + strconv.FormatInt(surplus, 10)
	expiredMillis := time.Until(endTime) + 24*time.Hour
	lock, err := d.redis.SetNX(ctx, lockKey, "1", time.Duration(expiredMillis)*time.Millisecond)
	if err != nil {
		return false, err
	}
	if !lock {
		slog.Info("活动sku库存加锁失败", "lockKey", lockKey)
		d.redis.Incr(ctx, cacheKey)
		return false, nil
	}

	return true, nil
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

	slog.Info("ActivitySkuStockConsumeSendQueue", "taskId", info.ID, "queue", info.Queue)
	return nil
}

// TakeQueueValue 消费活动库存队列消息
func (d *Repository) TakeQueueValue(ctx context.Context, task *asynq.Task) (*activity.ActivitySkuStockKey, error) {
	var skuStockKey activity.ActivitySkuStockKey
	if err := json.Unmarshal(task.Payload(), &skuStockKey); err != nil {
		return nil, fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
	}

	return &skuStockKey, nil
}

func (d *Repository) UpdateActivitySkuStock(ctx context.Context, sku int64) error {
	// 更新数据库库存
	err := d.db.Model(&po.RaffleActivitySku{}).
		Where("sku = ? AND stock_count_surplus > 0", sku).
		Update("stock_count_surplus", gorm.Expr("stock_count_surplus - 1")).Error

	if err != nil {
		slog.Error("UpdateActivitySkuStock failed", "sku", sku, "err", err)
		return err
	}

	slog.Info("UpdateActivitySkuStock success", "sku", sku)
	return nil
}
