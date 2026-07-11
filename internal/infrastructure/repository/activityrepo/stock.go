package activityrepo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"prizeforge/internal/domain/activity"
	"prizeforge/internal/infrastructure/adapter"
	"prizeforge/internal/infrastructure/repository/po"
	"prizeforge/internal/metrics"
	"prizeforge/pkg/logger"
	"prizeforge/pkg/rabbitmq"
	"strconv"
	"strings"
	"time"

	"github.com/hibiken/asynq"
	"gorm.io/gorm"
)

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
	stockKey := adapter.GetActivitySkuStockCountKey(skuID)

	const shardCount = 100
	shard := 0
	for _, c := range userID {
		shard = (shard*31 + int(c)) % shardCount
	}
	resultKey := adapter.GetActivityResultHashKey(activityID, shard)

	script := `
		local stock_key = KEYS[1]
		local result_key = KEYS[2]
		local user_id = ARGV[1]
		local sku_id = ARGV[2]
		local current_time = ARGV[3]
		local points_result = ARGV[4]

		local current_stock = redis.call('GET', stock_key)
		if not current_stock then
			return {-1, "库存未初始化"}
		end

		current_stock = tonumber(current_stock)
		if current_stock <= 0 then
			local result_json = string.format('{"u":"%s","s":2,"r":"%s","t":%s}', user_id, points_result, current_time)
			redis.call('HSET', result_key, user_id, result_json)
			return {2, result_json}
		end

		local new_stock = redis.call('DECR', stock_key)

		local result_json = string.format('{"u":"%s","s":1,"r":"SKU_%s","t":%s}', user_id, sku_id, current_time)
		redis.call('HSET', result_key, user_id, result_json)

		if new_stock == 0 then
			return {0, result_json}
		end

		return {1, result_json}
	`

	pointsResult := fmt.Sprintf("%s_%d", activity.ActivityResultPointsPrefix, 100)

	result, err := d.redis.Eval(ctx, script, []string{stockKey, resultKey}, userID, strconv.FormatInt(skuID, 10), strconv.FormatInt(time.Now().Unix(), 10), pointsResult)
	if err != nil {
		metrics.IncStockConsume(activityID, skuID, "error")
		return nil, err
	}

	resultArray := result.([]interface{})
	status := resultArray[0].(int64)
	resultJSON := resultArray[1].(string)

	var activityResult activity.ActivityResult
	if err := json.Unmarshal([]byte(resultJSON), &activityResult); err != nil {
		metrics.IncStockConsume(activityID, skuID, "error")
		return nil, err
	}

	switch status {
	case -1:
		metrics.IncStockConsume(activityID, skuID, "not_initialized")
		return nil, errors.New("库存未初始化")
	case 0:
		stockZeroEvent := rabbitmq.NewBaseEvent(skuID)
		if err := d.stockZeroPublisher.PublishStockZero(ctx, stockZeroEvent); err != nil {
			logger.Error("发送库存耗尽MQ消息失败", "skuID", skuID, "error", err)
		}
		metrics.IncStockConsume(activityID, skuID, "success")
		return &activityResult, nil
	case 1:
		metrics.IncStockConsume(activityID, skuID, "success")
		return &activityResult, nil
	case 2:
		metrics.IncStockConsume(activityID, skuID, "credit")
		return &activityResult, nil
	default:
		metrics.IncStockConsume(activityID, skuID, "unknown")
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
