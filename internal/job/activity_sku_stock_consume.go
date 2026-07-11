package job

import (
	"context"
	"encoding/json"
	"fmt"

	"prizeforge/internal/domain/activity"
	"prizeforge/pkg/logger"

	"github.com/hibiken/asynq"
)

// ActivitySkuStockConsumeJob 消费活动SKU库存任务。
//
// 当抽奖链路中 SKU 库存扣减到零时，会通过 Asynq 投递一条
// activity:sku_stock_consume 任务。本 Job 负责消费该任务，
// 调用 ActivityQuotaUsecase.UpdateActivitySkuStock 完成
// 数据库层面的库存同步（Redis 缓存已扣减，此处将最终结果持久化到 DB）。
type ActivitySkuStockConsumeJob struct {
	quotaSvc *activity.ActivityQuotaUsecase
}

// NewActivitySkuStockConsumeJob 创建 ActivitySkuStockConsumeJob。
func NewActivitySkuStockConsumeJob(quotaSvc *activity.ActivityQuotaUsecase) *ActivitySkuStockConsumeJob {
	return &ActivitySkuStockConsumeJob{
		quotaSvc: quotaSvc,
	}
}

// ProcessTask 是 asynq.HandlerFunc 的实现，由 Asynq Worker 回调。
//
// 入参 task.Payload() 为 ActivitySkuStockKey 的 JSON，包含 Sku 和 ActivityID。
// 解析后委托领域服务更新库存。
func (j *ActivitySkuStockConsumeJob) ProcessTask(ctx context.Context, task *asynq.Task) error {
	var skuStockKey activity.ActivitySkuStockKey
	if err := json.Unmarshal(task.Payload(), &skuStockKey); err != nil {
		return fmt.Errorf("解析 ActivitySkuStockKey 失败: %w", err)
	}

	if err := j.quotaSvc.UpdateActivitySkuStock(ctx, skuStockKey.Sku); err != nil {
		return fmt.Errorf("更新活动SKU库存失败: %w", err)
	}

	logger.Info("ActivitySkuStockConsumeJob 处理成功", "sku", skuStockKey.Sku)
	return nil
}
