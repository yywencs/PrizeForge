package job

import (
	"big-market-kratos/internal/biz/activity"
	"big-market-kratos/pkg/logger"
	"context"
	"encoding/json"
	"fmt"

	"github.com/hibiken/asynq"
)

type ActivitySkuStockConsumeJob struct {
	activityService *activity.ActivityQuotaUsecase
}

func NewActivitySkuStockConsumeJob(activityService *activity.ActivityQuotaUsecase) *ActivitySkuStockConsumeJob {
	return &ActivitySkuStockConsumeJob{
		activityService: activityService,
	}
}

func (j *ActivitySkuStockConsumeJob) ProcessTask(ctx context.Context, task *asynq.Task) error {
	// 1. 获取队列消息
	var skuStockKey activity.ActivitySkuStockKey
	if err := json.Unmarshal(task.Payload(), &skuStockKey); err != nil {
		return fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	// 2. 更新库存
	if err := j.activityService.UpdateActivitySkuStock(ctx, skuStockKey.Sku); err != nil {
		return fmt.Errorf("failed to update activity sku stock: %w", err)
	}

	logger.Info("ActivitySkuStockConsumeJob success", "sku", skuStockKey.Sku)
	return nil
}
