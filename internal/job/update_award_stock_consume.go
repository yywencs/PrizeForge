package job

import (
	"big-market-kratos/internal/biz/strategy"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/hibiken/asynq"
)

type StrategyAwardStockConsumeJob struct {
	service *strategy.StrategyUsecase
}

func NewStrategyAwardStockConsumeJob(service *strategy.StrategyUsecase) *StrategyAwardStockConsumeJob {
	return &StrategyAwardStockConsumeJob{
		service: service,
	}
}

// ProcessTask 更新策略奖品库存
func (j *StrategyAwardStockConsumeJob) ProcessTask(ctx context.Context, task *asynq.Task) error {
	var msg strategy.AwardStockConsumeMessage
	if err := json.Unmarshal(task.Payload(), &msg); err != nil {
		return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
	}

	if err := j.service.UpdateStrategyAwardStock(ctx, msg.StrategyID, msg.AwardID); err != nil {
		slog.Error("UpdateStrategyAwardStock failed", "strategyID", msg.StrategyID, "awardID", msg.AwardID, "err", err)
		return err
	}

	slog.Info("UpdateStrategyAwardStock success", "strategyID", msg.StrategyID, "awardID", msg.AwardID)
	return nil
}
