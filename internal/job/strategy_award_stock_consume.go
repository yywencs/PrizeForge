package job

import (
	"context"
	"encoding/json"
	"fmt"

	"prizeforge/internal/domain/strategy"
	"prizeforge/pkg/logger"

	"github.com/hibiken/asynq"
)

// StrategyAwardStockConsumeJob 消费策略奖品库存任务。
//
// 当抽奖决策树 ruleStockNode 扣减奖品库存成功后，会投递一条
// strategy:award_stock_consume 任务。本 Job 负责消费该任务，
// 调用 StrategyUsecase.UpdateStrategyAwardStock 将 Redis
// 中的库存扣减结果同步到数据库。
//
// Payload 格式为 AwardStockConsumeMessage JSON：
//
//	{"strategy_id": 100001, "award_id": 101}
type StrategyAwardStockConsumeJob struct {
	svc *strategy.StrategyUsecase
}

// NewStrategyAwardStockConsumeJob 创建 StrategyAwardStockConsumeJob。
func NewStrategyAwardStockConsumeJob(svc *strategy.StrategyUsecase) *StrategyAwardStockConsumeJob {
	return &StrategyAwardStockConsumeJob{
		svc: svc,
	}
}

// ProcessTask 处理策略奖品库存消费任务。
//
// Payload 解析失败时返回 asynq.SkipRetry（数据格式错误，重试无意义）。
// 领域服务调用失败时返回普通 error（Asynq 会自动重试）。
func (j *StrategyAwardStockConsumeJob) ProcessTask(ctx context.Context, task *asynq.Task) error {
	var msg strategy.AwardStockConsumeMessage
	if err := json.Unmarshal(task.Payload(), &msg); err != nil {
		// Payload 损坏，跳过重试避免死循环
		return fmt.Errorf("解析 AwardStockConsumeMessage 失败: %v: %w", err, asynq.SkipRetry)
	}

	if err := j.svc.UpdateStrategyAwardStock(ctx, msg.StrategyID, msg.AwardID); err != nil {
		logger.Error("UpdateStrategyAwardStock 失败", "strategyID", msg.StrategyID, "awardID", msg.AwardID, "err", err)
		return err
	}

	logger.Info("UpdateStrategyAwardStock 成功", "strategyID", msg.StrategyID, "awardID", msg.AwardID)
	return nil
}
