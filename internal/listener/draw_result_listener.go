package listener

import (
	"context"
	"encoding/json"
	"fmt"
	"prizeforge/internal/domain/activity"
	"prizeforge/pkg/rabbitmq"
)

// DrawResultListener 消费 Redis-first 完整抽奖结果。
// 业务服务在一个 MySQL 事务中保存订单、额度、中奖记录和后续发奖 Outbox。
type drawResultPersistenceService interface {
	SaveDrawResult(context.Context, *activity.DrawResult) error
}

type DrawResultListener struct {
	partakeSvc drawResultPersistenceService
}

func NewDrawResultListener(partakeSvc drawResultPersistenceService) *DrawResultListener {
	return &DrawResultListener{partakeSvc: partakeSvc}
}

func (l *DrawResultListener) Handle(ctx context.Context, body []byte) (retry bool, err error) {
	var event rabbitmq.BaseEvent
	if err := json.Unmarshal(body, &event); err != nil {
		return false, fmt.Errorf("解析抽奖结果信封失败: %w", err)
	}
	data, err := json.Marshal(event.Data)
	if err != nil {
		return false, fmt.Errorf("序列化抽奖结果失败: %w", err)
	}
	var result activity.DrawResult
	if err := json.Unmarshal(data, &result); err != nil {
		return false, fmt.Errorf("解析抽奖结果失败: %w", err)
	}
	if result.UserID == "" || result.ActivityID <= 0 || result.OrderID == "" ||
		result.RequestID == "" || result.StrategyID <= 0 || result.OrderTime.IsZero() ||
		result.AwardID <= 0 || result.AwardTime.IsZero() {
		return false, activity.ErrInvalidParams
	}
	if event.ID != "draw:"+result.UserID+":"+result.OrderID {
		return false, fmt.Errorf("抽奖结果消息ID与载荷不一致")
	}
	if err := l.partakeSvc.SaveDrawResult(ctx, &result); err != nil {
		return true, err
	}
	return false, nil
}
