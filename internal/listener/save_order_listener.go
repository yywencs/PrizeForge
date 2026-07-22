package listener

import (
	"context"
	"encoding/json"
	"fmt"

	"prizeforge/internal/domain/activity"
	"prizeforge/pkg/logger"
	"prizeforge/pkg/rabbitmq"
)

// SaveOrderListener 监听保存订单消息。
//
// Redis 原子预占抽奖额度并创建轻量订单后，系统通过 RabbitMQ fanout exchange
// "save_order_record" 广播数据库额度同步事件。
// 本 Listener 消费该事件，调用 ActivityPartakeUsecase.SaveOrderRecord
// 将订单对应的总、月、日额度扣减同步到数据库，并幂等更新 account_sync_state。
//
// 消息体为 rabbitmq.BaseEvent 信封，Data 字段为 CreatePartakeOrder 聚合。
type SaveOrderListener struct {
	partakeSvc *activity.ActivityPartakeUsecase
}

// NewSaveOrderListener 创建 SaveOrderListener。
func NewSaveOrderListener(partakeSvc *activity.ActivityPartakeUsecase) *SaveOrderListener {
	return &SaveOrderListener{
		partakeSvc: partakeSvc,
	}
}

// Handle 处理保存订单消息。
//
// 消息体经过两层反序列化：先解 BaseEvent 信封，再将其 Data 字段
// 重新 Marshal/Unmarshal 为 CreatePartakeOrder 聚合对象。
func (l *SaveOrderListener) Handle(ctx context.Context, body []byte) (retry bool, err error) {
	// 1. 解析 RabbitMQ 信封
	var req rabbitmq.BaseEvent
	if err := json.Unmarshal(body, &req); err != nil {
		logger.Error("SaveOrderListener 解析信封失败", "err", err)
		return false, fmt.Errorf("解析 BaseEvent 失败: %w", err)
	}

	// 2. Data 字段是 interface{}，需要重新 Marshal 再 Unmarshal 到具体类型
	dataBytes, err := json.Marshal(req.Data)
	if err != nil {
		logger.Error("SaveOrderListener Marshal Data 失败", "err", err)
		return false, fmt.Errorf("序列化 Data 失败: %w", err)
	}

	var aggregate activity.CreatePartakeOrder
	if err := json.Unmarshal(dataBytes, &aggregate); err != nil {
		logger.Error("SaveOrderListener 解析 CreatePartakeOrder 失败", "err", err)
		return false, fmt.Errorf("解析 CreatePartakeOrder 失败: %w", err)
	}

	// 3. 委托领域服务执行订单持久化
	if err := l.partakeSvc.SaveOrderRecord(ctx, &aggregate); err != nil {
		logger.Error("SaveOrderListener SaveOrderRecord 失败", "err", err)
		return true, err // 可重试
	}

	logger.Info("SaveOrderListener 处理成功", "userID", aggregate.UserID, "activityID", aggregate.ActivityID)
	return false, nil
}
