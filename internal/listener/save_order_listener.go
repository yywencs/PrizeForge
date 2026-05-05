package listener

import (
	"big-market-kratos/internal/biz/activity"
	"big-market-kratos/pkg/logger"
	"big-market-kratos/pkg/rabbitmq"
	"context"
	"encoding/json"
	"fmt"
)

type SaveOrderListener struct {
	activityPartakeService *activity.ActivityPartakeUsecase
}

func NewSaveOrderListener(activityPartakeService *activity.ActivityPartakeUsecase) *SaveOrderListener {
	return &SaveOrderListener{
		activityPartakeService: activityPartakeService,
	}
}

func (l *SaveOrderListener) Handle(ctx context.Context, body []byte) (retry bool, err error) {
	var req rabbitmq.BaseEvent
	if err := json.Unmarshal(body, &req); err != nil {
		logger.Error("SaveOrderListener Unmarshal message failed", "err", err)
		return false, fmt.Errorf("unmarshal failed: %w", err)
	}

	// 尝试将 Data 解析为 CreatePartakeOrder
	// 由于 Data 可能是 map[string]interface{}（由 JSON 反序列化而来），我们需要重新序列化再反序列化，或者直接反序列化 body 中的 Data 字段
	// 最好直接反序列化 Data 字段
	dataBytes, err := json.Marshal(req.Data)
	if err != nil {
		logger.Error("SaveOrderListener Marshal req.Data failed", "err", err)
		return false, fmt.Errorf("marshal data failed: %w", err)
	}

	var aggregate activity.CreatePartakeOrder
	if err := json.Unmarshal(dataBytes, &aggregate); err != nil {
		logger.Error("SaveOrderListener Unmarshal aggregate failed", "err", err)
		return false, fmt.Errorf("unmarshal aggregate failed: %w", err)
	}

	if err := l.activityPartakeService.SaveOrderRecord(ctx, &aggregate); err != nil {
		logger.Error("SaveOrderListener SaveOrderRecord failed", "err", err)
		return true, err // 发生错误重试
	}

	logger.Info("SaveOrderListener successfully processed order", "userID", aggregate.UserID, "activityID", aggregate.ActivityID)
	return false, nil
}
