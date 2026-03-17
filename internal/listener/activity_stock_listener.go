package listener

import (
	"big-market-kratos/internal/biz/activity"
	"big-market-kratos/pkg/rabbitmq"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
)

type ActivityStockListener struct {
	activityService *activity.ActivityQuotaUsecase
}

func NewActivityStockListener(activityService *activity.ActivityQuotaUsecase) *ActivityStockListener {
	return &ActivityStockListener{
		activityService: activityService,
	}
}

func (l *ActivityStockListener) Handle(ctx context.Context, body []byte) (retry bool, err error) {
	var req rabbitmq.BaseEvent
	if err := json.Unmarshal(body, &req); err != nil {
		slog.Error("Unmarshal message failed", "err", err)
		return false, fmt.Errorf("unmarshal failed: %w", err)
	}

	var sku int64
	switch v := req.Data.(type) {
	case float64:
		sku = int64(v)
	case int64:
		sku = v
	default:
		slog.Error("Invalid data type for sku", "type", fmt.Sprintf("%T", req.Data))
		return false, fmt.Errorf("invalid data type for sku: %T", req.Data)
	}

	if err := l.activityService.ClearActivitySkuStock(ctx, sku); err != nil {
		slog.Error("ClearActivitySkuStock failed", "err", err)
		return true, err
	}

	if err := l.activityService.ClearQueueValue(ctx); err != nil {
		slog.Error("ClearQueueValue failed", "err", err)
		return true, err
	}

	return false, nil
}
