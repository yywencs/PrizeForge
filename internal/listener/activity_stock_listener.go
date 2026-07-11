package listener

import (
	"context"
	"encoding/json"
	"fmt"

	"prizeforge/internal/domain/activity"
	"prizeforge/pkg/logger"
	"prizeforge/pkg/rabbitmq"
)

// ActivityStockListener 监听活动SKU库存归零事件。
//
// 当抽奖链路中某个 SKU 库存被扣减到零时，Activity 领域会通过
// RabbitMQ fanout exchange "activity_sku_stock_zero_topic" 广播事件。
// 本 Listener 消费该事件，执行：
//  1. ClearActivitySkuStock — 将 DB 中该 SKU 的库存清零
//  2. ClearQueueValue       — 清理 Redis 中对应的库存缓存队列
//
// 注意：消息体是 rabbitmq.BaseEvent 信封格式，Data 字段为 sku 数值。
type ActivityStockListener struct {
	quotaSvc *activity.ActivityQuotaUsecase
}

// NewActivityStockListener 创建 ActivityStockListener。
func NewActivityStockListener(quotaSvc *activity.ActivityQuotaUsecase) *ActivityStockListener {
	return &ActivityStockListener{
		quotaSvc: quotaSvc,
	}
}

// Handle 处理库存归零消息。
//
// 返回值 (retry, error)：
//   - retry=true  → 消息重回队列，等待重新消费
//   - retry=false → 消息被 Reject 丢弃（或 Ack 确认）
func (l *ActivityStockListener) Handle(ctx context.Context, body []byte) (retry bool, err error) {
	// 1. 解析 RabbitMQ 信封
	var req rabbitmq.BaseEvent
	if err := json.Unmarshal(body, &req); err != nil {
		logger.Error("解析库存归零消息失败", "err", err)
		return false, fmt.Errorf("解析 BaseEvent 失败: %w", err)
	}

	// 2. 从 Data 字段提取 sku（JSON 反序列化后数值可能是 float64 或 int64）
	var sku int64
	switch v := req.Data.(type) {
	case float64:
		sku = int64(v)
	case int64:
		sku = v
	default:
		logger.Error("sku 数据类型非法", "type", fmt.Sprintf("%T", req.Data))
		return false, fmt.Errorf("sku 数据类型非法: %T", req.Data)
	}

	// 3. 清零 DB 库存
	if err := l.quotaSvc.ClearActivitySkuStock(ctx, sku); err != nil {
		logger.Error("ClearActivitySkuStock 失败", "sku", sku, "err", err)
		return true, err // 可重试
	}

	// 4. 清理 Redis 库存缓存队列
	if err := l.quotaSvc.ClearQueueValue(ctx); err != nil {
		logger.Error("ClearQueueValue 失败", "err", err)
		return true, err // 可重试
	}

	return false, nil
}
