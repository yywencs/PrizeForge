package listener

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"prizeforge/internal/domain/activity"
	"prizeforge/internal/domain/rebate"
	"prizeforge/pkg/logger"
)

// RebateListener 监听返利发奖消息。
//
// 当用户完成签到等行为触发返利时，系统通过 RabbitMQ fanout exchange
// "activity_award_send_topic" 广播返利事件。本 Listener 消费该事件，
// 根据返利类型（SKU 发放 / 积分发放）执行对应的发奖逻辑。
//
// 消息体 Data 字段为 RebateMessage：
//
//	{
//	  "user_id": "10001",
//	  "rebate_type": "sku",
//	  "rebate_config": "100001",
//	  "biz_id": "20240101"
//	}
type RebateListener struct {
	quotaSvc *activity.ActivityQuotaUsecase
}

// NewRebateListener 创建 RebateListener。
func NewRebateListener(quotaSvc *activity.ActivityQuotaUsecase) *RebateListener {
	return &RebateListener{
		quotaSvc: quotaSvc,
	}
}

// Handle 处理返利发奖消息。
func (l *RebateListener) Handle(ctx context.Context, body []byte) (retry bool, err error) {
	// 1. 解析消息（Data 直接反序列化为 RebateMessage）
	var event struct {
		ID        string               `json:"id"`
		Timestamp time.Time            `json:"timestamp"`
		Data      rebate.RebateMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &event); err != nil {
		logger.Error("解析返利消息失败", "err", err, "body", string(body))
		return false, fmt.Errorf("解析返利消息失败: %w", err)
	}

	rebateMsg := event.Data
	logger.Info("收到返利消息", "userId", rebateMsg.UserID, "rebateType", rebateMsg.RebateType, "config", rebateMsg.RebateConfig)

	// 2. 按返利类型分发处理
	switch rebateMsg.RebateType {

	case string(rebate.Sku):
		// SKU 返利：将 RebateConfig 解析为 sku 数值，创建活动订单完成发奖
		sku, err := strconv.ParseInt(rebateMsg.RebateConfig, 10, 64)
		if err != nil {
			logger.Error("sku 配置非法，丢弃消息", "config", rebateMsg.RebateConfig, "err", err)
			return false, nil // 配置错误，重试无意义
		}

		skuRecharge := &activity.SkuRecharge{
			UserID:        rebateMsg.UserID,
			Sku:           sku,
			OutBusinessNo: rebateMsg.BizID,
		}

		orderID, err := l.quotaSvc.CreateRaffleActivityOrder(ctx, skuRecharge)
		if err != nil {
			// 唯一索引冲突说明是重复消息，幂等处理，视为成功
			if errors.Is(err, activity.ErrDBIndexDuplicate) {
				logger.Warn("重复返利订单，幂等跳过", "userId", rebateMsg.UserID, "bizId", rebateMsg.BizID)
				return false, nil
			}
			logger.Error("CreateRaffleActivityOrder 失败", "userId", rebateMsg.UserID, "sku", sku, "err", err)
			return true, err // 可重试
		}
		logger.Info("返利发奖成功", "userId", rebateMsg.UserID, "orderId", orderID)

	case string(rebate.Integral):
		// TODO: 积分返利暂未实现
		logger.Info("积分返利暂未实现", "userId", rebateMsg.UserID)
		return false, nil

	default:
		logger.Warn("未知返利类型，丢弃消息", "type", rebateMsg.RebateType)
		return false, nil
	}

	return false, nil
}
