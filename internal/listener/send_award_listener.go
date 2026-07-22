package listener

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"prizeforge/internal/domain/award"
	"prizeforge/pkg/logger"
)

type awardCompletionService interface {
	CompleteUserAward(ctx context.Context, userID string, orderID string) error
}

// SendAwardListener 消费 send_award 消息。
//
// 当前项目没有接入第三方奖品供应商，因此“将中奖记录幂等更新为 complete”就是现阶段
// 的发奖完成边界。未来接入真实发奖网关时，应先携带 order_id 调用幂等发奖接口，成功
// 后再更新中奖记录；只有 Handle 返回 nil，RabbitMQConsumer 才会 ACK 消息。
type SendAwardListener struct {
	awardSvc awardCompletionService
}

func NewSendAwardListener(awardSvc awardCompletionService) *SendAwardListener {
	return &SendAwardListener{awardSvc: awardSvc}
}

// Handle 解析发奖事件并幂等完成中奖记录。
// JSON/字段错误以及记录缺失、状态冲突属于不可重试错误；数据库等临时错误返回
// retry=true，由 RabbitMQConsumer NACK 后重新投递。
func (l *SendAwardListener) Handle(ctx context.Context, body []byte) (retry bool, err error) {
	var event struct {
		ID   string                 `json:"id"`
		Data award.SendAwardMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &event); err != nil {
		return false, fmt.Errorf("解析 send_award 消息失败: %w", err)
	}
	// 兼容升级前已经写入 Outbox 的消息：旧 payload 没有 order_id，但 BaseEvent.ID
	// 使用的就是 userID:orderID。取最后一个冒号可兼容 userID 自身包含冒号的情况。
	if event.Data.OrderID == "" {
		separator := strings.LastIndex(event.ID, ":")
		if separator >= 0 && separator+1 < len(event.ID) && event.ID[:separator] == event.Data.UserID {
			event.Data.OrderID = event.ID[separator+1:]
		}
	}
	if event.ID == "" || event.Data.UserID == "" || event.Data.OrderID == "" || event.Data.AwardID <= 0 {
		return false, fmt.Errorf(
			"send_award 消息字段不完整: message_id=%q user_id=%q order_id=%q award_id=%d",
			event.ID,
			event.Data.UserID,
			event.Data.OrderID,
			event.Data.AwardID,
		)
	}
	if l.awardSvc == nil {
		return true, errors.New("send_award service is nil")
	}

	if err := l.awardSvc.CompleteUserAward(ctx, event.Data.UserID, event.Data.OrderID); err != nil {
		if errors.Is(err, award.ErrAwardRecordNotFound) || errors.Is(err, award.ErrAwardStateConflict) {
			return false, err
		}
		return true, err
	}

	logger.Info(
		"发奖消息处理完成",
		"messageID", event.ID,
		"userID", event.Data.UserID,
		"orderID", event.Data.OrderID,
		"awardID", event.Data.AwardID,
	)
	return false, nil
}
