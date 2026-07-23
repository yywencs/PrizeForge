package job

import (
	"context"
	"fmt"
	"prizeforge/internal/domain/activity"
	"prizeforge/pkg/rabbitmq"
	"time"
)

type drawResultPublicationStore interface {
	MarkDrawResultPublished(context.Context, *activity.DrawResultPublication) error
}

type drawResultRabbitPublisher interface {
	PublishDrawResult(context.Context, *rabbitmq.BaseEvent) error
}

// DrawResultPublisher 负责单条抽奖结果的可靠投递：
// 发布 RabbitMQ 并等待 Confirm，成功后再更新 Redis 发布状态。
type DrawResultPublisher struct {
	publicationStore drawResultPublicationStore
	rabbitPublisher  drawResultRabbitPublisher
}

func NewDrawResultPublisher(publicationStore drawResultPublicationStore, rabbitPublisher drawResultRabbitPublisher) *DrawResultPublisher {
	return &DrawResultPublisher{
		publicationStore: publicationStore,
		rabbitPublisher:  rabbitPublisher,
	}
}

func (p *DrawResultPublisher) Publish(ctx context.Context, publication *activity.DrawResultPublication) error {
	if publication == nil || publication.Result == nil || publication.StreamID == "" {
		return activity.ErrInvalidParams
	}
	result := publication.Result
	event := &rabbitmq.BaseEvent{
		ID:        "draw:" + result.UserID + ":" + result.OrderID,
		Timestamp: result.AwardTime,
		Data:      result,
	}
	if err := p.rabbitPublisher.PublishDrawResult(ctx, event); err != nil {
		return err
	}

	markCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if err := p.publicationStore.MarkDrawResultPublished(markCtx, publication); err != nil {
		return fmt.Errorf("RabbitMQ confirmed but Redis publish state update failed: %w", err)
	}
	return nil
}
