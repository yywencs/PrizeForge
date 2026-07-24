package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"prizeforge/internal/metrics"
	"prizeforge/pkg/config"
	"prizeforge/pkg/rabbitmq"

	amqp "github.com/rabbitmq/amqp091-go"
)

const defaultRabbitMQPublisherPoolSize = 1

// eventPublisher is the low-level publish interface.
type eventPublisher interface {
	Publish(ctx context.Context, topic string, event *rabbitmq.BaseEvent) error
}

type publisherSlot interface {
	publish(ctx context.Context, topic string, event *rabbitmq.BaseEvent, body []byte) error
	close() error
}

// RabbitMQPublisher 通过独立 AMQP Channel 池并行发布消息。
//
// 每次 Publish 独占一个 slot，slot 内仍按顺序完成 mandatory return 与
// Broker Confirm 的关联；不同 slot 可以同时等待 Confirm，避免全局锁串行化。
type RabbitMQPublisher struct {
	slots chan publisherSlot
}

type amqpPublisherSlot struct {
	channel           *amqp.Channel
	declaredExchanges map[string]struct{}
	returns           <-chan amqp.Return
}

// NewRabbitMQPublisher 创建指定大小的 Publisher Channel 池。
// 非正数池大小回退为 1，确保旧配置不会导致没有可用发布者。
func NewRabbitMQPublisher(conn *amqp.Connection, poolSize int) (*RabbitMQPublisher, error) {
	if poolSize <= 0 {
		poolSize = defaultRabbitMQPublisherPoolSize
	}

	publisher := &RabbitMQPublisher{
		slots: make(chan publisherSlot, poolSize),
	}
	created := make([]publisherSlot, 0, poolSize)
	for i := 0; i < poolSize; i++ {
		slot, err := newAMQPPublisherSlot(conn)
		if err != nil {
			for _, existing := range created {
				_ = existing.close()
			}
			return nil, fmt.Errorf("create RabbitMQ publisher channel %d/%d: %w", i+1, poolSize, err)
		}
		created = append(created, slot)
		publisher.slots <- slot
	}
	return publisher, nil
}

func newAMQPPublisherSlot(conn *amqp.Connection) (*amqpPublisherSlot, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("enable publisher confirms: %w", err)
	}
	return &amqpPublisherSlot{
		channel:           ch,
		declaredExchanges: make(map[string]struct{}),
		returns:           ch.NotifyReturn(make(chan amqp.Return, 1)),
	}, nil
}

// Publish 等待并独占一个 Channel slot，发布结束后归还池中。
func (p *RabbitMQPublisher) Publish(ctx context.Context, topic string, event *rabbitmq.BaseEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		metrics.IncRabbitMQPublish(topic, "marshal_error")
		return fmt.Errorf("marshal event error: %w", err)
	}

	var slot publisherSlot
	select {
	case slot = <-p.slots:
	case <-ctx.Done():
		metrics.IncRabbitMQPublish(topic, "pool_wait_error")
		return fmt.Errorf("wait RabbitMQ publisher channel: %w", ctx.Err())
	}
	defer func() {
		p.slots <- slot
	}()

	return slot.publish(ctx, topic, event, body)
}

func (s *amqpPublisherSlot) publish(ctx context.Context, topic string, event *rabbitmq.BaseEvent, body []byte) error {
	_, ok := s.declaredExchanges[topic]
	if !ok {
		if err := s.channel.ExchangeDeclare(topic, "fanout", true, false, false, false, nil); err != nil {
			metrics.IncRabbitMQPublish(topic, "exchange_declare_error")
			return fmt.Errorf("exchange declare error: %w", err)
		}
		s.declaredExchanges[topic] = struct{}{}
	}

	confirmation, err := s.channel.PublishWithDeferredConfirmWithContext(ctx, topic, "", true, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		MessageId:    event.ID,
		Body:         body,
		Timestamp:    time.Now(),
	})
	if err != nil {
		metrics.IncRabbitMQPublish(topic, "publish_error")
		return err
	}
	acked, err := confirmation.WaitContext(ctx)
	if err != nil {
		metrics.IncRabbitMQPublish(topic, "confirm_error")
		return fmt.Errorf("wait RabbitMQ publisher confirm: %w", err)
	}
	if !acked {
		metrics.IncRabbitMQPublish(topic, "confirm_nack")
		return fmt.Errorf("RabbitMQ publisher confirm nack: topic=%s message_id=%s", topic, event.ID)
	}

	// RabbitMQ 会在 publisher confirm 之前发送 mandatory basic.return。
	// 因为每个 slot 同一时间只处理一条消息，此处 return 一定属于当前消息。
	select {
	case returned, ok := <-s.returns:
		if ok {
			metrics.IncRabbitMQPublish(topic, "unroutable")
			return fmt.Errorf(
				"RabbitMQ message was not routed: topic=%s message_id=%s reply=%d %s",
				topic,
				event.ID,
				returned.ReplyCode,
				returned.ReplyText,
			)
		}
	default:
	}

	metrics.IncRabbitMQPublish(topic, "success")
	return nil
}

func (s *amqpPublisherSlot) close() error {
	return s.channel.Close()
}

// Publisher is a typed facade over the low-level RabbitMQPublisher.
type Publisher struct {
	client eventPublisher
	topic  config.RabbitMQTopicConfig
}

// NewPublisher creates a typed publisher from config.
func NewPublisher(client eventPublisher, cfg *config.RabbitMQConfig) *Publisher {
	return &Publisher{
		client: client,
		topic:  cfg.Topic,
	}
}

func (p *Publisher) PublishStockZero(ctx context.Context, event *rabbitmq.BaseEvent) error {
	return p.client.Publish(ctx, p.topic.ActivitySkuStockZero, event)
}

func (p *Publisher) PublishSendRebate(ctx context.Context, event *rabbitmq.BaseEvent) error {
	return p.client.Publish(ctx, p.topic.SendRebate, event)
}

func (p *Publisher) PublishSendAward(ctx context.Context, event *rabbitmq.BaseEvent) error {
	return p.client.Publish(ctx, p.topic.SendAward, event)
}

func (p *Publisher) PublishTopic(ctx context.Context, topic string, event *rabbitmq.BaseEvent) error {
	return p.client.Publish(ctx, topic, event)
}

func (p *Publisher) PublishDrawResult(ctx context.Context, event *rabbitmq.BaseEvent) error {
	return p.client.Publish(ctx, p.topic.DrawResult, event)
}
