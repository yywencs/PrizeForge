package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"prizeforge/internal/domain/activity"
	"prizeforge/internal/metrics"
	"prizeforge/pkg/config"
	"prizeforge/pkg/rabbitmq"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// eventPublisher is the low-level publish interface.
type eventPublisher interface {
	Publish(ctx context.Context, topic string, event *rabbitmq.BaseEvent) error
}

// RabbitMQPublisher handles low-level AMQP publishing with fanout exchanges.
type RabbitMQPublisher struct {
	channel           *amqp.Channel
	mu                sync.Mutex
	declaredExchanges map[string]struct{}
	returns           <-chan amqp.Return
}

// NewRabbitMQPublisher creates a publisher from an AMQP connection.
func NewRabbitMQPublisher(conn *amqp.Connection) (*RabbitMQPublisher, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("enable publisher confirms: %w", err)
	}
	return &RabbitMQPublisher{
		channel:           ch,
		declaredExchanges: make(map[string]struct{}),
		returns:           ch.NotifyReturn(make(chan amqp.Return, 1)),
	}, nil
}

// Publish publishes a base event to the given topic (fanout exchange).
func (p *RabbitMQPublisher) Publish(ctx context.Context, topic string, event *rabbitmq.BaseEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		metrics.IncRabbitMQPublish(topic, "marshal_error")
		return fmt.Errorf("marshal event error: %w", err)
	}

	// 同一个 AMQP channel 上串行执行“发布 + mandatory return + Broker confirm”，
	// 避免并发消息的 basic.return 与 confirm 结果相互串扰。Outbox 调度器自身仍可
	// 并发处理数据库任务，只在需要确认消息可靠进入 Broker 的这一小段串行化。
	p.mu.Lock()
	defer p.mu.Unlock()

	_, ok := p.declaredExchanges[topic]
	if !ok {
		if err := p.channel.ExchangeDeclare(topic, "fanout", true, false, false, false, nil); err != nil {
			metrics.IncRabbitMQPublish(topic, "exchange_declare_error")
			return fmt.Errorf("exchange declare error: %w", err)
		}
		p.declaredExchanges[topic] = struct{}{}
	}

	confirmation, err := p.channel.PublishWithDeferredConfirmWithContext(ctx, topic, "", true, false, amqp.Publishing{
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
	// 因为发布过程已串行化，此处收到的 return 一定属于当前消息。
	select {
	case returned, ok := <-p.returns:
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

// Publisher is a typed facade over the low-level RabbitMQPublisher.
type Publisher struct {
	client eventPublisher
	topic  config.RabbitMQTopicConfig
}

// NewPublisher creates a typed publisher from config.
func NewPublisher(client *RabbitMQPublisher, cfg *config.RabbitMQConfig) *Publisher {
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

func (p *Publisher) PublishSaveOrder(ctx context.Context, event *rabbitmq.BaseEvent) error {
	return p.client.Publish(ctx, activity.SaveOrderRecordTopic, event)
}
