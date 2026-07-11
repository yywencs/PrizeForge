package adapter

import (
	"prizeforge/internal/domain/activity"
	"prizeforge/internal/metrics"
	"prizeforge/pkg/config"
	"prizeforge/pkg/rabbitmq"
	"context"
	"encoding/json"
	"fmt"
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
}

// NewRabbitMQPublisher creates a publisher from an AMQP connection.
func NewRabbitMQPublisher(conn *amqp.Connection) (*RabbitMQPublisher, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	return &RabbitMQPublisher{
		channel:           ch,
		declaredExchanges: make(map[string]struct{}),
	}, nil
}

// Publish publishes a base event to the given topic (fanout exchange).
func (p *RabbitMQPublisher) Publish(ctx context.Context, topic string, event *rabbitmq.BaseEvent) error {
	p.mu.Lock()
	_, ok := p.declaredExchanges[topic]
	if !ok {
		if err := p.channel.ExchangeDeclare(topic, "fanout", true, false, false, false, nil); err != nil {
			p.mu.Unlock()
			metrics.IncRabbitMQPublish(topic, "exchange_declare_error")
			return fmt.Errorf("exchange declare error: %w", err)
		}
		p.declaredExchanges[topic] = struct{}{}
	}
	p.mu.Unlock()

	body, err := json.Marshal(event)
	if err != nil {
		metrics.IncRabbitMQPublish(topic, "marshal_error")
		return fmt.Errorf("marshal event error: %w", err)
	}

	if err := p.channel.PublishWithContext(ctx, topic, "", false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		Body:         body,
		Timestamp:    time.Now(),
	}); err != nil {
		metrics.IncRabbitMQPublish(topic, "publish_error")
		return err
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
