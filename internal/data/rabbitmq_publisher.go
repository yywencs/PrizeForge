package data

import (
	"big-market-kratos/internal/conf"
	"big-market-kratos/pkg/rabbitmq"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type eventPublisher interface {
	Publish(ctx context.Context, topic string, event *rabbitmq.BaseEvent) error
}

type RabbitMQPublisher struct {
	channel           *amqp.Channel
	mu                sync.Mutex
	declaredExchanges map[string]struct{}
}

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

func (p *RabbitMQPublisher) Publish(ctx context.Context, topic string, event *rabbitmq.BaseEvent) error {
	p.mu.Lock()
	_, ok := p.declaredExchanges[topic]
	if !ok {
		if err := p.channel.ExchangeDeclare(topic, "fanout", true, false, false, false, nil); err != nil {
			p.mu.Unlock()
			return fmt.Errorf("exchange declare error: %w", err)
		}
		p.declaredExchanges[topic] = struct{}{}
	}
	p.mu.Unlock()

	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event error: %w", err)
	}

	return p.channel.PublishWithContext(ctx, topic, "", false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		Body:         body,
		Timestamp:    time.Now(),
	})
}

type Publisher struct {
	client eventPublisher
	topic  *conf.RabbitMQ_Topic
}

func NewPublisher(client *RabbitMQPublisher, c *conf.RabbitMQ) *Publisher {
	return &Publisher{
		client: client,
		topic:  c.Topic,
	}
}

func (p *Publisher) PublishStockZero(ctx context.Context, event *rabbitmq.BaseEvent) error {
	return p.client.Publish(ctx, p.topic.ActivitySkuStockZero, event)
}

func (p *Publisher) PublishSendRebate(ctx context.Context, event *rabbitmq.BaseEvent) error {
	return p.client.Publish(ctx, p.topic.SendRebate, event)
}

func (p *Publisher) PublishSendAward(ctx context.Context, event *rabbitmq.BaseEvent) error {
	return p.client.Publish(ctx, p.topic.SendRebate, event)
}
