package server

import (
	"big-market-kratos/internal/biz/activity"
	bizlistener "big-market-kratos/internal/listener"
	"context"
	"fmt"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Listener interface {
	Handle(ctx context.Context, body []byte) (retry bool, err error)
}

type RabbitMQServer struct {
	conn      *amqp.Connection
	listeners map[string]Listener
	channels  []*amqp.Channel
}

func NewRabbitMQServer(
	conn *amqp.Connection,
	stockListener *bizlistener.ActivityStockListener,
	rebateListener *bizlistener.RebateListener,
) *RabbitMQServer {
	s := &RabbitMQServer{
		conn:      conn,
		listeners: make(map[string]Listener),
	}

	s.RegisterListener(activity.ActivitySkuStockZeroTopic, stockListener)
	s.RegisterListener(activity.ActivityAwardSendTopic, rebateListener)

	return s
}

func (s *RabbitMQServer) RegisterListener(topic string, l Listener) {
	s.listeners[topic] = l
}

func (s *RabbitMQServer) Start(ctx context.Context) error {
	for topic, l := range s.listeners {
		if err := s.startConsumer(topic, l); err != nil {
			return err
		}
	}
	return nil
}

func (s *RabbitMQServer) startConsumer(topic string, l Listener) error {
	channel, err := s.conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}

	err = channel.ExchangeDeclare(
		topic,    // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare exchange: %w", err)
	}

	q, err := channel.QueueDeclare(
		topic+"_queue", // name
		true,           // durable
		false,          // delete when unused
		false,          // exclusive
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	err = channel.QueueBind(
		q.Name, // 队列名
		"",     // routing key (fanout 模式传空即可)
		topic,  // 交换机名
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to bind queue: %w", err)
	}

	// 设置 QoS
	err = channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	msgs, err := channel.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %w", err)
	}

	go s.handle(msgs, l)

	s.channels = append(s.channels, channel)

	slog.Info("RabbitMQ Consumer started listening", "queue", q.Name)
	return nil
}

func (s *RabbitMQServer) Stop(ctx context.Context) error {
	for _, ch := range s.channels {
		if err := ch.Close(); err != nil {
			slog.Error("failed to close rabbitmq channel", "err", err)
		}
	}
	if s.conn != nil {
		s.conn.Close()
	}
	return nil
}

func (s *RabbitMQServer) handle(msgs <-chan amqp.Delivery, l Listener) {
	for d := range msgs {
		func() {
			defer func() {
				if r := recover(); r != nil {
					slog.Error("Panic in worker", "reason", r)
					d.Nack(false, true) // 发生严重错误重回队列
				}
			}()

			if retry, err := l.Handle(context.Background(), d.Body); err != nil {
				slog.Error("Handle message failed", "err", err)
				if retry {
					slog.Warn("Retrying message", "err", err)
					d.Nack(false, true) // 或者根据错误类型决定是否重试
				} else {
					slog.Error("Fatal error, dropping message", "err", err)
					d.Reject(false)
				}
				return
			}

			d.Ack(false)
		}()
	}
}
