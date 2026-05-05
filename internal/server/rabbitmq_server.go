package server

import (
	"big-market-kratos/internal/biz/activity"
	bizlistener "big-market-kratos/internal/listener"
	"big-market-kratos/pkg/logger"
	"context"

	"github.com/go-kratos/kratos/v2/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	ErrRabbitMQChannelOpen      = errors.InternalServer("RABBITMQ_CHANNEL_OPEN", "failed to open channel")
	ErrRabbitMQExchangeDeclare  = errors.InternalServer("RABBITMQ_EXCHANGE_DECLARE", "failed to declare exchange")
	ErrRabbitMQQueueDeclare     = errors.InternalServer("RABBITMQ_QUEUE_DECLARE", "failed to declare queue")
	ErrRabbitMQQueueBind        = errors.InternalServer("RABBITMQ_QUEUE_BIND", "failed to bind queue")
	ErrRabbitMQQosSet           = errors.InternalServer("RABBITMQ_QOS_SET", "failed to set QoS")
	ErrRabbitMQConsumerRegister = errors.InternalServer("RABBITMQ_CONSUMER_REGISTER", "failed to register consumer")
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
	saveOrderListener *bizlistener.SaveOrderListener,
) *RabbitMQServer {
	s := &RabbitMQServer{
		conn:      conn,
		listeners: make(map[string]Listener),
	}

	s.RegisterListener(activity.ActivitySkuStockZeroTopic, stockListener)
	s.RegisterListener(activity.ActivityAwardSendTopic, rebateListener)
	s.RegisterListener(activity.SaveOrderRecordTopic, saveOrderListener)

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
		return ErrRabbitMQChannelOpen.WithCause(err)
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
		return ErrRabbitMQExchangeDeclare.WithCause(err)
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
		return ErrRabbitMQQueueDeclare.WithCause(err)
	}

	err = channel.QueueBind(
		q.Name, // 队列名
		"",     // routing key (fanout 模式传空即可)
		topic,  // 交换机名
		false,
		nil,
	)
	if err != nil {
		return ErrRabbitMQQueueBind.WithCause(err)
	}

	// 设置 QoS
	err = channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		return ErrRabbitMQQosSet.WithCause(err)
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
		return ErrRabbitMQConsumerRegister.WithCause(err)
	}

	go s.handle(msgs, l)

	s.channels = append(s.channels, channel)

	logger.Info("RabbitMQ Consumer started listening", "queue", q.Name)
	return nil
}

func (s *RabbitMQServer) Stop(ctx context.Context) error {
	for _, ch := range s.channels {
		if err := ch.Close(); err != nil {
			logger.Error("failed to close rabbitmq channel", "err", err)
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
					logger.Error("Panic in worker", "reason", r)
					d.Nack(false, true) // 发生严重错误重回队列
				}
			}()

			if retry, err := l.Handle(context.Background(), d.Body); err != nil {
				logger.Error("Handle message failed", "err", err)
				if retry {
					logger.Warn("Retrying message", "err", err)
					d.Nack(false, true) // 或者根据错误类型决定是否重试
				} else {
					logger.Error("Fatal error, dropping message", "err", err)
					d.Reject(false)
				}
				return
			}

			d.Ack(false)
		}()
	}
}
