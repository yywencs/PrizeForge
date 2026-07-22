package listener

import (
	"context"
	"fmt"

	"prizeforge/internal/domain/activity"
	"prizeforge/pkg/logger"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Listener 是 RabbitMQ 消息处理器的统一接口。
//
// 每个 Listener 对应一个 topic，由 RabbitMQConsumer 管理生命周期。
//
// Handle 返回值含义：
//   - retry=true  → Nack 并重回队列，等待重新消费（用于临时性错误）
//   - retry=false → Ack 确认消费成功，或 Reject 丢弃（用于永久性错误）
type Listener interface {
	Handle(ctx context.Context, body []byte) (retry bool, err error)
}

// RabbitMQConsumer 管理 RabbitMQ 消费端的生命周期。
//
// 职责：
//   - 为每个注册的 topic 创建 channel、声明 fanout exchange + durable queue、绑定并启动消费
//   - 消费循环中提供 panic recovery，防止单个消息处理崩溃导致 channel 关闭
//   - 优雅关闭时依次关闭所有 channel 和连接
//
// 使用方式：
//
//	consumer := NewRabbitMQConsumer(conn, stockLsn, rebateLsn, orderLsn)
//	go consumer.Start(ctx)
//	defer consumer.Shutdown()
type RabbitMQConsumer struct {
	conn      *amqp.Connection    // RabbitMQ 连接（复用自 bootstrap 创建的连接）
	listeners map[string]Listener // topic → Listener 映射
	channels  []*amqp.Channel     // 所有打开的 channel，Shutdown 时逐个关闭
}

// NewRabbitMQConsumer 创建 RabbitMQConsumer 并注册 Activity 领域的三个基础 topic。
// 其他领域监听器（例如 send_award）由 bootstrap 通过 RegisterListener 显式注册。
//
// Topic 与 Listener 对应关系：
//   - activity_sku_stock_zero_topic → ActivityStockListener（库存归零）
//   - activity_award_send_topic      → RebateListener（返利发奖）
//   - save_order_record              → SaveOrderListener（订单持久化）
func NewRabbitMQConsumer(
	conn *amqp.Connection,
	stockListener *ActivityStockListener,
	rebateListener *RebateListener,
	saveOrderListener *SaveOrderListener,
) *RabbitMQConsumer {
	c := &RabbitMQConsumer{
		conn:      conn,
		listeners: make(map[string]Listener),
	}

	if stockListener != nil {
		c.RegisterListener(activity.ActivitySkuStockZeroTopic, stockListener)
	}
	if rebateListener != nil {
		c.RegisterListener(activity.ActivityAwardSendTopic, rebateListener)
	}
	if saveOrderListener != nil {
		c.RegisterListener(activity.SaveOrderRecordTopic, saveOrderListener)
	}

	return c
}

// RegisterListener 注册 topic → Listener 映射。
// 需在 Start 之前调用。
func (c *RabbitMQConsumer) RegisterListener(topic string, l Listener) {
	c.listeners[topic] = l
}

// Start 为每个已注册的 topic 启动独立的消费者 goroutine。
// 任一 topic 启动失败则立即返回错误。
func (c *RabbitMQConsumer) Start(ctx context.Context) error {
	for topic, l := range c.listeners {
		if err := c.startConsumer(topic, l); err != nil {
			return fmt.Errorf("启动 topic %s 消费者失败: %w", topic, err)
		}
	}
	return nil
}

// Shutdown 优雅关闭：先关闭所有 channel，再关闭连接。
func (c *RabbitMQConsumer) Shutdown() {
	for _, ch := range c.channels {
		if err := ch.Close(); err != nil {
			logger.Error("关闭 RabbitMQ channel 失败", "err", err)
		}
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
}

// startConsumer 为单个 topic 创建消费者。
//
// AMQP 拓扑：
//
//	Exchange: topic 名, type=fanout, durable
//	Queue:    {topic}_queue, durable
//	Binding:  queue ← exchange (routing key 为空，fanout 模式下忽略)
//	QoS:      prefetch=1（逐条消费，配合手动 Ack/Nack）
func (c *RabbitMQConsumer) startConsumer(topic string, l Listener) error {
	channel, err := c.conn.Channel()
	if err != nil {
		return fmt.Errorf("打开 channel: %w", err)
	}

	// 声明 fanout 交换机（持久化，生产者可能先于消费者启动）
	if err := channel.ExchangeDeclare(
		topic,
		"fanout", // 广播模式，所有绑定队列都会收到消息
		true,     // durable — 重启后交换机不丢失
		false,    // auto-deleted — 没有队列绑定时不自动删除
		false,    // internal
		false,    // no-wait
		nil,
	); err != nil {
		return fmt.Errorf("声明交换机 %s: %w", topic, err)
	}

	// 声明持久化队列
	q, err := channel.QueueDeclare(
		topic+"_queue",
		true,  // durable — 重启后队列不丢失
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		return fmt.Errorf("声明队列: %w", err)
	}

	// 将队列绑定到 fanout 交换机
	if err := channel.QueueBind(q.Name, "", topic, false, nil); err != nil {
		return fmt.Errorf("绑定队列: %w", err)
	}

	// prefetch=1：每次只推送一条消息，处理完 Ack 后才推送下一条
	if err := channel.Qos(1, 0, false); err != nil {
		return fmt.Errorf("设置 QoS: %w", err)
	}

	// 注册消费者（auto-ack=false，由 handle 方法手动 Ack）
	msgs, err := channel.Consume(
		q.Name, // queue
		"",     // consumer tag
		false,  // auto-ack 关闭
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,
	)
	if err != nil {
		return fmt.Errorf("注册消费者: %w", err)
	}

	go c.handle(msgs, l)

	c.channels = append(c.channels, channel)

	logger.Info("RabbitMQ 消费者启动成功", "queue", q.Name)
	return nil
}

// handle 是消息消费循环，每个 topic 在独立 goroutine 中运行。
//
// 容错策略：
//   - Panic recovery：单个消息处理即使 panic 也不会导致消费循环退出
//   - 手动 Ack：成功消费后 Ack，失败根据 retry 决定 Nack（重回队列）或 Reject（丢弃）
//   - 错误日志记录但不中断循环
func (c *RabbitMQConsumer) handle(msgs <-chan amqp.Delivery, l Listener) {
	for d := range msgs {
		// 使用闭包 + defer 隔离单个消息的 panic
		func() {
			defer func() {
				if r := recover(); r != nil {
					logger.Error("RabbitMQ 消息处理 panic，重回队列", "reason", r)
					_ = d.Nack(false, true) // 重回队列，避免消息丢失
				}
			}()

			retry, err := l.Handle(context.Background(), d.Body)
			if err != nil {
				logger.Error("消息处理失败", "err", err)
				if retry {
					logger.Warn("消息重回队列等待重试")
					_ = d.Nack(false, true) // requeue=true，放回队列尾部
				} else {
					logger.Error("永久性错误，丢弃消息", "err", err)
					_ = d.Reject(false) // requeue=false，直接丢弃
				}
				return
			}

			_ = d.Ack(false) // 确认消费成功
		}()
	}
}
