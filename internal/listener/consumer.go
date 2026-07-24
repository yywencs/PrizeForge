package listener

import (
	"context"
	"fmt"
	"time"

	"prizeforge/pkg/logger"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	defaultMessageHandleTimeout = 30 * time.Second
	defaultPrefetchCount        = 1
	defaultConsumerConcurrency  = 1
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
//	consumer := NewRabbitMQConsumer(conn)
//	consumer.RegisterListener(stockTopic, stockLsn)
//	go consumer.Start(ctx)
//	defer consumer.Shutdown()
type RabbitMQConsumer struct {
	conn               *amqp.Connection    // RabbitMQ 连接（复用自 bootstrap 创建的连接）
	listeners          map[string]Listener // topic → Listener 映射
	queueConcurrency   map[string]int      // queue → 独立 Channel/消费 goroutine 数
	channels           []*amqp.Channel     // 所有打开的 channel，Shutdown 时逐个关闭
	prefetch           int                 // 每个 Channel 最多允许的未确认消息数
	defaultConcurrency int                 // 未单独配置队列时使用的消费者并发数
	handleTimeout      time.Duration       // 单条消息处理上限，防止一个调用永久占住消费者
}

// ConsumerOption 定制 RabbitMQ 消费端的 QoS 和队列并发度。
type ConsumerOption func(*RabbitMQConsumer)

// WithPrefetch 设置每个消费 Channel 的未确认消息上限；非法值回退为 1。
func WithPrefetch(prefetch int) ConsumerOption {
	return func(c *RabbitMQConsumer) {
		if prefetch > 0 {
			c.prefetch = prefetch
		}
	}
}

// WithDefaultConcurrency 设置未单独配置队列时的消费者并发数；非法值回退为 1。
func WithDefaultConcurrency(concurrency int) ConsumerOption {
	return func(c *RabbitMQConsumer) {
		if concurrency > 0 {
			c.defaultConcurrency = concurrency
		}
	}
}

// WithQueueConcurrency 设置 queue → 消费者并发数映射。
// 空队列名和非正数配置会被忽略，避免意外启动零个消费者。
func WithQueueConcurrency(concurrency map[string]int) ConsumerOption {
	return func(c *RabbitMQConsumer) {
		for queue, count := range concurrency {
			if queue != "" && count > 0 {
				c.queueConcurrency[queue] = count
			}
		}
	}
}

// NewRabbitMQConsumer 创建通用 RabbitMQConsumer。
// 所有 topic → Listener 映射由 bootstrap 从同一份 RabbitMQ topic 配置显式注册，
// 避免生产端使用配置、消费端使用硬编码常量而发生 Exchange 名称漂移。
func NewRabbitMQConsumer(
	conn *amqp.Connection,
	options ...ConsumerOption,
) *RabbitMQConsumer {
	c := &RabbitMQConsumer{
		conn:               conn,
		listeners:          make(map[string]Listener),
		queueConcurrency:   make(map[string]int),
		prefetch:           defaultPrefetchCount,
		defaultConcurrency: defaultConsumerConcurrency,
		handleTimeout:      defaultMessageHandleTimeout,
	}
	for _, option := range options {
		if option != nil {
			option(c)
		}
	}

	return c
}

// RegisterListener 注册 topic → Listener 映射。
// 需在 Start 之前调用。
func (c *RabbitMQConsumer) RegisterListener(topic string, l Listener) {
	c.listeners[topic] = l
}

// Start 按 topic 配置的并发度启动独立 Channel 和消费 goroutine。
// 任一消费者启动失败则立即返回错误。
func (c *RabbitMQConsumer) Start(ctx context.Context) error {
	for topic, l := range c.listeners {
		concurrency := c.consumerConcurrency(topic)
		for workerID := 1; workerID <= concurrency; workerID++ {
			if err := c.startConsumer(topic, l, workerID, concurrency); err != nil {
				return fmt.Errorf("启动 topic %s 消费者 %d/%d 失败: %w",
					topic, workerID, concurrency, err)
			}
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

// startConsumer 为单个 topic 创建一套独立 Channel 和消费 goroutine。
//
// AMQP 拓扑：
//
//	Exchange: topic 名, type=fanout, durable
//	Queue:    {topic}_queue, durable
//	Binding:  queue ← exchange (routing key 为空，fanout 模式下忽略)
//	QoS:      每个 Channel 使用独立 prefetch，配合手动 Ack/Nack
func (c *RabbitMQConsumer) startConsumer(topic string, l Listener, workerID, concurrency int) error {
	channel, err := c.conn.Channel()
	if err != nil {
		return fmt.Errorf("打开 channel: %w", err)
	}
	started := false
	defer func() {
		if !started {
			_ = channel.Close()
		}
	}()

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

	// 每个并行消费者使用独立 Channel，避免并发处理共享 ACK 状态。
	if err := channel.Qos(c.prefetchCount(), 0, false); err != nil {
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
	started = true

	logger.Info(
		"RabbitMQ 消费者启动成功",
		"queue", q.Name,
		"worker", workerID,
		"concurrency", concurrency,
		"prefetch", c.prefetchCount(),
	)
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

			handleCtx, cancel := context.WithTimeout(context.Background(), c.messageHandleTimeout())
			defer cancel()

			retry, err := l.Handle(handleCtx, d.Body)
			if err != nil {
				logger.Error("消息处理失败", "err", err)
				if retry {
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

func (c *RabbitMQConsumer) messageHandleTimeout() time.Duration {
	if c.handleTimeout > 0 {
		return c.handleTimeout
	}
	return defaultMessageHandleTimeout
}

func (c *RabbitMQConsumer) prefetchCount() int {
	if c.prefetch > 0 {
		return c.prefetch
	}
	return defaultPrefetchCount
}

func (c *RabbitMQConsumer) consumerConcurrency(topic string) int {
	if concurrency := c.queueConcurrency[topic+"_queue"]; concurrency > 0 {
		return concurrency
	}
	if c.defaultConcurrency > 0 {
		return c.defaultConcurrency
	}
	return defaultConsumerConcurrency
}
