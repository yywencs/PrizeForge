package listener

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type acknowledgement struct {
	tag      uint64
	multiple bool
	requeue  bool
}

type recordingAcknowledger struct {
	acks    []acknowledgement
	nacks   []acknowledgement
	rejects []acknowledgement
}

func (a *recordingAcknowledger) Ack(tag uint64, multiple bool) error {
	a.acks = append(a.acks, acknowledgement{tag: tag, multiple: multiple})
	return nil
}

func (a *recordingAcknowledger) Nack(tag uint64, multiple, requeue bool) error {
	a.nacks = append(a.nacks, acknowledgement{tag: tag, multiple: multiple, requeue: requeue})
	return nil
}

func (a *recordingAcknowledger) Reject(tag uint64, requeue bool) error {
	a.rejects = append(a.rejects, acknowledgement{tag: tag, requeue: requeue})
	return nil
}

type panicListener struct{}

func (panicListener) Handle(context.Context, []byte) (bool, error) {
	panic("listener test panic")
}

type contextTimeoutListener struct{}

func (contextTimeoutListener) Handle(ctx context.Context, _ []byte) (bool, error) {
	<-ctx.Done()
	return true, ctx.Err()
}

// TestRabbitMQConsumerUsesQueueConcurrencyMap 验证每个队列可以通过统一映射配置独立并发度，
// 未配置队列使用默认并发数，并且 prefetch 使用显式配置。
func TestRabbitMQConsumerUsesQueueConcurrencyMap(t *testing.T) {
	consumer := NewRabbitMQConsumer(
		nil,
		WithPrefetch(2),
		WithDefaultConcurrency(2),
		WithQueueConcurrency(map[string]int{
			"draw_result_queue": 8,
			"send_award_queue":  4,
		}),
	)

	if got := consumer.prefetchCount(); got != 2 {
		t.Fatalf("prefetchCount() = %d, want 2", got)
	}
	if got := consumer.consumerConcurrency("draw_result"); got != 8 {
		t.Fatalf("draw_result concurrency = %d, want 8", got)
	}
	if got := consumer.consumerConcurrency("send_award"); got != 4 {
		t.Fatalf("send_award concurrency = %d, want 4", got)
	}
	if got := consumer.consumerConcurrency("activity_sku_stock_zero_topic"); got != 2 {
		t.Fatalf("unconfigured topic concurrency = %d, want 2", got)
	}
}

// TestRabbitMQConsumerFallsBackFromInvalidOptions 验证非法 prefetch 和并发度
// 不会产生零消费者，而是回退到安全的单消息、单消费者配置。
func TestRabbitMQConsumerFallsBackFromInvalidOptions(t *testing.T) {
	consumer := NewRabbitMQConsumer(
		nil,
		WithPrefetch(0),
		WithDefaultConcurrency(0),
		WithQueueConcurrency(map[string]int{
			"draw_result_queue": 0,
			"":                  8,
		}),
	)

	if got := consumer.prefetchCount(); got != 1 {
		t.Fatalf("prefetchCount() = %d, want 1", got)
	}
	if got := consumer.consumerConcurrency("draw_result"); got != 1 {
		t.Fatalf("draw_result concurrency = %d, want 1", got)
	}
}

// TestRabbitMQConsumerRejectsMalformedMessageWithoutRequeue 验证非法消息会被视为永久错误并直接丢弃，不会 Ack 或重新入队。
func TestRabbitMQConsumerRejectsMalformedMessageWithoutRequeue(t *testing.T) {
	acknowledger := &recordingAcknowledger{}
	messages := make(chan amqp.Delivery, 1)
	messages <- amqp.Delivery{
		Acknowledger: acknowledger,
		DeliveryTag:  41,
		Body:         []byte(`{"data":`),
	}
	close(messages)

	consumer := &RabbitMQConsumer{}
	consumer.handle(messages, NewActivityStockListener(nil))

	if len(acknowledger.acks) != 0 {
		t.Fatalf("Ack() calls = %d, want 0", len(acknowledger.acks))
	}
	if len(acknowledger.nacks) != 0 {
		t.Fatalf("Nack() calls = %d, want 0", len(acknowledger.nacks))
	}
	if len(acknowledger.rejects) != 1 {
		t.Fatalf("Reject() calls = %d, want 1", len(acknowledger.rejects))
	}
	if got := acknowledger.rejects[0]; got.tag != 41 || got.requeue {
		t.Fatalf("Reject() = %+v, want tag=41 requeue=false", got)
	}
}

// TestRabbitMQConsumerRequeuesMessageAfterListenerPanic 验证 Listener panic 会被消费循环恢复，并通过 Nack 将消息重新放回队列。
func TestRabbitMQConsumerRequeuesMessageAfterListenerPanic(t *testing.T) {
	acknowledger := &recordingAcknowledger{}
	messages := make(chan amqp.Delivery, 1)
	messages <- amqp.Delivery{
		Acknowledger: acknowledger,
		DeliveryTag:  42,
		Body:         []byte(`{"data":"ignored"}`),
	}
	close(messages)

	consumer := &RabbitMQConsumer{}
	consumer.handle(messages, panicListener{})

	if len(acknowledger.acks) != 0 {
		t.Fatalf("Ack() calls = %d, want 0", len(acknowledger.acks))
	}
	if len(acknowledger.rejects) != 0 {
		t.Fatalf("Reject() calls = %d, want 0", len(acknowledger.rejects))
	}
	if len(acknowledger.nacks) != 1 {
		t.Fatalf("Nack() calls = %d, want 1", len(acknowledger.nacks))
	}
	if got := acknowledger.nacks[0]; got.tag != 42 || got.multiple || !got.requeue {
		t.Fatalf("Nack() = %+v, want tag=42 multiple=false requeue=true", got)
	}
}

// TestRabbitMQConsumerTimesOutAndRequeuesStuckMessage 验证单条消息处理超过上限后，
// Consumer 会取消处理上下文并将消息重新入队，而不是永久占住唯一的 unacked 配额。
func TestRabbitMQConsumerTimesOutAndRequeuesStuckMessage(t *testing.T) {
	acknowledger := &recordingAcknowledger{}
	messages := make(chan amqp.Delivery, 1)
	messages <- amqp.Delivery{
		Acknowledger: acknowledger,
		DeliveryTag:  43,
		Body:         []byte(`{"data":"ignored"}`),
	}
	close(messages)

	consumer := &RabbitMQConsumer{handleTimeout: 10 * time.Millisecond}
	consumer.handle(messages, contextTimeoutListener{})

	if len(acknowledger.acks) != 0 || len(acknowledger.rejects) != 0 {
		t.Fatalf("Ack/Reject calls = %d/%d, want 0/0", len(acknowledger.acks), len(acknowledger.rejects))
	}
	if len(acknowledger.nacks) != 1 {
		t.Fatalf("Nack() calls = %d, want 1", len(acknowledger.nacks))
	}
	if got := acknowledger.nacks[0]; got.tag != 43 || got.multiple || !got.requeue {
		t.Fatalf("Nack() = %+v, want tag=43 multiple=false requeue=true", got)
	}
}
