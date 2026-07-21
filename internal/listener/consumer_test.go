package listener

import (
	"context"
	"testing"

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
