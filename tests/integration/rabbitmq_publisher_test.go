//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"prizeforge/internal/infrastructure/adapter"
	"prizeforge/pkg/rabbitmq"
	"prizeforge/pkg/xrand"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TestRabbitMQPublisherRoutesStockZeroEvent 验证项目发布器会创建 fanout Exchange，拒绝
// 没有绑定队列的消息，并在消息持久化路由后等待 RabbitMQ Broker Confirm。
func TestRabbitMQPublisherRoutesStockZeroEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	exchangeName := "prizeforge.integration.stock-zero." + xrand.RandomNumeric(12)
	rabbitPublisher, err := adapter.NewRabbitMQPublisher(integrationRabbitMQConnection, 1)
	if err != nil {
		t.Fatalf("NewRabbitMQPublisher() error = %v, want nil", err)
	}
	publisherConfig := *integrationRabbitMQConfig
	publisherConfig.Topic.ActivitySkuStockZero = exchangeName
	publisher := adapter.NewPublisher(rabbitPublisher, &publisherConfig)

	// 第一次发布用于验证项目发布器能够自行声明 Exchange；因为此时没有绑定队列，
	// mandatory return 必须让调用方收到错误，Outbox 才不会误标 completed。
	if err := publisher.PublishStockZero(ctx, rabbitmq.NewBaseEvent(int64(-1))); err == nil {
		t.Fatal("PublishStockZero() without bound queue error = nil, want unroutable error")
	}

	channel, err := integrationRabbitMQConnection.Channel()
	if err != nil {
		t.Fatalf("open RabbitMQ test channel: %v", err)
	}
	t.Cleanup(func() { _ = channel.Close() })
	if err := channel.ExchangeDeclarePassive(exchangeName, "fanout", true, false, false, false, nil); err != nil {
		t.Fatalf("publisher did not declare expected fanout exchange: %v", err)
	}
	t.Cleanup(func() {
		if err := channel.ExchangeDelete(exchangeName, false, false); err != nil {
			t.Errorf("delete integration RabbitMQ exchange: %v", err)
		}
	})

	queue, err := channel.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatalf("declare integration RabbitMQ queue: %v", err)
	}
	if err := channel.QueueBind(queue.Name, "", exchangeName, false, nil); err != nil {
		t.Fatalf("bind integration RabbitMQ queue: %v", err)
	}
	deliveries, err := channel.Consume(queue.Name, "", false, true, false, false, nil)
	if err != nil {
		t.Fatalf("consume integration RabbitMQ queue: %v", err)
	}

	const skuID int64 = 7_000_001
	wantEvent := rabbitmq.NewBaseEvent(skuID)
	if err := publisher.PublishStockZero(ctx, wantEvent); err != nil {
		t.Fatalf("PublishStockZero() error = %v, want nil", err)
	}

	select {
	case delivery, ok := <-deliveries:
		if !ok {
			t.Fatal("RabbitMQ delivery channel closed before receiving message")
		}
		assertIntegrationStockZeroDelivery(t, delivery, exchangeName, wantEvent, skuID)
		if err := delivery.Ack(false); err != nil {
			t.Fatalf("ack RabbitMQ delivery: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for RabbitMQ stock-zero delivery: %v", ctx.Err())
	}
}

// TestRabbitMQPublisherPoolPublishesThroughIndependentChannels 验证真实 RabbitMQ 下
// Channel 池能够并发发布一批持久化消息，并为每条消息取得 Broker Confirm。
func TestRabbitMQPublisherPoolPublishesThroughIndependentChannels(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	topic := "prizeforge.integration.publisher.pool." + xrand.RandomNumeric(12)
	trackIntegrationRabbitMQTopology(t, topic)
	channel, err := integrationRabbitMQConnection.Channel()
	if err != nil {
		t.Fatalf("open RabbitMQ setup channel: %v", err)
	}
	t.Cleanup(func() { _ = channel.Close() })
	if err := channel.ExchangeDeclare(topic, "fanout", true, false, false, false, nil); err != nil {
		t.Fatalf("declare publisher pool exchange: %v", err)
	}
	queue, err := channel.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatalf("declare publisher pool queue: %v", err)
	}
	if err := channel.QueueBind(queue.Name, "", topic, false, nil); err != nil {
		t.Fatalf("bind publisher pool queue: %v", err)
	}
	deliveries, err := channel.Consume(queue.Name, "", true, true, false, false, nil)
	if err != nil {
		t.Fatalf("consume publisher pool queue: %v", err)
	}

	const (
		poolSize     = 3
		messageCount = 30
	)
	rabbitPublisher, err := adapter.NewRabbitMQPublisher(integrationRabbitMQConnection, poolSize)
	if err != nil {
		t.Fatalf("NewRabbitMQPublisher(pool=%d) error = %v", poolSize, err)
	}

	start := make(chan struct{})
	results := make(chan error, messageCount)
	var publishers sync.WaitGroup
	for i := 0; i < messageCount; i++ {
		publishers.Add(1)
		go func(value int) {
			defer publishers.Done()
			<-start
			results <- rabbitPublisher.Publish(ctx, topic, rabbitmq.NewBaseEvent(value))
		}(i)
	}
	close(start)
	publishers.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("pooled RabbitMQ Publish() error = %v, want nil", err)
		}
	}

	received := 0
	for received < messageCount {
		select {
		case _, ok := <-deliveries:
			if !ok {
				t.Fatalf("delivery channel closed after %d/%d messages", received, messageCount)
			}
			received++
		case <-ctx.Done():
			t.Fatalf("received %d/%d pooled messages: %v", received, messageCount, ctx.Err())
		}
	}
}

func assertIntegrationStockZeroDelivery(t *testing.T, delivery amqp.Delivery, exchangeName string, wantEvent *rabbitmq.BaseEvent, skuID int64) {
	t.Helper()
	if delivery.Exchange != exchangeName {
		t.Fatalf("RabbitMQ delivery exchange = %q, want %q", delivery.Exchange, exchangeName)
	}
	if delivery.ContentType != "application/json" {
		t.Fatalf("RabbitMQ delivery content type = %q, want application/json", delivery.ContentType)
	}
	if delivery.DeliveryMode != amqp.Persistent {
		t.Fatalf("RabbitMQ delivery mode = %d, want persistent", delivery.DeliveryMode)
	}

	var got struct {
		ID        string    `json:"id"`
		Timestamp time.Time `json:"timestamp"`
		Data      int64     `json:"data"`
	}
	if err := json.Unmarshal(delivery.Body, &got); err != nil {
		t.Fatalf("unmarshal RabbitMQ delivery %s: %v", fmt.Sprintf("%q", delivery.Body), err)
	}
	if got.ID != wantEvent.ID || got.Data != skuID || !got.Timestamp.Equal(wantEvent.Timestamp) {
		t.Fatalf("RabbitMQ event = %#v, want id=%q data=%d timestamp=%s", got, wantEvent.ID, skuID, wantEvent.Timestamp)
	}
}
