//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"prizeforge/internal/infrastructure/adapter"
	"prizeforge/pkg/rabbitmq"
	"prizeforge/pkg/xrand"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TestRabbitMQPublisherRoutesStockZeroEvent 验证项目发布器会创建 fanout Exchange，
// 将库存耗尽事件以持久化 JSON 消息路由到绑定队列，并且消息可以被正常确认。
func TestRabbitMQPublisherRoutesStockZeroEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	exchangeName := "prizeforge.integration.stock-zero." + xrand.RandomNumeric(12)
	rabbitPublisher, err := adapter.NewRabbitMQPublisher(integrationRabbitMQConnection)
	if err != nil {
		t.Fatalf("NewRabbitMQPublisher() error = %v, want nil", err)
	}
	publisherConfig := *integrationRabbitMQConfig
	publisherConfig.Topic.ActivitySkuStockZero = exchangeName
	publisher := adapter.NewPublisher(rabbitPublisher, &publisherConfig)

	// 第一次发布用于验证项目发布器能够自行声明 Exchange；当时还没有绑定测试队列，
	// 因此这条声明消息按 fanout 语义直接丢弃，不参与后续断言。
	if err := publisher.PublishStockZero(ctx, rabbitmq.NewBaseEvent(int64(-1))); err != nil {
		t.Fatalf("PublishStockZero() declaring exchange error = %v, want nil", err)
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
