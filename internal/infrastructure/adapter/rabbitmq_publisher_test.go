package adapter

import (
	"context"
	"strings"
	"testing"
	"time"

	"prizeforge/pkg/config"
	"prizeforge/pkg/rabbitmq"
)

type recordingEventPublisher struct {
	topics []string
	events []*rabbitmq.BaseEvent
}

func (p *recordingEventPublisher) Publish(_ context.Context, topic string, event *rabbitmq.BaseEvent) error {
	p.topics = append(p.topics, topic)
	p.events = append(p.events, event)
	return nil
}

type blockingPublisherSlot struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (s *blockingPublisherSlot) publish(ctx context.Context, _ string, _ *rabbitmq.BaseEvent, _ []byte) error {
	s.started <- struct{}{}
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *blockingPublisherSlot) close() error {
	return nil
}

// TestRabbitMQPublisherUsesPoolSlotsConcurrently 验证三个发布请求可以同时占用
// 三个独立 slot，而不是在一把全局锁后依次等待 Broker Confirm。
func TestRabbitMQPublisherUsesPoolSlotsConcurrently(t *testing.T) {
	const poolSize = 3

	started := make(chan struct{}, poolSize)
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	publisher := &RabbitMQPublisher{slots: make(chan publisherSlot, poolSize)}
	for i := 0; i < poolSize; i++ {
		publisher.slots <- &blockingPublisherSlot{started: started, release: release}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	results := make(chan error, poolSize)
	for i := 0; i < poolSize; i++ {
		go func(value int) {
			results <- publisher.Publish(ctx, "draw_result", rabbitmq.NewBaseEvent(value))
		}(i)
	}

	for i := 0; i < poolSize; i++ {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatalf("only %d/%d publishes entered slots concurrently: %v", i, poolSize, ctx.Err())
		}
	}
	close(release)

	for i := 0; i < poolSize; i++ {
		if err := <-results; err != nil {
			t.Fatalf("concurrent Publish() error = %v, want nil", err)
		}
	}
	if got := len(publisher.slots); got != poolSize {
		t.Fatalf("available publisher slots = %d, want %d", got, poolSize)
	}
}

// TestRabbitMQPublisherStopsWaitingForSlotWhenContextEnds 验证 Channel 池满时，
// 发布请求会遵守调用方超时返回，而不是永久阻塞等待空闲 slot。
func TestRabbitMQPublisherStopsWaitingForSlotWhenContextEnds(t *testing.T) {
	publisher := &RabbitMQPublisher{slots: make(chan publisherSlot, 1)}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := publisher.Publish(ctx, "draw_result", rabbitmq.NewBaseEvent("payload"))
	if err == nil || !strings.Contains(err.Error(), "wait RabbitMQ publisher channel") {
		t.Fatalf("Publish() error = %v, want publisher channel wait error", err)
	}
}

// TestPublisherUsesConfiguredTopics 验证类型化 Publisher 会将每种业务事件
// 发布到配置指定的 Topic，而不是依赖硬编码 Exchange 名称。
func TestPublisherUsesConfiguredTopics(t *testing.T) {
	client := &recordingEventPublisher{}
	publisher := NewPublisher(client, &config.RabbitMQConfig{
		Topic: config.RabbitMQTopicConfig{
			ActivitySkuStockZero: "configured-stock-zero",
			SendAward:            "configured-send-award",
			SendRebate:           "configured-send-rebate",
			DrawResult:           "configured-draw-result",
		},
	})
	event := rabbitmq.NewBaseEvent("payload")

	publishers := []struct {
		name      string
		wantTopic string
		publish   func(context.Context, *rabbitmq.BaseEvent) error
	}{
		{name: "stock zero", wantTopic: "configured-stock-zero", publish: publisher.PublishStockZero},
		{name: "send award", wantTopic: "configured-send-award", publish: publisher.PublishSendAward},
		{name: "send rebate", wantTopic: "configured-send-rebate", publish: publisher.PublishSendRebate},
		{name: "draw result", wantTopic: "configured-draw-result", publish: publisher.PublishDrawResult},
	}

	for _, testCase := range publishers {
		t.Run(testCase.name, func(t *testing.T) {
			client.topics = nil
			client.events = nil
			if err := testCase.publish(context.Background(), event); err != nil {
				t.Fatalf("publish error = %v, want nil", err)
			}
			if len(client.topics) != 1 || client.topics[0] != testCase.wantTopic {
				t.Fatalf("published topics = %#v, want [%q]", client.topics, testCase.wantTopic)
			}
			if len(client.events) != 1 || client.events[0] != event {
				t.Fatalf("published events = %#v, want original event", client.events)
			}
		})
	}
}
