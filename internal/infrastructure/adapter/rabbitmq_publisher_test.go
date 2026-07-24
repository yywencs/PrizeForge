package adapter

import (
	"context"
	"testing"

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
