package taskrepo

import (
	"context"
	"testing"

	"prizeforge/internal/domain/award"
	"prizeforge/pkg/rabbitmq"
)

type recordingTaskEventPublisher struct {
	sendAwardCalls int
	rawTopics      []string
}

func (p *recordingTaskEventPublisher) PublishSendAward(context.Context, *rabbitmq.BaseEvent) error {
	p.sendAwardCalls++
	return nil
}

func (p *recordingTaskEventPublisher) PublishTopic(_ context.Context, topic string, _ *rabbitmq.BaseEvent) error {
	p.rawTopics = append(p.rawTopics, topic)
	return nil
}

func TestTaskRepositoryMapsLogicalSendAwardTopicToConfiguredPublisher(t *testing.T) {
	publisher := &recordingTaskEventPublisher{}
	repository := NewTaskRepository(nil, publisher)
	event := rabbitmq.NewBaseEvent("payload")

	if err := repository.SendMessage(context.Background(), award.SendAwardTopic, event); err != nil {
		t.Fatalf("SendMessage(send_award) error = %v, want nil", err)
	}
	if publisher.sendAwardCalls != 1 {
		t.Fatalf("PublishSendAward calls = %d, want 1", publisher.sendAwardCalls)
	}
	if len(publisher.rawTopics) != 0 {
		t.Fatalf("raw published topics = %#v, want none", publisher.rawTopics)
	}

	const customTopic = "custom_topic"
	if err := repository.SendMessage(context.Background(), customTopic, event); err != nil {
		t.Fatalf("SendMessage(custom) error = %v, want nil", err)
	}
	if len(publisher.rawTopics) != 1 || publisher.rawTopics[0] != customTopic {
		t.Fatalf("raw published topics = %#v, want [%q]", publisher.rawTopics, customTopic)
	}
}
