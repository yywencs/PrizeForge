package job

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"prizeforge/internal/domain/activity"
	"prizeforge/pkg/rabbitmq"
)

type fakeDrawResultPublicationStore struct {
	markErr error
	marked  []*activity.DrawResultPublication
}

func (f *fakeDrawResultPublicationStore) MarkDrawResultPublished(_ context.Context, publication *activity.DrawResultPublication) error {
	if f.markErr != nil {
		return f.markErr
	}
	f.marked = append(f.marked, publication)
	return nil
}

type fakeDrawResultRabbitPublisher struct {
	err    error
	events []*rabbitmq.BaseEvent
}

func (f *fakeDrawResultRabbitPublisher) PublishDrawResult(_ context.Context, event *rabbitmq.BaseEvent) error {
	f.events = append(f.events, event)
	return f.err
}

func testDrawResultPublication(streamID, orderID string) *activity.DrawResultPublication {
	result := &activity.DrawResult{
		UserID:     "user-1",
		ActivityID: 100301,
		StrategyID: 100001,
		OrderID:    orderID,
		RequestID:  "request-" + orderID,
		AwardID:    101,
		AwardTime:  time.Date(2026, 7, 20, 13, 14, 15, 0, time.UTC),
	}
	return &activity.DrawResultPublication{StreamID: streamID, Result: result}
}

func TestDrawResultPublisherWaitsForRabbitAndThenMarksStreamEntry(t *testing.T) {
	publication := testDrawResultPublication("1-0", "000000000001")
	store := &fakeDrawResultPublicationStore{}
	publisher := &fakeDrawResultRabbitPublisher{}
	service := NewDrawResultPublisher(store, publisher)

	if err := service.Publish(context.Background(), publication); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if len(publisher.events) != 1 {
		t.Fatalf("published event count = %d, want 1", len(publisher.events))
	}
	event := publisher.events[0]
	if event.ID != "draw:user-1:000000000001" || event.Data != publication.Result {
		t.Fatalf("published event = %#v", event)
	}
	payload, err := json.Marshal(event.Data)
	if err != nil {
		t.Fatalf("marshal RabbitMQ payload: %v", err)
	}
	if strings.Contains(string(payload), "stream_id") || strings.Contains(string(payload), "broker_confirmed") {
		t.Fatalf("RabbitMQ business payload contains delivery metadata: %s", payload)
	}
	if len(store.marked) != 1 || store.marked[0] != publication {
		t.Fatalf("marked publications = %#v, want original publication", store.marked)
	}
}

func TestDrawResultPublisherKeepsStreamEntryWhenRabbitPublishFails(t *testing.T) {
	publishErr := errors.New("confirm timeout")
	store := &fakeDrawResultPublicationStore{}
	publisher := &fakeDrawResultRabbitPublisher{err: publishErr}
	service := NewDrawResultPublisher(store, publisher)

	err := service.Publish(context.Background(), testDrawResultPublication("2-0", "000000000002"))
	if !errors.Is(err, publishErr) {
		t.Fatalf("Publish() error = %v, want %v", err, publishErr)
	}
	if len(store.marked) != 0 {
		t.Fatalf("marked publications = %d, want 0", len(store.marked))
	}
}
