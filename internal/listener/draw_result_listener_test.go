package listener

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"prizeforge/internal/domain/activity"
	"prizeforge/pkg/rabbitmq"
)

type fakeDrawResultPersistenceService struct {
	saveFn func(context.Context, *activity.DrawResult) error
}

func (f *fakeDrawResultPersistenceService) SaveDrawResult(ctx context.Context, result *activity.DrawResult) error {
	return f.saveFn(ctx, result)
}

func TestDrawResultListenerPersistsCompleteResult(t *testing.T) {
	called := 0
	service := &fakeDrawResultPersistenceService{
		saveFn: func(_ context.Context, result *activity.DrawResult) error {
			called++
			if result.UserID != "user-1" || result.OrderID != "000000000001" ||
				result.RequestID != "request-1" || result.AwardID != 101 {
				t.Fatalf("SaveDrawResult() result = %#v", result)
			}
			return nil
		},
	}

	retry, err := NewDrawResultListener(service).Handle(context.Background(), drawResultEventBody(t))
	if err != nil || retry {
		t.Fatalf("Handle() = retry:%t err:%v, want success", retry, err)
	}
	if called != 1 {
		t.Fatalf("SaveDrawResult() calls = %d, want 1", called)
	}
}

func TestDrawResultListenerRetriesTransactionFailure(t *testing.T) {
	transactionErr := errors.New("database timeout")
	service := &fakeDrawResultPersistenceService{
		saveFn: func(context.Context, *activity.DrawResult) error { return transactionErr },
	}

	retry, err := NewDrawResultListener(service).Handle(context.Background(), drawResultEventBody(t))
	if !retry || !errors.Is(err, transactionErr) {
		t.Fatalf("Handle() = retry:%t err:%v, want retryable %v", retry, err, transactionErr)
	}
}

func TestDrawResultListenerRejectsMalformedResult(t *testing.T) {
	service := &fakeDrawResultPersistenceService{
		saveFn: func(context.Context, *activity.DrawResult) error {
			t.Fatal("SaveDrawResult() should not be called")
			return nil
		},
	}
	listener := NewDrawResultListener(service)

	for _, body := range [][]byte{
		[]byte(`{"data":`),
		[]byte(`{"id":"draw-1","data":{"user_id":"user-1","order_id":"000000000001"}}`),
	} {
		retry, err := listener.Handle(context.Background(), body)
		if err == nil || retry {
			t.Fatalf("Handle(%q) = retry:%t err:%v, want permanent error", body, retry, err)
		}
	}
}

func drawResultEventBody(t *testing.T) []byte {
	t.Helper()
	now := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	body, err := json.Marshal(rabbitmq.BaseEvent{
		ID:        "draw:user-1:000000000001",
		Timestamp: now,
		Data: activity.DrawResult{
			UserID:       "user-1",
			ActivityID:   100301,
			ActivityName: "活动",
			StrategyID:   100001,
			OrderID:      "000000000001",
			RequestID:    "request-1",
			OrderTime:    now,
			AwardID:      101,
			AwardTitle:   "一等奖",
			AwardTime:    now.Add(time.Second),
		},
	})
	if err != nil {
		t.Fatalf("marshal draw-result event: %v", err)
	}
	return body
}
