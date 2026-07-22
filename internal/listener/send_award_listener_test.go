package listener

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"prizeforge/internal/domain/award"
	"prizeforge/pkg/rabbitmq"
)

type fakeAwardCompletionService struct {
	completeFn func(context.Context, string, string) error
}

func (f *fakeAwardCompletionService) CompleteUserAward(ctx context.Context, userID string, orderID string) error {
	return f.completeFn(ctx, userID, orderID)
}

// TestSendAwardListenerCompletesAward 验证合法发奖消息会使用 user_id 与 order_id
// 调用幂等完成服务，并向 Consumer 返回成功以触发 ACK。
func TestSendAwardListenerCompletesAward(t *testing.T) {
	called := 0
	service := &fakeAwardCompletionService{
		completeFn: func(_ context.Context, userID string, orderID string) error {
			called++
			if userID != "user-1" || orderID != "000000000001" {
				t.Fatalf("CompleteUserAward() args = (%q, %q), want expected identity", userID, orderID)
			}
			return nil
		},
	}

	retry, err := NewSendAwardListener(service).Handle(context.Background(), sendAwardEventBody(t))
	if err != nil || retry {
		t.Fatalf("Handle() = retry:%t err:%v, want success", retry, err)
	}
	if called != 1 {
		t.Fatalf("CompleteUserAward() calls = %d, want 1", called)
	}
}

// TestSendAwardListenerClassifiesFailures 验证数据库临时错误会请求重新投递，而记录缺失、
// 状态冲突等永久错误不会进入无限重试。
func TestSendAwardListenerClassifiesFailures(t *testing.T) {
	tests := []struct {
		name       string
		serviceErr error
		wantRetry  bool
	}{
		{name: "temporary repository error", serviceErr: errors.New("database timeout"), wantRetry: true},
		{name: "award record missing", serviceErr: award.ErrAwardRecordNotFound, wantRetry: false},
		{name: "award state conflict", serviceErr: award.ErrAwardStateConflict, wantRetry: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := &fakeAwardCompletionService{
				completeFn: func(context.Context, string, string) error { return tt.serviceErr },
			}
			retry, err := NewSendAwardListener(service).Handle(context.Background(), sendAwardEventBody(t))
			if !errors.Is(err, tt.serviceErr) || retry != tt.wantRetry {
				t.Fatalf("Handle() = retry:%t err:%v, want retry:%t err:%v", retry, err, tt.wantRetry, tt.serviceErr)
			}
		})
	}
}

// TestSendAwardListenerRejectsMalformedMessages 验证损坏 JSON 或缺少幂等字段的消息会被
// 判定为永久错误，并且不会调用发奖服务。
func TestSendAwardListenerRejectsMalformedMessages(t *testing.T) {
	service := &fakeAwardCompletionService{
		completeFn: func(context.Context, string, string) error {
			t.Fatal("CompleteUserAward() should not be called")
			return nil
		},
	}
	listener := NewSendAwardListener(service)

	for _, body := range [][]byte{
		[]byte(`{"data":`),
		[]byte(`{"id":"message-1","data":{"user_id":"user-1","award_id":101}}`),
	} {
		retry, err := listener.Handle(context.Background(), body)
		if err == nil || retry {
			t.Fatalf("Handle(%q) = retry:%t err:%v, want permanent error", body, retry, err)
		}
	}
}

// TestSendAwardListenerSupportsLegacyMessageID 验证升级前不含 order_id 的 Outbox 消息，
// 可以从既有的 message_id=userID:orderID 中恢复订单幂等键并正常完成。
func TestSendAwardListenerSupportsLegacyMessageID(t *testing.T) {
	service := &fakeAwardCompletionService{
		completeFn: func(_ context.Context, userID string, orderID string) error {
			if userID != "legacy:user" || orderID != "000000000001" {
				t.Fatalf("CompleteUserAward() args = (%q, %q), want legacy identity", userID, orderID)
			}
			return nil
		},
	}
	body := []byte(`{"id":"legacy:user:000000000001","data":{"user_id":"legacy:user","award_id":101,"award_title":"一等奖"}}`)

	retry, err := NewSendAwardListener(service).Handle(context.Background(), body)
	if err != nil || retry {
		t.Fatalf("Handle() = retry:%t err:%v, want legacy message success", retry, err)
	}
}

func sendAwardEventBody(t *testing.T) []byte {
	t.Helper()
	body, err := json.Marshal(rabbitmq.BaseEvent{
		ID:        "message-1",
		Timestamp: time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC),
		Data: award.SendAwardMessage{
			UserID:     "user-1",
			OrderID:    "000000000001",
			AwardID:    101,
			AwardTitle: "一等奖",
		},
	})
	if err != nil {
		t.Fatalf("marshal send_award event: %v", err)
	}
	return body
}
