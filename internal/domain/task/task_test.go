package task

import (
	"context"
	"errors"
	"testing"
	"time"

	"prizeforge/pkg/rabbitmq"
)

type fakeTaskRepository struct {
	queryNoSendMessageTaskListFn          func(context.Context, int) ([]*Task, error)
	updateTaskSendMessageCompletedBatchFn func(context.Context, int, []uint64) error
	updateTaskSendMessageFailBatchFn      func(context.Context, int, []uint64) error
	sendMessageFn                         func(context.Context, string, *rabbitmq.BaseEvent) error
}

func (f *fakeTaskRepository) QueryNoSendMessageTaskList(ctx context.Context, dbIndex int) ([]*Task, error) {
	if f.queryNoSendMessageTaskListFn == nil {
		panic("unexpected QueryNoSendMessageTaskList call")
	}
	return f.queryNoSendMessageTaskListFn(ctx, dbIndex)
}

func (f *fakeTaskRepository) UpdateTaskSendMessageCompletedBatch(ctx context.Context, dbIndex int, taskIDs []uint64) error {
	if f.updateTaskSendMessageCompletedBatchFn == nil {
		panic("unexpected UpdateTaskSendMessageCompletedBatch call")
	}
	return f.updateTaskSendMessageCompletedBatchFn(ctx, dbIndex, taskIDs)
}

func (f *fakeTaskRepository) UpdateTaskSendMessageFailBatch(ctx context.Context, dbIndex int, taskIDs []uint64) error {
	if f.updateTaskSendMessageFailBatchFn == nil {
		panic("unexpected UpdateTaskSendMessageFailBatch call")
	}
	return f.updateTaskSendMessageFailBatchFn(ctx, dbIndex, taskIDs)
}

func (f *fakeTaskRepository) SendMessage(ctx context.Context, topic string, event *rabbitmq.BaseEvent) error {
	if f.sendMessageFn == nil {
		panic("unexpected SendMessage call")
	}
	return f.sendMessageFn(ctx, topic, event)
}

// TestTaskUsecaseSendMessageBuildsEvent 验证 Outbox task 会使用 messageID 和可控时间
// 构造 RabbitMQ 信封，合法 JSON 解析为结构化数据，非法 JSON 则保留原始字符串。
func TestTaskUsecaseSendMessageBuildsEvent(t *testing.T) {
	fixedNow := time.Date(2026, time.July, 20, 14, 0, 0, 0, time.UTC)
	tests := []struct {
		name       string
		message    string
		assertData func(*testing.T, interface{})
	}{
		{
			name:    "JSON message",
			message: `{"award_id":101,"user_id":"user-1"}`,
			assertData: func(t *testing.T, data interface{}) {
				t.Helper()
				value, ok := data.(map[string]interface{})
				if !ok {
					t.Fatalf("event.Data type = %T, want map[string]interface{}", data)
				}
				if value["award_id"] != float64(101) || value["user_id"] != "user-1" {
					t.Fatalf("event.Data = %#v, want parsed award payload", value)
				}
			},
		},
		{
			name:    "raw message",
			message: "not-json",
			assertData: func(t *testing.T, data interface{}) {
				t.Helper()
				if data != "not-json" {
					t.Fatalf("event.Data = %#v, want raw message %q", data, "not-json")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &fakeTaskRepository{
				sendMessageFn: func(_ context.Context, topic string, event *rabbitmq.BaseEvent) error {
					if topic != "send_award" {
						t.Fatalf("SendMessage() topic = %q, want %q", topic, "send_award")
					}
					if event == nil || event.ID != "message-1" || event.Timestamp != fixedNow {
						t.Fatalf("SendMessage() event = %#v, want ID message-1 and fixed timestamp", event)
					}
					tt.assertData(t, event.Data)
					return nil
				},
			}
			usecase := NewTaskUsecase(repo)
			usecase.now = func() time.Time { return fixedNow }

			err := usecase.SendMessage(context.Background(), &Task{
				Topic:     "send_award",
				MessageID: "message-1",
				Message:   tt.message,
			})

			if err != nil {
				t.Fatalf("SendMessage() error = %v, want nil", err)
			}
		})
	}
}

// TestTaskUsecaseSendMessagePropagatesRepositoryError 验证 RabbitMQ 发布失败时，
// TaskUsecase 不会吞掉或替换错误，使调度任务能够把对应 Outbox task 标记为 fail。
func TestTaskUsecaseSendMessagePropagatesRepositoryError(t *testing.T) {
	publishErr := errors.New("publish task")
	repo := &fakeTaskRepository{
		sendMessageFn: func(context.Context, string, *rabbitmq.BaseEvent) error {
			return publishErr
		},
	}
	usecase := NewTaskUsecase(repo)

	err := usecase.SendMessage(context.Background(), &Task{
		Topic:     "send_award",
		MessageID: "message-1",
		Message:   `{}`,
	})

	if !errors.Is(err, publishErr) {
		t.Fatalf("SendMessage() error = %v, want %v", err, publishErr)
	}
}

// TestTaskUsecaseDelegatesQueryAndStateUpdates 验证 TaskUsecase 会把分库编号和任务 ID
// 批次原样传给仓储，并原样返回待发送任务以及查询和状态更新错误。
func TestTaskUsecaseDelegatesQueryAndStateUpdates(t *testing.T) {
	queryErr := errors.New("query tasks")
	completedErr := errors.New("complete task")
	failErr := errors.New("fail task")
	wantTasks := []*Task{{UserID: "user-1", MessageID: "message-1"}}
	repo := &fakeTaskRepository{
		queryNoSendMessageTaskListFn: func(_ context.Context, dbIndex int) ([]*Task, error) {
			if dbIndex != 2 {
				t.Fatalf("QueryNoSendMessageTaskList() dbIndex = %d, want 2", dbIndex)
			}
			return wantTasks, queryErr
		},
		updateTaskSendMessageCompletedBatchFn: func(_ context.Context, dbIndex int, taskIDs []uint64) error {
			if dbIndex != 2 || len(taskIDs) != 2 || taskIDs[0] != 11 || taskIDs[1] != 12 {
				t.Fatalf("UpdateTaskSendMessageCompletedBatch() args = (%d, %#v), want (2, [11 12])", dbIndex, taskIDs)
			}
			return completedErr
		},
		updateTaskSendMessageFailBatchFn: func(_ context.Context, dbIndex int, taskIDs []uint64) error {
			if dbIndex != 1 || len(taskIDs) != 1 || taskIDs[0] != 21 {
				t.Fatalf("UpdateTaskSendMessageFailBatch() args = (%d, %#v), want (1, [21])", dbIndex, taskIDs)
			}
			return failErr
		},
	}
	usecase := NewTaskUsecase(repo)

	tasks, err := usecase.QueryNoSendMessageTaskList(context.Background(), 2)
	if !errors.Is(err, queryErr) || len(tasks) != 1 || tasks[0] != wantTasks[0] {
		t.Fatalf("QueryNoSendMessageTaskList() = (%#v, %v), want (%#v, %v)", tasks, err, wantTasks, queryErr)
	}
	if err := usecase.UpdateTaskSendMessageCompletedBatch(context.Background(), 2, []uint64{11, 12}); !errors.Is(err, completedErr) {
		t.Fatalf("UpdateTaskSendMessageCompletedBatch() error = %v, want %v", err, completedErr)
	}
	if err := usecase.UpdateTaskSendMessageFailBatch(context.Background(), 1, []uint64{21}); !errors.Is(err, failErr) {
		t.Fatalf("UpdateTaskSendMessageFailBatch() error = %v, want %v", err, failErr)
	}
}
