package job

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"prizeforge/internal/domain/activity"
	"prizeforge/internal/domain/award"
	"prizeforge/internal/domain/strategy"
	"prizeforge/internal/domain/task"
)

type fakeOutboxTaskService struct {
	queryNoSendMessageTaskListFn     func(context.Context, int) ([]*task.Task, error)
	sendMessageFn                    func(context.Context, *task.Task) error
	updateTaskSendMessageCompletedFn func(context.Context, string, string) error
	updateTaskSendMessageFailFn      func(context.Context, string, string) error
}

func (f *fakeOutboxTaskService) QueryNoSendMessageTaskList(ctx context.Context, dbIndex int) ([]*task.Task, error) {
	if f.queryNoSendMessageTaskListFn == nil {
		panic("unexpected QueryNoSendMessageTaskList call")
	}
	return f.queryNoSendMessageTaskListFn(ctx, dbIndex)
}

func (f *fakeOutboxTaskService) SendMessage(ctx context.Context, taskItem *task.Task) error {
	if f.sendMessageFn == nil {
		panic("unexpected SendMessage call")
	}
	return f.sendMessageFn(ctx, taskItem)
}

func (f *fakeOutboxTaskService) UpdateTaskSendMessageCompleted(ctx context.Context, userID string, messageID string) error {
	if f.updateTaskSendMessageCompletedFn == nil {
		panic("unexpected UpdateTaskSendMessageCompleted call")
	}
	return f.updateTaskSendMessageCompletedFn(ctx, userID, messageID)
}

func (f *fakeOutboxTaskService) UpdateTaskSendMessageFail(ctx context.Context, userID string, messageID string) error {
	if f.updateTaskSendMessageFailFn == nil {
		panic("unexpected UpdateTaskSendMessageFail call")
	}
	return f.updateTaskSendMessageFailFn(ctx, userID, messageID)
}

type fakePartakeOrderService struct {
	saveOrderRecordFn func(context.Context, *activity.CreatePartakeOrder) error
}

func (f *fakePartakeOrderService) SaveOrderRecord(ctx context.Context, aggregate *activity.CreatePartakeOrder) error {
	if f.saveOrderRecordFn == nil {
		panic("unexpected SaveOrderRecord call")
	}
	return f.saveOrderRecordFn(ctx, aggregate)
}

type fakeAwardStockService struct {
	updateStrategyAwardStockFn func(context.Context, string, string, int64, int64) error
}

func (f *fakeAwardStockService) UpdateStrategyAwardStock(ctx context.Context, userID string, orderID string, strategyID int64, awardID int64) error {
	if f.updateStrategyAwardStockFn == nil {
		panic("unexpected UpdateStrategyAwardStock call")
	}
	return f.updateStrategyAwardStockFn(ctx, userID, orderID, strategyID, awardID)
}

// TestSendAwardMessageProcessTaskScansConfiguredDatabases 验证调度任务会按顺序扫描全部
// 配置分库，并且非法 dbCount 会回退为一个分库，避免漏扫或完全不执行补偿任务。
func TestSendAwardMessageProcessTaskScansConfiguredDatabases(t *testing.T) {
	var scanned []int
	taskSvc := &fakeOutboxTaskService{
		queryNoSendMessageTaskListFn: func(_ context.Context, dbIndex int) ([]*task.Task, error) {
			scanned = append(scanned, dbIndex)
			return nil, nil
		},
	}
	job := NewSendAwardMessage(taskSvc, nil, nil, 3)

	err := job.ProcessTask(context.Background(), nil)

	if err != nil {
		t.Fatalf("ProcessTask() error = %v, want nil", err)
	}
	if !reflect.DeepEqual(scanned, []int{1, 2, 3}) {
		t.Fatalf("ProcessTask() scanned databases = %#v, want %#v", scanned, []int{1, 2, 3})
	}
	defaultJob := NewSendAwardMessage(taskSvc, nil, nil, 0)
	if defaultJob.dbCount != 1 {
		t.Fatalf("NewSendAwardMessage() default dbCount = %d, want 1", defaultJob.dbCount)
	}
}

// TestSendAwardMessageProcessTaskStopsOnQueryFailure 验证任意分库查询失败时会返回包含
// 分库编号的包装错误并停止后续扫描，让调度框架能够重试整轮任务。
func TestSendAwardMessageProcessTaskStopsOnQueryFailure(t *testing.T) {
	queryErr := errors.New("query outbox tasks")
	var scanned []int
	taskSvc := &fakeOutboxTaskService{
		queryNoSendMessageTaskListFn: func(_ context.Context, dbIndex int) ([]*task.Task, error) {
			scanned = append(scanned, dbIndex)
			if dbIndex == 2 {
				return nil, queryErr
			}
			return nil, nil
		},
	}
	job := NewSendAwardMessage(taskSvc, nil, nil, 3)

	err := job.ProcessTask(context.Background(), nil)

	if !errors.Is(err, queryErr) {
		t.Fatalf("ProcessTask() error = %v, want wrapped %v", err, queryErr)
	}
	if !reflect.DeepEqual(scanned, []int{1, 2}) {
		t.Fatalf("ProcessTask() scanned databases = %#v, want %#v", scanned, []int{1, 2})
	}
}

// TestSendAwardMessageProcessTaskDispatchesAndWaits 验证扫描得到的任务会进入异步分发，
// ProcessTask 会等待分发和 completed 状态更新结束后再返回，避免调度轮次提前退出。
func TestSendAwardMessageProcessTaskDispatchesAndWaits(t *testing.T) {
	taskItem := &task.Task{UserID: "user-1", Topic: award.SendAwardTopic, MessageID: "message-1"}
	var sentTask *task.Task
	completedCalls := 0
	taskSvc := &fakeOutboxTaskService{
		queryNoSendMessageTaskListFn: func(context.Context, int) ([]*task.Task, error) {
			return []*task.Task{taskItem}, nil
		},
		sendMessageFn: func(_ context.Context, got *task.Task) error {
			sentTask = got
			return nil
		},
		updateTaskSendMessageCompletedFn: func(_ context.Context, userID string, messageID string) error {
			if userID != "user-1" || messageID != "message-1" {
				t.Errorf("UpdateTaskSendMessageCompleted() args = (%q, %q), want user-1/message-1", userID, messageID)
			}
			completedCalls++
			return nil
		},
	}
	job := NewSendAwardMessage(taskSvc, nil, nil, 1)

	err := job.ProcessTask(context.Background(), nil)

	if err != nil {
		t.Fatalf("ProcessTask() error = %v, want nil", err)
	}
	if sentTask != taskItem || completedCalls != 1 {
		t.Fatalf("ProcessTask() result = (sent=%p, completed=%d), want (%p, 1)", sentTask, completedCalls, taskItem)
	}
}

// TestSendAwardMessageDispatchSingleTaskClosesStateLoop 验证消息分发成功后标记 completed，
// 分发失败时标记 fail，completed 更新失败时也降级为 fail，保证任务始终能够被再次补偿。
func TestSendAwardMessageDispatchSingleTaskClosesStateLoop(t *testing.T) {
	dispatchErr := errors.New("dispatch task")
	completedErr := errors.New("complete task")
	tests := []struct {
		name          string
		sendErr       error
		completedErr  error
		wantCompleted int
		wantFailed    int
	}{
		{name: "dispatch success", wantCompleted: 1},
		{name: "dispatch failure", sendErr: dispatchErr, wantFailed: 1},
		{name: "completed update failure", completedErr: completedErr, wantCompleted: 1, wantFailed: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			completedCalls := 0
			failedCalls := 0
			taskItem := &task.Task{UserID: "user-1", Topic: award.SendAwardTopic, MessageID: "message-1"}
			taskSvc := &fakeOutboxTaskService{
				sendMessageFn: func(_ context.Context, got *task.Task) error {
					if got != taskItem {
						t.Errorf("SendMessage() task = %p, want %p", got, taskItem)
					}
					return tt.sendErr
				},
				updateTaskSendMessageCompletedFn: func(_ context.Context, userID string, messageID string) error {
					completedCalls++
					if userID != "user-1" || messageID != "message-1" {
						t.Errorf("UpdateTaskSendMessageCompleted() args = (%q, %q), want user-1/message-1", userID, messageID)
					}
					return tt.completedErr
				},
				updateTaskSendMessageFailFn: func(_ context.Context, userID string, messageID string) error {
					failedCalls++
					if userID != "user-1" || messageID != "message-1" {
						t.Errorf("UpdateTaskSendMessageFail() args = (%q, %q), want user-1/message-1", userID, messageID)
					}
					return nil
				},
			}
			job := NewSendAwardMessage(taskSvc, nil, nil, 1)

			job.dispatchSingleTask(context.Background(), taskItem)

			if completedCalls != tt.wantCompleted || failedCalls != tt.wantFailed {
				t.Fatalf("state update calls = (completed=%d, fail=%d), want (%d, %d)", completedCalls, failedCalls, tt.wantCompleted, tt.wantFailed)
			}
		})
	}
}

// TestSendAwardMessageRouteTaskByTopicDispatchesSupportedTopics 验证发奖、保存订单和库存同步
// 三种 Outbox topic 会解析正确载荷，并把完整业务标识委托给各自领域服务。
func TestSendAwardMessageRouteTaskByTopicDispatchesSupportedTopics(t *testing.T) {
	t.Run("send award", func(t *testing.T) {
		taskItem := &task.Task{Topic: award.SendAwardTopic, MessageID: "message-1"}
		taskSvc := &fakeOutboxTaskService{
			sendMessageFn: func(_ context.Context, got *task.Task) error {
				if got != taskItem {
					t.Fatalf("SendMessage() task = %p, want %p", got, taskItem)
				}
				return nil
			},
		}
		job := NewSendAwardMessage(taskSvc, nil, nil, 1)

		if err := job.routeTaskByTopic(context.Background(), taskItem); err != nil {
			t.Fatalf("routeTaskByTopic() error = %v, want nil", err)
		}
	})

	t.Run("save order", func(t *testing.T) {
		partakeSvc := &fakePartakeOrderService{
			saveOrderRecordFn: func(_ context.Context, aggregate *activity.CreatePartakeOrder) error {
				if aggregate.UserID != "user-1" || aggregate.UserRaffleOrder == nil || aggregate.UserRaffleOrder.OrderID != "order-1" {
					t.Fatalf("SaveOrderRecord() aggregate = %#v, want user-1/order-1", aggregate)
				}
				return nil
			},
		}
		job := NewSendAwardMessage(nil, partakeSvc, nil, 1)

		err := job.routeTaskByTopic(context.Background(), &task.Task{
			Topic:   activity.SaveOrderRecordTopic,
			Message: `{"u":"user-1","o":"order-1"}`,
		})
		if err != nil {
			t.Fatalf("routeTaskByTopic() error = %v, want nil", err)
		}
	})

	t.Run("stock sync", func(t *testing.T) {
		strategySvc := &fakeAwardStockService{
			updateStrategyAwardStockFn: func(_ context.Context, userID string, orderID string, strategyID int64, awardID int64) error {
				if userID != "user-1" || orderID != "order-1" || strategyID != 100001 || awardID != 101 {
					t.Fatalf("UpdateStrategyAwardStock() args = (%q, %q, %d, %d), want user-1/order-1/100001/101", userID, orderID, strategyID, awardID)
				}
				return nil
			},
		}
		job := NewSendAwardMessage(nil, nil, strategySvc, 1)

		err := job.routeTaskByTopic(context.Background(), &task.Task{
			Topic:   strategy.AwardStockSyncTopic,
			Message: `{"user_id":"user-1","order_id":"order-1","strategy_id":100001,"award_id":101}`,
		})
		if err != nil {
			t.Fatalf("routeTaskByTopic() error = %v, want nil", err)
		}
	})
}

// TestSendAwardMessageRouteTaskByTopicRejectsInvalidTasks 验证损坏的订单/库存 JSON 和
// 未支持的 topic 都返回明确错误，不会被误标记为 completed。
func TestSendAwardMessageRouteTaskByTopicRejectsInvalidTasks(t *testing.T) {
	tests := []struct {
		name string
		task *task.Task
	}{
		{name: "invalid save order payload", task: &task.Task{Topic: activity.SaveOrderRecordTopic, Message: "{"}},
		{name: "invalid stock payload", task: &task.Task{Topic: strategy.AwardStockSyncTopic, Message: "{"}},
		{name: "unsupported topic", task: &task.Task{Topic: "unknown"}},
	}
	job := NewSendAwardMessage(nil, &fakePartakeOrderService{}, &fakeAwardStockService{}, 1)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := job.routeTaskByTopic(context.Background(), tt.task); err == nil {
				t.Fatal("routeTaskByTopic() error = nil, want non-nil")
			}
		})
	}
}
