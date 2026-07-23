package job

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"sync"
	"testing"

	"prizeforge/internal/domain/award"
	"prizeforge/internal/domain/strategy"
	"prizeforge/internal/domain/task"
)

type fakeOutboxTaskService struct {
	queryNoSendMessageTaskListFn          func(context.Context, int) ([]*task.Task, error)
	sendMessageFn                         func(context.Context, *task.Task) error
	updateTaskSendMessageCompletedBatchFn func(context.Context, int, []uint64) error
	updateTaskSendMessageFailBatchFn      func(context.Context, int, []uint64) error
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

func (f *fakeOutboxTaskService) UpdateTaskSendMessageCompletedBatch(ctx context.Context, dbIndex int, taskIDs []uint64) error {
	if f.updateTaskSendMessageCompletedBatchFn == nil {
		panic("unexpected UpdateTaskSendMessageCompletedBatch call")
	}
	return f.updateTaskSendMessageCompletedBatchFn(ctx, dbIndex, taskIDs)
}

func (f *fakeOutboxTaskService) UpdateTaskSendMessageFailBatch(ctx context.Context, dbIndex int, taskIDs []uint64) error {
	if f.updateTaskSendMessageFailBatchFn == nil {
		panic("unexpected UpdateTaskSendMessageFailBatch call")
	}
	return f.updateTaskSendMessageFailBatchFn(ctx, dbIndex, taskIDs)
}

type fakeAwardStockService struct {
	updateStrategyAwardStockBatchFn func(context.Context, []strategy.AwardStockConsumeMessage) error
}

func (f *fakeAwardStockService) UpdateStrategyAwardStockBatch(ctx context.Context, messages []strategy.AwardStockConsumeMessage) error {
	if f.updateStrategyAwardStockBatchFn == nil {
		panic("unexpected UpdateStrategyAwardStockBatch call")
	}
	return f.updateStrategyAwardStockBatchFn(ctx, messages)
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
	job := NewSendAwardMessage(taskSvc, nil, 3)

	err := job.ProcessTask(context.Background(), nil)

	if err != nil {
		t.Fatalf("ProcessTask() error = %v, want nil", err)
	}
	if !reflect.DeepEqual(scanned, []int{1, 2, 3}) {
		t.Fatalf("ProcessTask() scanned databases = %#v, want %#v", scanned, []int{1, 2, 3})
	}
	defaultJob := NewSendAwardMessage(taskSvc, nil, 0)
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
	job := NewSendAwardMessage(taskSvc, nil, 3)

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
	taskItem := &task.Task{ID: 101, UserID: "user-1", Topic: award.SendAwardTopic, MessageID: "message-1"}
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
		updateTaskSendMessageCompletedBatchFn: func(_ context.Context, dbIndex int, taskIDs []uint64) error {
			if dbIndex != 1 || !reflect.DeepEqual(taskIDs, []uint64{101}) {
				t.Errorf("UpdateTaskSendMessageCompletedBatch() args = (%d, %#v), want (1, [101])", dbIndex, taskIDs)
			}
			completedCalls++
			return nil
		},
	}
	job := NewSendAwardMessage(taskSvc, nil, 1)

	err := job.ProcessTask(context.Background(), nil)

	if err != nil {
		t.Fatalf("ProcessTask() error = %v, want nil", err)
	}
	if sentTask != taskItem || completedCalls != 1 {
		t.Fatalf("ProcessTask() result = (sent=%p, completed=%d), want (%p, 1)", sentTask, completedCalls, taskItem)
	}
}

// TestSendAwardMessageProcessTaskGroupsStockByAward 验证跨分库的同一策略奖品库存任务会
// 合并为一次业务批量调用，而处理结果仍按原始分库分别批量关闭 Outbox 状态。
func TestSendAwardMessageProcessTaskGroupsStockByAward(t *testing.T) {
	tasksByDB := map[int][]*task.Task{
		1: {
			{ID: 11, UserID: "user-1", Topic: strategy.AwardStockSyncTopic, MessageID: "stock-1", Message: `{"user_id":"user-1","order_id":"order-1","strategy_id":100001,"award_id":101}`},
			{ID: 12, UserID: "user-2", Topic: strategy.AwardStockSyncTopic, MessageID: "stock-2", Message: `{"user_id":"user-2","order_id":"order-2","strategy_id":100001,"award_id":101}`},
		},
		2: {
			{ID: 21, UserID: "user-3", Topic: strategy.AwardStockSyncTopic, MessageID: "stock-3", Message: `{"user_id":"user-3","order_id":"order-3","strategy_id":100001,"award_id":101}`},
			{ID: 22, UserID: "user-4", Topic: strategy.AwardStockSyncTopic, MessageID: "stock-4", Message: `{"user_id":"user-4","order_id":"order-4","strategy_id":100001,"award_id":102}`},
		},
	}
	var mu sync.Mutex
	var batchSizes []int
	var batchValidationErr error
	completedByDB := make(map[int][]uint64)
	taskSvc := &fakeOutboxTaskService{
		queryNoSendMessageTaskListFn: func(_ context.Context, dbIndex int) ([]*task.Task, error) {
			return tasksByDB[dbIndex], nil
		},
		updateTaskSendMessageCompletedBatchFn: func(_ context.Context, dbIndex int, taskIDs []uint64) error {
			mu.Lock()
			completedByDB[dbIndex] = append([]uint64(nil), taskIDs...)
			mu.Unlock()
			return nil
		},
	}
	strategySvc := &fakeAwardStockService{
		updateStrategyAwardStockBatchFn: func(_ context.Context, messages []strategy.AwardStockConsumeMessage) error {
			mu.Lock()
			defer mu.Unlock()
			if len(messages) == 0 {
				batchValidationErr = errors.New("UpdateStrategyAwardStockBatch() received empty batch")
				return nil
			}
			for _, message := range messages[1:] {
				if message.StrategyID != messages[0].StrategyID || message.AwardID != messages[0].AwardID {
					batchValidationErr = errors.New("batch contains mixed strategy awards")
					return nil
				}
			}
			batchSizes = append(batchSizes, len(messages))
			return nil
		},
	}

	job := NewSendAwardMessage(taskSvc, strategySvc, 2)
	if err := job.ProcessTask(context.Background(), nil); err != nil {
		t.Fatalf("ProcessTask() error = %v, want nil", err)
	}
	if batchValidationErr != nil {
		t.Fatal(batchValidationErr)
	}
	sort.Ints(batchSizes)
	if !reflect.DeepEqual(batchSizes, []int{1, 3}) {
		t.Fatalf("stock batch sizes = %#v, want [1 3]", batchSizes)
	}
	for dbIndex, wantIDs := range map[int][]uint64{1: {11, 12}, 2: {21, 22}} {
		gotIDs := completedByDB[dbIndex]
		sort.Slice(gotIDs, func(i, j int) bool { return gotIDs[i] < gotIDs[j] })
		if !reflect.DeepEqual(gotIDs, wantIDs) {
			t.Fatalf("completed IDs for database %d = %#v, want %#v", dbIndex, gotIDs, wantIDs)
		}
	}
}

// TestSendAwardMessageProcessTaskFailsWholeStockGroup 验证聚合库存事务失败时，同组所有
// Outbox 任务都会进入 fail，且不会有任务被错误标记为 completed。
func TestSendAwardMessageProcessTaskFailsWholeStockGroup(t *testing.T) {
	stockErr := errors.New("batch stock update")
	tasks := []*task.Task{
		{ID: 1, UserID: "user-1", Topic: strategy.AwardStockSyncTopic, MessageID: "stock-1", Message: `{"user_id":"user-1","order_id":"order-1","strategy_id":100001,"award_id":101}`},
		{ID: 2, UserID: "user-2", Topic: strategy.AwardStockSyncTopic, MessageID: "stock-2", Message: `{"user_id":"user-2","order_id":"order-2","strategy_id":100001,"award_id":101}`},
	}
	var failedIDs []uint64
	taskSvc := &fakeOutboxTaskService{
		queryNoSendMessageTaskListFn: func(context.Context, int) ([]*task.Task, error) {
			return tasks, nil
		},
		updateTaskSendMessageFailBatchFn: func(_ context.Context, dbIndex int, taskIDs []uint64) error {
			if dbIndex != 1 {
				t.Fatalf("UpdateTaskSendMessageFailBatch() dbIndex = %d, want 1", dbIndex)
			}
			failedIDs = append([]uint64(nil), taskIDs...)
			return nil
		},
	}
	strategySvc := &fakeAwardStockService{
		updateStrategyAwardStockBatchFn: func(context.Context, []strategy.AwardStockConsumeMessage) error {
			return stockErr
		},
	}

	job := NewSendAwardMessage(taskSvc, strategySvc, 1)
	if err := job.ProcessTask(context.Background(), nil); err != nil {
		t.Fatalf("ProcessTask() error = %v, want nil", err)
	}
	sort.Slice(failedIDs, func(i, j int) bool { return failedIDs[i] < failedIDs[j] })
	if !reflect.DeepEqual(failedIDs, []uint64{1, 2}) {
		t.Fatalf("failed IDs = %#v, want [1 2]", failedIDs)
	}
}

// TestSendAwardMessageProcessTaskBatchesResultsByShard 验证普通任务发送失败和损坏的库存
// 消息都会与成功任务分开收集，并按原始分库各执行一次 completed/fail 批量更新。
func TestSendAwardMessageProcessTaskBatchesResultsByShard(t *testing.T) {
	dispatchErr := errors.New("dispatch task")
	tasksByDB := map[int][]*task.Task{
		1: {
			{ID: 11, UserID: "user-11", Topic: award.SendAwardTopic, MessageID: "message-11"},
			{ID: 12, UserID: "user-12", Topic: award.SendAwardTopic, MessageID: "message-12"},
		},
		2: {
			{ID: 21, UserID: "user-21", Topic: award.SendAwardTopic, MessageID: "message-21"},
			{ID: 22, UserID: "user-22", Topic: strategy.AwardStockSyncTopic, MessageID: "message-22", Message: "{"},
		},
	}
	completedByDB := make(map[int][]uint64)
	failedByDB := make(map[int][]uint64)
	taskSvc := &fakeOutboxTaskService{
		queryNoSendMessageTaskListFn: func(_ context.Context, dbIndex int) ([]*task.Task, error) {
			return tasksByDB[dbIndex], nil
		},
		sendMessageFn: func(_ context.Context, taskItem *task.Task) error {
			if taskItem.ID == 12 {
				return dispatchErr
			}
			return nil
		},
		updateTaskSendMessageCompletedBatchFn: func(_ context.Context, dbIndex int, taskIDs []uint64) error {
			completedByDB[dbIndex] = append([]uint64(nil), taskIDs...)
			return nil
		},
		updateTaskSendMessageFailBatchFn: func(_ context.Context, dbIndex int, taskIDs []uint64) error {
			failedByDB[dbIndex] = append([]uint64(nil), taskIDs...)
			return nil
		},
	}

	job := NewSendAwardMessage(taskSvc, nil, 2)
	if err := job.ProcessTask(context.Background(), nil); err != nil {
		t.Fatalf("ProcessTask() error = %v, want nil", err)
	}

	for dbIndex, wantIDs := range map[int][]uint64{1: {11}, 2: {21}} {
		if !reflect.DeepEqual(completedByDB[dbIndex], wantIDs) {
			t.Fatalf("completed IDs for database %d = %#v, want %#v", dbIndex, completedByDB[dbIndex], wantIDs)
		}
	}
	for dbIndex, wantIDs := range map[int][]uint64{1: {12}, 2: {22}} {
		if !reflect.DeepEqual(failedByDB[dbIndex], wantIDs) {
			t.Fatalf("failed IDs for database %d = %#v, want %#v", dbIndex, failedByDB[dbIndex], wantIDs)
		}
	}
}

// TestSendAwardMessageProcessTaskFallsBackToFailBatch 验证 completed 批量更新失败时，
// 对应任务会降级进入 fail 批次，确保下一轮扫描仍能补偿。
func TestSendAwardMessageProcessTaskFallsBackToFailBatch(t *testing.T) {
	completedErr := errors.New("complete batch")
	var failedIDs []uint64
	taskSvc := &fakeOutboxTaskService{
		queryNoSendMessageTaskListFn: func(context.Context, int) ([]*task.Task, error) {
			return []*task.Task{{ID: 31, UserID: "user-31", Topic: award.SendAwardTopic, MessageID: "message-31"}}, nil
		},
		sendMessageFn: func(context.Context, *task.Task) error { return nil },
		updateTaskSendMessageCompletedBatchFn: func(context.Context, int, []uint64) error {
			return completedErr
		},
		updateTaskSendMessageFailBatchFn: func(_ context.Context, dbIndex int, taskIDs []uint64) error {
			if dbIndex != 1 {
				t.Fatalf("UpdateTaskSendMessageFailBatch() dbIndex = %d, want 1", dbIndex)
			}
			failedIDs = append([]uint64(nil), taskIDs...)
			return nil
		},
	}

	job := NewSendAwardMessage(taskSvc, nil, 1)
	if err := job.ProcessTask(context.Background(), nil); err != nil {
		t.Fatalf("ProcessTask() error = %v, want nil", err)
	}
	if !reflect.DeepEqual(failedIDs, []uint64{31}) {
		t.Fatalf("failed IDs = %#v, want [31]", failedIDs)
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
		job := NewSendAwardMessage(taskSvc, nil, 1)

		if err := job.routeTaskByTopic(context.Background(), taskItem); err != nil {
			t.Fatalf("routeTaskByTopic() error = %v, want nil", err)
		}
	})

	t.Run("stock sync", func(t *testing.T) {
		strategySvc := &fakeAwardStockService{
			updateStrategyAwardStockBatchFn: func(_ context.Context, messages []strategy.AwardStockConsumeMessage) error {
				if len(messages) != 1 {
					t.Fatalf("UpdateStrategyAwardStockBatch() size = %d, want 1", len(messages))
				}
				message := messages[0]
				if message.UserID != "user-1" || message.OrderID != "order-1" || message.StrategyID != 100001 || message.AwardID != 101 {
					t.Fatalf("UpdateStrategyAwardStockBatch() message = %#v, want user-1/order-1/100001/101", message)
				}
				return nil
			},
		}
		job := NewSendAwardMessage(nil, strategySvc, 1)

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
		{name: "invalid stock payload", task: &task.Task{Topic: strategy.AwardStockSyncTopic, Message: "{"}},
		{name: "unsupported topic", task: &task.Task{Topic: "unknown"}},
	}
	job := NewSendAwardMessage(nil, &fakeAwardStockService{}, 1)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := job.routeTaskByTopic(context.Background(), tt.task); err == nil {
				t.Fatal("routeTaskByTopic() error = nil, want non-nil")
			}
		})
	}
}
