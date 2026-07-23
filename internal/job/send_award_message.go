package job

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"prizeforge/internal/domain/award"
	"prizeforge/internal/domain/strategy"
	"prizeforge/internal/domain/task"
	"prizeforge/pkg/logger"

	"github.com/hibiken/asynq"
)

// outboxTaskService 定义 Outbox 调度器查询、发送和更新任务状态所需的能力。
type outboxTaskService interface {
	QueryNoSendMessageTaskList(context.Context, int) ([]*task.Task, error)
	SendMessage(context.Context, *task.Task) error
	UpdateTaskSendMessageCompletedBatch(context.Context, int, []uint64) error
	UpdateTaskSendMessageFailBatch(context.Context, int, []uint64) error
}

// awardStockService 定义同步策略奖品库存任务所需的能力。
type awardStockService interface {
	UpdateStrategyAwardStockBatch(context.Context, []strategy.AwardStockConsumeMessage) error
}

const (
	outboxDispatchConcurrency = 8
	stockGroupConcurrency     = 2
)

type stockGroupKey struct {
	strategyID int64
	awardID    int64
}

type scannedTask struct {
	dbIndex int
	task    *task.Task
}

type stockTaskGroup struct {
	key      stockGroupKey
	tasks    []*scannedTask
	messages []strategy.AwardStockConsumeMessage
}

type taskDispatchResult struct {
	task *scannedTask
	err  error
}

type taskStateBatch struct {
	completedIDs []uint64
	failedIDs    []uint64
}

// SendAwardMessage 定时扫描 task 表，将未发送的消息按 topic 分发。
//
// 这是一个定时任务（由 Asynq Scheduler 每 5 秒触发），不是消费型任务。
// 流程：
//  1. 遍历所有分库，查询状态为 create 的 task 记录
//  2. 对每条 task 按 topic 分发：
//     - send_award → 通过 TaskUsecase.SendMessage 投递到 RabbitMQ
//  3. 分发成功后标记 task 为 completed，失败则标记为 fail。
//
// 注意：类型名 SendAwardMessage 是历史遗留，实际职责是"扫描并分发 task 表消息"，
// 不限于发奖。考虑到引用点较多暂不改名。
type SendAwardMessage struct {
	taskSvc     outboxTaskService
	strategySvc awardStockService
	dbCount     int // 分库数量，决定需要扫描多少个数据库
	scanMu      sync.Mutex
}

// NewSendAwardMessage 创建 SendAwardMessage 定时任务。
//
// dbCount 为分库数量，<= 0 时默认为 1。
func NewSendAwardMessage(
	taskSvc outboxTaskService,
	strategySvc awardStockService,
	dbCount int,
) *SendAwardMessage {
	if dbCount <= 0 {
		dbCount = 1
	}
	return &SendAwardMessage{
		taskSvc:     taskSvc,
		strategySvc: strategySvc,
		dbCount:     dbCount,
	}
}

// ProcessTask 扫描所有分库中未发送的 task 记录并分发。
// 普通消息使用固定大小的工作池；库存消息按策略奖品分组，同组聚合为一次库存更新，
// 不再为每条消息创建 goroutine 去争抢同一条库存行。
func (j *SendAwardMessage) ProcessTask(ctx context.Context, _ *asynq.Task) error {
	// 当前进程内只允许一轮扫描运行，避免上一轮尚未完成时再次取到同一批 create 任务。
	if !j.scanMu.TryLock() {
		return nil
	}
	defer j.scanMu.Unlock()

	var scannedTasks []*scannedTask
	for dbIdx := 1; dbIdx <= j.dbCount; dbIdx++ {
		tasks, err := j.taskSvc.QueryNoSendMessageTaskList(ctx, dbIdx)
		if err != nil {
			return fmt.Errorf("查询分库%d未发送任务失败: %w", dbIdx, err)
		}
		for _, taskItem := range tasks {
			scannedTasks = append(scannedTasks, &scannedTask{dbIndex: dbIdx, task: taskItem})
		}
	}

	regularTasks, stockGroups, results := j.partitionTasks(scannedTasks)
	resultCh := make(chan taskDispatchResult, len(scannedTasks))
	for _, result := range results {
		resultCh <- result
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		runBounded(regularTasks, outboxDispatchConcurrency, func(taskItem *scannedTask) {
			resultCh <- taskDispatchResult{task: taskItem, err: j.dispatchSingleTask(ctx, taskItem.task)}
		})
	}()
	go func() {
		defer wg.Done()
		runBounded(stockGroups, stockGroupConcurrency, func(group *stockTaskGroup) {
			err := j.dispatchStockGroup(ctx, group)
			for _, taskItem := range group.tasks {
				resultCh <- taskDispatchResult{task: taskItem, err: err}
			}
		})
	}()
	wg.Wait()
	close(resultCh)
	j.persistTaskResults(ctx, resultCh)
	return nil
}

func (j *SendAwardMessage) partitionTasks(tasks []*scannedTask) ([]*scannedTask, []*stockTaskGroup, []taskDispatchResult) {
	regularTasks := make([]*scannedTask, 0, len(tasks))
	groups := make(map[stockGroupKey]*stockTaskGroup)
	failedResults := make([]taskDispatchResult, 0)
	for _, taskItem := range tasks {
		if taskItem.task.Topic != strategy.AwardStockSyncTopic {
			regularTasks = append(regularTasks, taskItem)
			continue
		}

		message, err := decodeStockSyncMessage(taskItem.task)
		if err != nil {
			failedResults = append(failedResults, taskDispatchResult{task: taskItem, err: err})
			continue
		}
		key := stockGroupKey{strategyID: message.StrategyID, awardID: message.AwardID}
		group := groups[key]
		if group == nil {
			group = &stockTaskGroup{key: key}
			groups[key] = group
		}
		group.tasks = append(group.tasks, taskItem)
		group.messages = append(group.messages, message)
	}

	stockGroups := make([]*stockTaskGroup, 0, len(groups))
	for _, group := range groups {
		stockGroups = append(stockGroups, group)
	}
	sort.Slice(stockGroups, func(i, k int) bool {
		if stockGroups[i].key.strategyID == stockGroups[k].key.strategyID {
			return stockGroups[i].key.awardID < stockGroups[k].key.awardID
		}
		return stockGroups[i].key.strategyID < stockGroups[k].key.strategyID
	})
	return regularTasks, stockGroups, failedResults
}

func (j *SendAwardMessage) dispatchStockGroup(ctx context.Context, group *stockTaskGroup) error {
	return j.strategySvc.UpdateStrategyAwardStockBatch(ctx, group.messages)
}

// dispatchSingleTask 处理单条普通任务，状态由本轮任务完成后统一批量回写。
func (j *SendAwardMessage) dispatchSingleTask(ctx context.Context, t *task.Task) error {
	return j.routeTaskByTopic(ctx, t)
}

// persistTaskResults 汇总本轮处理结果，并按分库分别批量写入 completed/fail 状态。
func (j *SendAwardMessage) persistTaskResults(ctx context.Context, results <-chan taskDispatchResult) {
	batches := make(map[int]*taskStateBatch, j.dbCount)
	for result := range results {
		if result.task == nil || result.task.task == nil {
			logger.Error("Outbox 处理结果缺少任务")
			continue
		}
		if result.task.task.ID == 0 {
			logger.Error("Outbox 任务缺少主键，无法批量更新状态", "messageID", result.task.task.MessageID)
			continue
		}

		batch := batches[result.task.dbIndex]
		if batch == nil {
			batch = &taskStateBatch{}
			batches[result.task.dbIndex] = batch
		}
		if result.err != nil {
			logger.Warn("分发消息失败，标记为 fail", "messageID", result.task.task.MessageID, "err", result.err)
			batch.failedIDs = append(batch.failedIDs, result.task.task.ID)
			continue
		}
		batch.completedIDs = append(batch.completedIDs, result.task.task.ID)
	}

	for dbIndex := 1; dbIndex <= j.dbCount; dbIndex++ {
		batch := batches[dbIndex]
		if batch == nil {
			continue
		}
		failedIDs := batch.failedIDs
		if len(batch.completedIDs) > 0 {
			if err := j.taskSvc.UpdateTaskSendMessageCompletedBatch(ctx, dbIndex, batch.completedIDs); err != nil {
				logger.Error("批量更新完成状态失败，降级标记为 fail", "dbIndex", dbIndex, "taskCount", len(batch.completedIDs), "err", err)
				failedIDs = append(failedIDs, batch.completedIDs...)
			}
		}
		if len(failedIDs) > 0 {
			if err := j.taskSvc.UpdateTaskSendMessageFailBatch(ctx, dbIndex, failedIDs); err != nil {
				logger.Error("批量更新失败状态失败", "dbIndex", dbIndex, "taskCount", len(failedIDs), "err", err)
			}
		}
	}
}

// routeTaskByTopic 根据 task 的 Topic 字段路由到不同的处理逻辑。
//
// Topic 路由表：
//   - send_award       → 投递到 RabbitMQ（TaskUsecase.SendMessage）
func (j *SendAwardMessage) routeTaskByTopic(ctx context.Context, t *task.Task) error {
	switch t.Topic {
	case award.SendAwardTopic:
		return j.taskSvc.SendMessage(ctx, t)

	case strategy.AwardStockSyncTopic:
		message, err := decodeStockSyncMessage(t)
		if err != nil {
			return err
		}
		return j.strategySvc.UpdateStrategyAwardStockBatch(ctx, []strategy.AwardStockConsumeMessage{message})

	default:
		return fmt.Errorf("不支持的任务 topic: %s", t.Topic)
	}
}

func decodeStockSyncMessage(t *task.Task) (strategy.AwardStockConsumeMessage, error) {
	var message strategy.AwardStockConsumeMessage
	if err := json.Unmarshal([]byte(t.Message), &message); err != nil {
		return strategy.AwardStockConsumeMessage{}, fmt.Errorf("解析 AwardStockConsumeMessage 失败: %w", err)
	}
	if message.UserID == "" || message.OrderID == "" || message.StrategyID <= 0 || message.AwardID <= 0 {
		return strategy.AwardStockConsumeMessage{}, fmt.Errorf("库存同步消息缺少必要字段")
	}
	return message, nil
}

func runBounded[T any](items []T, concurrency int, handler func(T)) {
	if len(items) == 0 {
		return
	}
	if concurrency <= 0 || concurrency > len(items) {
		concurrency = len(items)
	}

	jobs := make(chan T)
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for range concurrency {
		go func() {
			defer wg.Done()
			for item := range jobs {
				handler(item)
			}
		}()
	}
	for _, item := range items {
		jobs <- item
	}
	close(jobs)
	wg.Wait()
}
