package job

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"prizeforge/internal/domain/activity"
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
	UpdateTaskSendMessageCompleted(context.Context, string, string) error
	UpdateTaskSendMessageFail(context.Context, string, string) error
}

// partakeOrderService 定义保存异步抽奖订单任务所需的能力。
type partakeOrderService interface {
	SaveOrderRecord(context.Context, *activity.CreatePartakeOrder) error
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

type stockTaskGroup struct {
	key      stockGroupKey
	tasks    []*task.Task
	messages []strategy.AwardStockConsumeMessage
}

// SendAwardMessage 定时扫描 task 表，将未发送的消息按 topic 分发。
//
// 这是一个定时任务（由 Asynq Scheduler 每 5 秒触发），不是消费型任务。
// 流程：
//  1. 遍历所有分库，查询状态为 create 的 task 记录
//  2. 对每条 task 按 topic 分发：
//     - send_award → 通过 TaskUsecase.SendMessage 投递到 RabbitMQ
//     - save_order_record → 调用 ActivityPartakeUsecase.SaveOrderRecord 持久化订单
//  3. 分发成功后标记 task 为 completed，失败则标记为 fail
//
// 注意：类型名 SendAwardMessage 是历史遗留，实际职责是"扫描并分发 task 表消息"，
// 不限于发奖。考虑到引用点较多暂不改名。
type SendAwardMessage struct {
	taskSvc     outboxTaskService
	partakeSvc  partakeOrderService
	strategySvc awardStockService
	dbCount     int // 分库数量，决定需要扫描多少个数据库
	scanMu      sync.Mutex
}

// NewSendAwardMessage 创建 SendAwardMessage 定时任务。
//
// dbCount 为分库数量，<= 0 时默认为 1。
func NewSendAwardMessage(
	taskSvc outboxTaskService,
	partakeSvc partakeOrderService,
	strategySvc awardStockService,
	dbCount int,
) *SendAwardMessage {
	if dbCount <= 0 {
		dbCount = 1
	}
	return &SendAwardMessage{
		taskSvc:     taskSvc,
		partakeSvc:  partakeSvc,
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

	var scannedTasks []*task.Task
	for dbIdx := 1; dbIdx <= j.dbCount; dbIdx++ {
		tasks, err := j.taskSvc.QueryNoSendMessageTaskList(ctx, dbIdx)
		if err != nil {
			return fmt.Errorf("查询分库%d未发送任务失败: %w", dbIdx, err)
		}
		scannedTasks = append(scannedTasks, tasks...)
	}

	regularTasks, stockGroups := j.partitionTasks(ctx, scannedTasks)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		runBounded(regularTasks, outboxDispatchConcurrency, func(taskItem *task.Task) {
			j.dispatchSingleTask(ctx, taskItem)
		})
	}()
	go func() {
		defer wg.Done()
		runBounded(stockGroups, stockGroupConcurrency, func(group *stockTaskGroup) {
			j.dispatchStockGroup(ctx, group)
		})
	}()
	wg.Wait()
	return nil
}

func (j *SendAwardMessage) partitionTasks(ctx context.Context, tasks []*task.Task) ([]*task.Task, []*stockTaskGroup) {
	regularTasks := make([]*task.Task, 0, len(tasks))
	groups := make(map[stockGroupKey]*stockTaskGroup)
	for _, taskItem := range tasks {
		if taskItem.Topic != strategy.AwardStockSyncTopic {
			regularTasks = append(regularTasks, taskItem)
			continue
		}

		message, err := decodeStockSyncMessage(taskItem)
		if err != nil {
			j.failTask(ctx, taskItem, err)
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
	return regularTasks, stockGroups
}

func (j *SendAwardMessage) dispatchStockGroup(ctx context.Context, group *stockTaskGroup) {
	if err := j.strategySvc.UpdateStrategyAwardStockBatch(ctx, group.messages); err != nil {
		for _, taskItem := range group.tasks {
			j.failTask(ctx, taskItem, err)
		}
		return
	}
	for _, taskItem := range group.tasks {
		j.completeTask(ctx, taskItem)
	}
}

// dispatchSingleTask 处理单条 task，保证状态闭环。
//
// 成功 → UpdateTaskSendMessageCompleted
// 失败 → UpdateTaskSendMessageFail
// 更新完成状态也失败 → 依然标记为 fail（下次扫描会重试，保证幂等）
func (j *SendAwardMessage) dispatchSingleTask(ctx context.Context, t *task.Task) {
	if err := j.routeTaskByTopic(ctx, t); err != nil {
		j.failTask(ctx, t, err)
		return
	}
	j.completeTask(ctx, t)
}

func (j *SendAwardMessage) completeTask(ctx context.Context, t *task.Task) {
	if err := j.taskSvc.UpdateTaskSendMessageCompleted(ctx, t.UserID, t.MessageID); err != nil {
		logger.Error("更新完成状态失败，降级标记为 fail", "messageID", t.MessageID, "err", err)
		if failErr := j.taskSvc.UpdateTaskSendMessageFail(ctx, t.UserID, t.MessageID); failErr != nil {
			logger.Error("更新失败状态失败", "messageID", t.MessageID, "err", failErr)
		}
	}
}

func (j *SendAwardMessage) failTask(ctx context.Context, t *task.Task, cause error) {
	logger.Warn("分发消息失败，标记为 fail", "messageID", t.MessageID, "err", cause)
	if err := j.taskSvc.UpdateTaskSendMessageFail(ctx, t.UserID, t.MessageID); err != nil {
		logger.Error("更新失败状态失败", "messageID", t.MessageID, "err", err)
	}
}

// routeTaskByTopic 根据 task 的 Topic 字段路由到不同的处理逻辑。
//
// Topic 路由表：
//   - send_award       → 投递到 RabbitMQ（TaskUsecase.SendMessage）
//   - save_order_record → 调用领域服务持久化订单（ActivityPartakeUsecase.SaveOrderRecord）
func (j *SendAwardMessage) routeTaskByTopic(ctx context.Context, t *task.Task) error {
	switch t.Topic {
	case award.SendAwardTopic:
		return j.taskSvc.SendMessage(ctx, t)

	case activity.SaveOrderRecordTopic:
		var message activity.SaveOrderTaskMessage
		if err := json.Unmarshal([]byte(t.Message), &message); err != nil {
			return fmt.Errorf("解析 SaveOrderTaskMessage 失败: %w", err)
		}
		return j.partakeSvc.SaveOrderRecord(ctx, message.ToCreatePartakeOrder())

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
