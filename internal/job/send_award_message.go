package job

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"prizeforge/internal/domain/activity"
	"prizeforge/internal/domain/award"
	"prizeforge/internal/domain/task"
	"prizeforge/pkg/logger"

	"github.com/hibiken/asynq"
)

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
	taskSvc    *task.TaskUsecase
	partakeSvc *activity.ActivityPartakeUsecase
	dbCount    int // 分库数量，决定需要扫描多少个数据库
}

// NewSendAwardMessage 创建 SendAwardMessage 定时任务。
//
// dbCount 为分库数量，<= 0 时默认为 1。
func NewSendAwardMessage(
	taskSvc *task.TaskUsecase,
	partakeSvc *activity.ActivityPartakeUsecase,
	dbCount int,
) *SendAwardMessage {
	if dbCount <= 0 {
		dbCount = 1
	}
	return &SendAwardMessage{
		taskSvc:    taskSvc,
		partakeSvc: partakeSvc,
		dbCount:    dbCount,
	}
}

// ProcessTask 扫描所有分库中未发送的 task 记录并并发分发。
//
// 每个分库独立查询，每条 task 在独立 goroutine 中处理以提升吞吐。
// wg.Wait() 确保本轮所有分发完成后再返回。
func (j *SendAwardMessage) ProcessTask(ctx context.Context, _ *asynq.Task) error {
	var wg sync.WaitGroup
	for dbIdx := 1; dbIdx <= j.dbCount; dbIdx++ {
		tasks, err := j.taskSvc.QueryNoSendMessageTaskList(ctx, dbIdx)
		if err != nil {
			return fmt.Errorf("查询分库%d未发送任务失败: %w", dbIdx, err)
		}
		if tasks == nil {
			continue
		}
		for _, t := range tasks {
			wg.Add(1)
			// 务必将循环变量作为参数传入闭包，避免所有 goroutine 共享同一个引用
			go func(taskItem *task.Task) {
				defer wg.Done()
				j.dispatchSingleTask(ctx, taskItem)
			}(t)
		}
	}
	wg.Wait()
	return nil
}

// dispatchSingleTask 处理单条 task，保证状态闭环。
//
// 成功 → UpdateTaskSendMessageCompleted
// 失败 → UpdateTaskSendMessageFail
// 更新完成状态也失败 → 依然标记为 fail（下次扫描会重试，保证幂等）
func (j *SendAwardMessage) dispatchSingleTask(ctx context.Context, t *task.Task) {
	if err := j.routeTaskByTopic(ctx, t); err != nil {
		logger.Warn("分发消息失败，标记为 fail", "messageID", t.MessageID, "err", err)
		j.taskSvc.UpdateTaskSendMessageFail(ctx, t.UserID, t.MessageID)
		return
	}

	if err := j.taskSvc.UpdateTaskSendMessageCompleted(ctx, t.UserID, t.MessageID); err != nil {
		logger.Error("更新完成状态失败，降级标记为 fail", "messageID", t.MessageID, "err", err)
		j.taskSvc.UpdateTaskSendMessageFail(ctx, t.UserID, t.MessageID)
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

	default:
		return fmt.Errorf("不支持的任务 topic: %s", t.Topic)
	}
}
