package job

import (
	"big-market-kratos/internal/biz/activity"
	"big-market-kratos/internal/biz/award"
	taskSvc "big-market-kratos/internal/biz/task"
	"big-market-kratos/internal/conf"
	"big-market-kratos/pkg/logger"
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/hibiken/asynq"
)

type SendAwardMessage struct {
	taskSvc            *taskSvc.TaskUsecase
	activityPartakeSvc *activity.ActivityPartakeUsecase
	dbCount            int
}

func NewSendAwardMessage(taskSvc *taskSvc.TaskUsecase, activityPartakeSvc *activity.ActivityPartakeUsecase, conf *conf.Data_Mysql) *SendAwardMessage {
	return &SendAwardMessage{
		taskSvc:            taskSvc,
		activityPartakeSvc: activityPartakeSvc,
		dbCount:            int(conf.DbCount),
	}
}

func (j *SendAwardMessage) ProcessTask(ctx context.Context, task *asynq.Task) error {
	var wg sync.WaitGroup
	for i := 1; i <= j.dbCount; i++ {
		task, err := j.taskSvc.QueryNoSendMessageTaskList(ctx, i)
		if err != nil {
			return err
		}
		if task == nil {
			continue
		}
		for _, t := range task {
			wg.Add(1)
			// 务必将 t 作为参数传入，避免闭包引用同一个变量
			go func(taskItem *taskSvc.Task) {
				defer wg.Done()
				j.dispatchSingleTask(ctx, taskItem)
			}(t)
		}
	}
	wg.Wait()
	return nil
}

// dispatchSingleTask 封装单条任务的处理逻辑，保证状态闭环
func (j *SendAwardMessage) dispatchSingleTask(ctx context.Context, t *taskSvc.Task) {
	err := j.routeTaskByTopic(ctx, t)

	if err != nil {
		// 发送失败：更新状态为 Fail
		logger.Warn("Send message failed, updating status to fail", "taskID", t.MessageID)
		j.taskSvc.UpdateTaskSendMessageFail(ctx, t.UserID, t.MessageID)
		return
	}

	// 发送成功：尝试更新状态为 Completed
	if err := j.taskSvc.UpdateTaskSendMessageCompleted(ctx, t.UserID, t.MessageID); err != nil {
		// 更新成功状态失败：依然标记为 Fail，交给下次扫描重试（保证幂等）
		logger.Error("Update completed status failed", "taskID", t.MessageID, "err", err)
		j.taskSvc.UpdateTaskSendMessageFail(ctx, t.UserID, t.MessageID)
		return
	}

}

func (j *SendAwardMessage) routeTaskByTopic(ctx context.Context, t *taskSvc.Task) error {
	switch t.Topic {
	case award.SendAwardTopic:
		return j.taskSvc.SendMessage(ctx, t)
	case activity.SaveOrderRecordTopic:
		var message activity.SaveOrderTaskMessage
		if err := json.Unmarshal([]byte(t.Message), &message); err != nil {
			return fmt.Errorf("unmarshal save order task failed: %w", err)
		}
		return j.activityPartakeSvc.SaveOrderRecord(ctx, message.ToCreatePartakeOrder())
	default:
		return fmt.Errorf("unsupported task topic: %s", t.Topic)
	}
}
