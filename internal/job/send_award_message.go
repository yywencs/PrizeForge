package job

import (
	taskSvc "big-market-kratos/internal/biz/task"
	"big-market-kratos/internal/conf"
	"context"
	"log/slog"
	"sync"

	"github.com/hibiken/asynq"
)

type SendAwardMessage struct {
	taskSvc *taskSvc.TaskUsecase
	dbCount int
}

func NewSendAwardMessage(taskSvc *taskSvc.TaskUsecase, conf *conf.Data_Mysql) *SendAwardMessage {
	return &SendAwardMessage{taskSvc: taskSvc, dbCount: int(conf.DbCount)}
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
				// 执行你定义的 dispatchSingleTask
				// 内部已经处理了 UpdateTaskSendMessageFail，所以这里不需要返回 error
				j.dispatchSingleTask(ctx, taskItem)
			}(t)
		}
	}
	wg.Wait()
	return nil
}

// dispatchSingleTask 封装单条任务的处理逻辑，保证状态闭环
func (j *SendAwardMessage) dispatchSingleTask(ctx context.Context, t *taskSvc.Task) {
	// 尝试发送消息
	err := j.taskSvc.SendMessage(ctx, t)

	if err != nil {
		// 发送失败：更新状态为 Fail
		slog.Warn("Send message failed, updating status to fail", "taskID", t.MessageID)
		j.taskSvc.UpdateTaskSendMessageFail(ctx, t.UserID, t.MessageID)
		return
	}

	// 发送成功：尝试更新状态为 Completed
	if err := j.taskSvc.UpdateTaskSendMessageCompleted(ctx, t.UserID, t.MessageID); err != nil {
		// 更新成功状态失败：依然标记为 Fail，交给下次扫描重试（保证幂等）
		slog.Error("Update completed status failed", "taskID", t.MessageID, "err", err)
		j.taskSvc.UpdateTaskSendMessageFail(ctx, t.UserID, t.MessageID)
		return
	}

}
