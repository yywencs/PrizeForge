package taskrepo

import (
	"context"
	"fmt"
	"time"

	"prizeforge/internal/domain/award"
	"prizeforge/internal/domain/task"
	"prizeforge/internal/infrastructure/adapter"
	"prizeforge/internal/infrastructure/repository/po"
	"prizeforge/pkg/rabbitmq"
)

const failedTaskRetryDelay = 6 * time.Minute
const outboxScanBatchSize = 500
const outboxStateUpdateBatchSize = 500

type TaskRepository struct {
	routerDB  *adapter.DBRouter
	publisher taskEventPublisher
}

type taskEventPublisher interface {
	PublishSendAward(context.Context, *rabbitmq.BaseEvent) error
	PublishTopic(context.Context, string, *rabbitmq.BaseEvent) error
}

func NewTaskRepository(routerDB *adapter.DBRouter, publisher taskEventPublisher) task.Repo {
	return &TaskRepository{
		routerDB:  routerDB,
		publisher: publisher,
	}
}

func (r *TaskRepository) QueryNoSendMessageTaskList(ctx context.Context, dbIndx int) ([]*task.Task, error) {
	db := r.routerDB.GetDB(dbIndx).WithContext(ctx)
	retryBefore := time.Now().Add(-failedTaskRetryDelay)
	columns := []string{"id", "user_id", "topic", "message_id", "message"}

	// 失败任务只有经过退避时间后才允许重试，避免下游故障时每轮调度反复发送。
	var tasks []po.Task
	err := db.
		Select(columns).
		Where("state = ? AND update_time < ?", "fail", retryBefore).
		Order("update_time ASC, id ASC").
		Limit(outboxScanBatchSize).
		Find(&tasks).Error
	if err != nil {
		return nil, err
	}

	if len(tasks) < outboxScanBatchSize {
		// 新建任务尚未尝试过，应在下一轮调度立即发送，不需要等待失败退避时间。
		var createTasks []po.Task
		err = db.
			Select(columns).
			Where("state = ?", "create").
			Order("update_time ASC, id ASC").
			Limit(outboxScanBatchSize - len(tasks)).
			Find(&createTasks).Error
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, createTasks...)
	}

	result := make([]*task.Task, 0, len(tasks))
	for _, task := range tasks {
		result = append(result, task.ToEntity())
	}
	return result, nil
}

func (r *TaskRepository) UpdateTaskSendMessageCompletedBatch(ctx context.Context, dbIndex int, taskIDs []uint64) error {
	return r.updateTaskStateBatch(ctx, dbIndex, taskIDs, "completed")
}

func (r *TaskRepository) UpdateTaskSendMessageFailBatch(ctx context.Context, dbIndex int, taskIDs []uint64) error {
	return r.updateTaskStateBatch(ctx, dbIndex, taskIDs, "fail")
}

// updateTaskStateBatch 在指定分库内按主键批量更新 Outbox 状态。
// 状态条件避免并发补偿把已经 completed 的任务重新降级为 fail。
func (r *TaskRepository) updateTaskStateBatch(ctx context.Context, dbIndex int, taskIDs []uint64, state string) error {
	if len(taskIDs) == 0 {
		return nil
	}
	db := r.routerDB.GetDB(dbIndex)
	if db == nil {
		return fmt.Errorf("database shard %d is not configured", dbIndex)
	}

	for start := 0; start < len(taskIDs); start += outboxStateUpdateBatchSize {
		end := min(start+outboxStateUpdateBatchSize, len(taskIDs))
		if err := db.WithContext(ctx).
			Model(&po.Task{}).
			Where("id IN ? AND state IN ?", taskIDs[start:end], []string{"create", "fail"}).
			Update("state", state).Error; err != nil {
			return fmt.Errorf("update shard %d task state to %s: %w", dbIndex, state, err)
		}
	}
	return nil
}

func (r *TaskRepository) SendMessage(ctx context.Context, topic string, event *rabbitmq.BaseEvent) error {
	if topic == award.SendAwardTopic {
		return r.publisher.PublishSendAward(ctx, event)
	}
	return r.publisher.PublishTopic(ctx, topic, event)
}
