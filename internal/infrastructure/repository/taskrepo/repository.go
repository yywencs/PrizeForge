package taskrepo

import (
	"context"
	"prizeforge/internal/domain/task"
	"prizeforge/internal/infrastructure/adapter"
	"prizeforge/internal/infrastructure/repository/po"
	"prizeforge/pkg/rabbitmq"
	"time"
)

const failedTaskRetryDelay = 6 * time.Minute

type TaskRepository struct {
	routerDB  *adapter.DBRouter
	publisher *adapter.Publisher
}

func NewTaskRepository(routerDB *adapter.DBRouter, publisher *adapter.Publisher) task.Repo {
	return &TaskRepository{
		routerDB:  routerDB,
		publisher: publisher,
	}
}

func (r *TaskRepository) QueryNoSendMessageTaskList(ctx context.Context, dbIndx int) ([]*task.Task, error) {
	const limit = 10

	db := r.routerDB.GetDB(dbIndx).WithContext(ctx)
	retryBefore := time.Now().Add(-failedTaskRetryDelay)
	columns := []string{"user_id", "topic", "message_id", "message"}

	// 失败任务只有经过退避时间后才允许重试，避免下游故障时每轮调度反复发送。
	var tasks []po.Task
	err := db.
		Select(columns).
		Where("state = ? AND update_time < ?", "fail", retryBefore).
		Order("update_time ASC, id ASC").
		Limit(limit).
		Find(&tasks).Error
	if err != nil {
		return nil, err
	}

	if len(tasks) < limit {
		// 新建任务尚未尝试过，应在下一轮调度立即发送，不需要等待失败退避时间。
		var createTasks []po.Task
		err = db.
			Select(columns).
			Where("state = ?", "create").
			Order("update_time ASC, id ASC").
			Limit(limit - len(tasks)).
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

func (r *TaskRepository) UpdateTaskSendMessageCompleted(ctx context.Context, userID, messageID string) error {
	db, _ := r.routerDB.DBStrategy(userID)

	return db.WithContext(ctx).
		Model(&po.Task{}).
		Where("user_id = ? AND message_id = ?", userID, messageID).
		Update("state", "completed").Error
}

func (r *TaskRepository) UpdateTaskSendMessageFail(ctx context.Context, userID, messageID string) error {
	db, _ := r.routerDB.DBStrategy(userID)

	return db.WithContext(ctx).
		Model(&po.Task{}).
		Where("user_id = ? AND message_id = ?", userID, messageID).
		Update("state", "fail").Error
}

func (r *TaskRepository) SendMessage(ctx context.Context, topic string, event *rabbitmq.BaseEvent) error {
	return r.publisher.PublishTopic(ctx, topic, event)
}
