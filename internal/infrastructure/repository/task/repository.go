package task

import (
	taskbiz "prizeforge/internal/domain/task"
	"prizeforge/internal/infrastructure/adapter"
	"prizeforge/internal/infrastructure/repository/po"
	"prizeforge/pkg/rabbitmq"
	"context"
	"time"
)

type TaskRepository struct {
	routerDB  *adapter.DBRouter
	publisher *adapter.Publisher
}

func NewTaskRepository(routerDB *adapter.DBRouter, publisher *adapter.Publisher) taskbiz.Repo {
	return &TaskRepository{
		routerDB:  routerDB,
		publisher: publisher,
	}
}

func (r *TaskRepository) QueryNoSendMessageTaskList(ctx context.Context, dbIndx int) ([]*taskbiz.Task, error) {
	const limit = 10

	db := r.routerDB.GetDB(dbIndx).WithContext(ctx)
	timeoutAt := time.Now().Add(-6 * time.Minute)
	columns := []string{"user_id", "topic", "message_id", "message"}

	var tasks []po.Task
	err := db.
		Select(columns).
		Where("state = ?", "fail").
		Order("update_time ASC, id ASC").
		Limit(limit).
		Find(&tasks).Error
	if err != nil {
		return nil, err
	}

	if len(tasks) < limit {
		var createTasks []po.Task
		err = db.
			Select(columns).
			Where("state = ? AND update_time < ?", "create", timeoutAt).
			Order("update_time ASC, id ASC").
			Limit(limit - len(tasks)).
			Find(&createTasks).Error
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, createTasks...)
	}

	result := make([]*taskbiz.Task, 0, len(tasks))
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
