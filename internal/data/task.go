package data

import (
	"big-market-kratos/internal/biz/task"
	"big-market-kratos/internal/data/po"
	"big-market-kratos/pkg/rabbitmq"
	"context"
	"time"
)

type TaskRepository struct {
	routerDB  *DBRouter
	publisher *Publisher
}

func NewTaskRepository(routerDB *DBRouter, publisher *Publisher) task.Repo {
	return &TaskRepository{
		routerDB:  routerDB,
		publisher: publisher,
	}
}

func (r *TaskRepository) QueryNoSendMessageTaskList(ctx context.Context, dbIndx int) ([]*task.Task, error) {
	var tasks []po.Task
	err := r.routerDB.GetDB(dbIndx).WithContext(ctx).
		Where("state = ?", "fail").
		Or("state = ? AND update_time < ?", "create", time.Now().Add(-6*time.Minute)).
		Limit(10).
		Find(&tasks).Error
	if err != nil {
		return nil, err
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
