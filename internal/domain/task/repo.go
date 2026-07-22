package task

import (
	"context"
	"prizeforge/pkg/rabbitmq"
)

type Repo interface {
	QueryNoSendMessageTaskList(ctx context.Context, dbIndx int) ([]*Task, error)
	UpdateTaskSendMessageCompletedBatch(ctx context.Context, dbIndex int, taskIDs []uint64) error
	UpdateTaskSendMessageFailBatch(ctx context.Context, dbIndex int, taskIDs []uint64) error
	SendMessage(ctx context.Context, topic string, event *rabbitmq.BaseEvent) error
}
