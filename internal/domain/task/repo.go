package task

import (
	"context"
	"prizeforge/pkg/rabbitmq"
)

type Repo interface {
	QueryNoSendMessageTaskList(ctx context.Context, dbIndx int) ([]*Task, error)
	UpdateTaskSendMessageCompleted(ctx context.Context, userID, messageID string) error
	UpdateTaskSendMessageFail(ctx context.Context, userID, messageID string) error
	SendMessage(ctx context.Context, topic string, event *rabbitmq.BaseEvent) error
}
