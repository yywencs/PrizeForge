package task

import (
	"big-market-kratos/pkg/rabbitmq"
	"context"
)

type Repo interface {
	QueryNoSendMessageTaskList(ctx context.Context, dbIndx int) ([]*Task, error)
	UpdateTaskSendMessageCompleted(ctx context.Context, userID, messageID string) error
	UpdateTaskSendMessageFail(ctx context.Context, userID, messageID string) error
	SendMessage(ctx context.Context, topic string, event *rabbitmq.BaseEvent) error
}
