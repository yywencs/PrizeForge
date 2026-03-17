package task

import (
	"big-market-kratos/pkg/rabbitmq"
	"context"
	"encoding/json"
	"time"
)

type TaskUsecase struct {
	taskRepository Repo
}

func NewTaskUsecase(taskRepository Repo) *TaskUsecase {
	return &TaskUsecase{
		taskRepository: taskRepository,
	}
}

func (s *TaskUsecase) QueryNoSendMessageTaskList(ctx context.Context, dbIndx int) ([]*Task, error) {
	return s.taskRepository.QueryNoSendMessageTaskList(ctx, dbIndx)
}

func (s *TaskUsecase) SendMessage(ctx context.Context, task *Task) error {
	var data interface{}
	// 尝试解析 JSON，如果失败则直接使用字符串
	if err := json.Unmarshal([]byte(task.Message), &data); err != nil {
		data = task.Message
	}

	event := &rabbitmq.BaseEvent{
		ID:        task.MessageID,
		Timestamp: time.Now(),
		Data:      data,
	}

	if err := s.taskRepository.SendAwardMessage(ctx, event); err != nil {
		return err
	}
	return nil
}

func (s *TaskUsecase) UpdateTaskSendMessageCompleted(ctx context.Context, userID, messageID string) error {
	return s.taskRepository.UpdateTaskSendMessageCompleted(ctx, userID, messageID)
}

func (s *TaskUsecase) UpdateTaskSendMessageFail(ctx context.Context, userID, messageID string) error {
	return s.taskRepository.UpdateTaskSendMessageFail(ctx, userID, messageID)
}
