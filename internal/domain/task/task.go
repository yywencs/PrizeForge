package task

import (
	"context"
	"encoding/json"
	"prizeforge/pkg/rabbitmq"
	"time"
)

type TaskUsecase struct {
	taskRepository Repo
	now            func() time.Time
}

func NewTaskUsecase(taskRepository Repo) *TaskUsecase {
	return &TaskUsecase{
		taskRepository: taskRepository,
		now:            time.Now,
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
		Timestamp: s.now(),
		Data:      data,
	}

	if err := s.taskRepository.SendMessage(ctx, task.Topic, event); err != nil {
		return err
	}
	return nil
}

func (s *TaskUsecase) UpdateTaskSendMessageCompletedBatch(ctx context.Context, dbIndex int, taskIDs []uint64) error {
	return s.taskRepository.UpdateTaskSendMessageCompletedBatch(ctx, dbIndex, taskIDs)
}

func (s *TaskUsecase) UpdateTaskSendMessageFailBatch(ctx context.Context, dbIndex int, taskIDs []uint64) error {
	return s.taskRepository.UpdateTaskSendMessageFailBatch(ctx, dbIndex, taskIDs)
}
