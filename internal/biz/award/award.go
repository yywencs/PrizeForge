package award

import (
	"big-market-kratos/pkg/rabbitmq"
	"context"
)

type AwardUsecase struct {
	repo Repo
}

func NewAwardUsecase(repo Repo) *AwardUsecase {
	return &AwardUsecase{
		repo: repo,
	}
}

func (s *AwardUsecase) SaveUserAwardRecord(ctx context.Context, userAwardRecord *UserAwardRecord) error {
	sendAwardMessage := SendAwardMessage{
		UserID:     userAwardRecord.UserID,
		AwardID:    userAwardRecord.AwardID,
		AwardTitle: userAwardRecord.AwardTitle,
	}

	baseEvent := rabbitmq.NewBaseEvent(sendAwardMessage)

	task := &Task{
		UserID:    userAwardRecord.UserID,
		Topic:     SendAwardTopic,
		MessageID: baseEvent.ID,
		Message:   sendAwardMessage,
		State:     TaskStateCreate,
	}

	aggregate := &UserAwardTaskInfo{
		UserAwardRecord: userAwardRecord,
		Task:            task,
	}

	return s.repo.SaveUserAwardRecord(ctx, aggregate)
}
