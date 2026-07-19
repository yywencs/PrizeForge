package award

import (
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

func (s *AwardUsecase) SaveUserAwardRecord(ctx context.Context, userAwardRecord *UserAwardRecord) (*UserAwardRecord, error) {
	sendAwardMessage := SendAwardMessage{
		UserID:     userAwardRecord.UserID,
		AwardID:    userAwardRecord.AwardID,
		AwardTitle: userAwardRecord.AwardTitle,
	}

	task := &Task{
		UserID: userAwardRecord.UserID,
		Topic:  SendAwardTopic,
		// userID+orderID 作为跨分片发奖幂等键；同一中奖订单只允许产生一条发奖任务。
		MessageID: userAwardRecord.UserID + ":" + userAwardRecord.OrderID,
		Message:   sendAwardMessage,
		State:     TaskStateCreate,
	}

	aggregate := &UserAwardTaskInfo{
		UserAwardRecord: userAwardRecord,
		Task:            task,
	}

	return s.repo.SaveUserAwardRecord(ctx, aggregate)
}

// QueryByOrderID 按 orderID 查询中奖记录。nil 表示该订单尚未落库。
func (s *AwardUsecase) QueryByOrderID(ctx context.Context, userID string, orderID string) (*UserAwardRecord, error) {
	return s.repo.QueryByOrderID(ctx, userID, orderID)
}
