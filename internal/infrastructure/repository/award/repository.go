package award

import (
	awardbiz "prizeforge/internal/domain/award"
	"prizeforge/internal/infrastructure/adapter"
	"prizeforge/internal/infrastructure/repository/po"
	"prizeforge/internal/metrics"
	"prizeforge/pkg/cache"
	"prizeforge/pkg/logger"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"gorm.io/gorm"
)

type UserAwardRecord struct {
	routerDB  *adapter.DBRouter
	publisher *adapter.Publisher
	redis     *cache.Cache
}

func NewUserAwardRecordRepository(db *adapter.DBRouter, redis *cache.Cache, publisher *adapter.Publisher) awardbiz.Repo {
	return &UserAwardRecord{
		routerDB:  db,
		redis:     redis,
		publisher: publisher,
	}
}

func (r *UserAwardRecord) SaveUserAwardRecord(ctx context.Context, aggregate *awardbiz.UserAwardTaskInfo) error {
	result := "success"
	defer func() {
		metrics.IncAward(aggregate.UserAwardRecord.AwardID, result)
	}()

	userAwardRecordPO := convertToUserAwardRecordPO(aggregate.UserAwardRecord)
	taskPO, taskErr := convertToTaskPO(aggregate.Task)
	if taskErr != nil {
		result = "payload_marshal_error"
		return taskErr
	}

	db, tableSuffix := r.routerDB.DBStrategy(aggregate.UserAwardRecord.UserID)
	duplicate := false

	txnErr := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if createAwardErr := tx.Table("user_award_record_" + tableSuffix).Create(userAwardRecordPO).Error; createAwardErr != nil {
			if errors.Is(createAwardErr, gorm.ErrDuplicatedKey) || strings.Contains(createAwardErr.Error(), "Duplicate entry") {
				duplicate = true
				return nil
			}
			return createAwardErr
		}

		if createTaskErr := tx.Table("task").Create(taskPO).Error; createTaskErr != nil {
			return createTaskErr
		}

		if updateOrderErr := tx.Table("user_raffle_order_"+tableSuffix).
			Where("order_id = ?", userAwardRecordPO.OrderID).
			Update("order_state", "used").Error; updateOrderErr != nil {
			return updateOrderErr
		}

		return nil
	})

	if txnErr != nil {
		result = "error"
		return txnErr
	}
	if duplicate {
		result = "duplicate"
	}

	pendingOrderKey := adapter.GetPendingRaffleOrderKey(aggregate.UserAwardRecord.ActivityID, aggregate.UserAwardRecord.UserID)
	if err := r.redis.Delete(ctx, pendingOrderKey); err != nil {
		logger.Warn("delete pending raffle order failed", "key", pendingOrderKey, "err", err)
	}

	return nil
}

func convertToUserAwardRecordPO(entity *awardbiz.UserAwardRecord) *po.UserAwardRecord {
	return &po.UserAwardRecord{
		UserID:     entity.UserID,
		ActivityID: entity.ActivityID,
		StrategyID: entity.StrategyID,
		OrderID:    entity.OrderID,
		AwardID:    int64(entity.AwardID),
		AwardTitle: entity.AwardTitle,
		AwardTime:  entity.AwardTime,
		AwardState: string(entity.AwardState),
		CreateTime: time.Now(),
		UpdateTime: time.Now(),
	}
}

func convertToTaskPO(entity *awardbiz.Task) (*po.Task, error) {
	msgBytes, err := json.Marshal(entity.Message)
	if err != nil {
		return nil, awardbiz.ErrorTaskPayloadMarshal.WithCause(err)
	}

	return &po.Task{
		UserID:     entity.UserID,
		Topic:      entity.Topic,
		MessageID:  entity.MessageID,
		Message:    string(msgBytes),
		State:      string(entity.State),
		CreateTime: time.Now(),
		UpdateTime: time.Now(),
	}, nil
}
