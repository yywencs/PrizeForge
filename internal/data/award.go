package data

import (
	"big-market-kratos/internal/biz/award"
	"big-market-kratos/internal/data/po"
	"big-market-kratos/pkg/cache"
	"big-market-kratos/pkg/logger"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"gorm.io/gorm"
)

type UserAwardRecord struct {
	routerDB  *DBRouter
	publisher *Publisher
	redis     *cache.Cache
}

func NewUserAwardRecordRepository(db *DBRouter, redis *cache.Cache, publisher *Publisher) award.Repo {
	return &UserAwardRecord{
		routerDB:  db,
		redis:     redis,
		publisher: publisher,
	}
}

func (r *UserAwardRecord) SaveUserAwardRecord(ctx context.Context, aggregate *award.UserAwardTaskInfo) error {
	userAwardRecordPO := convertToUserAwardRecordPO(aggregate.UserAwardRecord)
	taskPO, taskErr := convertToTaskPO(aggregate.Task)
	if taskErr != nil {
		return taskErr
	}

	// Calculate DB suffix based on UserID
	db, tableSuffix := r.routerDB.DBStrategy(aggregate.UserAwardRecord.UserID)

	// 1. Transaction
	txnErr := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 1.1 Create UserAwardRecord
		if createAwardErr := tx.Table("user_award_record_" + tableSuffix).Create(userAwardRecordPO).Error; createAwardErr != nil {
			// Handle duplicate key error (idempotency)
			if errors.Is(createAwardErr, gorm.ErrDuplicatedKey) || strings.Contains(createAwardErr.Error(), "Duplicate entry") {
				return nil
			}
			return createAwardErr
		}

		// 1.2 Create Task
		if createTaskErr := tx.Table("task").Create(taskPO).Error; createTaskErr != nil {
			return createTaskErr
		}

		// 1.3 Update Raffle Order State
		// Note: user_raffle_order table might also be sharded. Assuming same shard as user_award_record for now as it is based on UserID usually.
		if updateOrderErr := tx.Table("user_raffle_order_"+tableSuffix).
			Where("order_id = ?", userAwardRecordPO.OrderID).
			Update("order_state", "used").Error; updateOrderErr != nil {
			return updateOrderErr
		}

		return nil
	})

	if txnErr != nil {
		return txnErr
	}

	pendingOrderKey := GetPendingRaffleOrderKey(aggregate.UserAwardRecord.ActivityID, aggregate.UserAwardRecord.UserID)
	if err := r.redis.Delete(ctx, pendingOrderKey); err != nil {
		// Redis pending 订单是流程态缓存，不影响中奖账本主流程。
		logger.Warn("delete pending raffle order failed", "key", pendingOrderKey, "err", err)
	}

	return nil
}

func convertToUserAwardRecordPO(entity *award.UserAwardRecord) *po.UserAwardRecord {
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

func convertToTaskPO(entity *award.Task) (*po.Task, error) {
	msgBytes, err := json.Marshal(entity.Message)
	if err != nil {
		return nil, award.ErrorTaskPayloadMarshal.WithCause(err)
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
