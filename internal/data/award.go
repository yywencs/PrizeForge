package data

import (
	"big-market-kratos/internal/biz/award"
	"big-market-kratos/internal/data/po"
	"big-market-kratos/pkg/rabbitmq"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
)

type UserAwardRecord struct {
	routerDB  *DBRouter
	publisher *Publisher
}

func NewUserAwardRecordRepository(db *DBRouter, publisher *Publisher) award.Repo {
	return &UserAwardRecord{
		routerDB:  db,
		publisher: publisher,
	}
}

func (r *UserAwardRecord) SaveUserAwardRecord(ctx context.Context, aggregate *award.UserAwardTaskInfo) error {
	userAwardRecordPO := convertToUserAwardRecordPO(aggregate.UserAwardRecord)
	taskPO, err := convertToTaskPO(aggregate.Task)
	if err != nil {
		return err
	}

	// Calculate DB suffix based on UserID
	db, tableSuffix := r.routerDB.DBStrategy(aggregate.UserAwardRecord.UserID)

	// 1. Transaction
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 1.1 Create UserAwardRecord
		if err := tx.Table("user_award_record_" + tableSuffix).Create(userAwardRecordPO).Error; err != nil {
			// Handle duplicate key error (idempotency)
			if errors.Is(err, gorm.ErrDuplicatedKey) || strings.Contains(err.Error(), "Duplicate entry") {
				return nil
			}
			return err
		}

		// 1.2 Create Task
		if err := tx.Table("task_" + tableSuffix).Create(taskPO).Error; err != nil {
			return err
		}

		// 1.3 Update Raffle Order State
		// Note: user_raffle_order table might also be sharded. Assuming same shard as user_award_record for now as it is based on UserID usually.
		if err := tx.Table("user_raffle_order_"+tableSuffix).
			Where("order_id = ?", userAwardRecordPO.OrderID).
			Update("order_state", "used").Error; err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return err
	}

	// 2. Send MQ Message (Async)
	go func() {
		// Construct BaseEvent
		baseEvent := rabbitmq.BaseEvent{
			ID:        aggregate.Task.MessageID,
			Timestamp: time.Now(),
			Data:      aggregate.Task.Message,
		}

		// Publish message
		// Note: using context.Background() for async task
		_ = r.publisher.PublishSendAward(context.Background(), &baseEvent)
	}()

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
		return nil, fmt.Errorf("marshal task message error: %v", err)
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
