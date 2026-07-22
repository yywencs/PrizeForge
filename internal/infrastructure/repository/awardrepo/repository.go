package awardrepo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"prizeforge/internal/domain/award"
	"prizeforge/internal/domain/strategy"
	"prizeforge/internal/infrastructure/adapter"
	"prizeforge/internal/infrastructure/repository/po"
	"prizeforge/internal/metrics"
	"prizeforge/pkg/cache"
	"prizeforge/pkg/logger"
	"strings"
	"time"

	"gorm.io/gorm"
)

type UserAwardRecord struct {
	routerDB  *adapter.DBRouter
	publisher *adapter.Publisher
	redis     *cache.Cache
}

func NewUserAwardRecordRepository(db *adapter.DBRouter, redis *cache.Cache, publisher *adapter.Publisher) award.Repo {
	return &UserAwardRecord{
		routerDB:  db,
		redis:     redis,
		publisher: publisher,
	}
}

func (r *UserAwardRecord) SaveUserAwardRecord(ctx context.Context, aggregate *award.UserAwardTaskInfo) (*award.UserAwardRecord, error) {
	result := "success"
	defer func() {
		metrics.IncAward(aggregate.UserAwardRecord.AwardID, result)
	}()

	userAwardRecordPO := convertToUserAwardRecordPO(aggregate.UserAwardRecord)
	taskPO, taskErr := convertToTaskPO(aggregate.Task)
	if taskErr != nil {
		result = "payload_marshal_error"
		return nil, taskErr
	}
	var stockTaskPO *po.Task
	if aggregate.UserAwardRecord.StockReserved {
		var stockTaskErr error
		stockTaskPO, stockTaskErr = convertToStockTaskPO(aggregate.UserAwardRecord)
		if stockTaskErr != nil {
			result = "payload_marshal_error"
			return nil, stockTaskErr
		}
	}

	db, tableSuffix := r.routerDB.DBStrategy(aggregate.UserAwardRecord.UserID)
	if db == nil {
		result = "error"
		return nil, errors.New("database router returned nil")
	}
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
		if stockTaskPO != nil {
			if createStockTaskErr := tx.Table("task").Create(stockTaskPO).Error; createStockTaskErr != nil {
				return createStockTaskErr
			}
		}

		updateOrder := tx.Table("user_raffle_order_"+tableSuffix).
			Where("user_id = ? AND order_id = ? AND draw_state = ? AND draw_owner = ?",
				userAwardRecordPO.UserID, userAwardRecordPO.OrderID, "processing", aggregate.UserAwardRecord.DrawOwner).
			Updates(map[string]interface{}{
				"order_state":   "used",
				"draw_state":    "success",
				"processing_at": nil,
				"draw_owner":    "",
				"update_time":   time.Now(),
			})
		if updateOrder.Error != nil {
			return updateOrder.Error
		}
		if updateOrder.RowsAffected != 1 {
			return errors.New("raffle order not found while saving award result")
		}

		if clearCurrentOrderErr := tx.Table("raffle_activity_account").
			Where("user_id = ? AND activity_id = ? AND current_order_id = ?",
				userAwardRecordPO.UserID, userAwardRecordPO.ActivityID, userAwardRecordPO.OrderID).
			Updates(map[string]interface{}{
				"current_order_id": "",
				"update_time":      time.Now(),
			}).Error; clearCurrentOrderErr != nil {
			return clearCurrentOrderErr
		}

		return nil
	})

	if txnErr != nil {
		result = "error"
		return nil, txnErr
	}
	canonical := userAwardRecordPO.ToEntity()
	if duplicate {
		result = "duplicate"
		existing, queryErr := r.QueryByOrderID(ctx, aggregate.UserAwardRecord.UserID, aggregate.UserAwardRecord.OrderID)
		if queryErr != nil {
			return nil, queryErr
		}
		if existing == nil {
			return nil, errors.New("duplicate award order exists but canonical record cannot be loaded")
		}
		canonical = existing
	}

	pendingOrderKey := adapter.GetPendingRaffleOrderKey(aggregate.UserAwardRecord.ActivityID, aggregate.UserAwardRecord.UserID)
	if err := r.redis.Delete(ctx, pendingOrderKey); err != nil {
		logger.Warn("delete pending raffle order failed", "key", pendingOrderKey, "err", err)
	}
	accountKey := adapter.GetActivityAccountKey(aggregate.UserAwardRecord.ActivityID, aggregate.UserAwardRecord.UserID)
	if err := r.redis.Delete(ctx, accountKey); err != nil {
		logger.Warn("delete activity account cache failed", "key", accountKey, "err", err)
	}

	return canonical, nil
}

// QueryByOrderID 按订单 ID 查询中奖记录。nil 表示该订单尚未落库（未抽奖完成）。
func (r *UserAwardRecord) QueryByOrderID(ctx context.Context, userID string, orderID string) (*award.UserAwardRecord, error) {
	db, tableSuffix := r.routerDB.DBStrategy(userID)
	if db == nil {
		return nil, errors.New("database router returned nil")
	}

	var po po.UserAwardRecord
	err := db.WithContext(ctx).Table("user_award_record_"+tableSuffix).
		Where("user_id = ? AND order_id = ?", userID, orderID).
		First(&po).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return po.ToEntity(), nil
}

// CompleteUserAward 将中奖记录从 create 幂等推进到 complete。
// 先使用带状态条件的 UPDATE 争抢处理权；并发重复消息没有更新到记录时，再读取当前
// 状态区分“已经完成”的幂等成功与“记录缺失/状态冲突”的异常情况。
func (r *UserAwardRecord) CompleteUserAward(ctx context.Context, userID string, orderID string) error {
	db, tableSuffix := r.routerDB.DBStrategy(userID)
	if db == nil {
		return errors.New("database router returned nil")
	}
	tableName := "user_award_record_" + tableSuffix

	result := db.WithContext(ctx).
		Table(tableName).
		Where("user_id = ? AND order_id = ? AND award_state = ?", userID, orderID, string(award.AwardStateCreate)).
		Updates(map[string]interface{}{
			"award_state": string(award.AwardStateComplete),
			"update_time": time.Now(),
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 1 {
		return nil
	}

	var stored struct {
		AwardState string `gorm:"column:award_state"`
	}
	err := db.WithContext(ctx).
		Table(tableName).
		Select("award_state").
		Where("user_id = ? AND order_id = ?", userID, orderID).
		Take(&stored).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return award.ErrAwardRecordNotFound
	}
	if err != nil {
		return err
	}
	if stored.AwardState == string(award.AwardStateComplete) {
		return nil
	}
	return fmt.Errorf("%w: user_id=%s order_id=%s state=%s", award.ErrAwardStateConflict, userID, orderID, stored.AwardState)
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

func convertToStockTaskPO(record *award.UserAwardRecord) (*po.Task, error) {
	message := strategy.AwardStockConsumeMessage{
		UserID:     record.UserID,
		OrderID:    record.OrderID,
		StrategyID: record.StrategyID,
		AwardID:    int64(record.AwardID),
	}
	messageBytes, err := json.Marshal(message)
	if err != nil {
		return nil, err
	}
	return &po.Task{
		UserID:     record.UserID,
		Topic:      strategy.AwardStockSyncTopic,
		MessageID:  "stock:" + record.UserID + ":" + record.OrderID,
		Message:    string(messageBytes),
		State:      string(award.TaskStateCreate),
		CreateTime: time.Now(),
		UpdateTime: time.Now(),
	}, nil
}
