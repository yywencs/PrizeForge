package activityrepo

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"prizeforge/internal/domain/activity"
	"prizeforge/internal/domain/award"
	"prizeforge/internal/domain/strategy"
	"prizeforge/internal/infrastructure/adapter"
	"prizeforge/internal/infrastructure/repository/po"
	"prizeforge/pkg/cache"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (r *Repository) QueryRaffleActivity(ctx context.Context, activityID int64) (*activity.Activity, error) {
	var activity activity.Activity

	activityKey := adapter.GetActivityKey(activityID)

	err := r.redis.Once(&cache.Item{
		Ctx:   ctx,
		Key:   activityKey,
		Value: &activity,
		TTL:   10 * 24 * time.Hour,
		Do: func(*cache.Item) (interface{}, error) {
			var activityPO po.RaffleActivity
			if err := r.db.WithContext(ctx).Where("activity_id = ?", activityID).First(&activityPO).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return nil, nil
				}
				return nil, err
			}

			return activityPO.ToEntity(), nil
		},
	})

	if err != nil {
		return nil, err
	}

	return &activity, nil
}

func (r *Repository) QueryActivityAccount(ctx context.Context, userID string, activityID int64) (*activity.ActivityAccount, error) {
	var activityAccount activity.ActivityAccount
	key := adapter.GetActivityAccountKey(activityID, userID)

	err := r.redis.Once(&cache.Item{
		Ctx:   ctx,
		Key:   key,
		Value: &activityAccount,
		TTL:   10 * 24 * time.Hour,
		Do: func(*cache.Item) (interface{}, error) {
			var po po.RaffleActivityAccount
			db, _ := r.routerDB.DBStrategy(userID)
			if err := db.WithContext(ctx).Where("user_id = ? AND activity_id = ?", userID, activityID).First(&po).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return nil, nil
				}
				return nil, err
			}
			return po.ToEntity(), nil
		},
	})

	if err != nil {
		return nil, err
	}

	return &activityAccount, nil
}

func (r *Repository) QueryNoUsedRaffleOrder(ctx context.Context, userID string, activityID int64) (*activity.UserRaffleOrder, error) {
	cacheKey := fmt.Sprintf("no_used_raffle_order_%d_%s", activityID, userID)
	var order activity.UserRaffleOrder

	err := r.redis.Once(&cache.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &order,
		TTL:   5 * time.Second,
		Do: func(*cache.Item) (interface{}, error) {
			db, tableSuffix := r.routerDB.DBStrategy(userID)
			if db == nil {
				return nil, activity.ErrDBRouterError
			}

			var po po.UserRaffleOrder
			if err := db.WithContext(ctx).Table("user_raffle_order_"+tableSuffix).
				Where("user_id = ? AND activity_id = ? AND order_state = ?", userID, activityID, activity.UserRaffleOrderStateCreate).
				First(&po).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return nil, nil
				}
				return nil, err
			}

			return po.ToEntity(), nil
		},
	})

	if err != nil {
		return nil, err
	}

	if order.OrderID == "" {
		return nil, nil
	}

	return &order, nil
}

// CreateOrLoadUserRaffleOrder 使用 Redis Lua 原子预占总、月、日额度和完整临时订单：
//  1. 同 request_id 重试复用 Redis 中第一次分配的 order_id；
//  2. 用户已有未完成订单时拒绝新的 request_id；
//  3. 已完成请求直接复用 Redis 保存的标准抽奖结果。
func (r *Repository) CreateOrLoadUserRaffleOrder(ctx context.Context, order *activity.UserRaffleOrder) (*activity.UserRaffleOrder, *activity.DrawResultPublication, bool, error) {
	if order == nil || order.UserID == "" || order.ActivityID <= 0 || order.RequestID == "" || order.OrderID == "" {
		return nil, nil, false, activity.ErrInvalidParams
	}
	return r.reserveActivityQuota(ctx, order)
}

func newDrawOwner() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func (r *Repository) QueryActivityAccountDay(ctx context.Context, userID string, activityID int64, day string) (*activity.ActivityAccountDay, error) {
	var po po.RaffleActivityAccountDay
	db, _ := r.routerDB.DBStrategy(userID)
	err := db.WithContext(ctx).
		Where("user_id = ? AND activity_id = ? AND day = ?", userID, activityID, day).
		First(&po).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}

	return po.ToEntity(), nil
}

func (r *Repository) QueryActivityAccountMonth(ctx context.Context, userID string, activityID int64, month string) (*activity.ActivityAccountMonth, error) {
	var po po.RaffleActivityAccountMonth
	db, _ := r.routerDB.DBStrategy(userID)
	err := db.WithContext(ctx).
		Where("user_id = ? AND activity_id = ? AND month = ?", userID, activityID, month).
		First(&po).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}

	return po.ToEntity(), nil
}

// SaveDrawResult 将 Redis 已完成的抽奖结果一次性持久化：
// 订单、MySQL 总/月/日额度、中奖记录和后续发奖/库存 Outbox 在同一事务提交。
func (r *Repository) SaveDrawResult(ctx context.Context, result *activity.DrawResult) error {
	if result == nil || result.UserID == "" || result.ActivityID <= 0 || result.StrategyID <= 0 ||
		result.OrderID == "" || result.RequestID == "" || result.OrderTime.IsZero() ||
		result.AwardID <= 0 || result.AwardTime.IsZero() {
		return activity.ErrInvalidParams
	}
	userID, orderID := result.UserID, result.OrderID
	db, tableSuffix := r.routerDB.DBStrategy(userID)
	if db == nil {
		return activity.ErrDBRouterError
	}

	txnErr := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		now := time.Now()
		orderTable := "user_raffle_order_" + tableSuffix
		awardTable := "user_award_record_" + tableSuffix

		// 先锁用户活动账户，使同一用户的重复 RabbitMQ 消息串行化。
		var accountPO po.RaffleActivityAccount
		if err := tx.Table("raffle_activity_account").
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("user_id = ? AND activity_id = ?", userID, result.ActivityID).
			First(&accountPO).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return activity.ErrRecordNotFound
			}
			return err
		}

		// 订单存在即表示这条完整结果已提交；校验标准结果后幂等成功，不再扣额度。
		var existingOrder po.UserRaffleOrder
		orderErr := tx.Table(orderTable).
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("user_id = ? AND activity_id = ? AND request_id = ?", userID, result.ActivityID, result.RequestID).
			First(&existingOrder).Error
		if orderErr == nil {
			if existingOrder.OrderID != orderID {
				return fmt.Errorf("draw result request conflict: request_id=%s existing_order=%s incoming_order=%s",
					result.RequestID, existingOrder.OrderID, orderID)
			}
			var existingAward po.UserAwardRecord
			if err := tx.Table(awardTable).
				Where("user_id = ? AND order_id = ?", userID, orderID).
				First(&existingAward).Error; err != nil {
				return fmt.Errorf("load persisted draw award: %w", err)
			}
			if existingAward.AwardID != int64(result.AwardID) {
				return fmt.Errorf("draw result award conflict: order_id=%s existing_award=%d incoming_award=%d",
					orderID, existingAward.AwardID, result.AwardID)
			}
			return nil
		}
		if !errors.Is(orderErr, gorm.ErrRecordNotFound) {
			return orderErr
		}

		if accountPO.TotalCountSurplus <= 0 {
			return activity.ErrActivityQuotaError
		}

		res := tx.Table("raffle_activity_account").
			Where("user_id = ? AND activity_id = ?", userID, result.ActivityID).
			Updates(map[string]interface{}{
				"total_count_surplus": gorm.Expr("total_count_surplus - 1"),
				"current_order_id":    "",
				"update_time":         now,
			})
		if res.Error != nil {
			return res.Error
		}

		orderMonth := result.OrderTime.Format("2006-01")
		orderDay := result.OrderTime.Format("2006-01-02")
		var monthPO po.RaffleActivityAccountMonth
		err := tx.
			Table("raffle_activity_account_month").
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("user_id = ? AND activity_id = ? AND month = ?", userID, result.ActivityID, orderMonth).
			First(&monthPO).Error
		if err == nil {
			if monthPO.MonthCountSurplus <= 0 {
				return activity.ErrActivityAccountMonthCountSurplusNotEnough
			}
			resMonth := tx.Table("raffle_activity_account_month").
				Where("user_id = ? AND activity_id = ? AND month = ?",
					userID, result.ActivityID, orderMonth).
				Updates(map[string]interface{}{
					"month_count_surplus": gorm.Expr("month_count_surplus - 1"),
					"update_time":         now,
				})
			if resMonth.Error != nil {
				return resMonth.Error
			}
			if resMonth.RowsAffected == 0 {
				return activity.ErrActivityAccountMonthCountSurplusNotEnough
			}
		} else if errors.Is(err, gorm.ErrRecordNotFound) {
			monthPO = po.RaffleActivityAccountMonth{
				UserID:            userID,
				ActivityID:        result.ActivityID,
				Month:             orderMonth,
				MonthCount:        accountPO.MonthCount,
				MonthCountSurplus: accountPO.MonthCount - 1,
			}
			if createMonthErr := tx.Table("raffle_activity_account_month").Create(&monthPO).Error; createMonthErr != nil {
				if errors.Is(createMonthErr, gorm.ErrDuplicatedKey) {
					return activity.ErrDBIndexDuplicate
				}
				return createMonthErr
			}
		} else {
			return err
		}

		var dayPO po.RaffleActivityAccountDay
		err = tx.
			Table("raffle_activity_account_day").
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("user_id = ? AND activity_id = ? AND day = ?", userID, result.ActivityID, orderDay).
			First(&dayPO).Error
		if err == nil {
			if dayPO.DayCountSurplus <= 0 {
				return activity.ErrActivityAccountDayCountSurplusNotEnough
			}
			resDay := tx.Table("raffle_activity_account_day").
				Where("user_id = ? AND activity_id = ? AND day = ?",
					userID, result.ActivityID, orderDay).
				Updates(map[string]interface{}{
					"day_count_surplus": gorm.Expr("day_count_surplus - 1"),
					"update_time":       now,
				})
			if resDay.Error != nil {
				return resDay.Error
			}
			if resDay.RowsAffected == 0 {
				return activity.ErrActivityAccountDayCountSurplusNotEnough
			}
		} else if errors.Is(err, gorm.ErrRecordNotFound) {
			dayPO = po.RaffleActivityAccountDay{
				UserID:          userID,
				ActivityID:      result.ActivityID,
				Day:             orderDay,
				DayCount:        accountPO.DayCount,
				DayCountSurplus: accountPO.DayCount - 1,
			}
			if createDayErr := tx.Table("raffle_activity_account_day").Create(&dayPO).Error; createDayErr != nil {
				if errors.Is(createDayErr, gorm.ErrDuplicatedKey) {
					return activity.ErrDBIndexDuplicate
				}
				return createDayErr
			}
		} else {
			return err
		}

		orderPO := &po.UserRaffleOrder{
			UserID:       userID,
			ActivityID:   result.ActivityID,
			ActivityName: result.ActivityName,
			StrategyID:   result.StrategyID,
			OrderID:      orderID,
			RequestID:    result.RequestID,
			OrderTime:    result.OrderTime,
			OrderState:   string(activity.UserRaffleOrderStateUsed),
			DrawState:    string(activity.DrawStateSuccess),
			CreateTime:   now,
			UpdateTime:   now,
		}
		if err := tx.Table(orderTable).Create(orderPO).Error; err != nil {
			return err
		}

		awardPO := &po.UserAwardRecord{
			UserID:     userID,
			ActivityID: result.ActivityID,
			StrategyID: result.StrategyID,
			OrderID:    orderID,
			AwardID:    int64(result.AwardID),
			AwardTitle: result.AwardTitle,
			AwardTime:  result.AwardTime,
			AwardState: string(award.AwardStateCreate),
			CreateTime: now,
			UpdateTime: now,
		}
		if err := tx.Table(awardTable).Create(awardPO).Error; err != nil {
			return err
		}

		sendAwardPayload, err := json.Marshal(&award.SendAwardMessage{
			UserID:     userID,
			OrderID:    orderID,
			AwardID:    result.AwardID,
			AwardTitle: result.AwardTitle,
		})
		if err != nil {
			return err
		}
		sendAwardTask := &po.Task{
			UserID:     userID,
			Topic:      award.SendAwardTopic,
			MessageID:  userID + ":" + orderID,
			Message:    string(sendAwardPayload),
			State:      string(award.TaskStateCreate),
			CreateTime: now,
			UpdateTime: now,
		}
		if err := tx.Table("task").Create(sendAwardTask).Error; err != nil {
			return err
		}

		if result.StockReserved {
			stockPayload, err := json.Marshal(&strategy.AwardStockConsumeMessage{
				UserID:     userID,
				OrderID:    orderID,
				StrategyID: result.StrategyID,
				AwardID:    int64(result.AwardID),
			})
			if err != nil {
				return err
			}
			stockTask := &po.Task{
				UserID:     userID,
				Topic:      strategy.AwardStockSyncTopic,
				MessageID:  "stock:" + userID + ":" + orderID,
				Message:    string(stockPayload),
				State:      string(award.TaskStateCreate),
				CreateTime: now,
				UpdateTime: now,
			}
			if err := tx.Table("task").Create(stockTask).Error; err != nil {
				return err
			}
		}

		return nil
	})
	if txnErr != nil {
		return txnErr
	}

	if err := r.clearPersistedPendingDraw(context.WithoutCancel(ctx), result); err != nil {
		return fmt.Errorf("clear persisted Redis draw: %w", err)
	}
	accountKey := adapter.GetActivityAccountKey(result.ActivityID, result.UserID)
	_ = r.redis.Delete(context.WithoutCancel(ctx), accountKey)
	return nil
}

func (r *Repository) QueryRaffleActivityAccountPartakeCount(ctx context.Context, userID string, activityID int64) (int64, error) {
	db, _ := r.routerDB.DBStrategy(userID)
	var po po.RaffleActivityAccount
	err := db.WithContext(ctx).Table("raffle_activity_account").
		Where("user_id = ? AND activity_id = ?", userID, activityID).
		First(&po).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, nil
		}
		return 0, err
	}
	return int64(po.TotalCount - po.TotalCountSurplus), nil
}

func (r *Repository) QueryRaffleActivityAccountDayPartakeCount(ctx context.Context, userID string, activityID int64) (int64, error) {
	db, _ := r.routerDB.DBStrategy(userID)
	var po po.RaffleActivityAccountDay
	day := time.Now().Format("2006-01-02")
	err := db.WithContext(ctx).Table("raffle_activity_account_day").
		Where("user_id = ? AND activity_id = ? AND day = ?", userID, activityID, day).
		First(&po).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, nil
		}
		return 0, err
	}
	return int64(po.DayCount - po.DayCountSurplus), nil
}
