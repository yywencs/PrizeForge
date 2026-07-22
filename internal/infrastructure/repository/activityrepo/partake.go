package activityrepo

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"prizeforge/internal/domain/activity"
	"prizeforge/internal/infrastructure/adapter"
	"prizeforge/internal/infrastructure/repository/po"
	"prizeforge/pkg/cache"
	"prizeforge/pkg/rabbitmq"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const drawProcessingLease = 30 * time.Second

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

// CreateOrLoadUserRaffleOrder 使用 Redis Lua 原子预占总、月、日额度，并同步创建轻量订单：
//  1. 同 request_id 重试复用 Redis 中第一次分配的 order_id；
//  2. 用户已有未完成订单时拒绝新的 request_id；
//  3. 新订单只写入 MySQL，数据库额度由 save_order_record 消费者异步同步。
func (r *Repository) CreateOrLoadUserRaffleOrder(ctx context.Context, order *activity.UserRaffleOrder) (*activity.UserRaffleOrder, bool, error) {
	if order == nil || order.UserID == "" || order.ActivityID <= 0 || order.RequestID == "" || order.OrderID == "" {
		return nil, false, activity.ErrInvalidParams
	}

	db, tableSuffix := r.routerDB.DBStrategy(order.UserID)
	if db == nil {
		return nil, false, activity.ErrDBRouterError
	}
	orderTable := "user_raffle_order_" + tableSuffix

	// 正常重试走有索引的只读快路径。
	var existingPO po.UserRaffleOrder
	err := db.WithContext(ctx).
		Table(orderTable).
		Where("user_id = ? AND activity_id = ? AND request_id = ?", order.UserID, order.ActivityID, order.RequestID).
		First(&existingPO).Error
	if err == nil {
		return existingPO.ToEntity(), true, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, false, err
	}

	canonicalOrderID, reservationReused, err := r.reserveActivityQuota(ctx, order)
	if err != nil {
		return nil, false, err
	}

	orderPO := &po.UserRaffleOrder{
		UserID:           order.UserID,
		ActivityID:       order.ActivityID,
		ActivityName:     order.ActivityName,
		StrategyID:       order.StrategyID,
		OrderID:          canonicalOrderID,
		RequestID:        order.RequestID,
		OrderTime:        order.OrderTime,
		OrderState:       string(activity.UserRaffleOrderStateCreate),
		DrawState:        string(activity.DrawStateCreated),
		AccountSyncState: string(activity.AccountSyncStateCreate),
	}
	createResult := db.WithContext(ctx).
		Table(orderTable).
		Clauses(clause.OnConflict{DoNothing: true}).
		Create(orderPO)
	if createResult.Error != nil {
		// Redis 中的预占和 pending order 会被保留；同 request_id 重试会复用并再次尝试落单。
		return nil, false, createResult.Error
	}
	if createResult.RowsAffected == 1 {
		return orderPO.ToEntity(), reservationReused, nil
	}

	// 并发相同 request_id 可能同时通过 Redis 复用结果并尝试插入，唯一索引决定标准订单。
	if err := db.WithContext(ctx).
		Table(orderTable).
		Where("user_id = ? AND activity_id = ? AND request_id = ?", order.UserID, order.ActivityID, order.RequestID).
		First(&existingPO).Error; err != nil {
		return nil, false, err
	}
	return existingPO.ToEntity(), reservationReused, nil
}

func (r *Repository) TryClaimUserRaffleOrder(ctx context.Context, userID string, orderID string) (*activity.DrawClaim, error) {
	db, tableSuffix := r.routerDB.DBStrategy(userID)
	if db == nil {
		return nil, activity.ErrDBRouterError
	}
	orderTable := "user_raffle_order_" + tableSuffix
	now := time.Now()
	owner, err := newDrawOwner()
	if err != nil {
		return nil, err
	}

	res := db.WithContext(ctx).Table(orderTable).
		Where("user_id = ? AND order_id = ? AND draw_state = ?", userID, orderID, activity.DrawStateCreated).
		Updates(map[string]interface{}{
			"draw_state":    activity.DrawStateProcessing,
			"processing_at": now,
			"draw_owner":    owner,
			"update_time":   now,
		})
	if res.Error != nil {
		return nil, res.Error
	}
	if res.RowsAffected == 1 {
		return &activity.DrawClaim{Status: activity.DrawClaimAcquired, Owner: owner}, nil
	}

	staleBefore := now.Add(-drawProcessingLease)
	res = db.WithContext(ctx).Table(orderTable).
		Where("user_id = ? AND order_id = ? AND draw_state = ? AND (processing_at IS NULL OR processing_at < ?)",
			userID, orderID, activity.DrawStateProcessing, staleBefore).
		Updates(map[string]interface{}{
			"processing_at": now,
			"draw_owner":    owner,
			"update_time":   now,
		})
	if res.Error != nil {
		return nil, res.Error
	}
	if res.RowsAffected == 1 {
		return &activity.DrawClaim{Status: activity.DrawClaimAcquired, Owner: owner}, nil
	}

	var orderPO po.UserRaffleOrder
	if err := db.WithContext(ctx).Table(orderTable).
		Where("user_id = ? AND order_id = ?", userID, orderID).
		First(&orderPO).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, activity.ErrRecordNotFound
		}
		return nil, err
	}

	switch activity.DrawState(orderPO.DrawState) {
	case activity.DrawStateProcessing:
		return &activity.DrawClaim{Status: activity.DrawClaimProcessing}, nil
	case activity.DrawStateSuccess:
		return &activity.DrawClaim{Status: activity.DrawClaimCompleted}, nil
	case activity.DrawStateCancelled:
		return &activity.DrawClaim{Status: activity.DrawClaimCancelled}, nil
	default:
		return nil, fmt.Errorf("unexpected draw state %q for order %s", orderPO.DrawState, orderID)
	}
}

func newDrawOwner() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func (r *Repository) ReleaseUserRaffleOrderClaim(ctx context.Context, userID string, orderID string, owner string) error {
	db, tableSuffix := r.routerDB.DBStrategy(userID)
	if db == nil {
		return activity.ErrDBRouterError
	}
	return db.WithContext(ctx).Table("user_raffle_order_"+tableSuffix).
		Where("user_id = ? AND order_id = ? AND draw_state = ? AND draw_owner = ?",
			userID, orderID, activity.DrawStateProcessing, owner).
		Updates(map[string]interface{}{
			"draw_state":    activity.DrawStateCreated,
			"processing_at": nil,
			"draw_owner":    "",
			"update_time":   time.Now(),
		}).Error
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

func (r *Repository) AsyncSaveCreatePartakeOrderAggregate(ctx context.Context, createPartakeOrderAggregate *activity.CreatePartakeOrder) error {
	baseEvent := rabbitmq.NewBaseEvent(createPartakeOrderAggregate)
	return r.stockZeroPublisher.PublishSaveOrder(ctx, baseEvent)
}

func (r *Repository) SaveCreatePartakeOrderAggregate(ctx context.Context, createPartakeOrderAggregate *activity.CreatePartakeOrder) error {
	userID := createPartakeOrderAggregate.UserID
	if createPartakeOrderAggregate.UserRaffleOrder == nil || createPartakeOrderAggregate.UserRaffleOrder.OrderID == "" {
		return activity.ErrInvalidParams
	}

	orderID := createPartakeOrderAggregate.UserRaffleOrder.OrderID
	db, tableSuffix := r.routerDB.DBStrategy(userID)
	if db == nil {
		return activity.ErrDBRouterError
	}

	return db.Transaction(func(tx *gorm.DB) error {
		orderTable := "user_raffle_order_" + tableSuffix
		var orderPO po.UserRaffleOrder
		if err := tx.WithContext(ctx).
			Table(orderTable).
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("user_id = ? AND order_id = ?", userID, orderID).
			First(&orderPO).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return activity.ErrRecordNotFound
			}
			return err
		}

		if orderPO.AccountSyncState == string(activity.AccountSyncStateCompleted) {
			return nil
		}

		activityID := orderPO.ActivityID
		orderMonth := orderPO.OrderTime.Format("2006-01")
		orderDay := orderPO.OrderTime.Format("2006-01-02")
		now := time.Now()

		var accountPO po.RaffleActivityAccount
		if err := tx.WithContext(ctx).
			Table("raffle_activity_account").
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("user_id = ? AND activity_id = ?", userID, activityID).
			First(&accountPO).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return activity.ErrRecordNotFound
			}
			return err
		}

		if accountPO.TotalCountSurplus <= 0 {
			return activity.ErrActivityQuotaError
		}
		if accountPO.MonthCountSurplus <= 0 {
			return activity.ErrActivityAccountMonthCountSurplusNotEnough
		}
		if accountPO.DayCountSurplus <= 0 {
			return activity.ErrActivityAccountDayCountSurplusNotEnough
		}

		res := tx.Table("raffle_activity_account").
			Where("user_id = ? AND activity_id = ?", userID, activityID).
			Updates(map[string]interface{}{
				"total_count_surplus": gorm.Expr("total_count_surplus - 1"),
				"day_count_surplus":   gorm.Expr("day_count_surplus - 1"),
				"month_count_surplus": gorm.Expr("month_count_surplus - 1"),
				"update_time":         now,
			})
		if res.Error != nil {
			return res.Error
		}

		var monthPO po.RaffleActivityAccountMonth
		err := tx.WithContext(ctx).
			Table("raffle_activity_account_month").
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("user_id = ? AND activity_id = ? AND month = ?", userID, activityID, orderMonth).
			First(&monthPO).Error
		if err == nil {
			if monthPO.MonthCountSurplus <= 0 {
				return activity.ErrActivityAccountMonthCountSurplusNotEnough
			}
			resMonth := tx.Table("raffle_activity_account_month").
				Where("user_id = ? AND activity_id = ? AND month = ?",
					userID, activityID, orderMonth).
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
				ActivityID:        activityID,
				Month:             orderMonth,
				MonthCount:        accountPO.MonthCount,
				MonthCountSurplus: accountPO.MonthCountSurplus - 1,
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
		err = tx.WithContext(ctx).
			Table("raffle_activity_account_day").
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("user_id = ? AND activity_id = ? AND day = ?", userID, activityID, orderDay).
			First(&dayPO).Error
		if err == nil {
			if dayPO.DayCountSurplus <= 0 {
				return activity.ErrActivityAccountDayCountSurplusNotEnough
			}
			resDay := tx.Table("raffle_activity_account_day").
				Where("user_id = ? AND activity_id = ? AND day = ?",
					userID, activityID, orderDay).
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
				ActivityID:      activityID,
				Day:             orderDay,
				DayCount:        accountPO.DayCount,
				DayCountSurplus: accountPO.DayCountSurplus - 1,
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

		if err := tx.Table(orderTable).
			Where("user_id = ? AND order_id = ?", userID, orderID).
			Updates(map[string]interface{}{
				"account_sync_state": string(activity.AccountSyncStateCompleted),
				"update_time":        now,
			}).Error; err != nil {
			return err
		}

		return nil
	})
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
