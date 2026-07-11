package activity

import (
	activitybiz "prizeforge/internal/domain/activity"
	"prizeforge/internal/infrastructure/adapter"
	"prizeforge/internal/infrastructure/repository/po"
	"prizeforge/pkg/cache"
	"prizeforge/pkg/logger"
	"prizeforge/pkg/rabbitmq"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const pendingRaffleOrderTTL = 30 * time.Minute

func (r *Repository) QueryRaffleActivity(ctx context.Context, activityID int64) (*activitybiz.Activity, error) {
	var activity activitybiz.Activity

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

func (r *Repository) QueryActivityAccount(ctx context.Context, userID string, activityID int64) (*activitybiz.ActivityAccount, error) {
	var activityAccount activitybiz.ActivityAccount
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

func (r *Repository) QueryNoUsedRaffleOrder(ctx context.Context, userID string, activityID int64) (*activitybiz.UserRaffleOrder, error) {
	cacheKey := fmt.Sprintf("no_used_raffle_order_%d_%s", activityID, userID)
	var order activitybiz.UserRaffleOrder

	err := r.redis.Once(&cache.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &order,
		TTL:   5 * time.Second,
		Do: func(*cache.Item) (interface{}, error) {
			db, tableSuffix := r.routerDB.DBStrategy(userID)
			if db == nil {
				return nil, activitybiz.ErrDBRouterError
			}

			var po po.UserRaffleOrder
			if err := db.WithContext(ctx).Table("user_raffle_order_"+tableSuffix).
				Where("user_id = ? AND activity_id = ? AND order_state = ?", userID, activityID, activitybiz.UserRaffleOrderStateCreate).
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

func (r *Repository) CacheGetOrCreateNoUsedRaffleOrder(ctx context.Context, order *activitybiz.UserRaffleOrder) (*activitybiz.UserRaffleOrder, bool, error) {
	totalKey := adapter.GetActivityAccountTotalSurplusKey(order.ActivityID, order.UserID)
	dayKey := adapter.GetActivityAccountDaySurplusKey(order.ActivityID, order.UserID, time.Now().Format("2006-01-02"))
	monthKey := adapter.GetActivityAccountMonthSurplusKey(order.ActivityID, order.UserID, time.Now().Format("2006-01"))
	pendingOrderKey := adapter.GetPendingRaffleOrderKey(order.ActivityID, order.UserID)

	script := `
		local pending = redis.call("HMGET", KEYS[4], "oid", "sid", "st")
		if pending[1] and pending[2] and pending[3] then
			if pending[3] == "0" then
				return {2, pending[1], pending[2], pending[3]}
			end

			redis.call("DEL", KEYS[4])
		end

		local total = redis.call("GET", KEYS[1])
		local day = redis.call("GET", KEYS[2])
		local month = redis.call("GET", KEYS[3])

		if not total or not day or not month then
			return {-1}
		end

		if tonumber(total) <= 0 then return {-2} end
		if tonumber(day) <= 0 then return {-3} end
		if tonumber(month) <= 0 then return {-4} end

		redis.call("DECR", KEYS[1])
		redis.call("DECR", KEYS[2])
		redis.call("DECR", KEYS[3])
		redis.call("HSET", KEYS[4],
			"oid", ARGV[1],
			"sid", ARGV[2],
			"st", ARGV[3])
		redis.call("EXPIRE", KEYS[4], ARGV[4])

		return {1, ARGV[1], ARGV[2], ARGV[3]}
	`

	result, err := r.redis.Eval(ctx, script, []string{totalKey, dayKey, monthKey, pendingOrderKey},
		order.OrderID,
		strconv.FormatInt(order.StrategyID, 10),
		"0",
		strconv.FormatInt(int64(pendingRaffleOrderTTL/time.Second), 10),
	)
	if err != nil {
		return nil, false, err
	}

	resultArray, ok := result.([]interface{})
	if !ok || len(resultArray) == 0 {
		return nil, false, fmt.Errorf("unexpected redis eval result: %T", result)
	}

	status, ok := resultArray[0].(int64)
	if !ok {
		return nil, false, fmt.Errorf("unexpected redis eval status type: %T", resultArray[0])
	}

	switch status {
	case -1, -2:
		return nil, false, activitybiz.ErrActivityQuotaError
	case -3:
		return nil, false, activitybiz.ErrActivityAccountDayCountSurplusNotEnough
	case -4:
		return nil, false, activitybiz.ErrActivityAccountMonthCountSurplusNotEnough
	case 1, 2:
		if len(resultArray) < 4 {
			return nil, false, fmt.Errorf("unexpected redis eval payload length: %d", len(resultArray))
		}

		strategyID, err := strconv.ParseInt(fmt.Sprint(resultArray[2]), 10, 64)
		if err != nil {
			return nil, false, err
		}

		return &activitybiz.UserRaffleOrder{
			UserID:       order.UserID,
			ActivityID:   order.ActivityID,
			ActivityName: order.ActivityName,
			StrategyID:   strategyID,
			OrderID:      fmt.Sprint(resultArray[1]),
			OrderTime:    order.OrderTime,
			OrderState:   activitybiz.UserRaffleOrderStateCreate,
		}, status == 2, nil
	default:
		return nil, false, fmt.Errorf("unexpected redis eval status: %d", status)
	}
}

func (r *Repository) SaveLiteUserRaffleOrder(ctx context.Context, aggregate *activitybiz.CreatePartakeOrder) error {
	order := aggregate.UserRaffleOrder
	db, tableSuffix := r.routerDB.DBStrategy(order.UserID)
	if db == nil {
		compensateErr := r.compensatePendingRaffleOrder(ctx, order)
		if compensateErr != nil {
			logger.Warn("compensate pending raffle order failed after db router error", "orderID", order.OrderID, "err", compensateErr)
		}
		return activitybiz.ErrDBRouterError
	}

	orderPO := &po.UserRaffleOrder{
		UserID:           order.UserID,
		ActivityID:       order.ActivityID,
		ActivityName:     order.ActivityName,
		StrategyID:       order.StrategyID,
		OrderID:          order.OrderID,
		OrderTime:        order.OrderTime,
		OrderState:       string(order.OrderState),
		AccountSyncState: string(activitybiz.AccountSyncStateCreate),
	}

	baseEvent := rabbitmq.NewBaseEvent(aggregate)
	taskPO, taskErr := buildSaveOrderTaskPO(aggregate.UserID, baseEvent.ID, aggregate)
	if taskErr != nil {
		compensateErr := r.compensatePendingRaffleOrder(ctx, order)
		if compensateErr != nil {
			logger.Warn("compensate pending raffle order failed after task build error", "orderID", order.OrderID, "err", compensateErr)
		}
		return taskErr
	}

	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if createOrderErr := tx.Table("user_raffle_order_" + tableSuffix).Create(orderPO).Error; createOrderErr != nil {
			return createOrderErr
		}

		if createTaskErr := tx.Table("task").Create(taskPO).Error; createTaskErr != nil {
			return createTaskErr
		}

		return nil
	})
	if err == nil {
		return nil
	}

	if errors.Is(err, gorm.ErrDuplicatedKey) || strings.Contains(err.Error(), "Duplicate entry") {
		return nil
	}

	compensateErr := r.compensatePendingRaffleOrder(ctx, order)
	if compensateErr != nil {
		logger.Warn("compensate pending raffle order failed", "orderID", order.OrderID, "err", compensateErr)
	}

	return err
}

func (r *Repository) QueryActivityAccountDay(ctx context.Context, userID string, activityID int64, day string) (*activitybiz.ActivityAccountDay, error) {
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

func (r *Repository) QueryActivityAccountMonth(ctx context.Context, userID string, activityID int64, month string) (*activitybiz.ActivityAccountMonth, error) {
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

func (r *Repository) AsyncSaveCreatePartakeOrderAggregate(ctx context.Context, createPartakeOrderAggregate *activitybiz.CreatePartakeOrder) error {
	baseEvent := rabbitmq.NewBaseEvent(createPartakeOrderAggregate)
	return r.stockZeroPublisher.PublishSaveOrder(ctx, baseEvent)
}

func (r *Repository) SaveCreatePartakeOrderAggregate(ctx context.Context, createPartakeOrderAggregate *activitybiz.CreatePartakeOrder) error {
	userID := createPartakeOrderAggregate.UserID
	if createPartakeOrderAggregate.UserRaffleOrder == nil || createPartakeOrderAggregate.UserRaffleOrder.OrderID == "" {
		return activitybiz.ErrInvalidParams
	}

	orderID := createPartakeOrderAggregate.UserRaffleOrder.OrderID
	db, tableSuffix := r.routerDB.DBStrategy(userID)
	if db == nil {
		return activitybiz.ErrDBRouterError
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
				return activitybiz.ErrRecordNotFound
			}
			return err
		}

		if orderPO.AccountSyncState == string(activitybiz.AccountSyncStateCompleted) {
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
				return activitybiz.ErrRecordNotFound
			}
			return err
		}

		if accountPO.TotalCountSurplus <= 0 {
			return activitybiz.ErrActivityQuotaError
		}
		if accountPO.MonthCountSurplus <= 0 {
			return activitybiz.ErrActivityAccountMonthCountSurplusNotEnough
		}
		if accountPO.DayCountSurplus <= 0 {
			return activitybiz.ErrActivityAccountDayCountSurplusNotEnough
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
				return activitybiz.ErrActivityAccountMonthCountSurplusNotEnough
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
				return activitybiz.ErrActivityAccountMonthCountSurplusNotEnough
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
					return activitybiz.ErrDBIndexDuplicate
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
				return activitybiz.ErrActivityAccountDayCountSurplusNotEnough
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
				return activitybiz.ErrActivityAccountDayCountSurplusNotEnough
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
					return activitybiz.ErrDBIndexDuplicate
				}
				return createDayErr
			}
		} else {
			return err
		}

		if err := tx.Table(orderTable).
			Where("user_id = ? AND order_id = ?", userID, orderID).
			Updates(map[string]interface{}{
				"account_sync_state": string(activitybiz.AccountSyncStateCompleted),
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

func (r *Repository) compensatePendingRaffleOrder(ctx context.Context, order *activitybiz.UserRaffleOrder) error {
	totalKey := adapter.GetActivityAccountTotalSurplusKey(order.ActivityID, order.UserID)
	dayKey := adapter.GetActivityAccountDaySurplusKey(order.ActivityID, order.UserID, order.OrderTime.Format("2006-01-02"))
	monthKey := adapter.GetActivityAccountMonthSurplusKey(order.ActivityID, order.UserID, order.OrderTime.Format("2006-01"))
	pendingOrderKey := adapter.GetPendingRaffleOrderKey(order.ActivityID, order.UserID)

	script := `
		local pending = redis.call("HMGET", KEYS[4], "oid", "st")
		if not pending[1] or pending[1] ~= ARGV[1] then
			return 0
		end

		if pending[2] ~= "0" then
			return 0
		end

		redis.call("INCR", KEYS[1])
		redis.call("INCR", KEYS[2])
		redis.call("INCR", KEYS[3])
		redis.call("DEL", KEYS[4])
		return 1
	`

	_, err := r.redis.Eval(ctx, script, []string{totalKey, dayKey, monthKey, pendingOrderKey}, order.OrderID)
	return err
}

func buildSaveOrderTaskPO(userID, messageID string, aggregate *activitybiz.CreatePartakeOrder) (*po.Task, error) {
	message := activitybiz.SaveOrderTaskMessage{
		UserID:  aggregate.UserID,
		OrderID: aggregate.UserRaffleOrder.OrderID,
	}

	messageBytes, err := json.Marshal(message)
	if err != nil {
		return nil, err
	}

	return &po.Task{
		UserID:     userID,
		Topic:      activitybiz.SaveOrderRecordTopic,
		MessageID:  messageID,
		Message:    string(messageBytes),
		State:      "create",
		CreateTime: time.Now(),
		UpdateTime: time.Now(),
	}, nil
}
