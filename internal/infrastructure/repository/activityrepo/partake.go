package activityrepo

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"prizeforge/internal/domain/activity"
	"prizeforge/internal/infrastructure/adapter"
	"prizeforge/internal/infrastructure/repository/po"
	"prizeforge/pkg/cache"
	"prizeforge/pkg/logger"
	"prizeforge/pkg/rabbitmq"
	"strconv"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const pendingRaffleOrderTTL = 30 * time.Minute
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

// CreateOrLoadUserRaffleOrder 以 MySQL 为订单与额度真相源：
//  1. 同 request_id 重试直接复用原订单；
//  2. 用户已有未完成订单时继续该订单；
//  3. 否则在同一事务内扣减总/月/日额度并创建新订单。
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

	var result *activity.UserRaffleOrder
	reused := false
	quotaChanged := false

	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var accountPO po.RaffleActivityAccount
		if lockErr := tx.Table("raffle_activity_account").
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("user_id = ? AND activity_id = ?", order.UserID, order.ActivityID).
			First(&accountPO).Error; lockErr != nil {
			if errors.Is(lockErr, gorm.ErrRecordNotFound) {
				return activity.ErrActivityQuotaError
			}
			return lockErr
		}

		// 账户锁拿到以后再查一次，覆盖并发请求同时未命中快路径的窗口。
		var requestOrderPO po.UserRaffleOrder
		requestErr := tx.Table(orderTable).
			Where("user_id = ? AND activity_id = ? AND request_id = ?", order.UserID, order.ActivityID, order.RequestID).
			First(&requestOrderPO).Error
		if requestErr == nil {
			result = requestOrderPO.ToEntity()
			reused = true
			return nil
		}
		if !errors.Is(requestErr, gorm.ErrRecordNotFound) {
			return requestErr
		}

		// 即使客户端生成了新的 request_id，也必须优先完成已经扣过额度的订单。
		if accountPO.CurrentOrderID != "" {
			var activeOrderPO po.UserRaffleOrder
			activeErr := tx.Table(orderTable).
				Where("user_id = ? AND activity_id = ? AND order_id = ?", order.UserID, order.ActivityID, accountPO.CurrentOrderID).
				First(&activeOrderPO).Error
			if activeErr == nil &&
				(activeOrderPO.DrawState == string(activity.DrawStateCreated) ||
					activeOrderPO.DrawState == string(activity.DrawStateProcessing)) {
				if activeOrderPO.RequestID != order.RequestID {
					// 不把一个新的点击幂等键静默绑定到旧订单；客户端应保存并复用原 request_id。
					return activity.ErrDrawInProgress
				}
				result = activeOrderPO.ToEntity()
				reused = true
				return nil
			}
			if activeErr != nil && !errors.Is(activeErr, gorm.ErrRecordNotFound) {
				return activeErr
			}

			// success/cancelled 或悬空引用不应阻塞下一次真实抽奖。
			if clearErr := tx.Table("raffle_activity_account").
				Where("user_id = ? AND activity_id = ?", order.UserID, order.ActivityID).
				Update("current_order_id", "").Error; clearErr != nil {
				return clearErr
			}
			accountPO.CurrentOrderID = ""
		}

		if accountPO.TotalCountSurplus <= 0 {
			return activity.ErrActivityQuotaError
		}
		if accountPO.DayCountSurplus <= 0 {
			return activity.ErrActivityAccountDayCountSurplusNotEnough
		}
		if accountPO.MonthCountSurplus <= 0 {
			return activity.ErrActivityAccountMonthCountSurplusNotEnough
		}

		orderDay := order.OrderTime.Format("2006-01-02")
		orderMonth := order.OrderTime.Format("2006-01")

		if monthErr := consumeMonthQuota(tx, &accountPO, orderMonth); monthErr != nil {
			return monthErr
		}
		if dayErr := consumeDayQuota(tx, &accountPO, orderDay); dayErr != nil {
			return dayErr
		}

		orderPO := &po.UserRaffleOrder{
			UserID:           order.UserID,
			ActivityID:       order.ActivityID,
			ActivityName:     order.ActivityName,
			StrategyID:       order.StrategyID,
			OrderID:          order.OrderID,
			RequestID:        order.RequestID,
			OrderTime:        order.OrderTime,
			OrderState:       string(activity.UserRaffleOrderStateCreate),
			DrawState:        string(activity.DrawStateCreated),
			AccountSyncState: string(activity.AccountSyncStateCompleted),
		}
		if createErr := tx.Table(orderTable).Create(orderPO).Error; createErr != nil {
			return createErr
		}

		accountUpdate := tx.Table("raffle_activity_account").
			Where("user_id = ? AND activity_id = ?", order.UserID, order.ActivityID).
			Updates(map[string]interface{}{
				"total_count_surplus": gorm.Expr("total_count_surplus - 1"),
				"day_count_surplus":   gorm.Expr("day_count_surplus - 1"),
				"month_count_surplus": gorm.Expr("month_count_surplus - 1"),
				"current_order_id":    order.OrderID,
				"update_time":         time.Now(),
			})
		if accountUpdate.Error != nil {
			return accountUpdate.Error
		}
		if accountUpdate.RowsAffected != 1 {
			return activity.ErrActivityQuotaError
		}

		result = orderPO.ToEntity()
		quotaChanged = true
		return nil
	})
	if err != nil {
		return nil, false, err
	}

	if quotaChanged {
		r.invalidateActivityAccountCache(ctx, order.UserID, order.ActivityID, order.OrderTime)
	}
	// 清除旧版 pending key，避免部署升级期间旧数据影响其他兼容入口。
	_ = r.redis.Delete(ctx, adapter.GetPendingRaffleOrderKey(order.ActivityID, order.UserID))
	return result, reused, nil
}

func consumeMonthQuota(tx *gorm.DB, accountPO *po.RaffleActivityAccount, month string) error {
	var monthPO po.RaffleActivityAccountMonth
	err := tx.Table("raffle_activity_account_month").
		Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("user_id = ? AND activity_id = ? AND month = ?", accountPO.UserID, accountPO.ActivityID, month).
		First(&monthPO).Error
	if err == nil {
		if monthPO.MonthCountSurplus <= 0 {
			return activity.ErrActivityAccountMonthCountSurplusNotEnough
		}
		res := tx.Table("raffle_activity_account_month").
			Where("user_id = ? AND activity_id = ? AND month = ?", accountPO.UserID, accountPO.ActivityID, month).
			Update("month_count_surplus", gorm.Expr("month_count_surplus - 1"))
		return res.Error
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	if accountPO.MonthCountSurplus <= 0 {
		return activity.ErrActivityAccountMonthCountSurplusNotEnough
	}
	return tx.Table("raffle_activity_account_month").Create(&po.RaffleActivityAccountMonth{
		UserID:            accountPO.UserID,
		ActivityID:        accountPO.ActivityID,
		Month:             month,
		MonthCount:        accountPO.MonthCount,
		MonthCountSurplus: accountPO.MonthCountSurplus - 1,
	}).Error
}

func consumeDayQuota(tx *gorm.DB, accountPO *po.RaffleActivityAccount, day string) error {
	var dayPO po.RaffleActivityAccountDay
	err := tx.Table("raffle_activity_account_day").
		Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("user_id = ? AND activity_id = ? AND day = ?", accountPO.UserID, accountPO.ActivityID, day).
		First(&dayPO).Error
	if err == nil {
		if dayPO.DayCountSurplus <= 0 {
			return activity.ErrActivityAccountDayCountSurplusNotEnough
		}
		res := tx.Table("raffle_activity_account_day").
			Where("user_id = ? AND activity_id = ? AND day = ?", accountPO.UserID, accountPO.ActivityID, day).
			Update("day_count_surplus", gorm.Expr("day_count_surplus - 1"))
		return res.Error
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	if accountPO.DayCountSurplus <= 0 {
		return activity.ErrActivityAccountDayCountSurplusNotEnough
	}
	return tx.Table("raffle_activity_account_day").Create(&po.RaffleActivityAccountDay{
		UserID:          accountPO.UserID,
		ActivityID:      accountPO.ActivityID,
		Day:             day,
		DayCount:        accountPO.DayCount,
		DayCountSurplus: accountPO.DayCountSurplus - 1,
	}).Error
}

func (r *Repository) invalidateActivityAccountCache(ctx context.Context, userID string, activityID int64, orderTime time.Time) {
	keys := []string{
		adapter.GetActivityAccountKey(activityID, userID),
		adapter.GetActivityAccountTotalSurplusKey(activityID, userID),
		adapter.GetActivityAccountDaySurplusKey(activityID, userID, orderTime.Format("2006-01-02")),
		adapter.GetActivityAccountMonthSurplusKey(activityID, userID, orderTime.Format("2006-01")),
	}
	for _, key := range keys {
		if err := r.redis.Delete(ctx, key); err != nil {
			logger.Warn("invalidate activity account cache failed", "key", key, "err", err)
		}
	}
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

func (r *Repository) CacheGetOrCreateNoUsedRaffleOrder(ctx context.Context, order *activity.UserRaffleOrder) (*activity.UserRaffleOrder, bool, error) {
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
		return nil, false, activity.ErrActivityQuotaError
	case -3:
		return nil, false, activity.ErrActivityAccountDayCountSurplusNotEnough
	case -4:
		return nil, false, activity.ErrActivityAccountMonthCountSurplusNotEnough
	case 1, 2:
		if len(resultArray) < 4 {
			return nil, false, fmt.Errorf("unexpected redis eval payload length: %d", len(resultArray))
		}

		strategyID, err := strconv.ParseInt(fmt.Sprint(resultArray[2]), 10, 64)
		if err != nil {
			return nil, false, err
		}

		return &activity.UserRaffleOrder{
			UserID:       order.UserID,
			ActivityID:   order.ActivityID,
			ActivityName: order.ActivityName,
			StrategyID:   strategyID,
			OrderID:      fmt.Sprint(resultArray[1]),
			OrderTime:    order.OrderTime,
			OrderState:   activity.UserRaffleOrderStateCreate,
		}, status == 2, nil
	default:
		return nil, false, fmt.Errorf("unexpected redis eval status: %d", status)
	}
}

func (r *Repository) SaveLiteUserRaffleOrder(ctx context.Context, aggregate *activity.CreatePartakeOrder) error {
	order := aggregate.UserRaffleOrder
	db, tableSuffix := r.routerDB.DBStrategy(order.UserID)
	if db == nil {
		compensateErr := r.compensatePendingRaffleOrder(ctx, order)
		if compensateErr != nil {
			logger.Warn("compensate pending raffle order failed after db router error", "orderID", order.OrderID, "err", compensateErr)
		}
		return activity.ErrDBRouterError
	}

	orderPO := &po.UserRaffleOrder{
		UserID:           order.UserID,
		ActivityID:       order.ActivityID,
		ActivityName:     order.ActivityName,
		StrategyID:       order.StrategyID,
		OrderID:          order.OrderID,
		OrderTime:        order.OrderTime,
		OrderState:       string(order.OrderState),
		AccountSyncState: string(activity.AccountSyncStateCreate),
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

func (r *Repository) compensatePendingRaffleOrder(ctx context.Context, order *activity.UserRaffleOrder) error {
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

func buildSaveOrderTaskPO(userID, messageID string, aggregate *activity.CreatePartakeOrder) (*po.Task, error) {
	message := activity.SaveOrderTaskMessage{
		UserID:  aggregate.UserID,
		OrderID: aggregate.UserRaffleOrder.OrderID,
	}

	messageBytes, err := json.Marshal(message)
	if err != nil {
		return nil, err
	}

	return &po.Task{
		UserID:     userID,
		Topic:      activity.SaveOrderRecordTopic,
		MessageID:  messageID,
		Message:    string(messageBytes),
		State:      "create",
		CreateTime: time.Now(),
		UpdateTime: time.Now(),
	}, nil
}
