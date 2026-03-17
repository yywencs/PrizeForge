package data

import (
	"big-market-kratos/internal/biz/activity"
	"big-market-kratos/internal/data/po"
	"big-market-kratos/pkg/cache"
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
)

func (r *Repository) QueryRaffleActivity(ctx context.Context, activityID int64) (*activity.Activity, error) {
	var activity activity.Activity

	activityKey := GetActivityKey(activityID)

	err := r.redis.Once(&cache.Item{
		Ctx:   ctx,
		Key:   activityKey,
		Value: &activity,
		TTL:   10 * time.Minute,
		Do: func(*cache.Item) (interface{}, error) {
			var activityPO po.RaffleActivity
			if err := r.db.Where("activity_id = ?", activityID).First(&activityPO).Error; err != nil {
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
	key := GetActivityAccountKey(activityID, userID)

	err := r.redis.Once(&cache.Item{
		Ctx:   ctx,
		Key:   key,
		Value: &activityAccount,
		TTL:   10 * time.Minute,
		Do: func(*cache.Item) (interface{}, error) {
			var po po.RaffleActivityAccount
			db, _ := r.routerDB.DBStrategy(userID)
			if err := db.Where("user_id = ? AND activity_id = ?", userID, activityID).First(&po).Error; err != nil {
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
	// 1. 获取分库分表 DB
	db, tableSuffix := r.routerDB.DBStrategy(userID)
	if db == nil {
		return nil, activity.ErrDBRouterError
	}

	// 2. 查询未被使用的订单
	var po po.UserRaffleOrder
	if err := db.WithContext(ctx).Table("user_raffle_order_"+tableSuffix).
		Where("user_id = ? AND activity_id = ? AND order_state = ?", userID, activityID, activity.UserRaffleOrderStateCreate).
		First(&po).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
	}

	// 3. 转换对象
	return po.ToEntity(), nil
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

func (r *Repository) SaveCreatePartakeOrderAggregate(ctx context.Context, createPartakeOrderAggregate *activity.CreatePartakeOrder) error {
	// 1. 获取 DB 和 分表后缀
	userID := createPartakeOrderAggregate.UserID
	activityID := createPartakeOrderAggregate.ActivityID
	db, tableSuffix := r.routerDB.DBStrategy(userID)
	if db == nil {
		return activity.ErrDBRouterError
	}

	// 2. 执行事务
	return db.Transaction(func(tx *gorm.DB) error {
		// 2.1 更新总账户额度
		// update raffle_activity_account set total_count_surplus = total_count_surplus - 1 where user_id = ? and activity_id = ? and total_count_surplus > 0
		res := tx.Table("raffle_activity_account").
			Where("user_id = ? AND activity_id = ? AND total_count_surplus > 0", userID, activityID).
			Updates(map[string]interface{}{
				"total_count_surplus": gorm.Expr("total_count_surplus - 1"),
				"day_count_surplus":   gorm.Expr("day_count_surplus - 1"),
				"month_count_surplus": gorm.Expr("month_count_surplus - 1"),
				"update_time":         time.Now(),
			})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return activity.ErrActivityQuotaError
		}

		// 2.2 创建或更新月账户
		if createPartakeOrderAggregate.IsExistAccountMonth {
			resMonth := tx.Table("raffle_activity_account_month").
				Where("user_id = ? AND activity_id = ? AND month = ? AND month_count_surplus > 0",
					userID, activityID, createPartakeOrderAggregate.ActivityAccountMonth.Month).
				Updates(map[string]interface{}{
					"month_count_surplus": gorm.Expr("month_count_surplus - 1"),
					"update_time":         time.Now(),
				})
			if resMonth.Error != nil {
				return resMonth.Error
			}
			if resMonth.RowsAffected == 0 {
				return activity.ErrActivityAccountMonthCountSurplusNotEnough
			}
		} else {
			// 插入月账户
			monthEntity := createPartakeOrderAggregate.ActivityAccountMonth
			monthPO := po.RaffleActivityAccountMonth{
				UserID:            monthEntity.UserID,
				ActivityID:        monthEntity.ActivityID,
				Month:             monthEntity.Month,
				MonthCount:        monthEntity.MonthCount,
				MonthCountSurplus: monthEntity.MonthCountSurplus - 1,
			}
			if err := tx.Table("raffle_activity_account_month").Create(&monthPO).Error; err != nil {
				if errors.Is(err, gorm.ErrDuplicatedKey) {
					return activity.ErrDBIndexDuplicate
				}
				return err
			}
			// 更新总账户月镜像
			if err := tx.Table("raffle_activity_account").
				Where("user_id = ? AND activity_id = ?", userID, activityID).
				Updates(map[string]interface{}{
					"month_count_surplus": monthEntity.MonthCountSurplus - 1,
					"update_time":         time.Now(),
				}).Error; err != nil {
				return err
			}
		}

		// 2.3 创建或更新日账户
		if createPartakeOrderAggregate.IsExistAccountDay {
			resDay := tx.Table("raffle_activity_account_day").
				Where("user_id = ? AND activity_id = ? AND day = ? AND day_count_surplus > 0",
					userID, activityID, createPartakeOrderAggregate.ActivityAccountDay.Day).
				Updates(map[string]interface{}{
					"day_count_surplus": gorm.Expr("day_count_surplus - 1"),
					"update_time":       time.Now(),
				})
			if resDay.Error != nil {
				return resDay.Error
			}
			if resDay.RowsAffected == 0 {
				return activity.ErrActivityAccountDayCountSurplusNotEnough
			}
		} else {
			// 插入日账户
			dayEntity := createPartakeOrderAggregate.ActivityAccountDay
			dayPO := po.RaffleActivityAccountDay{
				UserID:          dayEntity.UserID,
				ActivityID:      dayEntity.ActivityID,
				Day:             dayEntity.Day,
				DayCount:        dayEntity.DayCount,
				DayCountSurplus: dayEntity.DayCountSurplus - 1,
			}
			if err := tx.Table("raffle_activity_account_day").Create(&dayPO).Error; err != nil {
				if errors.Is(err, gorm.ErrDuplicatedKey) {
					return activity.ErrDBIndexDuplicate
				}
				return err
			}
			// 更新总账户日镜像
			if err := tx.Table("raffle_activity_account").
				Where("user_id = ? AND activity_id = ?", userID, activityID).
				Updates(map[string]interface{}{
					"day_count_surplus": dayEntity.DayCountSurplus - 1,
					"update_time":       time.Now(),
				}).Error; err != nil {
				return err
			}
		}

		// 2.4 写入参与活动订单
		orderEntity := createPartakeOrderAggregate.UserRaffleOrder
		orderPO := po.UserRaffleOrder{
			UserID:       orderEntity.UserID,
			ActivityID:   orderEntity.ActivityID,
			ActivityName: orderEntity.ActivityName,
			StrategyID:   orderEntity.StrategyID,
			OrderID:      orderEntity.OrderID,
			OrderTime:    orderEntity.OrderTime,
			OrderState:   string(orderEntity.OrderState),
		}
		if err := tx.Table("user_raffle_order_" + tableSuffix).Create(&orderPO).Error; err != nil {
			if errors.Is(err, gorm.ErrDuplicatedKey) {
				return activity.ErrDBIndexDuplicate
			}
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
