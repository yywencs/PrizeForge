package activity

import (
	activitybiz "big-market-kratos/internal/biz/activity"
	"big-market-kratos/internal/data/po"
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
)

func (d *Repository) SaveOrder(ctx context.Context, activityOrderAggregate *activitybiz.CreateQuotaOrder) error {
	// 1. 获取 DB 和 分表后缀
	db, tableSuffix := d.routerDB.DBStrategy(activityOrderAggregate.UserID)
	if db == nil {
		return activitybiz.ErrDBRouterError
	}

	// 2. 转换对象 PO
	// 2.1 订单对象
	activityOrderEntity := activityOrderAggregate.ActivityOrder
	raffleActivityOrder := &po.RaffleActivityOrder{
		UserID:        activityOrderEntity.UserID,
		Sku:           activityOrderEntity.Sku,
		ActivityID:    activityOrderEntity.ActivityID,
		ActivityName:  activityOrderEntity.ActivityName,
		StrategyID:    activityOrderEntity.StrategyID,
		OrderID:       activityOrderEntity.OrderID,
		OrderTime:     activityOrderEntity.OrderTime,
		TotalCount:    activityOrderEntity.TotalCount,
		DayCount:      activityOrderEntity.DayCount,
		MonthCount:    activityOrderEntity.MonthCount,
		State:         activityOrderEntity.State,
		OutBusinessNo: activityOrderEntity.OutBusinessNo,
	}
	// 2.2 账户对象
	raffleActivityAccount := &po.RaffleActivityAccount{
		UserID:            activityOrderAggregate.UserID,
		ActivityID:        activityOrderAggregate.ActivityID,
		TotalCount:        activityOrderAggregate.TotalCount,
		TotalCountSurplus: activityOrderAggregate.TotalCount,
		DayCount:          activityOrderAggregate.DayCount,
		DayCountSurplus:   activityOrderAggregate.DayCount,
		MonthCount:        activityOrderAggregate.MonthCount,
		MonthCountSurplus: activityOrderAggregate.MonthCount,
	}

	// 2.3 账户对象 - 日
	raffleActivityAccountDay := &po.RaffleActivityAccountDay{
		UserID:          activityOrderAggregate.UserID,
		ActivityID:      activityOrderAggregate.ActivityID,
		Day:             activityOrderAggregate.ActivityOrder.OrderTime.Format("2006-01-02"),
		DayCount:        activityOrderAggregate.DayCount,
		DayCountSurplus: activityOrderAggregate.DayCount,
	}

	// 2.4 账户对象 - 月
	raffleActivityAccountMonth := &po.RaffleActivityAccountMonth{
		UserID:            activityOrderAggregate.UserID,
		ActivityID:        activityOrderAggregate.ActivityID,
		Month:             activityOrderAggregate.ActivityOrder.OrderTime.Format("2006-01"),
		MonthCount:        activityOrderAggregate.MonthCount,
		MonthCountSurplus: activityOrderAggregate.MonthCount,
	}

	// 3. 执行事务
	return db.Transaction(func(tx *gorm.DB) error {
		// 3.1 写入订单
		if err := tx.Table("raffle_activity_order_" + tableSuffix).Create(raffleActivityOrder).Error; err != nil {
			if errors.Is(err, gorm.ErrDuplicatedKey) {
				return activitybiz.ErrDBIndexDuplicate
			}
			return err
		}

		// 3.2 更新账户
		res := tx.Table("raffle_activity_account").
			Where("user_id = ? AND activity_id = ?", raffleActivityAccount.UserID, raffleActivityAccount.ActivityID).
			Updates(map[string]interface{}{
				"total_count":         gorm.Expr("total_count + ?", raffleActivityAccount.TotalCount),
				"total_count_surplus": gorm.Expr("total_count_surplus + ?", raffleActivityAccount.TotalCountSurplus),
				"day_count":           gorm.Expr("day_count + ?", raffleActivityAccount.DayCount),
				"day_count_surplus":   gorm.Expr("day_count_surplus + ?", raffleActivityAccount.DayCountSurplus),
				"month_count":         gorm.Expr("month_count + ?", raffleActivityAccount.MonthCount),
				"month_count_surplus": gorm.Expr("month_count_surplus + ?", raffleActivityAccount.MonthCountSurplus),
				"update_time":         time.Now(),
			})
		if res.Error != nil {
			return res.Error
		}

		// 3.3 创建账户 - 更新为0，则账户不存在，创建新账户
		if res.RowsAffected == 0 {
			if err := tx.Table("raffle_activity_account").Create(raffleActivityAccount).Error; err != nil {
				if errors.Is(err, gorm.ErrDuplicatedKey) {
					return activitybiz.ErrDBIndexDuplicate
				}
				return err
			}
		}

		// 3.4 更新账户 - 日
		if raffleActivityAccountDay.DayCount != 0 {
			resDay := tx.Table("raffle_activity_account_day").
				Where("user_id = ? AND activity_id = ? AND day = ?", raffleActivityAccountDay.UserID, raffleActivityAccountDay.ActivityID, raffleActivityAccountDay.Day).
				Updates(map[string]interface{}{
					"day_count":         gorm.Expr("day_count + ?", raffleActivityAccountDay.DayCount),
					"day_count_surplus": gorm.Expr("day_count_surplus + ?", raffleActivityAccountDay.DayCountSurplus),
					"update_time":       time.Now(),
				})
			if resDay.Error != nil {
				return resDay.Error
			}
			if resDay.RowsAffected == 0 {
				if err := tx.Table("raffle_activity_account_day").Create(raffleActivityAccountDay).Error; err != nil {
					if errors.Is(err, gorm.ErrDuplicatedKey) {
						return activitybiz.ErrDBIndexDuplicate
					}
					return err
				}
			}
		}

		// 3.5 更新账户 - 月
		if raffleActivityAccountMonth.MonthCount != 0 {
			resMonth := tx.Table("raffle_activity_account_month").
				Where("user_id = ? AND activity_id = ? AND month = ?", raffleActivityAccountMonth.UserID, raffleActivityAccountMonth.ActivityID, raffleActivityAccountMonth.Month).
				Updates(map[string]interface{}{
					"month_count":         gorm.Expr("month_count + ?", raffleActivityAccountMonth.MonthCount),
					"month_count_surplus": gorm.Expr("month_count_surplus + ?", raffleActivityAccountMonth.MonthCountSurplus),
					"update_time":         time.Now(),
				})
			if resMonth.Error != nil {
				return resMonth.Error
			}
			if resMonth.RowsAffected == 0 {
				if err := tx.Table("raffle_activity_account_month").Create(raffleActivityAccountMonth).Error; err != nil {
					if errors.Is(err, gorm.ErrDuplicatedKey) {
						return activitybiz.ErrDBIndexDuplicate
					}
					return err
				}
			}
		}

		return nil
	})
}
