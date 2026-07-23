package activityrepo

import (
	"context"
	"errors"
	"prizeforge/internal/domain/activity"
	"prizeforge/internal/infrastructure/adapter"
	"prizeforge/internal/infrastructure/repository/po"
	"prizeforge/pkg/cache"
	"prizeforge/pkg/logger"
	"time"

	"gorm.io/gorm"
)

func (d *Repository) QueryActivityAccountEntity(ctx context.Context, userID string, activityID int64) (*activity.ActivityAccount, error) {
	return d.queryActivityAccountEntityAt(ctx, userID, activityID, time.Now())
}

func (d *Repository) queryActivityAccountEntityAt(ctx context.Context, userID string, activityID int64, currentTime time.Time) (*activity.ActivityAccount, error) {
	// 1. 查询总账户
	var accountPO po.RaffleActivityAccount
	db, _ := d.routerDB.DBStrategy(userID)
	if db == nil {
		return nil, activity.ErrDBRouterError
	}
	err := db.WithContext(ctx).Table("raffle_activity_account").
		Where("user_id = ? AND activity_id = ?", userID, activityID).
		First(&accountPO).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, activity.ErrRecordNotFound
		}
		return nil, err
	}

	// 2. 查询月账户
	var accountMonthPO po.RaffleActivityAccountMonth
	month := currentTime.Format("2006-01")
	err = db.WithContext(ctx).Table("raffle_activity_account_month").
		Where("user_id = ? AND activity_id = ? AND month = ?", userID, activityID, month).
		First(&accountMonthPO).Error

	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	// 3. 查询日账户
	var accountDayPO po.RaffleActivityAccountDay
	day := currentTime.Format("2006-01-02")
	err = db.WithContext(ctx).Table("raffle_activity_account_day").
		Where("user_id = ? AND activity_id = ? AND day = ?", userID, activityID, day).
		First(&accountDayPO).Error

	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	// 4. 组装实体
	activityAccount := &activity.ActivityAccount{
		UserID:            accountPO.UserID,
		ActivityID:        accountPO.ActivityID,
		TotalCount:        accountPO.TotalCount,
		TotalCountSurplus: accountPO.TotalCountSurplus,
		DayCount:          accountPO.DayCount,
		DayCountSurplus:   accountPO.DayCount,
		MonthCount:        accountPO.MonthCount,
		MonthCountSurplus: accountPO.MonthCount,
	}

	if accountMonthPO.ID > 0 {
		activityAccount.MonthCount = accountMonthPO.MonthCount
		activityAccount.MonthCountSurplus = accountMonthPO.MonthCountSurplus
	}

	if accountDayPO.ID > 0 {
		activityAccount.DayCount = accountDayPO.DayCount
		activityAccount.DayCountSurplus = accountDayPO.DayCountSurplus
	}

	return activityAccount, nil
}

func (d *Repository) AssembleActivityAccountByUserId(ctx context.Context, userID string, activityID int64) error {
	currentTime := time.Now()
	if d.activityAccountCacheReady(ctx, userID, activityID, currentTime) {
		return nil
	}

	account, err := d.queryActivityAccountEntityAt(ctx, userID, activityID, currentTime)
	if err != nil {
		return err
	}
	return d.cacheActivityAccountSnapshot(ctx, account, currentTime)
}

// WARNING：为了测压测，会把所有用户的额度都装配到缓存中
func (d *Repository) AssembleActivityAccountByActivityId(ctx context.Context, activityID int64) error {
	assembleCtx := context.Background()
	currentTime := time.Now()

	dbCount := d.routerDB.GetDBCount()
	for i := 1; i <= dbCount; i++ {
		db := d.routerDB.GetDB(i)
		if db == nil {
			continue
		}

		var accounts []po.RaffleActivityAccount
		err := db.WithContext(assembleCtx).Table("raffle_activity_account").
			Where("activity_id = ?", activityID).
			Find(&accounts).Error
		if err != nil {
			logger.Error("AssembleActivityAccountByActivityId query failed", "db", i, "err", err)
			continue
		}

		for _, account := range accounts {
			var accountMonthPO po.RaffleActivityAccountMonth
			month := currentTime.Format("2006-01")
			_ = db.WithContext(assembleCtx).Table("raffle_activity_account_month").
				Where("user_id = ? AND activity_id = ? AND month = ?", account.UserID, activityID, month).
				First(&accountMonthPO).Error

			var accountDayPO po.RaffleActivityAccountDay
			day := currentTime.Format("2006-01-02")
			_ = db.WithContext(assembleCtx).Table("raffle_activity_account_day").
				Where("user_id = ? AND activity_id = ? AND day = ?", account.UserID, activityID, day).
				First(&accountDayPO).Error

			activityAccount := &activity.ActivityAccount{
				UserID:            account.UserID,
				ActivityID:        account.ActivityID,
				TotalCount:        account.TotalCount,
				TotalCountSurplus: account.TotalCountSurplus,
				DayCount:          account.DayCount,
				DayCountSurplus:   account.DayCountSurplus,
				MonthCount:        account.MonthCount,
				MonthCountSurplus: account.MonthCountSurplus,
			}

			if accountMonthPO.ID > 0 {
				activityAccount.MonthCount = accountMonthPO.MonthCount
				activityAccount.MonthCountSurplus = accountMonthPO.MonthCountSurplus
			} else {
				activityAccount.MonthCount = account.MonthCount
				activityAccount.MonthCountSurplus = account.MonthCount
			}

			if accountDayPO.ID > 0 {
				activityAccount.DayCount = accountDayPO.DayCount
				activityAccount.DayCountSurplus = accountDayPO.DayCountSurplus
			} else {
				activityAccount.DayCount = account.DayCount
				activityAccount.DayCountSurplus = account.DayCount
			}

			_ = d.cacheActivityAccountSnapshot(assembleCtx, activityAccount, currentTime)
		}
	}
	return nil
}

func (d *Repository) cacheActivityAccountSnapshot(ctx context.Context, activityAccount *activity.ActivityAccount, currentTime time.Time) error {
	key := adapter.GetActivityAccountKey(activityAccount.ActivityID, activityAccount.UserID)
	if err := d.redis.Set(&cache.Item{
		Ctx:   ctx,
		Key:   key,
		Value: activityAccount,
		TTL:   time.Hour,
	}); err != nil {
		return err
	}

	return d.initializeActivityQuotaValues(
		ctx,
		activityAccount.UserID,
		activityAccount.ActivityID,
		currentTime,
		activityAccount.TotalCountSurplus,
		activityAccount.MonthCountSurplus,
		activityAccount.DayCountSurplus,
	)
}

func (d *Repository) activityAccountCacheReady(ctx context.Context, userID string, activityID int64, currentTime time.Time) bool {
	keys := []string{adapter.GetActivityAccountKey(activityID, userID)}
	keys = append(keys, activityQuotaKeys(userID, activityID, currentTime)[:3]...)
	result, err := d.redis.Eval(ctx, `return redis.call('EXISTS', unpack(KEYS))`, keys)
	if err != nil {
		return false
	}
	existing, ok := result.(int64)
	return ok && existing == int64(len(keys))
}
