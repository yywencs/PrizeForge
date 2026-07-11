package strategyrepo

import (
	"context"
	"errors"
	"fmt"
	"prizeforge/internal/domain/activity"
	"prizeforge/internal/domain/strategy"
	"prizeforge/internal/infrastructure/adapter"
	"prizeforge/internal/infrastructure/repository/po"
	"prizeforge/pkg/cache"
	"time"

	"github.com/hibiken/asynq"
	"gorm.io/gorm"
)

// 专门负责数据库的实现
// 实现 StrategyRepository 接口方法
type strategyRepository struct {
	db       *gorm.DB
	redis    *cache.Cache
	routerDB *adapter.DBRouter
	queue    *asynq.Client
}

// 4. 构造函数分别返回
func NewStrategyRepository(db *gorm.DB, redis *cache.Cache, queue *asynq.Client, routerDB *adapter.DBRouter) strategy.Repo {
	repo := &strategyRepository{
		db:       db,
		routerDB: routerDB,
		redis:    redis,
		queue:    queue,
	}

	return repo
}

func (sr *strategyRepository) QueryStrategyIdByActivityId(ctx context.Context, activityID int64) (int64, error) {
	cacheKey := fmt.Sprintf("strategy_id_by_activity_%d", activityID)
	var strategyID int64

	err := sr.redis.Once(&cache.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &strategyID,
		TTL:   10 * 24 * time.Hour,
		Do: func(*cache.Item) (interface{}, error) {
			var activityPO po.RaffleActivity
			err := sr.db.WithContext(ctx).
				Where("activity_id = ?", activityID).
				First(&activityPO).Error

			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return nil, nil
				}
				return nil, err
			}
			return activityPO.StrategyID, nil
		},
	})

	if err != nil {
		return 0, err
	}

	return strategyID, nil
}

// 通过StrategyId查询StrategyEntity
func (sr *strategyRepository) QueryStrategyEntityByStrategyId(ctx context.Context, strategyID int64) (*strategy.Strategy, error) {
	var strategyEntity strategy.Strategy

	cacheKey := adapter.GetStrategyKey(strategyID)

	err := sr.redis.Once(&cache.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &strategyEntity,
		TTL:   10 * 24 * time.Hour,

		Do: func(*cache.Item) (interface{}, error) {
			var dbResult po.Strategy
			err := sr.db.WithContext(context.Background()).
				Where("strategy_id = ?", strategyID).
				First(&dbResult).Error

			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return nil, nil
				}
				return nil, err
			}

			return dbResult.ToEntity(), nil
		},
	})

	if err != nil {
		return nil, err
	}

	return &strategyEntity, nil
}

// QueryActivityAccountTotalUseCount 查询用户在某个策略下的总参与次数
func (d *strategyRepository) QueryActivityAccountTotalUseCount(ctx context.Context, userID string, strategyID int64) (int64, error) {
	var activityPO po.RaffleActivity
	activityKey := fmt.Sprintf("activity_by_strategy_%d", strategyID)

	err := d.redis.Once(&cache.Item{
		Ctx:   ctx,
		Key:   activityKey,
		Value: &activityPO,
		TTL:   10 * 24 * time.Hour,
		Do: func(*cache.Item) (interface{}, error) {
			var act po.RaffleActivity
			err := d.db.WithContext(ctx).
				Where("strategy_id = ?", strategyID).
				First(&act).Error
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return nil, nil
				}
				return nil, err
			}
			return &act, nil
		},
	})

	if err != nil {
		return 0, err
	}
	if activityPO.ActivityID == 0 {
		return 0, nil
	}

	accountKey := adapter.GetActivityAccountKey(activityPO.ActivityID, userID)
	var account activity.ActivityAccount

	err = d.redis.Once(&cache.Item{
		Ctx:   ctx,
		Key:   accountKey,
		Value: &account,
		TTL:   10 * 24 * time.Hour,
		Do: func(*cache.Item) (interface{}, error) {
			db, _ := d.routerDB.DBStrategy(userID)
			if db == nil {
				return nil, errors.New("db router failed")
			}
			var acc po.RaffleActivityAccount
			err = db.WithContext(ctx).Table("raffle_activity_account").
				Where("user_id = ? AND activity_id = ?", userID, activityPO.ActivityID).
				First(&acc).Error

			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return nil, nil
				}
				return nil, err
			}
			return &activity.ActivityAccount{
				UserID:            acc.UserID,
				ActivityID:        acc.ActivityID,
				TotalCount:        acc.TotalCount,
				TotalCountSurplus: acc.TotalCountSurplus,
				DayCount:          acc.DayCount,
				DayCountSurplus:   acc.DayCountSurplus,
				MonthCount:        acc.MonthCount,
				MonthCountSurplus: acc.MonthCountSurplus,
			}, nil
		},
	})

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, nil
		}
		return 0, err
	}

	return int64(account.TotalCount - account.TotalCountSurplus), nil
}
