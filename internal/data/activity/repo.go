package activity

import (
	activitybiz "big-market-kratos/internal/biz/activity"
	"big-market-kratos/internal/data/infra"
	"big-market-kratos/internal/data/po"
	"big-market-kratos/pkg/cache"
	"context"
	"errors"
	"time"

	"github.com/hibiken/asynq"
	"gorm.io/gorm"
)

type Repository struct {
	routerDB           *infra.DBRouter
	db                 *gorm.DB
	redis              *cache.Cache
	stockZeroPublisher *infra.Publisher
	queue              *asynq.Client
	inspector          *asynq.Inspector
}

// NewRepository 构造活动仓储实现
func NewRepository(routerDB *infra.DBRouter, db *gorm.DB, redis *cache.Cache, stockZeroPublisher *infra.Publisher, queue *asynq.Client, inspector *asynq.Inspector) activitybiz.Repo {
	return &Repository{
		routerDB:           routerDB,
		db:                 db,
		redis:              redis,
		stockZeroPublisher: stockZeroPublisher,
		queue:              queue,
		inspector:          inspector,
	}
}

// QueryActivitySkuByActivityID 根据活动ID查询活动商品配置数量
func (d *Repository) QueryActivitySkuByActivityID(ctx context.Context, activityID int64) ([]*activitybiz.ActivitySku, error) {
	var activitySkus []*po.RaffleActivitySku
	err := d.db.WithContext(ctx).
		Model(&po.RaffleActivitySku{}).
		Where("activity_id = ?", activityID).
		Find(&activitySkus).Error
	if err != nil {
		return nil, err
	}
	var activitySkusResult []*activitybiz.ActivitySku
	for _, activitySku := range activitySkus {
		activitySkusResult = append(activitySkusResult, activitySku.ToEntity())
	}
	return activitySkusResult, nil
}

// QueryActivitySku 根据 sku 查询活动商品配置
func (d *Repository) QueryActivitySku(ctx context.Context, sku int64) (*activitybiz.ActivitySku, error) {
	var activitySku activitybiz.ActivitySku

	cacheKey := infra.GetActivitySkuKey(sku)
	err := d.redis.Once(&cache.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &activitySku,
		TTL:   10 * 24 * time.Hour,
		Do: func(*cache.Item) (interface{}, error) {
			var dbResult po.RaffleActivitySku
			err := d.db.WithContext(ctx).
				Where("sku = ?", sku).
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
	if activitySku.Sku == 0 {
		return nil, nil
	}
	return &activitySku, nil
}

// QueryRaffleActivityByActivityId 根据活动ID查询活动配置
func (d *Repository) QueryRaffleActivityByActivityId(ctx context.Context, activityID int64) (*activitybiz.Activity, error) {
	var activity activitybiz.Activity

	cacheKey := infra.GetActivityKey(activityID)
	err := d.redis.Once(&cache.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &activity,
		TTL:   10 * 24 * time.Hour,
		Do: func(*cache.Item) (interface{}, error) {
			var dbResult po.RaffleActivity
			err := d.db.WithContext(ctx).
				Where("activity_id = ?", activityID).
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
	if activity.ActivityID == 0 {
		return nil, nil
	}
	return &activity, nil
}

// QueryRaffleActivityCountByActivityCountId 根据次数配置ID查询活动次数配置
func (d *Repository) QueryRaffleActivityCountByActivityCountId(ctx context.Context, activityCountID int64) (*activitybiz.ActivityCount, error) {
	var activityCount activitybiz.ActivityCount

	cacheKey := infra.GetActivityCountKey(activityCountID)
	err := d.redis.Once(&cache.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &activityCount,
		TTL:   10 * 24 * time.Hour,
		Do: func(*cache.Item) (interface{}, error) {
			var dbResult po.RaffleActivityCount
			err := d.db.WithContext(ctx).
				Where("activity_count_id = ?", activityCountID).
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
	if activityCount.ActivityCountID == 0 {
		return nil, nil
	}
	return &activityCount, nil
}
