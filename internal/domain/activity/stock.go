package activity

import (
	"context"
	"fmt"
	"prizeforge/pkg/logger"
	"time"
)

type StockManager struct {
	repo Repo
}

func NewStockManager(repo Repo) *StockManager {
	return &StockManager{repo: repo}
}

func (s *StockManager) AssembleActivitySkuByActivityId(ctx context.Context, activityID int64) (bool, error) {
	return s.assembleActivitySkuByActivityId(ctx, activityID)
}

func (s *StockManager) assembleActivitySkuByActivityId(ctx context.Context, activityID int64) (bool, error) {
	// 1. 预热活动商品信息
	activitySkus, err := s.repo.QueryActivitySkuByActivityID(ctx, activityID)
	if err != nil {
		return false, err
	}
	if activitySkus == nil {
		logger.Info("assembleActivitySkuByActivityId: activitySkus is nil", "activityID", activityID)
		return false, nil
	}

	for _, activitySku := range activitySkus {
		s.cacheActivitySkuStockCount(ctx, activitySku.Sku, activitySku.StockCount)
		activityCount, err := s.repo.QueryRaffleActivityCountByActivityCountId(ctx, activitySku.ActivityCountID)
		if err != nil {
			return false, err
		}
		if activityCount == nil {
			logger.Info("assembleActivitySkuByActivityId: activityCount is nil", "activityCountID", activitySku.ActivityCountID)
			return false, nil
		}
	}

	activity, err := s.repo.QueryRaffleActivityByActivityId(ctx, activityID)
	if err != nil {
		return false, err
	}
	if activity == nil {
		logger.Info("assembleActivitySkuByActivityId: activity is nil", "activityID", activityID)
		return false, nil
	}

	return true, nil
}

func (s *StockManager) assembleActivitySku(ctx context.Context, skuID int64) (bool, error) {
	// 1. 预热活动商品信息
	activitySku, err := s.repo.QueryActivitySku(ctx, skuID)
	if err != nil {
		return false, err
	}
	if activitySku == nil {
		return false, nil
	}

	s.cacheActivitySkuStockCount(ctx, skuID, activitySku.StockCount)

	// 2. 预热活动配置信息
	activity, err := s.repo.QueryRaffleActivityByActivityId(ctx, activitySku.ActivityID)
	if err != nil {
		return false, err
	}

	if activity == nil {
		return false, nil
	}

	// 3. 查询活动库存信息
	activityCount, err := s.repo.QueryRaffleActivityCountByActivityCountId(ctx, activitySku.ActivityCountID)
	if err != nil {
		return false, err
	}
	if activityCount == nil {
		return false, nil
	}

	return true, nil
}

func (s *StockManager) cacheActivitySkuStockCount(ctx context.Context, skuID int64, stockCount int) error {
	cacheKey := getActivitySkuStockCountKey(skuID)
	if err := s.repo.CacheActivitySkuStockCount(ctx, cacheKey, stockCount); err != nil {
		return err
	}
	return nil
}

func (s *StockManager) subtractionActivitySkuStock(ctx context.Context, skuID int64, activityID int64, userID string, endDateTime time.Time) (*ActivityResult, error) {
	return s.repo.SubtractionActivitySkuStock(ctx, skuID, activityID, userID, endDateTime)
}

func getActivitySkuStockCountKey(sku int64) string {
	return fmt.Sprintf("activity_sku_stock_count_%d", sku)
}
