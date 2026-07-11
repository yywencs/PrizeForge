package strategy

import (
	"big-market-kratos/internal/data/infra"
	"big-market-kratos/pkg/cache"
	"context"
	"strconv"
	"time"
)

func (d *strategyRepository) StoreStrategyAwardPool(ctx context.Context, strategyID string, rateRange int, idxToAwardIDMap map[int]int64) error {
	err := d.redis.Set(&cache.Item{
		Ctx:   ctx,
		Key:   infra.GetStrategyRateRangeKey(strategyID),
		Value: rateRange,
		TTL:   10 * 24 * time.Hour,
	})

	if err != nil {
		return err
	}

	values := make(map[string]interface{}, len(idxToAwardIDMap))
	for k, v := range idxToAwardIDMap {
		values[strconv.Itoa(k)] = v
	}
	err = d.redis.HSetWithTTL(ctx, infra.GetStrategyRateTableKey(strategyID), values, 10*24*time.Hour)

	return err
}

func (d *strategyRepository) GetRateRange(ctx context.Context, strategyID string) (int, error) {
	var rateRange int
	err := d.redis.Get(ctx, infra.GetStrategyRateRangeKey(strategyID), &rateRange)

	return rateRange, err
}

func (d *strategyRepository) GetStrategyAwardAssemble(ctx context.Context, strategyID string, randomVal int) (int64, error) {
	valStr, err := d.redis.HGet(ctx, infra.GetStrategyRateTableKey(strategyID), strconv.Itoa(randomVal))

	if err != nil {
		return -1, err
	}

	return strconv.ParseInt(valStr, 10, 64)
}
