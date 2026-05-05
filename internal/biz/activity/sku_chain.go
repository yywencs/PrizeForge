package activity

import (
	"big-market-kratos/pkg/logger"
	"context"
	"time"
)

type BaseActionChain struct {
	BaseChain
}

func newBaseActionChain() ActionChain {
	return &BaseActionChain{}
}

func (c *BaseActionChain) Action(ctx context.Context, activitySku *ActivitySku, activity *Activity, activityCount *ActivityCount) (bool, error) {
	logger.Info("活动责任链-基础信息【有效期、状态】校验开始。")

	if activity.State != ActivityStateOpen {
		return false, ErrActivityStateError
	}

	if activity.BeginDateTime.After(time.Now()) || activity.EndDateTime.Before(time.Now()) {
		return false, ErrActivityTimeError
	}

	if activitySku.StockCount <= 0 {
		return false, ErrActivityStockError
	}

	if c.nextChain != nil {
		return c.nextChain.Action(ctx, activitySku, activity, activityCount)
	}
	return true, nil
}

type Factory struct {
	repo Repo
}

func NewFactory(repo Repo) *Factory {
	return &Factory{
		repo: repo,
	}
}

func (f *Factory) OpenLogicChain() ActionChain {
	baseAction := newBaseActionChain()
	skuStockAction := newSkuStockActionChain(f.repo)

	baseAction.appendNext(skuStockAction)
	return baseAction
}

type ActionChain interface {
	Action(ctx context.Context, activitySku *ActivitySku, activity *Activity, activityCount *ActivityCount) (bool, error)
	appendNext(chain ActionChain)
}

type BaseChain struct {
	nextChain ActionChain
}

func (c *BaseChain) appendNext(chain ActionChain) {
	c.nextChain = chain
}

func (c *BaseChain) next() ActionChain {
	return c.nextChain
}

type SkuStockActionChain struct {
	repo Repo
	BaseChain
}

func newSkuStockActionChain(repo Repo) ActionChain {
	return &SkuStockActionChain{
		repo: repo,
	}
}

func (c *SkuStockActionChain) Action(ctx context.Context, activitySku *ActivitySku, activity *Activity, activityCount *ActivityCount) (bool, error) {
	logger.Info("活动责任链-商品库存处理【校验&扣减】开始。")

	result, err := c.repo.SubtractionActivitySkuStock(ctx, activitySku.Sku, activity.ActivityID, "user_id", activity.EndDateTime)
	if err != nil {
		return false, err
	}

	if result.Status == ActivityResultStatusSuccess {
		logger.Info("活动责任链-商品库存处理【校验&扣减】成功。",
			"sku", activitySku.Sku,
			"activityID", activity.ActivityID,
		)
		if err := c.repo.ActivitySkuStockConsumeSendQueue(ctx, &ActivitySkuStockKey{
			Sku:        activitySku.Sku,
			ActivityID: activity.ActivityID,
		}); err != nil {
			return false, err
		}
		return true, nil
	}

	return false, ErrActivitySkuStockError
}
