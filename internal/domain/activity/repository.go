package activity

import (
	"context"
	"time"
)

// QuotaRepository 定义额度充值、账户查询和缓存预热用例所需的仓储能力。
type QuotaRepository interface {
	QueryActivitySku(ctx context.Context, sku int64) (*ActivitySku, error)
	QueryRaffleActivityByActivityId(ctx context.Context, activityID int64) (*Activity, error)
	QueryRaffleActivityCountByActivityCountId(ctx context.Context, activityCountID int64) (*ActivityCount, error)
	SaveOrder(ctx context.Context, activityOrder *CreateQuotaOrder) error
	ClearActivitySkuStock(ctx context.Context, sku int64) error
	ClearQueueValue(ctx context.Context) error
	UpdateActivitySkuStock(ctx context.Context, sku int64) error
	QueryActivityAccountEntity(ctx context.Context, userID string, activityID int64) (*ActivityAccount, error)
	QueryRaffleActivityAccountPartakeCount(ctx context.Context, userID string, activityID int64) (int64, error)
	QueryRaffleActivityAccountDayPartakeCount(ctx context.Context, userID string, activityID int64) (int64, error)
	AssembleActivityAccountByActivityId(ctx context.Context, activityID int64) error
	AssembleActivityAccountByUserId(ctx context.Context, userID string, activityID int64) error
}

// StockRepository 定义活动 SKU 库存装配和 Redis 扣减所需的仓储能力。
type StockRepository interface {
	QueryActivitySku(ctx context.Context, sku int64) (*ActivitySku, error)
	QueryActivitySkuByActivityID(ctx context.Context, activityID int64) ([]*ActivitySku, error)
	QueryRaffleActivityByActivityId(ctx context.Context, activityID int64) (*Activity, error)
	QueryRaffleActivityCountByActivityCountId(ctx context.Context, activityCountID int64) (*ActivityCount, error)
	CacheActivitySkuStockCount(ctx context.Context, cacheKey string, stockCount int) error
	SubtractionActivitySkuStock(ctx context.Context, skuID int64, activityID int64, userID string, endTime time.Time) (*ActivityResult, error)
}

// SkuStockActionRepository 是库存责任链运行时所需的最小接口。
type SkuStockActionRepository interface {
	SubtractionActivitySkuStock(ctx context.Context, skuID int64, activityID int64, userID string, endTime time.Time) (*ActivityResult, error)
	ActivitySkuStockConsumeSendQueue(ctx context.Context, key *ActivitySkuStockKey) error
}

// PartakeRepository 定义 Redis-first 抽奖生命周期、Stream 发布和结果落库能力。
type PartakeRepository interface {
	QueryRaffleActivity(ctx context.Context, activityID int64) (*Activity, error)
	CreateOrLoadUserRaffleOrder(ctx context.Context, order *UserRaffleOrder) (*UserRaffleOrder, *DrawResultPublication, bool, error)
	TryClaimUserRaffleOrder(ctx context.Context, userID string, activityID int64, requestID string, orderID string) (*DrawClaim, error)
	ReleaseUserRaffleOrderClaim(ctx context.Context, userID string, activityID int64, orderID string, owner string) error
	CompleteUserRaffleOrder(ctx context.Context, result *DrawResult, owner string) (*DrawResultPublication, error)
	QueryPendingDrawResultPublications(ctx context.Context, limit int64) ([]*DrawResultPublication, error)
	MarkDrawResultPublished(ctx context.Context, publication *DrawResultPublication) error
	SaveDrawResult(ctx context.Context, result *DrawResult) error
}
