package activity

import (
	"context"
	"time"
)

type Repo interface {
	// QueryActivitySku 根据 sku 查询活动商品信息
	QueryActivitySku(ctx context.Context, sku int64) (*ActivitySku, error)
	// QueryRaffleActivityByActivityId 根据活动ID查询抽奖活动信息
	QueryRaffleActivityByActivityId(ctx context.Context, activityID int64) (*Activity, error)
	// QueryRaffleActivityCountByActivityCountId 根据活动库存ID查询活动库存信息
	QueryRaffleActivityCountByActivityCountId(ctx context.Context, activityCountID int64) (*ActivityCount, error)
	// SaveOrder 保存活动订单信息
	SaveOrder(ctx context.Context, activityOrder *CreateQuotaOrder) error
	// CacheActivitySkuStockCount 缓存活动商品库存信息
	CacheActivitySkuStockCount(ctx context.Context, cacheKey string, stockCount int) error
	// SubtractionActivitySkuStock 活动商品库存减一
	SubtractionActivitySkuStock(ctx context.Context, skuID int64, endTime time.Time) (bool, error)
	// ActivitySkuStockConsumeSendQueue 活动商品库存消耗队列
	ActivitySkuStockConsumeSendQueue(ctx context.Context, key *ActivitySkuStockKey) error
	// ClearActivitySkuStock 清除活动商品库存缓存
	ClearActivitySkuStock(ctx context.Context, sku int64) error
	// ClearQueueValue 清除rabbitMQ队列
	ClearQueueValue(ctx context.Context) error
	// 查询抽奖活动信息
	QueryRaffleActivity(ctx context.Context, activityID int64) (*Activity, error)
	// 查询用户未使用的抽奖订单
	QueryNoUsedRaffleOrder(ctx context.Context, userID string, activityID int64) (*UserRaffleOrder, error)
	// 查询用户活动账户信息
	QueryActivityAccount(ctx context.Context, userID string, activityID int64) (*ActivityAccount, error)
	// 查询用户活动账户日信息
	QueryActivityAccountDay(ctx context.Context, userID string, activityID int64, day string) (*ActivityAccountDay, error)
	// 查询用户活动账户月信息
	QueryActivityAccountMonth(ctx context.Context, userID string, activityID int64, month string) (*ActivityAccountMonth, error)
	// 保存创建参与订单聚合根
	SaveCreatePartakeOrderAggregate(ctx context.Context, createPartakeOrderAggregate *CreatePartakeOrder) error
	// UpdateActivitySkuStock 更新活动商品库存
	UpdateActivitySkuStock(ctx context.Context, sku int64) error
	// 根据活动ID查询Sku
	QueryActivitySkuByActivityID(ctx context.Context, activityID int64) ([]*ActivitySku, error)
	// QueryActivityAccountEntity 查询用户活动账户实体
	QueryActivityAccountEntity(ctx context.Context, userID string, activityID int64) (*ActivityAccount, error)
	// QueryRaffleActivityAccountPartakeCount 查询用户活动账户参与次数
	QueryRaffleActivityAccountPartakeCount(ctx context.Context, userID string, activityID int64) (int64, error)
	// QueryRaffleActivityAccountDayPartakeCount 查询用户活动账户日参与次数
	QueryRaffleActivityAccountDayPartakeCount(ctx context.Context, userID string, activityID int64) (int64, error)
}
