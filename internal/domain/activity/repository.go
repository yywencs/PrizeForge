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
	// SubtractionActivitySkuStock 活动商品库存减一，返回抽奖结果
	SubtractionActivitySkuStock(ctx context.Context, skuID int64, activityID int64, userID string, endTime time.Time) (*ActivityResult, error)
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
	// CreateOrLoadUserRaffleOrder 使用 Redis 原子预占额度，并创建或复用轻量抽奖订单。
	CreateOrLoadUserRaffleOrder(ctx context.Context, order *UserRaffleOrder) (*UserRaffleOrder, bool, error)
	// TryClaimUserRaffleOrder 原子抢占订单执行权；超时的 processing 订单允许被接管。
	TryClaimUserRaffleOrder(ctx context.Context, userID string, orderID string) (*DrawClaim, error)
	// ReleaseUserRaffleOrderClaim 将 processing 订单释放为 created，供明确失败后的请求立即重试。
	ReleaseUserRaffleOrderClaim(ctx context.Context, userID string, orderID string, owner string) error
	// 查询用户活动账户信息
	QueryActivityAccount(ctx context.Context, userID string, activityID int64) (*ActivityAccount, error)
	// 查询用户活动账户日信息
	QueryActivityAccountDay(ctx context.Context, userID string, activityID int64, day string) (*ActivityAccountDay, error)
	// 查询用户活动账户月信息
	QueryActivityAccountMonth(ctx context.Context, userID string, activityID int64, month string) (*ActivityAccountMonth, error)
	// SaveCreatePartakeOrderAggregate 保存创建参与活动订单聚合
	SaveCreatePartakeOrderAggregate(ctx context.Context, createPartakeOrderAggregate *CreatePartakeOrder) error
	// AsyncSaveCreatePartakeOrderAggregate 异步保存创建参与活动订单聚合
	AsyncSaveCreatePartakeOrderAggregate(ctx context.Context, createPartakeOrderAggregate *CreatePartakeOrder) error
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
	// AssembleActivityAccountByActivityId 组装活动对应的所有用户额度到缓存
	AssembleActivityAccountByActivityId(ctx context.Context, activityID int64) error
	// AssembleActivityAccountByUserId 组装单个用户活动额度到缓存
	AssembleActivityAccountByUserId(ctx context.Context, userID string, activityID int64) error
}
