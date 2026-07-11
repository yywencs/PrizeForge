package activity

import "prizeforge/internal/shared/xerr"

var (
	ErrActivityStateError                        = xerr.New("ACTIVITY_STATE_ERROR", "活动状态非开启")
	ErrActivityTimeError                         = xerr.New("ACTIVITY_TIME_ERROR", "活动时间未开始或已结束")
	ErrActivityQuotaError                        = xerr.New("ACTIVITY_QUOTA_ERROR", "活动账户额度不足")
	ErrActivityAccountDayCountSurplusNotEnough   = xerr.New("ACTIVITY_ACCOUNT_DAY_QUOTA_ERROR", "今日活动账户额度不足")
	ErrActivityAccountMonthCountSurplusNotEnough = xerr.New("ACTIVITY_ACCOUNT_MONTH_QUOTA_ERROR", "本月活动账户额度不足")
	ErrActivityStockError                        = xerr.New("ACTIVITY_STOCK_ERROR", "活动库存不足")
	ErrActivitySkuStockError                     = xerr.New("ACTIVITY_SKU_STOCK_ERROR", "活动商品库存不足")
	ErrInvalidParams                             = xerr.New("INVALID_PARAMS", "非法参数")
	ErrDBRouterError                             = xerr.New("DB_ROUTER_ERROR", "数据库路由错误")
	ErrDBIndexDuplicate                          = xerr.New("DB_INDEX_DUPLICATE", "数据库索引重复")
	ErrRecordNotFound                            = xerr.New("RECORD_NOT_FOUND", "记录不存在")
	ErrClearActivitySkuStockError                = xerr.New("CLEAR_ACTIVITY_SKU_STOCK_ERROR", "清除活动商品库存失败")
)

var (
	ErrActivitySkuStockKeyUnmarshal = xerr.New("ACTIVITY_SKU_STOCK_KEY_UNMARSHAL", "活动商品库存key反序列化失败")
)
