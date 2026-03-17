package activity

import "github.com/go-kratos/kratos/v2/errors"

var (
	ErrActivityStateError                        = errors.BadRequest("ACTIVITY_STATE_ERROR", "活动状态非开启")
	ErrActivityTimeError                         = errors.BadRequest("ACTIVITY_TIME_ERROR", "活动时间未开始或已结束")
	ErrActivityQuotaError                        = errors.BadRequest("ACTIVITY_QUOTA_ERROR", "活动账户额度不足")
	ErrActivityAccountDayCountSurplusNotEnough   = errors.BadRequest("ACTIVITY_ACCOUNT_DAY_QUOTA_ERROR", "今日活动账户额度不足")
	ErrActivityAccountMonthCountSurplusNotEnough = errors.BadRequest("ACTIVITY_ACCOUNT_MONTH_QUOTA_ERROR", "本月活动账户额度不足")
	ErrActivityStockError                        = errors.BadRequest("ACTIVITY_STOCK_ERROR", "活动库存不足")
	ErrActivitySkuStockError                     = errors.BadRequest("ACTIVITY_SKU_STOCK_ERROR", "活动商品库存不足")
	ErrInvalidParams                             = errors.BadRequest("INVALID_PARAMS", "非法参数")
)

var (
	ErrDBRouterError              = errors.BadRequest("DB_ROUTER_ERROR", "数据库路由错误")
	ErrDBIndexDuplicate           = errors.BadRequest("DB_INDEX_DUPLICATE", "数据库索引重复")
	ErrRecordNotFound             = errors.NotFound("RECORD_NOT_FOUND", "记录不存在")
	ErrClearActivitySkuStockError = errors.BadRequest("CLEAR_ACTIVITY_SKU_STOCK_ERROR", "清除活动商品库存失败")
)
