package xerr

const (
	ActivityStateError    ErrCode = "ERR_ACTIVITY_001"
	ActivityTimeError     ErrCode = "ERR_ACTIVITY_002"
	ActivityStockError    ErrCode = "ERR_ACTIVITY_003"
	ActivityDateError     ErrCode = "ERR_ACTIVITY_004"
	ActivitySkuStockError ErrCode = "ERR_ACTIVITY_005"
)

var activityMsg = map[ErrCode]string{
	ActivityStateError:    "活动状态错误",
	ActivityTimeError:     "活动时间错误",
	ActivityStockError:    "活动库存不足",
	ActivityDateError:     "活动日期错误",
	ActivitySkuStockError: "活动商品库存不足",
}
