package xerr

// MapErrMsg 获取错误描述
func MapErrMsg(code ErrCode) string {
	if msg, ok := commonMsg[code]; ok {
		return msg
	}
	if msg, ok := activityMsg[code]; ok {
		return msg
	}
	if msg, ok := strategyMsg[code]; ok {
		return msg
	}
	return "未知错误"
}
