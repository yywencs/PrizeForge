package api

// RaffleResponse 抽奖结果 HTTP 响应
type RaffleResponse struct {
	AwardID    int64  `json:"award_id"`
	AwardTitle string `json:"award_title"`
	AwardIndex int    `json:"award_index"`
}

// CalendarSignRebateResponse 签到返利响应
type CalendarSignRebateResponse struct {
	Success bool `json:"success"`
}

// IsCalendarSignRebateResponse 是否签到响应
type IsCalendarSignRebateResponse struct {
	IsSigned bool `json:"is_signed"`
}

// QueryUserActivityAccountResponse 用户活动账户响应
type QueryUserActivityAccountResponse struct {
	ActivityID        int64 `json:"activity_id"`
	TotalCount        int64 `json:"total_count"`
	TotalCountSurplus int64 `json:"total_count_surplus"`
	DayCount          int64 `json:"day_count"`
	DayCountSurplus   int64 `json:"day_count_surplus"`
	MonthCount        int64 `json:"month_count"`
	MonthCountSurplus int64 `json:"month_count_surplus"`
}

// LoadUserActivityAccountResponse 加载账户响应
type LoadUserActivityAccountResponse struct {
	Success bool `json:"success"`
}
