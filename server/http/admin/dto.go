package admin

// StrategyArmoryResponse 策略装配响应
type StrategyArmoryResponse struct {
	Success bool `json:"success"`
}

// StrategyAwardList 策略奖品列表项
type StrategyAwardList struct {
	AwardID       int64  `json:"award_id"`
	AwardTitle    string `json:"award_title"`
	AwardSubtitle string `json:"award_subtitle"`
	Sort          int    `json:"sort"`
}

// StrategyAwardBase 基础奖品信息
type StrategyAwardBase struct {
	AwardID    int64  `json:"award_id"`
	AwardTitle string `json:"award_title"`
}

// RuleWeightResponse 规则权重响应
type RuleWeightResponse struct {
	RuleWeightCount                  int64               `json:"rule_weight_count"`
	UserActivityAccountTotalUseCount int64               `json:"user_activity_account_total_use_count"`
	StrategyAwards                   []StrategyAwardBase `json:"strategy_awards"`
}
