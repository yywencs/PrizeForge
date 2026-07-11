package xerr

const (
	StrategyRuleWeightMissing ErrCode = "ERR_BIZ_001"
	StrategyNotAssembled      ErrCode = "ERR_BIZ_002"
)

var strategyMsg = map[ErrCode]string{
	StrategyRuleWeightMissing: "业务异常，策略规则中 rule_weight 权重规则已适用但未配置",
	StrategyNotAssembled:      "抽奖策略配置未装配，请通过IStrategyArmory完成装配",
}
