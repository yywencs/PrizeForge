package strategy

import "prizeforge/internal/shared/xerr"

var (
	ErrRuleTreeInvalid              = xerr.New("RULE_TREE_INVALID", "规则树无效")
	ErrBlackListUser                = xerr.New("BLACK_LIST_USER", "黑名单用户")
	ErrStrategyRateRangeFailed      = xerr.New("STRATEGY_RATE_RANGE_FAILED", "获取概率范围失败")
	ErrStrategyRateRangeInvalid     = xerr.New("STRATEGY_RATE_RANGE_INVALID", "概率范围无效")
	ErrStrategyRandomValGenFailed   = xerr.New("STRATEGY_RANDOM_VAL_GEN_FAILED", "随机数生成失败")
	ErrStrategyAwardAssembleFailed  = xerr.New("STRATEGY_AWARD_ASSEMBLE_FAILED", "奖品装配失败")
	ErrRuleWeightValueInvalidFormat = xerr.New("RULE_WEIGHT_VALUE_INVALID_FORMAT", "权重规则值格式无效")
)

var (
	ErrBlackListConfigInvalid     = xerr.New("BLACK_LIST_CONFIG_INVALID", "黑名单配置无效")
	ErrBlackListConfigParseFailed = xerr.New("BLACK_LIST_CONFIG_PARSE_FAILED", "黑名单配置解析失败")
	ErrRuleWeightConfigInvalid    = xerr.New("RULE_WEIGHT_CONFIG_INVALID", "权重规则配置无效")
)
