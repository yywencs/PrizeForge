package strategy

import "github.com/go-kratos/kratos/v2/errors"

var (
	// ErrRuleTreeInvalid 规则树配置异常 (对外映射为 HTTP 400 Bad Request)
	ErrRuleTreeInvalid = errors.BadRequest("RULE_TREE_INVALID", "规则树配置异常")

	// ErrBlackListUser 触发黑名单拦截 (对外映射为 HTTP 403 Forbidden)
	ErrBlackListUser = errors.Forbidden("USER_IN_BLACKLIST", "抱歉，您暂无抽奖权限")

	// ErrStrategyRateRangeFailed 获取抽奖概率范围失败
	ErrStrategyRateRangeFailed = errors.InternalServer("STRATEGY_RATE_RANGE_FAILED", "获取抽奖概率范围失败")

	// ErrStrategyRateRangeInvalid 抽奖概率范围无效
	ErrStrategyRateRangeInvalid = errors.InternalServer("STRATEGY_RATE_RANGE_INVALID", "抽奖概率范围无效")

	// ErrStrategyRandomValGenFailed 生成随机抽奖值失败
	ErrStrategyRandomValGenFailed = errors.InternalServer("STRATEGY_RANDOM_VAL_GEN_FAILED", "生成随机抽奖值失败")

	// ErrStrategyAwardAssembleFailed 获取奖品装配信息失败
	ErrStrategyAwardAssembleFailed = errors.InternalServer("STRATEGY_AWARD_ASSEMBLE_FAILED", "获取奖品装配信息失败")

	// ErrBlackListConfigInvalid 黑名单规则配置无效
	ErrBlackListConfigInvalid = errors.InternalServer("BLACK_LIST_CONFIG_INVALID", "黑名单规则配置无效")

	// ErrBlackListConfigParseFailed 解析黑名单配置失败
	ErrBlackListConfigParseFailed = errors.InternalServer("BLACK_LIST_CONFIG_PARSE_FAILED", "解析黑名单配置失败")

	// ErrRuleWeightConfigInvalid 权重规则配置无效
	ErrRuleWeightConfigInvalid = errors.InternalServer("RULE_WEIGHT_CONFIG_INVALID", "权重规则配置无效")

	// ErrRuleWeightValueInvalidFormat 权重规则值格式无效
	ErrRuleWeightValueInvalidFormat = errors.InternalServer("RULE_WEIGHT_VALUE_INVALID_FORMAT", "权重规则值格式无效")
)
