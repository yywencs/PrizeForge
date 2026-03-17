package strategy

import "github.com/go-kratos/kratos/v2/errors"

var (
	// ErrRuleTreeInvalid 规则树配置异常 (对外映射为 HTTP 400 Bad Request)
	ErrRuleTreeInvalid = errors.BadRequest("RULE_TREE_INVALID", "规则树配置异常")

	// ErrBlackListUser 触发黑名单拦截 (对外映射为 HTTP 403 Forbidden)
	ErrBlackListUser = errors.Forbidden("USER_IN_BLACKLIST", "抱歉，您暂无抽奖权限")
)
