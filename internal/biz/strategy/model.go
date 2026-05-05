package strategy

import (
	"big-market-kratos/pkg/common"
	"slices"
	"strconv"
	"strings"
)

type AwardStockConsumeMessage struct {
	StrategyID int64 `json:"strategy_id"`
	AwardID    int64 `json:"award_id"`
}

// ================== Enums ==================

type RuleTreeName string
type RuleChainName string

const (
	RuleBlacklist RuleChainName = "rule_blacklist"
	RuleWeight    RuleChainName = "rule_weight"
	RuleDefault   RuleChainName = "rule_default"
	RuleLock      RuleTreeName  = "rule_lock"
	RuleLuckAward RuleTreeName  = "rule_luck_award"
	RuleStock     RuleTreeName  = "rule_stock"
)

type RuleLimitType int

const (
	EQUAL RuleLimitType = iota
	GT
	LT
	GE
	LE
)

type RuleLogicCheckType int

const (
	RuleCheckAllow RuleLogicCheckType = iota
	RuleCheckTakeOver
)

// ================== Entities ==================
type Strategy struct {
	StrategyID int64
	// StrategyDesc 抽奖策略描述
	StrategyDesc string
	// 抽奖策略模型
	RuleModels string
}

func (s *Strategy) GetRuleModels() []RuleChainName {
	if s.RuleModels == "" {
		return nil
	}
	strs := strings.Split(s.RuleModels, common.SPLIT)
	res := make([]RuleChainName, len(strs))
	for i, str := range strs {
		res[i] = RuleChainName(str)
	}
	return res
}

// 获取rule_weight策略，如果没有就返回空
func (s *Strategy) GetRuleWeight() string {
	if s.RuleModels == "" {
		return ""
	}
	ruleModels := strings.Split(s.RuleModels, common.SPLIT)
	if slices.Contains(ruleModels, "rule_weight") {
		return "rule_weight"
	}
	return ""
}

// 获取rule_black_list策略，如果没有就返回空
func (s *Strategy) GetRuleBlackList() string {
	if s.RuleModels == "" {
		return ""
	}
	ruleModels := strings.Split(s.RuleModels, common.SPLIT)
	if slices.Contains(ruleModels, "rule_blacklist") {
		return "rule_blacklist"
	}
	return ""
}

// StrategyRule 抽奖策略规则实体
type StrategyRule struct {
	// StrategyID 抽奖策略ID
	StrategyID int64
	// AwardID 抽奖奖品ID
	AwardID int64
	// RuleType 抽象规则类型；1-策略规则、2-奖品规则
	RuleType int
	// RuleModel 抽奖规则类型
	RuleModel string
	// RuleValue 抽奖规则比值
	RuleValue string
	// RuleDesc 抽奖规则描述
	RuleDesc string
}

// GetRuleWeightValues 解析并返回“权重规则”配置。
func (sr *StrategyRule) GetRuleWeightValues() (map[string][]int64, error) {
	if "rule_weight" != sr.RuleModel {
		return nil, nil
	}
	resultMap := make(map[string][]int64)
	ruleValueGroups := strings.Split(sr.RuleValue, common.SPACE)

	for _, ruleValue := range ruleValueGroups {
		if ruleValue == "" {
			continue
		}

		parts := strings.Split(ruleValue, common.COLON)
		key, valueString := parts[0], parts[1]

		var values []int64
		for _, value := range strings.Split(valueString, common.SPLIT) {
			if value == "" {
				continue
			}
			val, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, ErrRuleWeightValueInvalidFormat.WithCause(err).WithMetadata(map[string]string{"rule_value": ruleValue})
			}
			values = append(values, val)
		}
		resultMap[key] = values
	}

	return resultMap, nil
}

// StrategyAward 策略奖品实体
type StrategyAward struct {
	StrategyID        int64
	AwardID           int64
	AwardTitle        string
	AwardSubtitle     string
	AwardCount        int
	AwardCountSurplus int
	AwardRate         float64
	Sort              int
	RuleModels        string
}

// RaffleAward 抽奖奖品实体（结果）
type RaffleAward struct {
	//  奖品ID
	AwardID int64
	//  奖品标题
	AwardTitle string
	//  奖品顺序号
	Sort int
}

// 规则树
type RuleTree struct {
	// TreeID 规则树ID
	TreeID string
	// TreeName 规则树名称
	TreeName string
	// TreeDesc 规则树描述
	TreeDesc string
	// TreeRootRuleNode 规则树根节点
	TreeRootRuleNode RuleTreeName
	// NodeMap 规则树节点映射
	NodeMap map[RuleTreeName]*RuleTreeNode
}

// / 规则树节点
type RuleTreeNode struct {
	// RuleNodeID 规则树节点ID
	TreeID string
	// RuleKey 规则树节点key
	RuleKey RuleTreeName
	// RuleDesc 规则树节点描述
	RuleDesc string
	// RuleValue 规则树节点比值
	RuleValue string
	// TreeNodeLine 规则连线
	TreeNodeLine []*RuleTreeNodeLine
}

// 规则树节点连线
type RuleTreeNodeLine struct {
	// RuleNodeLineID 规则树节点连线ID
	TreeID string
	// RuleNodeFrom 规则树节点来源
	RuleNodeFrom string
	// RuleNodeTo 规则树节点目标
	RuleNodeTo string
	// RuleLimitType 规则树节点限制类型
	RuleLimitType RuleLimitType
	// RuleLimitValue 规则树节点限制值
	RuleLimitValue RuleLogicCheckType
}

// ================== Value Objects ==================

type WeightBucket struct {
	// 原始规则值配置
	RuleValue string
	// 权重值
	Weight int
	// 奖品配置
	AwardIds []int
	// 奖品列表
	AwardList []Award
}

type Award struct {
	AwardId    int
	AwardTitle string
}

// RaffleFactor 抽奖因子
type RaffleFactor struct {
	UserID     string
	StrategyID int64
}
