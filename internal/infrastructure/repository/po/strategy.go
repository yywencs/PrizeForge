package po

import (
	"prizeforge/internal/domain/strategy"
	"time"
)

// 抽奖策略表
type Strategy struct {
	// ID 自增ID
	ID uint64 `gorm:"column:id;primaryKey;autoIncrement;comment:自增ID"`

	// StrategyID 抽奖策略ID
	StrategyID int64 `gorm:"column:strategy_id;not null;comment:抽奖策略ID"`

	// StrategyDesc 抽奖策略描述
	StrategyDesc string `gorm:"column:strategy_desc;type:varchar(128);not null;comment:抽奖策略描述"`

	// 抽奖策略模型
	RuleModels string `gorm:"column:rule_models;type:varchar(64);not null;comment:规则模型"`

	// CreateTime 创建时间
	CreateTime time.Time `gorm:"column:create_time;type:datetime;not null;autoCreateTime;comment:创建时间"`

	// UpdateTime 更新时间
	UpdateTime time.Time `gorm:"column:update_time;type:datetime;not null;autoUpdateTime;comment:更新时间"`
}

// 抽奖策略奖品明细配置表
type StrategyAward struct {
	// ID 自增ID
	ID uint64 `gorm:"column:id;primaryKey;autoIncrement;comment:自增ID"`

	// StrategyID 抽奖策略ID
	StrategyID int64 `gorm:"column:strategy_id;not null;index;comment:抽奖策略ID"`

	// AwardID 抽奖奖品ID - 内部流转使用
	AwardID int64 `gorm:"column:award_id;not null;comment:抽奖奖品ID - 内部流转使用"`

	// AwardTitle 抽奖奖品标题
	AwardTitle string `gorm:"column:award_title;type:varchar(128);not null;comment:抽奖奖品标题"`

	// AwardSubtitle 抽奖奖品副标题
	AwardSubtitle string `gorm:"column:award_subtitle;type:varchar(128);comment:抽奖奖品副标题"`

	// AwardCount 奖品库存总量
	AwardCount int `gorm:"column:award_count;not null;default:0;comment:奖品库存总量"`

	// AwardCountSurplus 奖品库存剩余
	// 对应 SQL: `award_count_surplus` int NOT NULL DEFAULT '0'
	AwardCountSurplus int `gorm:"column:award_count_surplus;not null;default:0;comment:奖品库存剩余"`

	// AwardRate 奖品中奖概率
	// 对应 SQL: `award_rate` decimal(6,4) NOT NULL
	// 注意：虽然数据库是 decimal，但 Go 语言中处理概率计算通常使用 float64
	AwardRate float64 `gorm:"column:award_rate;type:decimal(6,4);not null;comment:奖品中奖概率"`

	// RuleModels 规则模型
	RuleModels string `gorm:"column:rule_models;type:varchar(256);comment:规则模型，rule配置的模型同步到此表，便于使用"`

	// Sort 排序
	Sort int `gorm:"column:sort;not null;default:0;comment:排序"`

	// CreateTime 创建时间
	CreateTime time.Time `gorm:"column:create_time;type:datetime;not null;autoCreateTime;comment:创建时间"`

	// UpdateTime 修改时间
	UpdateTime time.Time `gorm:"column:update_time;type:datetime;not null;autoUpdateTime;comment:修改时间"`
}

// StrategyAwardStockReservation 记录订单对应的数据库库存扣减，供异步任务幂等消费。
type StrategyAwardStockReservation struct {
	ID         uint64    `gorm:"column:id;primaryKey;autoIncrement"`
	UserID     string    `gorm:"column:user_id;type:varchar(32);not null;uniqueIndex:uq_user_order"`
	OrderID    string    `gorm:"column:order_id;type:varchar(12);not null;uniqueIndex:uq_user_order"`
	StrategyID int64     `gorm:"column:strategy_id;not null"`
	AwardID    int64     `gorm:"column:award_id;not null"`
	CreateTime time.Time `gorm:"column:create_time;type:datetime;not null;autoCreateTime"`
}

func (StrategyAwardStockReservation) TableName() string {
	return "strategy_award_stock_reservation"
}

// 抽奖策略规则表
type StrategyRule struct {
	// ID 自增ID
	ID uint64 `gorm:"column:id;primaryKey;autoIncrement;comment:自增ID"`

	// StrategyID 抽奖策略ID
	StrategyID int64 `gorm:"column:strategy_id;not null;comment:抽奖策略ID"`

	// AwardID 抽奖奖品ID
	AwardID int64 `gorm:"column:award_id;default:null;comment:抽奖奖品ID【规则类型为策略，则不需要奖品ID】"`

	// RuleType 抽象规则类型；1-策略规则、2-奖品规则
	RuleType int `gorm:"column:rule_type;type:tinyint(1);not null;default:0;comment:抽象规则类型；1-策略规则、2-奖品规则"`

	// RuleModel 抽奖规则类型
	RuleModel string `gorm:"column:rule_model;type:varchar(16);not null;comment:抽奖规则类型【rule_random - 随机值计算、rule_lock - 抽奖几次后解锁、rule_luck_award - 幸运奖(兜底奖品)】"`

	// RuleValue 抽奖规则比值
	RuleValue string `gorm:"column:rule_value;type:varchar(64);not null;comment:抽奖规则比值"`

	// RuleDesc 抽奖规则描述
	RuleDesc string `gorm:"column:rule_desc;type:varchar(128);not null;comment:抽奖规则描述"`

	// CreateTime 创建时间
	CreateTime time.Time `gorm:"column:create_time;type:datetime;not null;autoCreateTime;comment:创建时间"`

	// UpdateTime 更新时间
	UpdateTime time.Time `gorm:"column:update_time;type:datetime;not null;autoUpdateTime;comment:更新时间"`
}

type RuleTree struct {
	ID              uint64    `gorm:"column:id;primaryKey;autoIncrement;comment:自增ID"`
	TreeID          string    `gorm:"column:tree_id;type:varchar(32);not null;comment:规则树ID"`
	TreeName        string    `gorm:"column:tree_name;type:varchar(64);not null;comment:规则树名称"`
	TreeDesc        string    `gorm:"column:tree_desc;type:varchar(128);comment:规则树描述"`
	TreeNodeRuleKey string    `gorm:"column:tree_node_rule_key;type:varchar(32);not null;comment:规则树根入口规则"`
	CreateTime      time.Time `gorm:"column:create_time;type:datetime;not null;autoCreateTime;comment:创建时间"`
	UpdateTime      time.Time `gorm:"column:update_time;type:datetime;not null;autoUpdateTime;comment:更新时间"`
}

type RuleTreeNode struct {
	ID         uint64    `gorm:"column:id;primaryKey;autoIncrement;comment:自增ID"`
	TreeID     string    `gorm:"column:tree_id;type:varchar(32);not null;comment:规则树ID"`
	RuleKey    string    `gorm:"column:rule_key;type:varchar(32);not null;comment:规则Key"`
	RuleDesc   string    `gorm:"column:rule_desc;type:varchar(64);not null;comment:规则描述"`
	RuleValue  string    `gorm:"column:rule_value;type:varchar(128);comment:规则比值"`
	CreateTime time.Time `gorm:"column:create_time;type:datetime;not null;autoCreateTime;comment:创建时间"`
	UpdateTime time.Time `gorm:"column:update_time;type:datetime;not null;autoUpdateTime;comment:更新时间"`
}

type RuleTreeNodeLine struct {
	ID             uint64    `gorm:"column:id;primaryKey;autoIncrement;comment:自增ID"`
	TreeID         string    `gorm:"column:tree_id;type:varchar(32);not null;comment:规则树ID"`
	RuleNodeFrom   string    `gorm:"column:rule_node_from;type:varchar(32);not null;comment:规则Key节点 From"`
	RuleNodeTo     string    `gorm:"column:rule_node_to;type:varchar(32);not null;comment:规则Key节点 To"`
	RuleLimitType  string    `gorm:"column:rule_limit_type;type:varchar(8);not null;comment:限定类型"`
	RuleLimitValue string    `gorm:"column:rule_limit_value;type:varchar(32);not null;comment:限定值"`
	CreateTime     time.Time `gorm:"column:create_time;type:datetime;not null;autoCreateTime;comment:创建时间"`
	UpdateTime     time.Time `gorm:"column:update_time;type:datetime;not null;autoUpdateTime;comment:更新时间"`
}

func (Strategy) TableName() string {
	return "strategy"
}

func (StrategyAward) TableName() string {
	return "strategy_award"
}

func (StrategyRule) TableName() string {
	return "strategy_rule"
}

func (RuleTree) TableName() string {
	return "rule_tree"
}

func (RuleTreeNode) TableName() string {
	return "rule_tree_node"
}

func (RuleTreeNodeLine) TableName() string {
	return "rule_tree_node_line"
}

func (p *Strategy) ToEntity() *strategy.Strategy {
	return &strategy.Strategy{
		StrategyID:   p.StrategyID,
		StrategyDesc: p.StrategyDesc,
		RuleModels:   p.RuleModels,
	}
}

func (p *StrategyAward) ToEntity() *strategy.StrategyAward {
	return &strategy.StrategyAward{
		StrategyID:        p.StrategyID,
		AwardID:           p.AwardID,
		AwardTitle:        p.AwardTitle,
		AwardSubtitle:     p.AwardSubtitle,
		AwardCount:        p.AwardCount,
		AwardCountSurplus: p.AwardCountSurplus,
		AwardRate:         p.AwardRate,
		Sort:              p.Sort,
		RuleModels:        p.RuleModels,
	}
}

func (p *StrategyRule) ToEntity() *strategy.StrategyRule {
	return &strategy.StrategyRule{
		StrategyID: p.StrategyID,
		AwardID:    p.AwardID,
		RuleType:   p.RuleType,
		RuleModel:  p.RuleModel,
		RuleValue:  p.RuleValue,
		RuleDesc:   p.RuleDesc,
	}
}

func (p *RuleTree) ToEntity() *strategy.RuleTree {
	entity := &strategy.RuleTree{
		TreeID:           p.TreeID,
		TreeName:         p.TreeName,
		TreeDesc:         p.TreeDesc,
		TreeRootRuleNode: strategy.RuleTreeName(p.TreeNodeRuleKey),
	}
	return entity
}

func (p *RuleTreeNode) ToEntity() *strategy.RuleTreeNode {
	entity := &strategy.RuleTreeNode{
		TreeID:    p.TreeID,
		RuleDesc:  p.RuleDesc,
		RuleValue: p.RuleValue,
		RuleKey:   strategy.RuleTreeName(p.RuleKey),
	}
	return entity
}

func (p *RuleTreeNodeLine) ToEntity() *strategy.RuleTreeNodeLine {
	return &strategy.RuleTreeNodeLine{
		TreeID:         p.TreeID,
		RuleNodeFrom:   p.RuleNodeFrom,
		RuleNodeTo:     p.RuleNodeTo,
		RuleLimitType:  parseRuleLimitType(p.RuleLimitType),
		RuleLimitValue: parseRuleLogicCheckType(p.RuleLimitValue),
	}
}

func parseRuleLimitType(value string) strategy.RuleLimitType {
	switch value {
	case "EQUAL", "equal", "=":
		return strategy.EQUAL
	case "GT", "gt", ">":
		return strategy.GT
	case "LT", "lt", "<":
		return strategy.LT
	case "GE", "ge", ">=":
		return strategy.GE
	case "LE", "le", "<=":
		return strategy.LE
	}
	return strategy.EQUAL
}

func parseRuleLogicCheckType(value string) strategy.RuleLogicCheckType {
	switch value {
	case "allow", "ALLOW":
		return strategy.RuleCheckAllow
	case "take_over", "TAKE_OVER":
		return strategy.RuleCheckTakeOver
	}
	return strategy.RuleCheckAllow
}
