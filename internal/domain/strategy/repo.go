package strategy

import (
	"context"
)

// strategyRepository 负责策略配置的元数据查询 (MySQL)
type Repo interface {
	// 查询奖品配置列表
	QueryStrategyAwardList(ctx context.Context, strategyID int64) ([]*StrategyAward, error)

	// 查询策略基础信息
	QueryStrategyEntityByStrategyId(ctx context.Context, strategyId int64) (*Strategy, error)

	// 查询具体规则
	QueryStrategyRule(ctx context.Context, strategyID int64, ruleModel string) (*StrategyRule, error)

	QueryStrategyRuleValue(ctx context.Context, strategyID int64, ruleModel RuleChainName) (string, error)

	QueryStrategyValue(ctx context.Context, strategyID int64, ruleModel RuleChainName) (string, error)

	// 查询具体规则模型
	QueryStrategyRuleModel(ctx context.Context, strategyID int64, awardID int64) (RuleTreeName, error)

	QueryRuleTree(ctx context.Context, ruleModel RuleTreeName) (*RuleTree, error)

	// 查询具体奖品
	QueryStrategyAward(ctx context.Context, strategyID int64, awardID int64) (*StrategyAward, error)

	// UpdateStrategyAwardStock 根据队列消费结果，持久化扣减库存到数据库
	UpdateStrategyAwardStock(ctx context.Context, userID string, orderID string, strategyID int64, awardID int64) error
	// UpdateStrategyAwardStockBatch 将同一奖品的一批订单幂等落库，并聚合为一次库存扣减。
	UpdateStrategyAwardStockBatch(ctx context.Context, messages []AwardStockConsumeMessage) error

	// 根据活动ID查询策略ID
	QueryStrategyIdByActivityId(ctx context.Context, activityID int64) (int64, error)

	// QueryAwardRuleWeight 查询策略规则权重配置
	QueryAwardRuleWeight(ctx context.Context, strategyID int64) ([]*WeightBucket, error)

	// 查询用户活动账户总使用次数
	QueryActivityAccountTotalUseCount(ctx context.Context, userID string, strategyID int64) (int64, error)

	// 初始化/装配：将计算好的抽奖表存入缓存
	StoreStrategyAwardPool(ctx context.Context, strategyID string, rateRange int, idxToAwardIDMap map[int]int64) error

	// 获取概率范围 (例如 10000)
	GetRateRange(ctx context.Context, strategyID string) (int, error)

	// 执行抽奖：根据随机数获取奖品ID
	GetStrategyAwardAssemble(ctx context.Context, strategyID string, randomVal int) (int64, error)

	// 库存扣减
	SubtractionAwardStock(ctx context.Context, strategyID int64, awardID int64) (bool, error)
	// ReserveAwardStock 使用 orderID 幂等预占库存；返回实际为该订单预占的奖品ID。
	ReserveAwardStock(ctx context.Context, userID string, orderID string, strategyID int64, awardID int64) (reservedAwardID int64, ok bool, err error)

	// 库存扣减后，发送消息到队列
	AwardStockConsumeSendQueue(ctx context.Context, userID string, orderID string, strategyID int64, awardID int64) error
}
