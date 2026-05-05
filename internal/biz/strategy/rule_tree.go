package strategy

import (
	"big-market-kratos/pkg/common"
	"big-market-kratos/pkg/logger"
	"context"
	"strconv"
	"strings"
)

type treeStrategyAward struct {
	AwardID        int64
	AwardRuleValue string
}

type ruleTreeFactory struct {
	repository Repo
}

func newRuleTreeFactory(repository Repo) *ruleTreeFactory {
	return &ruleTreeFactory{
		repository: repository,
	}
}

func (f *ruleTreeFactory) newDecisionTreeEngine(ruleTree *RuleTree) (*ruleTreeEngine, error) {
	// 1. 校验规则树是否为空
	if ruleTree == nil {
		logger.Error("规则树引擎执行失败：树对象为空")
		return nil, ErrRuleTreeInvalid
	}

	// 2. 校验规则树是否包含根节点
	if _, ok := ruleTree.NodeMap[ruleTree.TreeRootRuleNode]; !ok {
		logger.Error("规则树引擎执行失败：未找到根节点",
			"expected_root_node", ruleTree.TreeRootRuleNode,
		)
		return nil, ErrRuleTreeInvalid
	}

	group := map[RuleTreeName]ruleNode{
		RuleLock:  newRuleLockNode(),
		RuleLuck:  newRuleLuckNode(),
		RuleStock: newRuleStockNode(f.repository),
	}

	// 3. 返回组装好的引擎
	return &ruleTreeEngine{
		TreeNodeGroup: group,
		RuleTree:      ruleTree,
	}, nil
}

// ===========
type treeAction struct {
	RuleLogicCheckType RuleLogicCheckType
	Award              treeStrategyAward
}

type tree struct {
	NodeMap map[RuleTreeName]*ruleNode
}

type ruleNode interface {
	logic(ctx context.Context, strategyID int64, awardID int64, ruleValue string) (*treeAction, error)
}

type ruleTreeEngine struct {
	TreeNodeGroup map[RuleTreeName]ruleNode
	RuleTree      *RuleTree
}

func (e *ruleTreeEngine) process(strategyID int64, awardID int64) (*treeStrategyAward, error) {
	nextNode := e.RuleTree.TreeRootRuleNode
	strategyAward := &treeStrategyAward{}
	for nextNode != "" {
		treeNode := e.RuleTree.NodeMap[nextNode]
		logicNode := e.TreeNodeGroup[nextNode]

		treeAction, err := logicNode.logic(context.Background(), strategyID, awardID, treeNode.RuleValue)
		if err != nil {
			return nil, err
		}

		nextNode = e.next(treeAction.RuleLogicCheckType, treeNode.TreeNodeLine)

		strategyAward = &treeAction.Award
	}
	return strategyAward, nil
}

func (e *ruleTreeEngine) next(matterValue RuleLogicCheckType, treeNodeLines []*RuleTreeNodeLine) RuleTreeName {
	if len(treeNodeLines) == 0 {
		return ""
	}

	for _, line := range treeNodeLines {
		if e.decisionLogic(matterValue, line) {
			return RuleTreeName(line.RuleNodeTo)
		}
	}
	panic("决策树引擎，nextNode 计算失败，未找到可执行节点！")
}

func (e *ruleTreeEngine) decisionLogic(matterValue RuleLogicCheckType, treeNodeLine *RuleTreeNodeLine) bool {
	switch treeNodeLine.RuleLimitType {
	case EQUAL:
		return matterValue == treeNodeLine.RuleLimitValue
	case GT:

	case LT:

	case GE:

	case LE:
	}
	return false
}

// ================== 次数锁 ==================

type ruleLockNode struct {
}

func newRuleLockNode() ruleNode {
	return &ruleLockNode{}
}
func (r *ruleLockNode) logic(ctx context.Context, strategyID int64, awardID int64, ruleValue string) (*treeAction, error) {
	logger.Info("规则过滤-次数锁",
		"strategyID", strategyID,
		"awardID", awardID,
		"ruleValue", ruleValue,
	)

	raffleCount, err := strconv.Atoi(ruleValue)
	if err != nil {
		panic("规则过滤-次数锁异常 ruleValue: " + ruleValue + " 配置不正确" + err.Error())
	}

	// TODO 后续需要修改为从缓存中获取用户抽奖次数
	userRafferCount := 10
	if userRafferCount >= raffleCount {
		return &treeAction{
			RuleLogicCheckType: RuleCheckAllow,
			Award: treeStrategyAward{
				AwardID:        awardID,
				AwardRuleValue: ruleValue,
			},
		}, nil
	}

	return &treeAction{
		RuleLogicCheckType: RuleCheckTakeOver,
		Award: treeStrategyAward{
			AwardID:        awardID,
			AwardRuleValue: ruleValue,
		},
	}, nil
}

// =====
type ruleLuckNode struct {
}

func newRuleLuckNode() ruleNode {
	return &ruleLuckNode{}
}
func (r *ruleLuckNode) logic(ctx context.Context, strategyID int64, awardID int64, ruleValue string) (*treeAction, error) {
	logger.Info("规则过滤-兜底奖品",
		"strategyID", strategyID,
		"awardID", awardID,
		"ruleValue", ruleValue,
	)

	split := strings.Split(ruleValue, common.COLON)

	if len(split) != 2 {
		panic("兜底奖品规则配置错误，ruleValue: " + ruleValue)
	}

	luckAwardID, awardRuleValue := split[0], split[1]
	luckAwardIDInt64, err := strconv.ParseInt(luckAwardID, 10, 64)
	if err != nil {
		panic("兜底奖品规则配置错误，luckAwardID: " + luckAwardID)
	}

	logger.Info("规则过滤-兜底奖品结果",
		"strategyID", strategyID,
		"awardID", luckAwardIDInt64,
		"awardRuleValue", awardRuleValue,
	)
	return &treeAction{
		RuleLogicCheckType: RuleCheckTakeOver,
		Award: treeStrategyAward{
			AwardID:        luckAwardIDInt64,
			AwardRuleValue: awardRuleValue,
		},
	}, nil
}

type ruleStockNode struct {
	repository Repo
}

func newRuleStockNode(repository Repo) ruleNode {
	return &ruleStockNode{repository: repository}
}

func (r *ruleStockNode) logic(ctx context.Context, strategyID int64, awardID int64, ruleValue string) (*treeAction, error) {
	logger.Info("规则过滤-库存扣减",
		"strategyID", strategyID,
		"awardID", awardID,
		"ruleValue", ruleValue,
	)

	status, err := r.repository.SubtractionAwardStock(ctx, strategyID, awardID)
	if err != nil {
		return nil, err
	}

	if status {
		logger.Info("规则过滤-库存扣减-成功",
			"strategyID", strategyID,
			"awardID", awardID,
		)
		err = r.repository.AwardStockConsumeSendQueue(ctx, strategyID, awardID)
		if err != nil {
			return nil, err
		}

		return &treeAction{
			RuleLogicCheckType: RuleCheckAllow,
			Award: treeStrategyAward{
				AwardID:        awardID,
				AwardRuleValue: ruleValue,
			},
		}, nil
	}

	logger.Info("规则过滤-库存扣减-告警，库存不足",
		"strategyID", strategyID,
		"awardID", awardID,
		"ruleValue", ruleValue,
	)
	return &treeAction{
		RuleLogicCheckType: RuleCheckTakeOver,
		Award: treeStrategyAward{
			AwardID:        awardID,
			AwardRuleValue: ruleValue,
		},
	}, nil
}
