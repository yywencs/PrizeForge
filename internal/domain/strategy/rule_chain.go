package strategy

import (
	"prizeforge/pkg/common"
	"prizeforge/pkg/logger"
	"context"
	"slices"
	"strconv"
	"strings"
)

// logicChain 责任链模式接口
type logicChain interface {
	logic(ctx context.Context, userID string, strategyID int64) (*logicResult, error)
	appendNext(next logicChain) logicChain
	next() logicChain
	ruleModel() RuleChainName
	clone() logicChain
}

type baseLogicChain struct {
	nextChain logicChain
}

func (c *baseLogicChain) appendNext(next logicChain) logicChain {
	c.nextChain = next
	return next
}

func (c *baseLogicChain) next() logicChain {
	return c.nextChain
}

// ==================== factory ====================

type logicResult struct {
	AwardID    int64
	LogicModel RuleChainName
}

type logicFactory struct {
	logicChainGroup map[RuleChainName]logicChain
	repo            Repo
}

func newLogicFactory(repo Repo, armory *armoryDispatch) *logicFactory {
	logicChainGroup := map[RuleChainName]logicChain{
		RuleBlacklist: newRuleBlackListLogic(repo),
		RuleWeight:    newRuleWeightLogic(repo, armory),
		RuleDefault:   newRuleDefaultLogic(armory),
	}
	return &logicFactory{
		logicChainGroup: logicChainGroup,
		repo:            repo,
	}
}

func (l *logicFactory) openLogicChain(ctx context.Context, strategyID int64) (logicChain, error) {
	strategy, err := l.repo.QueryStrategyEntityByStrategyId(ctx, strategyID)
	if err != nil {
		return nil, err
	}

	ruleModels := strategy.GetRuleModels()

	logger.InfoContext(ctx, "openLogicChain", "strategyID", strategyID, "ruleModels", ruleModels)

	if len(ruleModels) == 0 {
		return l.logicChainGroup[RuleDefault].clone(), nil
	}

	firstNode, ok := l.logicChainGroup[ruleModels[0]]
	if !ok {
		logger.WarnContext(ctx, "未知的规则节点", "ruleModel", ruleModels[0])
		// 如果第一个节点都不存在，可以直接降级走默认逻辑
		return l.logicChainGroup[RuleDefault].clone(), nil
	}

	chain := firstNode.clone()
	head := chain
	for i := 1; i < len(ruleModels); i++ {
		model := ruleModels[i]
		node, ok := l.logicChainGroup[model]
		if ok {
			newNode := node.clone()
			chain.appendNext(newNode)
			chain = newNode
		}
	}
	defaultNode := l.logicChainGroup[RuleDefault].clone()
	chain.appendNext(defaultNode)

	return head, nil
}

// ==================== default ====================
type defaultChain struct {
	baseLogicChain
	armoryDispatch *armoryDispatch
}

func newRuleDefaultLogic(armory *armoryDispatch) *defaultChain {
	return &defaultChain{
		armoryDispatch: armory,
	}
}

func (d *defaultChain) logic(ctx context.Context, userID string, strategyID int64) (*logicResult, error) {
	logger.Info("责任链-默认抽奖，随机抽取一个奖品", "userID", userID, "strategyID", strategyID)

	awardID, err := d.armoryDispatch.getRandomAwardIDWithWeight(ctx, strategyID, "")
	if err != nil {
		return nil, err
	}

	return &logicResult{
		AwardID:    awardID,
		LogicModel: RuleDefault,
	}, nil
}

func (d *defaultChain) ruleModel() RuleChainName {
	return RuleDefault
}

func (d *defaultChain) clone() logicChain {
	newD := *d
	return &newD
}

// ==================== black list ====================
type ruleBlackListLogic struct {
	baseLogicChain
	repo Repo
}

func newRuleBlackListLogic(repo Repo) *ruleBlackListLogic {
	return &ruleBlackListLogic{repo: repo}
}

func (r *ruleBlackListLogic) logic(ctx context.Context, userID string, strategyID int64) (*logicResult, error) {
	logger.Info("规则过滤-黑名单",
		"userID", userID,
		"strategyID", strategyID,
		"ruleModel", RuleBlacklist,
	)

	ruleValue, err := r.repo.QueryStrategyRuleValue(ctx, strategyID, RuleBlacklist)
	if err != nil {
		return nil, err
	}

	parts := strings.Split(ruleValue, common.COLON)
	if len(parts) < 2 {
		return nil, ErrBlackListConfigInvalid.WithMetadata(map[string]string{"rule_value": ruleValue})
	}

	awardID, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, ErrBlackListConfigParseFailed.WithCause(err)
	}

	for _, blackID := range strings.Split(parts[1], common.SPLIT) {
		if userID == blackID {
			logger.Info("用户在黑名单中，不允许抽奖",
				"userID", userID,
				"strategyID", strategyID,
				"awardID", awardID,
			)
			return &logicResult{AwardID: awardID, LogicModel: RuleBlacklist}, nil
		}
	}

	return r.next().logic(ctx, userID, strategyID)
}

func (r *ruleBlackListLogic) ruleModel() RuleChainName {
	return RuleBlacklist
}

func (r *ruleBlackListLogic) clone() logicChain {
	newR := *r
	return &newR
}

// ==================== rule weight ====================
type ruleWeightLogic struct {
	baseLogicChain
	repo           Repo
	armoryDispatch *armoryDispatch
}

func newRuleWeightLogic(repo Repo, armory *armoryDispatch) *ruleWeightLogic {
	return &ruleWeightLogic{repo: repo, armoryDispatch: armory}
}

func (r *ruleWeightLogic) logic(ctx context.Context, userID string, strategyID int64) (*logicResult, error) {
	logger.Info("规则过滤-权重",
		"userID", userID,
		"strategyID", strategyID,
		"ruleModel", RuleWeight,
	)

	ruleValue, err := r.repo.QueryStrategyValue(ctx, strategyID, RuleWeight)
	if err != nil {
		return nil, err
	}

	analyticalValueGroup, err := r.getAnalyticalValue(ruleValue)
	if err != nil {
		return nil, err
	}

	if len(analyticalValueGroup) == 0 {
		return r.next().logic(ctx, userID, strategyID)
	}

	keys := make([]int64, 0, len(analyticalValueGroup))
	for k := range analyticalValueGroup {
		keys = append(keys, int64(k))
	}

	slices.Sort(keys)

	// UserScore is hardcoded for now, as per requirements
	userScore, err := r.repo.QueryActivityAccountTotalUseCount(ctx, userID, strategyID)
	if err != nil {
		return nil, err
	}

	// TODO: 之后要写一个根据RuleValue解析后随机抽奖的方法
	for i := len(keys) - 1; i >= 0; i-- {
		if userScore >= keys[i] {
			awardID, err := r.armoryDispatch.getRandomAwardIDWithWeight(ctx, strategyID, strconv.FormatInt(keys[i], 10))
			if err != nil {
				return nil, err
			}
			return &logicResult{AwardID: awardID, LogicModel: RuleWeight}, nil
		}
	}

	return r.next().logic(ctx, userID, strategyID)
}

func (r *ruleWeightLogic) getAnalyticalValue(ruleValue string) (map[int]string, error) {
	ruleValues := strings.Split(ruleValue, common.SPACE)

	resMap := make(map[int]string, len(ruleValues))
	for _, singleRule := range ruleValues {
		if singleRule == "" {
			continue
		}

		parts := strings.Split(singleRule, common.COLON)
		if len(parts) < 2 {
			return nil, ErrRuleWeightConfigInvalid.WithMetadata(map[string]string{"rule": singleRule})
		}

		key, err := strconv.Atoi(parts[0])

		if err != nil {
			return nil, err
		}

		resMap[key] = parts[1]
	}
	return resMap, nil
}

func (r *ruleWeightLogic) ruleModel() RuleChainName {
	return RuleWeight
}

func (r *ruleWeightLogic) clone() logicChain {
	newR := *r
	return &newR
}
