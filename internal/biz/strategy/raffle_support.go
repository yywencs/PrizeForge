package strategy

import (
	"big-market-kratos/pkg/common"
	"big-market-kratos/pkg/xrand"
	"context"
	"fmt"
	"math"
	mrand "math/rand"
	"strconv"
	"strings"
)

type raffleStrategy struct {
	repo         Repo
	chainFactory *logicFactory
	treeFactory  *ruleTreeFactory
}

func newRaffleStrategy(repo Repo, chainFactory *logicFactory, treeFactory *ruleTreeFactory) *raffleStrategy {
	return &raffleStrategy{
		chainFactory: chainFactory,
		treeFactory:  treeFactory,
		repo:         repo,
	}
}

func (r *raffleStrategy) raffleLogicChain(ctx context.Context, factor *RaffleFactor) (*logicResult, error) {
	logicChain, err := r.chainFactory.openLogicChain(ctx, factor.StrategyID)
	if err != nil {
		return nil, err
	}

	return logicChain.logic(ctx, factor.UserID, factor.StrategyID)
}

func (r *raffleStrategy) raffleRuleTree(ctx context.Context, userID string, strategyID int64, awardID int64) (*treeStrategyAward, error) {
	ruleModel, err := r.repo.QueryStrategyRuleModel(ctx, strategyID, awardID)
	if err != nil {
		return nil, err
	}

	if ruleModel == "" {
		return &treeStrategyAward{
			AwardID: awardID,
		}, nil
	}

	ruleTree, err := r.repo.QueryRuleTree(ctx, ruleModel)
	if err != nil || ruleTree == nil {
		panic("存在抽奖策略配置的规则模型 Key，未在库表 rule_tree、rule_tree_node、rule_tree_line 配置对应的规则树信息 " + ruleModel)
	}

	engine, err := r.treeFactory.newDecisionTreeEngine(ruleTree)
	if err != nil {
		return nil, err
	}
	return engine.process(strategyID, awardID)
}

func (r *raffleStrategy) queryStrategyAwardList(ctx context.Context, strategyID int64) ([]*StrategyAward, error) {
	return r.repo.QueryStrategyAwardList(ctx, strategyID)
}

func (r *raffleStrategy) queryAwardRuleWeightByActivityId(ctx context.Context, activityID int64) ([]*WeightBucket, error) {
	strategyID, err := r.repo.QueryStrategyIdByActivityId(ctx, activityID)
	if err != nil {
		return nil, err
	}
	return r.repo.QueryAwardRuleWeight(ctx, strategyID)
}

type armoryDispatch struct {
	repo Repo
}

func newArmoryDispatch(repo Repo) *armoryDispatch {
	return &armoryDispatch{
		repo: repo,
	}
}

func (s *armoryDispatch) assembleLotteryStrategyByActivityId(ctx context.Context, activityID int64) (bool, error) {
	strategyID, err := s.repo.QueryStrategyIdByActivityId(ctx, activityID)
	if err != nil {
		return false, err
	}
	return s.assembleLotteryStrategy(ctx, strategyID)
}

// AssembleLotteryStrategy 负责将指定策略下的奖品列表按概率规则组装成奖池：
// 1. 若策略无权重规则，直接为默认策略生成奖池；
// 2. 若存在权重规则，则按权重维度拆分奖品，为每个维度生成独立子策略奖池；
// 最终把奖池数据缓存，供后续抽奖阶段使用。
func (s *armoryDispatch) assembleLotteryStrategy(ctx context.Context, strategyID int64) (bool, error) {
	// 1. 查询策略实体与奖品列表；
	strategyEntity, err := s.repo.QueryStrategyEntityByStrategyId(ctx, strategyID)

	stringStrategyID := strconv.FormatInt(strategyID, 10)
	if err != nil {
		return false, err
	}

	strategyAwards, err := s.repo.QueryStrategyAwardList(ctx, strategyID)
	if err != nil {
		return false, err
	}

	rule_weight := strategyEntity.GetRuleWeight()
	// 2. 若无规则权重，直接组装默认策略；
	if rule_weight == "" {
		err = s.assembleLotteryStrategyWithAwards(ctx, stringStrategyID, strategyAwards)
		return true, err
	}

	// 3. 若存在规则权重，按权重维度拆分奖品并分别组装子策略；
	strategyRuleEntity, err := s.repo.QueryStrategyRule(ctx, strategyID, rule_weight)
	if err != nil {
		return false, err
	}

	// 4. 解析权重规则，获取各权重维度对应的奖品ID列表
	ruleWeightMap, err := strategyRuleEntity.GetRuleWeightValues()
	if err != nil {
		return false, err
	}

	// 5. 遍历每个权重维度，为每个维度单独组装奖池
	for key, v := range ruleWeightMap {
		// 5.1 将当前维度奖品ID列表转为Set，方便快速查找
		ruleValuesSet := make(map[int64]bool, len(v))
		for _, val := range v {
			ruleValuesSet[val] = true
		}

		// 5.2 过滤出属于当前权重维度的奖品
		var filedAwards []*StrategyAward
		for _, award := range strategyAwards {
			if ruleValuesSet[award.AwardID] {
				filedAwards = append(filedAwards, award)
			}
		}

		// 5.3 生成子策略ID：原策略ID_权重维度key
		newStrategyID := stringStrategyID + common.UNDERLINE + key
		// 5.4 为当前维度组装独立奖池并缓存
		err = s.assembleLotteryStrategyWithAwards(ctx, newStrategyID, filedAwards)
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

// assembleLotteryStrategy 根据奖品概率构造奖池：将每个奖品按概率填充为对应数量的索引，打乱后生成索引→奖品ID的映射，并缓存到存储中，供后续抽奖使用。
func (s *armoryDispatch) assembleLotteryStrategyWithAwards(ctx context.Context, strategyId string, strategyAwards []*StrategyAward) error {

	// 1. 获取奖品概率总和，用于归一化计算
	totalRate := 0.0
	for _, award := range strategyAwards {
		totalRate += award.AwardRate
	}

	// 2. 找出最长的小数位
	maxDecimalLen := 0

	for _, award := range strategyAwards {
		// 计算小数位长度
		rateStr := strconv.FormatFloat(award.AwardRate, 'f', -1, 64)

		parts := strings.Split(rateStr, ".")
		if len(parts) == 2 {
			decimalPart := parts[1]
			if len(decimalPart) > maxDecimalLen {
				maxDecimalLen = len(decimalPart)
			}
		}
	}

	// 3. 根据小数位算出倍数
	rateRange := math.Pow(10, float64(maxDecimalLen))

	// 4. 初始化奖池切片，容量为预估容量，减少后续扩容
	expectedCap := int(rateRange)
	strategyAwardPool := make([]int64, 0, expectedCap)

	// 5. 按概率比例将奖品ID填充到奖池切片中（中奖率归一化）
	for _, strategyAward := range strategyAwards {
		awardID := strategyAward.AwardID
		// 归一化计算：当前奖品概率 / 总概率 * rateRange
		count := int(math.Round((strategyAward.AwardRate / totalRate) * rateRange))
		for i := 0; i < count; i++ {
			strategyAwardPool = append(strategyAwardPool, awardID)
		}
	}

	// 6. 打乱奖池顺序，保证抽奖随机性
	mrand.Shuffle(len(strategyAwardPool), func(i, j int) {
		strategyAwardPool[i], strategyAwardPool[j] =
			strategyAwardPool[j], strategyAwardPool[i]
	})

	// 7. 构建索引到奖品ID的映射，用于后续快速查找
	idxToAwardIDMap := make(map[int]int64, len(strategyAwardPool))

	for idx, awardID := range strategyAwardPool {
		idxToAwardIDMap[idx] = awardID
	}

	// 8. 将奖池长度与索引映射缓存到存储，供抽奖阶段使用
	err := s.repo.StoreStrategyAwardPool(ctx, strategyId, len(strategyAwardPool), idxToAwardIDMap)

	return err
}

// getRandomAwardID 根据策略ID从已缓存的奖池中随机抽取一个奖品ID
func (s *armoryDispatch) getRandomAwardID(ctx context.Context, strategyID string) (int64, error) {
	rateRange, err := s.repo.GetRateRange(ctx, strategyID)
	if err != nil {
		return 0, fmt.Errorf("failed to get rate range: %w", err)
	}

	if rateRange <= 0 {
		return 0, fmt.Errorf("invalid rate range: %d", rateRange)
	}

	randomVal, err := xrand.GetSecureRandomInt(rateRange)
	if err != nil {
		return 0, fmt.Errorf("failed to generate random val: %w", err)
	}

	awardID, err := s.repo.GetStrategyAwardAssemble(ctx, strategyID, randomVal)
	if err != nil {
		return 0, fmt.Errorf("failed to get award assemble: %w", err)
	}

	return awardID, nil
}

func (s *armoryDispatch) getRandomAwardIDWithWeight(ctx context.Context, strategyID int64, ruleWeight string) (int64, error) {
	stringStrategyID := strconv.FormatInt(strategyID, 10)
	if ruleWeight != "" {
		stringStrategyID += common.UNDERLINE + ruleWeight
	}

	return s.getRandomAwardID(ctx, stringStrategyID)
}
