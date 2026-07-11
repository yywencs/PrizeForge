package strategy

import (
	"context"
	"errors"
	"prizeforge/internal/metrics"
	"prizeforge/pkg/logger"
	"time"
)

type StrategyUsecase struct {
	repo   Repo
	raffle *raffleStrategy
	armory *armoryDispatch
}

func NewStrategyUsecase(repo Repo) *StrategyUsecase {
	armorSvc := newArmoryDispatch(repo)
	logicFactory := newLogicFactory(repo, armorSvc)
	ruleTreeFactory := newRuleTreeFactory(repo)
	raffleSvc := newRaffleStrategy(repo, logicFactory, ruleTreeFactory)

	return &StrategyUsecase{
		repo:   repo,
		raffle: raffleSvc,
		armory: armorSvc,
	}
}

// PerformRaffle 执行抽奖方法
func (s *StrategyUsecase) PerformRaffle(ctx context.Context, factor *RaffleFactor) (award *RaffleAward, err error) {
	start := time.Now()
	defer func() {
		metrics.IncRaffle(factor.ActivityID, factor.StrategyID, raffleResultFromErr(err))
		metrics.ObserveRaffleDuration(factor.ActivityID, factor.StrategyID, time.Since(start))
	}()

	// 1. 执行抽奖逻辑链（前置规则：黑名单、权重等）
	strategyAwardBefore, err := s.raffle.raffleLogicChain(ctx, factor)
	if err != nil {
		return nil, err
	}

	logger.Info("抽奖逻辑链执行完成",
		"strategyID", factor.StrategyID,
		"userID", factor.UserID,
		"awardID", strategyAwardBefore.AwardID,
		"logicModel", strategyAwardBefore.LogicModel,
	)

	// 2. 如果非默认规则（即直接被接管），直接返回奖品
	if strategyAwardBefore.LogicModel != RuleDefault {
		return s.buildRaffleAward(ctx, factor.StrategyID, strategyAwardBefore.AwardID)
	}

	// 3. 执行抽奖规则树
	strategyAwardAfter, err := s.raffle.raffleRuleTree(ctx, factor.UserID, factor.StrategyID, strategyAwardBefore.AwardID)
	if err != nil {
		return nil, err
	}

	return s.buildRaffleAward(ctx, factor.StrategyID, strategyAwardAfter.AwardID)
}

func raffleResultFromErr(err error) string {
	switch {
	case err == nil:
		return "success"
	case errors.Is(err, ErrBlackListUser):
		return "blacklist"
	case errors.Is(err, ErrRuleTreeInvalid):
		return "rule_invalid"
	default:
		return "error"
	}
}

// AssembleLotteryStrategy 装配策略（预热缓存）
func (s *StrategyUsecase) AssembleLotteryStrategy(ctx context.Context, strategyID int64) (bool, error) {
	return s.armory.assembleLotteryStrategy(ctx, strategyID)
}

// QueryStrategyAwardList 查询策略奖品列表
func (s *StrategyUsecase) QueryStrategyAwardList(ctx context.Context, strategyID int64) ([]*StrategyAward, error) {
	return s.raffle.queryStrategyAwardList(ctx, strategyID)
}

// QueryAwardRuleWeightByActivityId 根据活动ID查询奖品权重配置
func (s *StrategyUsecase) QueryAwardRuleWeightByActivityId(ctx context.Context, activityID int64) ([]*WeightBucket, error) {
	return s.raffle.queryAwardRuleWeightByActivityId(ctx, activityID)
}

// buildRaffleAward 构建返回结果
func (s *StrategyUsecase) UpdateStrategyAwardStock(ctx context.Context, strategyID int64, awardID int64) error {
	return s.repo.UpdateStrategyAwardStock(ctx, strategyID, awardID)
}

func (s *StrategyUsecase) AssembleLotteryStrategyByActivityId(ctx context.Context, activityID int64) (bool, error) {
	strategyID, err := s.repo.QueryStrategyIdByActivityId(ctx, activityID)
	if err != nil {
		return false, err
	}
	return s.AssembleLotteryStrategy(ctx, strategyID)
}

// buildRaffleAward 构建返回结果
func (s *StrategyUsecase) buildRaffleAward(ctx context.Context, strategyID int64, awardID int64) (*RaffleAward, error) {
	strategyAward, err := s.repo.QueryStrategyAward(ctx, strategyID, awardID)
	if err != nil {
		return nil, err
	}
	return &RaffleAward{
		AwardID:    strategyAward.AwardID,
		AwardTitle: strategyAward.AwardTitle,
		Sort:       strategyAward.Sort,
	}, nil
}
