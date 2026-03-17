package service

import (
	v1 "big-market-kratos/api/bigmarket/v1"
	"big-market-kratos/internal/biz/activity"
	"big-market-kratos/internal/biz/strategy"
	"context"

	kerrors "github.com/go-kratos/kratos/v2/errors"
)

type StrategyService struct {
	v1.UnimplementedStrategyServer
	strategyUsecase *strategy.StrategyUsecase
	quotaService    *activity.ActivityQuotaUsecase
}

func NewStrategyService(strategyUsecase *strategy.StrategyUsecase, quotaService *activity.ActivityQuotaUsecase) *StrategyService {
	return &StrategyService{strategyUsecase: strategyUsecase, quotaService: quotaService}
}

func (s *StrategyService) StrategyArmory(ctx context.Context, req *v1.StrategyArmoryRequest) (*v1.StrategyArmoryReply, error) {
	if req.GetStrategyId() <= 0 {
		return nil, kerrors.BadRequest("INVALID_STRATEGY_ID", "invalid strategy_id")
	}
	success, err := s.strategyUsecase.AssembleLotteryStrategy(ctx, req.GetStrategyId())
	if err != nil {
		return nil, err
	}
	return &v1.StrategyArmoryReply{Success: success}, nil
}

func (s *StrategyService) QueryRaffleAwardList(ctx context.Context, req *v1.QueryRaffleAwardListRequest) (*v1.QueryRaffleAwardListReply, error) {
	if req.GetStrategyId() <= 0 {
		return nil, kerrors.BadRequest("INVALID_STRATEGY_ID", "invalid strategy_id")
	}
	awards, err := s.strategyUsecase.QueryStrategyAwardList(ctx, req.GetStrategyId())
	if err != nil {
		return nil, err
	}
	replyAwards := make([]*v1.RaffleAward, 0, len(awards))
	for _, award := range awards {
		replyAwards = append(replyAwards, &v1.RaffleAward{
			AwardId:       award.AwardID,
			AwardTitle:    award.AwardTitle,
			AwardSubtitle: award.AwardSubtitle,
			Sort:          int32(award.Sort),
		})
	}
	return &v1.QueryRaffleAwardListReply{Awards: replyAwards}, nil
}

func (s *StrategyService) RandomRaffle(ctx context.Context, req *v1.RandomRaffleRequest) (*v1.RandomRaffleReply, error) {
	if req.GetStrategyId() <= 0 || req.GetUserId() == "" {
		return nil, kerrors.BadRequest("INVALID_RAFFLE_PARAMS", "invalid strategy_id or user_id")
	}
	raffleAward, err := s.strategyUsecase.PerformRaffle(ctx, &strategy.RaffleFactor{
		UserID:     req.GetUserId(),
		StrategyID: req.GetStrategyId(),
	})
	if err != nil {
		return nil, err
	}
	return &v1.RandomRaffleReply{
		AwardId:    raffleAward.AwardID,
		AwardTitle: raffleAward.AwardTitle,
		AwardIndex: int32(raffleAward.Sort),
	}, nil
}

func (s *StrategyService) QueryRaffleStrategyRuleWeight(ctx context.Context, req *v1.QueryRaffleStrategyRuleWeightRequest) (*v1.QueryRaffleStrategyRuleWeightReply, error) {
	if req.GetActivityId() <= 0 || req.GetUserId() == "" {
		return nil, kerrors.BadRequest("INVALID_RULE_WEIGHT_PARAMS", "invalid activity_id or user_id")
	}
	buckets, err := s.strategyUsecase.QueryAwardRuleWeightByActivityId(ctx, req.GetActivityId())
	if err != nil {
		return nil, err
	}

	totalUseCount, err := s.quotaService.QueryRaffleActivityAccountPartakeCount(ctx, req.GetUserId(), req.GetActivityId())
	if err != nil {
		return nil, err
	}

	weightList := make([]*v1.RuleWeightResponse, 0, len(buckets))
	for _, bucket := range buckets {
		strategyAwards := make([]*v1.StrategyAward, 0, len(bucket.AwardList))
		for _, award := range bucket.AwardList {
			strategyAwards = append(strategyAwards, &v1.StrategyAward{
				AwardId:    int64(award.AwardId),
				AwardTitle: award.AwardTitle,
			})
		}
		weightList = append(weightList, &v1.RuleWeightResponse{
			RuleWeightCount:                  int64(bucket.Weight),
			UserActivityAccountTotalUseCount: totalUseCount,
			StrategyAwards:                   strategyAwards,
		})
	}
	return &v1.QueryRaffleStrategyRuleWeightReply{RuleWeightList: weightList}, nil
}
