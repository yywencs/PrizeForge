package api

import (
	"prizeforge/internal/domain/strategy"
	"context"
)

// StrategyUsecase API侧策略用例——薄封装，直接委托给 domain service
type StrategyUsecase struct {
	svc *strategy.StrategyUsecase
}

func NewStrategyUsecase(svc *strategy.StrategyUsecase) *StrategyUsecase {
	return &StrategyUsecase{svc: svc}
}

// PerformRaffleRequest API 抽奖请求（应用层 DTO）
type PerformRaffleRequest struct {
	UserID     string
	StrategyID int64
	ActivityID int64
}

// RaffleAward 中奖结果（应用层 DTO）
type RaffleAward struct {
	AwardID    int64
	AwardTitle string
	Sort       int
}

func (u *StrategyUsecase) PerformRaffle(ctx context.Context, req *PerformRaffleRequest) (*RaffleAward, error) {
	factor := &strategy.RaffleFactor{
		UserID:     req.UserID,
		StrategyID: req.StrategyID,
		ActivityID: req.ActivityID,
	}
	award, err := u.svc.PerformRaffle(ctx, factor)
	if err != nil {
		return nil, err
	}
	return &RaffleAward{
		AwardID:    award.AwardID,
		AwardTitle: award.AwardTitle,
		Sort:       award.Sort,
	}, nil
}
