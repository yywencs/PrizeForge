package admin

import (
	"prizeforge/pkg/logger"
	"prizeforge/server/http/common"

	"github.com/gin-gonic/gin"
)

// StrategyArmory handles POST /admin/v1/strategy/armory
// Assembles lottery strategy into cache.
func (s *Server) StrategyArmory(c *gin.Context) {
	strategyID, ok := common.ParseStrategyID(c)
	if !ok {
		return
	}

	success, err := s.strategyUsecase.AssembleLotteryStrategy(c.Request.Context(), strategyID)
	if err != nil {
		logger.Error("strategy armory failed",
			"strategyID", strategyID,
			"error", err,
		)
		common.Error(c, 500, err.Error())
		return
	}

	logger.Info("strategy armory success", "strategyID", strategyID)
	common.Success(c, StrategyArmoryResponse{Success: success})
}

// QueryRaffleAwardList handles POST /admin/v1/strategy/query_raffle_award_list
func (s *Server) QueryRaffleAwardList(c *gin.Context) {
	strategyID, ok := common.ParseStrategyID(c)
	if !ok {
		return
	}

	awards, err := s.strategyUsecase.QueryStrategyAwardList(c.Request.Context(), strategyID)
	if err != nil {
		logger.Error("query raffle award list failed",
			"strategyID", strategyID,
			"error", err,
		)
		common.Error(c, 500, err.Error())
		return
	}

	result := make([]StrategyAwardList, 0, len(awards))
	for _, a := range awards {
		result = append(result, StrategyAwardList{
			AwardID:       a.AwardID,
			AwardTitle:    a.AwardTitle,
			AwardSubtitle: a.AwardSubtitle,
			Sort:          a.Sort,
		})
	}

	common.Success(c, result)
}

// QueryRaffleStrategyRuleWeight handles POST /admin/v1/strategy/query_raffle_strategy_rule_weight
func (s *Server) QueryRaffleStrategyRuleWeight(c *gin.Context) {
	var req struct {
		ActivityID int64  `json:"activity_id"`
		UserID     string `json:"user_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		common.Error(c, 400, "invalid request body")
		return
	}

	buckets, totalUseCount, err := s.strategyUsecase.QueryAwardRuleWeightByActivityId(
		c.Request.Context(), req.ActivityID, req.UserID,
	)
	if err != nil {
		logger.Error("query raffle strategy rule weight failed",
			"activityID", req.ActivityID,
			"error", err,
		)
		common.Error(c, 500, err.Error())
		return
	}

	weightList := make([]RuleWeightResponse, 0, len(buckets))
	for _, bucket := range buckets {
		strategyAwards := make([]StrategyAwardBase, 0, len(bucket.AwardList))
		for _, award := range bucket.AwardList {
			strategyAwards = append(strategyAwards, StrategyAwardBase{
				AwardID:    int64(award.AwardId),
				AwardTitle: award.AwardTitle,
			})
		}
		weightList = append(weightList, RuleWeightResponse{
			RuleWeightCount:                  int64(bucket.Weight),
			UserActivityAccountTotalUseCount: totalUseCount,
			StrategyAwards:                   strategyAwards,
		})
	}

	common.Success(c, weightList)
}
