package api

import (
	"prizeforge/internal/application/api"
	"prizeforge/pkg/logger"
	"prizeforge/server/http/common"

	"github.com/gin-gonic/gin"
)

// RandomRaffle handles POST /api/v1/raffle/random_raffle
// Performs a random raffle draw for the given strategy.
func (s *Server) RandomRaffle(c *gin.Context) {
	strategyID, ok := common.ParseStrategyID(c)
	if !ok {
		return
	}

	userID := c.Query("user_id")
	if userID == "" {
		userID = "user001"
	}

	req := &api.PerformRaffleRequest{
		UserID:     userID,
		StrategyID: strategyID,
	}

	award, err := s.strategyUsecase.PerformRaffle(c.Request.Context(), req)
	if err != nil {
		logger.Error("random raffle failed",
			"strategyID", strategyID,
			"userID", userID,
			"error", err,
		)
		common.Error(c, 500, err.Error())
		return
	}

	if award == nil {
		logger.Error("random raffle returned empty award",
			"strategyID", strategyID,
			"userID", userID,
		)
		common.Error(c, 500, "empty raffle result")
		return
	}

	logger.Info("random raffle success",
		"strategyID", strategyID,
		"userID", userID,
		"awardID", award.AwardID,
	)

	common.Success(c, RaffleResponse{
		AwardID:    award.AwardID,
		AwardTitle: award.AwardTitle,
		AwardIndex: award.Sort,
	})
}
