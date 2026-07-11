package api

import (
	"prizeforge/pkg/logger"
	"prizeforge/server/http/common"

	"github.com/gin-gonic/gin"
)

// Draw handles POST /api/v1/raffle/activity/draw
// Full draw flow: create order → perform raffle → save award record.
func (s *Server) Draw(c *gin.Context) {
	// TODO: full Draw flow requires ActivityUsecase, AwardUsecase, StrategyUsecase wired together
	// This mirrors Kratos service/activity.go Draw() method
	common.Error(c, 501, "draw endpoint not yet wired")
}

// CalendarSignRebate handles POST /api/v1/raffle/activity/calendar_sign_rebate
func (s *Server) CalendarSignRebate(c *gin.Context) {
	userID := c.Query("user_id")
	if userID == "" {
		common.Error(c, 400, "invalid user_id")
		return
	}
	// TODO: wire BehaviorRebateUsecase
	logger.Info("calendar sign rebate", "userID", userID)
	common.Success(c, CalendarSignRebateResponse{Success: true})
}

// IsCalendarSignRebate handles POST /api/v1/raffle/activity/is_calendar_sign_rebate
func (s *Server) IsCalendarSignRebate(c *gin.Context) {
	userID := c.Query("user_id")
	if userID == "" {
		common.Error(c, 400, "invalid user_id")
		return
	}
	// TODO: wire BehaviorRebateUsecase
	common.Success(c, IsCalendarSignRebateResponse{IsSigned: false})
}

// QueryUserActivityAccount handles POST /api/v1/raffle/activity/query_user_activity_account
func (s *Server) QueryUserActivityAccount(c *gin.Context) {
	common.Error(c, 501, "query_user_activity_account not yet wired")
}

// LoadUserActivityAccount handles POST /api/v1/raffle/activity/load_user_activity_account
func (s *Server) LoadUserActivityAccount(c *gin.Context) {
	common.Error(c, 501, "load_user_activity_account not yet wired")
}
