package api

import (
	"prizeforge/pkg/logger"
	"prizeforge/server/http/common"

	"github.com/gin-gonic/gin"
)

// ---- request DTOs (used only in this file) ----

type drawRequest struct {
	UserID     string `json:"user_id"`
	ActivityID int64  `json:"activity_id"`
}

type queryAccountRequest struct {
	UserID     string `json:"user_id"`
	ActivityID int64  `json:"activity_id"`
}

// ---- handlers ----

// Draw handles POST /api/v1/raffle/activity/draw
// Full draw flow: create order → perform raffle → save award record.
func (s *Server) Draw(c *gin.Context) {
	var req drawRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		common.Error(c, 400, "invalid request body: "+err.Error())
		return
	}
	if req.UserID == "" || req.ActivityID <= 0 {
		common.Error(c, 400, "invalid user_id or activity_id")
		return
	}

	awardID, awardTitle, awardIndex, err := s.activityUsecase.Draw(c.Request.Context(), req.UserID, req.ActivityID)
	if err != nil {
		logger.Error("draw failed", "userID", req.UserID, "activityID", req.ActivityID, "error", err)
		common.Error(c, 500, err.Error())
		return
	}

	logger.Info("draw success", "userID", req.UserID, "activityID", req.ActivityID, "awardID", awardID)
	common.Success(c, RaffleResponse{
		AwardID:    awardID,
		AwardTitle: awardTitle,
		AwardIndex: awardIndex,
	})
}

// CalendarSignRebate handles POST /api/v1/raffle/activity/calendar_sign_rebate
func (s *Server) CalendarSignRebate(c *gin.Context) {
	userID := c.Query("user_id")
	if userID == "" {
		common.Error(c, 400, "invalid user_id")
		return
	}

	success, err := s.activityUsecase.CalendarSignRebate(c.Request.Context(), userID)
	if err != nil {
		logger.Error("calendar sign rebate failed", "userID", userID, "error", err)
		common.Error(c, 500, err.Error())
		return
	}

	common.Success(c, CalendarSignRebateResponse{Success: success})
}

// IsCalendarSignRebate handles POST /api/v1/raffle/activity/is_calendar_sign_rebate
func (s *Server) IsCalendarSignRebate(c *gin.Context) {
	userID := c.Query("user_id")
	if userID == "" {
		common.Error(c, 400, "invalid user_id")
		return
	}

	isSigned, err := s.activityUsecase.IsCalendarSignRebate(c.Request.Context(), userID)
	if err != nil {
		logger.Error("check calendar sign rebate failed", "userID", userID, "error", err)
		common.Error(c, 500, err.Error())
		return
	}

	common.Success(c, IsCalendarSignRebateResponse{IsSigned: isSigned})
}

// QueryUserActivityAccount handles POST /api/v1/raffle/activity/query_user_activity_account
func (s *Server) QueryUserActivityAccount(c *gin.Context) {
	var req queryAccountRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		common.Error(c, 400, "invalid request body: "+err.Error())
		return
	}
	if req.UserID == "" || req.ActivityID <= 0 {
		common.Error(c, 400, "invalid user_id or activity_id")
		return
	}

	account, err := s.activityUsecase.QueryUserActivityAccount(c.Request.Context(), req.UserID, req.ActivityID)
	if err != nil {
		logger.Error("query user activity account failed", "userID", req.UserID, "activityID", req.ActivityID, "error", err)
		common.Error(c, 500, err.Error())
		return
	}

	common.Success(c, QueryUserActivityAccountResponse{
		ActivityID:        account.ActivityID,
		TotalCount:        int64(account.TotalCount),
		TotalCountSurplus: int64(account.TotalCountSurplus),
		DayCount:          int64(account.DayCount),
		DayCountSurplus:   int64(account.DayCountSurplus),
		MonthCount:        int64(account.MonthCount),
		MonthCountSurplus: int64(account.MonthCountSurplus),
	})
}

// LoadUserActivityAccount handles POST /api/v1/raffle/activity/load_user_activity_account
func (s *Server) LoadUserActivityAccount(c *gin.Context) {
	var req queryAccountRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		common.Error(c, 400, "invalid request body: "+err.Error())
		return
	}
	if req.UserID == "" || req.ActivityID <= 0 {
		common.Error(c, 400, "invalid user_id or activity_id")
		return
	}

	if err := s.activityUsecase.LoadUserActivityAccount(c.Request.Context(), req.UserID, req.ActivityID); err != nil {
		logger.Error("load user activity account failed", "userID", req.UserID, "activityID", req.ActivityID, "error", err)
		common.Error(c, 500, err.Error())
		return
	}

	common.Success(c, LoadUserActivityAccountResponse{Success: true})
}
