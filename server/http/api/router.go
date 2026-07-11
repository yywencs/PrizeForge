package api

// registerRoutes registers all API routes.
//
// Route mapping (from Kratos proto definitions):
//
//	POST /api/v1/raffle/random_raffle              → RandomRaffle
//	POST /api/v1/raffle/activity/draw              → Draw
//	POST /api/v1/raffle/activity/calendar_sign_rebate → CalendarSignRebate
//	POST /api/v1/raffle/activity/is_calendar_sign_rebate → IsCalendarSignRebate
//	POST /api/v1/raffle/activity/query_user_activity_account → QueryUserActivityAccount
//	POST /api/v1/raffle/activity/load_user_activity_account → LoadUserActivityAccount
func (s *Server) registerRoutes() {
	g := s.engine.Group("/api/v1/raffle")
	{
		// Strategy
		g.POST("/random_raffle", s.RandomRaffle)

		// Activity
		g.POST("/activity/draw", s.Draw)
		g.POST("/activity/calendar_sign_rebate", s.CalendarSignRebate)
		g.POST("/activity/is_calendar_sign_rebate", s.IsCalendarSignRebate)
		g.POST("/activity/query_user_activity_account", s.QueryUserActivityAccount)
		g.POST("/activity/load_user_activity_account", s.LoadUserActivityAccount)
	}
}
