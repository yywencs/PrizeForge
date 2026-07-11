package admin

// registerRoutes registers all Admin routes.
//
// Route mapping (from Kratos proto definitions):
//
//	POST /admin/v1/strategy/armory                         → StrategyArmory
//	POST /admin/v1/strategy/query_raffle_award_list         → QueryRaffleAwardList
//	POST /admin/v1/strategy/query_raffle_strategy_rule_weight → QueryRaffleStrategyRuleWeight
func (s *Server) registerRoutes() {
	g := s.engine.Group("/admin/v1/strategy")
	{
		g.POST("/armory", s.StrategyArmory)
		g.POST("/query_raffle_award_list", s.QueryRaffleAwardList)
		g.POST("/query_raffle_strategy_rule_weight", s.QueryRaffleStrategyRuleWeight)
	}
}
